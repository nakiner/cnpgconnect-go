package pgxpool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/nakiner/cnpgconnect-go"
)

func TestStartupRetriesConnectionEOFAndPostgresStarting(t *testing.T) {
	for _, mode := range []string{"socket_eof", "tls_eof", "postgres_starting"} {
		t.Run(mode, func(t *testing.T) {
			serverTLS, ca := testCertificate(t, "pg.test")
			settings := postgresSettings{}
			if mode == "tls_eof" {
				settings.tls = serverTLS
			}
			healthy := newPostgres(t, "healthy", false, settings)
			target := healthy.target(1)
			cfg := testConfig(t)
			cfg.MaxConns = 1
			broken := failedStartupServer(t, mode)
			dial := cfg.ConnConfig.DialFunc
			var attempts atomic.Int32
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				if attempts.Add(1) == 1 {
					address = broken
				}
				return dial(ctx, network, address)
			}
			config := Config{Resolver: newResolver(target), ConnConfig: cfg, StartupTimeout: time.Second}
			if mode == "tls_eof" {
				target.Endpoint.ServerName = "pg.test"
				target.Connection = cnpgconnectgo.ConnectionParameters{Database: "test", ServerCAPEM: string(ca)}
				config.Resolver = newResolver(target)
				config.Username = "test"
			}
			pool, err := Open(context.Background(), config)
			if err != nil {
				t.Fatalf("initial transient failure escaped startup: %v", err)
			}
			defer pool.Close()
			// pgx may open a separate CancelRequest socket while cleaning up
			// the failed connection. Count constructors, not control sockets.
			if got := pool.Stat().NewConnsCount(); got != 2 {
				t.Fatalf("connection constructors=%d, want one failure followed by success", got)
			}
			var identity string
			if err := pool.QueryRow(context.Background(), "SELECT identity").Scan(&identity); err != nil || identity != healthy.identity {
				t.Fatalf("Open returned an unusable pool: identity=%q error=%v", identity, err)
			}
		})
	}
}

func TestStartupRetryHonorsBudgetAndCancellation(t *testing.T) {
	for _, mode := range []string{"startup_timeout", "caller_deadline", "caller_cancel"} {
		t.Run(mode, func(t *testing.T) {
			healthy := newPostgres(t, "target", false)
			resolver := newResolver(healthy.target(1))
			cfg := testConfig(t)
			cfg.MaxConns = 1
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startupTimeout := 80 * time.Millisecond
			want := context.DeadlineExceeded
			if mode == "caller_deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 50*time.Millisecond)
				defer stop()
				startupTimeout = time.Minute
			}
			var attempts atomic.Int32
			cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
				if attempts.Add(1) == 1 && mode == "caller_cancel" {
					time.AfterFunc(5*time.Millisecond, cancel)
				}
				return nil, io.EOF
			}
			if mode == "caller_cancel" {
				want = context.Canceled
				startupTimeout = time.Minute
			}
			started := time.Now()
			pool, err := Open(ctx, Config{Resolver: resolver, ConnConfig: cfg, StartupTimeout: startupTimeout})
			if pool != nil || !errors.Is(err, want) {
				t.Fatalf("failed startup returned pool=%v error=%v, want %v", pool, err, want)
			}
			if time.Since(started) > time.Second || attempts.Load() == 0 {
				t.Fatal("startup retry exceeded its cancellation budget")
			}
			resolver.mu.Lock()
			defer resolver.mu.Unlock()
			if len(resolver.subscribers) != 0 {
				t.Fatal("failed startup retained its discovery subscription")
			}
		})
	}
}

func TestStartupDoesNotRetryApplicationHookErrors(t *testing.T) {
	// Use a real ConnectError as a hook result too: type matching alone must
	// not mistake an application callback's own failed operation for pool I/O.
	config, err := pgconn.ParseConfig("sslmode=disable host=localhost user=test")
	if err != nil {
		t.Fatal(err)
	}
	config.DialFunc = func(context.Context, string, string) (net.Conn, error) { return nil, io.EOF }
	_, connectionErr := pgconn.ConnectConfig(context.Background(), config)
	if connectionErr == nil {
		t.Fatal("fixture did not return a connection error")
	}
	for _, hook := range []string{"before_connect", "after_net_connect", "validate_connect", "pgconn_after_connect", "after_connect", "prepare_conn", "oauth_token"} {
		t.Run(hook, func(t *testing.T) {
			server := newPostgres(t, "healthy", false)
			cfg := testConfig(t)
			cfg.MaxConns = 1
			var calls atomic.Int32
			fail := func() error { calls.Add(1); return connectionErr }
			switch hook {
			case "before_connect":
				cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error { return fail() }
			case "after_net_connect":
				cfg.ConnConfig.AfterNetConnect = func(_ context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) { return conn, fail() }
			case "validate_connect":
				cfg.ConnConfig.ValidateConnect = func(context.Context, *pgconn.PgConn) error { return fail() }
			case "pgconn_after_connect":
				cfg.ConnConfig.AfterConnect = func(context.Context, *pgconn.PgConn) error { return fail() }
			case "after_connect":
				cfg.AfterConnect = func(context.Context, *pgx.Conn) error { return fail() }
			case "prepare_conn":
				cfg.PrepareConn = func(context.Context, *pgx.Conn) (bool, error) { return false, fail() }
			case "oauth_token":
				address := failedStartupServer(t, "oauth_token")
				dial := cfg.ConnConfig.DialFunc
				cfg.ConnConfig.DialFunc = func(ctx context.Context, network, _ string) (net.Conn, error) { return dial(ctx, network, address) }
				cfg.ConnConfig.OAuthTokenProvider = func(context.Context) (string, error) { return "", fail() }
			}
			pool, err := Open(context.Background(), Config{Resolver: newResolver(server.target(1)), ConnConfig: cfg, StartupTimeout: time.Second})
			if pool != nil || !errors.Is(err, connectionErr) || calls.Load() != 1 {
				t.Fatalf("application hook was retried or lost its error: pool=%v calls=%d err=%v", pool, calls.Load(), err)
			}
		})
	}
}

func TestStartupDoesNotRetryAuthenticationOrTLSVerification(t *testing.T) {
	for _, mode := range []string{"authentication", "wrong_server_name", "verify_peer", "verify_connection", "client_certificate"} {
		t.Run(mode, func(t *testing.T) {
			serverTLS, ca := testCertificate(t, "pg.test")
			settings := postgresSettings{tls: serverTLS}
			if mode == "authentication" {
				settings.authError = "28P01"
			}
			if mode == "client_certificate" {
				serverTLS.ClientAuth = tls.RequireAnyClientCert
			}
			server := newPostgres(t, "healthy", false, settings)
			target := server.target(1)
			target.Endpoint.ServerName = "pg.test"
			roots := x509.NewCertPool()
			roots.AppendCertsFromPEM(ca)
			cfg := testConfig(t)
			cfg.MaxConns = 1
			cfg.ConnConfig.TLSConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
			var attempts, hooks atomic.Int32
			dial := cfg.ConnConfig.DialFunc
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				attempts.Add(1)
				return dial(ctx, network, address)
			}
			fail := func() error { hooks.Add(1); return io.EOF }
			switch mode {
			case "wrong_server_name":
				target.Endpoint.ServerName = "wrong.test"
			case "verify_peer":
				cfg.ConnConfig.TLSConfig.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error { return fail() }
			case "verify_connection":
				cfg.ConnConfig.TLSConfig.VerifyConnection = func(tls.ConnectionState) error { return fail() }
			case "client_certificate":
				cfg.ConnConfig.TLSConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return nil, fail() }
			}
			pool, err := Open(context.Background(), Config{Resolver: newResolver(target), ConnConfig: cfg, StartupTimeout: time.Second})
			if pool != nil || err == nil || attempts.Load() != 1 {
				t.Fatalf("permanent startup failure was retried: pool=%v attempts=%d error=%v", pool, attempts.Load(), err)
			}
			if mode == "verify_peer" || mode == "verify_connection" || mode == "client_certificate" {
				if hooks.Load() != 1 || !errors.Is(err, io.EOF) {
					t.Fatalf("TLS callback retried or lost error identity: calls=%d error=%v", hooks.Load(), err)
				}
			}
		})
	}
}

func TestStartupDoesNotRetryEstablishedConnectionPing(t *testing.T) {
	server := newPostgres(t, "healthy", false)
	resolver := newResolver(server.target(1))
	cfg := testConfig(t)
	cfg.MaxConns = 1
	var preparations atomic.Int32
	cfg.PrepareConn = func(_ context.Context, conn *pgx.Conn) (bool, error) {
		preparations.Add(1)
		// Closing after successful construction makes the startup Ping fail
		// outside ConnectConfig. The startup helper must not replay that work.
		return true, conn.Close(context.Background())
	}
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg, StartupTimeout: time.Second})
	if pool != nil || err == nil || preparations.Load() != 1 {
		t.Fatalf("established-connection failure was retried: pool=%v preparations=%d err=%v", pool, preparations.Load(), err)
	}
}

// Fail before authentication, over real PostgreSQL/TLS framing. Reading the
// complete request before closing avoids replacing the intended EOF with a TCP
// reset caused by unread data in the socket.
func failedStartupServer(t *testing.T, mode string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Go(func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				backend := pgproto3.NewBackend(conn, conn)
				if _, err := backend.ReceiveStartupMessage(); err != nil {
					return
				}
				switch mode {
				case "tls_eof":
					if _, err := conn.Write([]byte{'S'}); err != nil {
						return
					}
					var header [5]byte
					if _, err := io.ReadFull(conn, header[:]); err != nil {
						return
					}
					_, _ = io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint16(header[3:])))
				case "postgres_starting":
					backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "57P03", Message: "database is starting"})
					_ = backend.Flush()
				case "oauth_token":
					backend.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"OAUTHBEARER"}})
					_ = backend.Flush()
				}
			})
		}
	})
	t.Cleanup(func() { _ = listener.Close(); workers.Wait() })
	return listener.Addr().String()
}
