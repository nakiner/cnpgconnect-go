package pgxpool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	native "github.com/jackc/pgx/v5/pgxpool"
	"github.com/nakiner/cnpgconnect-go"
)

func TestSwitchRetiresIdleAndPreservesBorrowedConnection(t *testing.T) {
	a, b := newPostgres(t, "a", false), newPostgres(t, "b", false)
	resolver := newResolver(a.target(1))
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: testConfig(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	borrowed, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer borrowed.Release()
	tx, err := borrowed.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	resolver.set(b.target(2))
	if pool.Valid(borrowed.Conn()) {
		t.Fatal("old connection remains eligible")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var identity string
	if err := pool.QueryRow(ctx, "SELECT identity").Scan(&identity); err != nil || identity != "b" {
		t.Fatalf("new query = %q, %v; want b", identity, err)
	}
	if err := tx.QueryRow(ctx, "SELECT identity").Scan(&identity); err != nil || identity != "a" {
		t.Fatalf("borrowed transaction = %q, %v; want a", identity, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	old := borrowed.Conn().PgConn()
	borrowed.Release()
	waitFor(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.targets[old]
		return !exists
	})
	// Background retirement must leave the new member available.
	if err := pool.QueryRow(ctx, "SELECT identity").Scan(&identity); err != nil || identity != "b" {
		t.Fatalf("after release = %q, %v", identity, err)
	}
}

func TestUnavailableRouteWaitsForContextAndRecovers(t *testing.T) {
	server := newPostgres(t, "a", false)
	resolver := newResolver(server.target(1))
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: testConfig(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	resolver.set(cnpgconnectgo.Target{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = pool.Exec(ctx, "SELECT identity")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unavailable query error = %v", err)
	}
	resolver.set(server.target(2))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var identity string
	if err := pool.QueryRow(ctx, "SELECT identity").Scan(&identity); err != nil || identity != "a" {
		t.Fatalf("recovered query = %q, %v", identity, err)
	}
}

func TestCloseCancelsWaitingBackgroundConstructors(t *testing.T) {
	resolver := newResolver(cnpgconnectgo.Target{})
	cfg := testConfig(t)
	cfg.MinConns = 2
	start := time.Now()
	_, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg, StartupTimeout: 40 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup error = %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("closing failed startup waited on discovery indefinitely")
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.subscribers) != 0 {
		t.Fatal("failed startup leaked subscription")
	}
}

func TestStartupDefaultAndEarlierCallerDeadline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline time.Duration
		want     time.Duration
	}{
		{"default", 0, 30 * time.Second},
		{"caller deadline", 200 * time.Millisecond, 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				if tc.deadline > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.deadline)
					defer cancel()
				}
				resolver := newResolver(cnpgconnectgo.Target{})
				started := time.Now()
				_, err := Open(ctx, Config{Resolver: resolver, ConnConfig: testConfig(t)})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("startup error = %v; want deadline exceeded", err)
				}
				if elapsed := time.Since(started); elapsed != tc.want {
					t.Fatalf("startup took %s, want %s", elapsed, tc.want)
				}
			})
		})
	}
}

func TestRoleValidationBeforeUserHooks(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recovery bool
		role     cnpgconnectgo.Role
		wantErr  bool
	}{
		{"primary", false, cnpgconnectgo.Primary, false},
		{"replica", true, cnpgconnectgo.Replica, false},
		{"promoted replica", false, cnpgconnectgo.Replica, true},
		{"demoted primary", true, cnpgconnectgo.Primary, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newPostgres(t, tc.name, tc.recovery)
			target := server.target(1)
			target.Role = tc.role
			var hooks atomic.Int32
			cfg := testConfig(t)
			cfg.ConnConfig.AfterConnect = func(context.Context, *pgconn.PgConn) error { hooks.Add(1); return nil }
			cfg.AfterConnect = func(context.Context, *pgx.Conn) error { hooks.Add(1); return nil }
			pool, err := Open(context.Background(), Config{Resolver: newResolver(target), ConnConfig: cfg})
			if pool != nil {
				pool.Close()
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("Open error = %v", err)
			}
			if tc.wantErr && hooks.Load() != 0 {
				t.Fatal("user hook ran before role mismatch rejection")
			}
		})
	}
}

func TestCloseBoundsBackgroundNetworkStartup(t *testing.T) {
	server := newPostgres(t, "healthy", false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn // Deliberately do not answer PostgreSQL startup.
		}
	}()
	resolver := newResolver(server.target(1))
	cfg := testConfig(t)
	cfg.MinConns, cfg.MaxConns = 1, 1
	cfg.HealthCheckPeriod = 10 * time.Millisecond
	cfg.ConnConfig.ConnectTimeout = 150 * time.Millisecond
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	blocked := server.target(2)
	host, port, _ := net.SplitHostPort(listener.Addr().String())
	parsedPort, _ := strconv.Atoi(port)
	blocked.Endpoint = cnpgconnectgo.Endpoint{Host: host, Port: uint16(parsedPort)}
	resolver.set(blocked)
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(time.Second):
		t.Fatal("background constructor did not start")
	}
	start := time.Now()
	pool.Close()
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Close did not bound blocked background startup")
	}
}

func TestEndpointOverridesAndTLSIsolation(t *testing.T) {
	for _, mode := range []string{"disable", "allow", "prefer", "verify-full"} {
		t.Run(mode, func(t *testing.T) {
			cfg, err := pgx.ParseConfig("host=old,old,also-old port=5432,5433,5432 user=test dbname=test sslmode=" + mode)
			if err != nil {
				t.Fatal(err)
			}
			original := cfg.Copy()
			originalTLS := cfg.TLSConfig
			originalFallbacks := append([]*pgconn.FallbackConfig(nil), cfg.Fallbacks...)
			setEndpoint(cfg, cnpgconnectgo.Endpoint{Host: "10.1.2.3", Port: 15432, ServerName: "app-rw.db.svc"})
			if cfg.Host != "10.1.2.3" || cfg.Port != 15432 {
				t.Fatalf("endpoint not replaced: %+v", cfg.Config)
			}
			if mode == "disable" || mode == "allow" {
				if cfg.TLSConfig != nil {
					t.Fatal("plaintext first attempt unexpectedly changed to TLS")
				}
			} else {
				if cfg.TLSConfig == nil || cfg.TLSConfig == originalTLS || cfg.TLSConfig.ServerName != "app-rw.db.svc" {
					t.Fatal("TLS config missing, changed in place, or wrong server name")
				}
				if originalTLS != nil && originalTLS.ServerName != original.TLSConfig.ServerName {
					t.Fatal("original TLS configuration mutated")
				}
			}
			wantFallbacks := 0
			if mode == "allow" || mode == "prefer" {
				wantFallbacks = 1
			}
			if len(cfg.Fallbacks) != wantFallbacks {
				t.Fatalf("fallback count = %d, want %d", len(cfg.Fallbacks), wantFallbacks)
			}
			for _, fallback := range cfg.Fallbacks {
				if fallback.Host != "10.1.2.3" || fallback.Port != 15432 {
					t.Fatalf("fallback bypasses discovered endpoint: %+v", fallback)
				}
				if mode == "allow" {
					if fallback.TLSConfig == nil || fallback.TLSConfig.ServerName != "app-rw.db.svc" || fallback.TLSConfig == originalFallbacks[0].TLSConfig {
						t.Fatal("allow mode lost or mutated its TLS fallback")
					}
				} else if fallback.TLSConfig != nil {
					t.Fatal("prefer mode lost its plaintext fallback")
				}
			}
			for i, fallback := range originalFallbacks {
				if fallback.Host != original.Fallbacks[i].Host || fallback.Port != original.Fallbacks[i].Port {
					t.Fatal("original fallback endpoint mutated")
				}
				if fallback.TLSConfig != nil && fallback.TLSConfig.ServerName != original.Fallbacks[i].TLSConfig.ServerName {
					t.Fatal("original fallback TLS configuration mutated")
				}
			}
		})
	}
}

func TestAfterConnectErrorDoesNotRetainIdentity(t *testing.T) {
	server := newPostgres(t, "a", false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &Pool{resolver: newResolver(server.target(1)), ctx: ctx, targets: make(map[*pgconn.PgConn]cnpgconnectgo.Target)}
	cfg := testConfig(t)
	cfg.ConnConfig.ConnectTimeout = time.Second
	errHook := errors.New("application initialization failed")
	cfg.AfterConnect = func(context.Context, *pgx.Conn) error { return errHook }
	p.installHooks(cfg)
	var err error
	p.Pool, err = native.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Pool.Close()
	if err := p.Ping(ctx); !errors.Is(err, errHook) {
		t.Fatalf("Ping error = %v", err)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.targets) != 0 {
		t.Fatal("failed AfterConnect retained connection identity")
	}
}

func TestHijackedConnectionCleanup(t *testing.T) {
	server := newPostgres(t, "a", false)
	pool, err := Open(context.Background(), Config{Resolver: newResolver(server.target(1)), ConnConfig: testConfig(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	hijacked := conn.Hijack()
	if err := hijacked.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, exists := pool.targets[hijacked.PgConn()]
		return !exists
	})
}

func TestHooksAndCallerConfigPreserved(t *testing.T) {
	server := newPostgres(t, "a", false)
	resolver := newResolver(server.target(1))
	cfg := testConfig(t)
	var before, pgAfter, after, prepare, release, closing atomic.Int32
	cfg.BeforeConnect = func(_ context.Context, config *pgx.ConnConfig) error {
		before.Add(1)
		config.Host = "should-be-overridden.invalid"
		return nil
	}
	cfg.ConnConfig.AfterConnect = func(context.Context, *pgconn.PgConn) error { pgAfter.Add(1); return nil }
	cfg.AfterConnect = func(context.Context, *pgx.Conn) error { after.Add(1); return nil }
	cfg.PrepareConn = func(context.Context, *pgx.Conn) (bool, error) { prepare.Add(1); return true, nil }
	cfg.BeforeAcquire = func(context.Context, *pgx.Conn) bool {
		t.Error("PrepareConn must supersede BeforeAcquire")
		return false
	}
	cfg.AfterRelease = func(*pgx.Conn) bool { release.Add(1); return true }
	cfg.BeforeClose = func(*pgx.Conn) { closing.Add(1) }
	oldHost := cfg.ConnConfig.Host
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	pool.Close()
	if before.Load() == 0 || pgAfter.Load() == 0 || after.Load() == 0 || prepare.Load() == 0 || release.Load() == 0 || closing.Load() == 0 {
		t.Fatalf("hooks before=%d pgAfter=%d after=%d prepare=%d release=%d close=%d", before.Load(), pgAfter.Load(), after.Load(), prepare.Load(), release.Load(), closing.Load())
	}
	if cfg.ConnConfig.Host != oldHost || cfg.BeforeAcquire == nil {
		t.Fatal("caller configuration mutated")
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.subscribers) != 0 {
		t.Fatal("Close leaked subscription")
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for _, cfg := range []Config{{}, {ConnConfig: testConfig(t), StartupTimeout: -1}, {ConnConfig: testConfig(t), Policy: cnpgconnectgo.Policy{Role: "typo"}}} {
		if _, err := Open(context.Background(), cfg); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}

func testConfig(t *testing.T) *native.Config {
	t.Helper()
	cfg, err := native.ParseConfig("host=ignored.invalid user=test dbname=test sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return cfg
}

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

type fakeResolver struct {
	mu          sync.Mutex
	target      cnpgconnectgo.Target
	changed     chan struct{}
	subscribers map[chan struct{}]bool
}

func newResolver(target cnpgconnectgo.Target) *fakeResolver {
	return &fakeResolver{target: target, changed: make(chan struct{}), subscribers: make(map[chan struct{}]bool)}
}

func (r *fakeResolver) Resolve(ctx context.Context, _ cnpgconnectgo.Policy) (cnpgconnectgo.Target, error) {
	for {
		r.mu.Lock()
		target, changed := r.target, r.changed
		r.mu.Unlock()
		if target.MemberID != "" {
			return target, nil
		}
		select {
		case <-ctx.Done():
			return cnpgconnectgo.Target{}, ctx.Err()
		case <-changed:
		}
	}
}

func (r *fakeResolver) Valid(_ cnpgconnectgo.Policy, target cnpgconnectgo.Target) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return target.MemberID != "" && target == r.target
}

func (r *fakeResolver) Subscribe() (<-chan struct{}, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan struct{}, 1)
	r.subscribers[ch] = true
	return ch, func() {
		r.mu.Lock()
		delete(r.subscribers, ch)
		r.mu.Unlock()
	}
}

func (r *fakeResolver) set(target cnpgconnectgo.Target) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.target = target
	close(r.changed)
	r.changed = make(chan struct{})
	for ch := range r.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// postgresServer exercises pgx's real startup/query/pool lifecycle over the
// PostgreSQL wire protocol. It has deterministic identity and recovery state.
type postgresServer struct {
	listener net.Listener
	identity string
	recovery bool
	mu       sync.Mutex
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
	closed   bool
	settings postgresSettings
}

type postgresSettings struct {
	tls       *tls.Config
	startup   chan map[string]string
	password  chan string
	authError string
}

func newPostgres(t *testing.T, identity string, recovery bool, settings ...postgresSettings) *postgresServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &postgresServer{listener: listener, identity: identity, recovery: recovery, conns: make(map[net.Conn]bool)}
	if len(settings) > 0 {
		s.settings = settings[0]
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				_ = conn.Close()
				return
			}
			s.conns[conn] = true
			s.wg.Add(1)
			s.mu.Unlock()
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() {
		_ = s.listener.Close()
		s.mu.Lock()
		s.closed = true
		for conn := range s.conns {
			_ = conn.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *postgresServer) target(generation uint64) cnpgconnectgo.Target {
	host, port, _ := net.SplitHostPort(s.listener.Addr().String())
	p, _ := strconv.Atoi(port)
	return cnpgconnectgo.Target{
		ClusterUID: "cluster", MemberID: s.identity, Name: s.identity,
		Endpoint: cnpgconnectgo.Endpoint{Host: host, Port: uint16(p)}, Role: cnpgconnectgo.Primary, Generation: generation,
	}
}

func (s *postgresServer) serve(conn net.Conn) {
	defer s.wg.Done()
	originalConn := conn
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, originalConn)
		s.mu.Unlock()
	}()
	backend := pgproto3.NewBackend(conn, conn)
	message, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, ok := message.(*pgproto3.SSLRequest); ok {
		if s.settings.tls == nil {
			_, _ = conn.Write([]byte{'N'})
			return
		}
		if _, err := conn.Write([]byte{'S'}); err != nil {
			return
		}
		conn = tls.Server(conn, s.settings.tls)
		backend = pgproto3.NewBackend(conn, conn)
		message, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}
	startup, ok := message.(*pgproto3.StartupMessage)
	if !ok {
		return
	}
	if s.settings.startup != nil {
		s.settings.startup <- startup.Parameters
	}
	if s.settings.authError != "" {
		backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: s.settings.authError, Message: "test authentication failure"})
		_ = backend.Flush()
		return
	}
	if s.settings.password != nil {
		_ = backend.SetAuthType(pgproto3.AuthTypeCleartextPassword)
		backend.Send(&pgproto3.AuthenticationCleartextPassword{})
		if backend.Flush() != nil {
			return
		}
		message, err := backend.Receive()
		if err != nil {
			return
		}
		password, ok := message.(*pgproto3.PasswordMessage)
		if !ok {
			return
		}
		s.settings.password <- password.Password
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	status := byte('I')
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		switch msg := message.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Query:
			sql := strings.TrimSpace(strings.ToLower(msg.String))
			switch {
			case strings.HasPrefix(sql, "select pg_is_in_recovery()"):
				recovery, readOnly := "f", "off"
				if s.recovery {
					recovery, readOnly = "t", "on"
				}
				backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("recovery"), DataTypeOID: 16, DataTypeSize: 1}, {Name: []byte("read_only"), DataTypeOID: 25, DataTypeSize: -1}}})
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(recovery), []byte(readOnly)}})
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.HasPrefix(sql, "select"):
				backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("identity"), DataTypeOID: 25, DataTypeSize: -1}}})
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(s.identity)}})
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.HasPrefix(sql, "begin"):
				status = 'T'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case strings.HasPrefix(sql, "commit"), strings.HasPrefix(sql, "rollback"):
				status = 'I'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(strings.ToUpper(sql))})
			case sql == "-- ping", sql == "":
				backend.Send(&pgproto3.EmptyQueryResponse{})
			default:
				backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000", Message: fmt.Sprintf("unsupported test query %q", sql)})
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
			if backend.Flush() != nil {
				return
			}
		default:
			return
		}
	}
}
