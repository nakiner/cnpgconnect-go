package pgxpool

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"github.com/nakiner/cnpgconnect-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Exercise the simple API over both real protocols: discovery returns the
// database/CA and PostgreSQL receives credentials only after verified TLS.
func TestAutomaticConnectionFromDiscovery(t *testing.T) {
	pgTLS, ca := testCertificate(t, "rent-rw.dev.svc")
	startup := make(chan map[string]string, 8)
	passwords := make(chan string, 8)
	pg := newPostgres(t, "rent-primary", false, postgresSettings{tls: pgTLS, startup: startup, password: passwords})
	target := pg.target(1)
	target.Endpoint.ServerName = "rent-rw.dev.svc"

	address, discoveryTLS := startAutomaticDiscovery(t, target, ca)

	pool, err := Open(context.Background(), Config{
		Address: address, Namespace: "dev", Cluster: "rent",
		Username: "rent@app", Password: "test password '@&\\",
		// Only the test fixture needs a private discovery root. Production uses
		// its publicly trusted endpoint and these five fields alone.
		Discovery: cnpgconnectgo.Config{TLSConfig: discoveryTLS},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var identity string
	if err := pool.QueryRow(ctx, "SELECT identity", pgx.QueryExecModeSimpleProtocol).Scan(&identity); err != nil || identity != pg.identity {
		t.Fatalf("query = %q, %v", identity, err)
	}
	select {
	case params := <-startup:
		if params["database"] != "rent_data" || params["user"] != "rent@app" {
			t.Fatalf("PostgreSQL startup did not use discovery database and application user: %v", params)
		}
	case <-ctx.Done():
		t.Fatal("PostgreSQL did not receive startup")
	}
	select {
	case password := <-passwords:
		if password != "test password '@&\\" {
			t.Fatal("PostgreSQL received the wrong password")
		}
	case <-ctx.Done():
		t.Fatal("PostgreSQL did not receive password")
	}
}

func startAutomaticDiscovery(t *testing.T, target cnpgconnectgo.Target, ca []byte) (string, *tls.Config) {
	t.Helper()
	discoveryTLS, discoveryCA := testCertificate(t, "discovery.test")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(discoveryTLS)))
	connectv1.RegisterTopologyServiceServer(server, automaticDiscovery{t: t, target: target, ca: ca})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(discoveryCA)
	return listener.Addr().String(), &tls.Config{RootCAs: roots, ServerName: "discovery.test", MinVersion: tls.VersionTLS12}
}

type automaticDiscovery struct {
	connectv1.UnimplementedTopologyServiceServer
	t      *testing.T
	target cnpgconnectgo.Target
	ca     []byte
}

func (s automaticDiscovery) WatchTopology(req *connectv1.WatchTopologyRequest, stream grpc.ServerStreamingServer[connectv1.Snapshot]) error {
	if req.Namespace != "dev" || req.Name != "rent" {
		s.t.Error("simple configuration did not select the requested cluster")
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	if len(md.Get("authorization")) != 0 {
		s.t.Error("simple client unexpectedly sent discovery credentials")
	}
	for key, values := range md {
		for _, value := range values {
			if strings.Contains(value, "rent@app") || strings.Contains(value, "test password") {
				s.t.Errorf("database credentials leaked into discovery metadata %s", key)
			}
		}
	}
	now := time.Now()
	endpoints := map[string]*connectv1.Endpoint{"internal": {
		Host: s.target.Endpoint.Host, Port: uint32(s.target.Endpoint.Port), ServerName: s.target.Endpoint.ServerName,
	}}
	if endpoint := s.target.FallbackEndpoint; endpoint.Host != "" {
		endpoints["external"] = &connectv1.Endpoint{Host: endpoint.Host, Port: uint32(endpoint.Port), ServerName: endpoint.ServerName}
	}
	if err := stream.Send(&connectv1.Snapshot{
		ApiVersion: "connect.cnpg.io/v1alpha1",
		Cluster:    &connectv1.ClusterRef{Namespace: "dev", Name: "rent", Uid: "cluster"},
		Revision:   "1", ObservedAt: timestamppb.New(now), ValidUntil: timestamppb.New(now.Add(time.Minute)),
		Available: true, PrimaryId: s.target.MemberID,
		Connection: &connectv1.ConnectionParameters{Database: "rent_data", ServerCaPem: s.ca},
		Members: []*connectv1.Member{{
			Id: s.target.MemberID, Name: s.target.Name, Ready: true, Role: connectv1.Role_ROLE_PRIMARY,
			Endpoints: endpoints,
		}},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestAutomaticConnectionVerifiesCertificate(t *testing.T) {
	serverTLS, ca := testCertificate(t, "rent-rw.dev.svc")
	_, wrongCA := testCertificate(t, "unrelated.test")
	for _, tc := range []struct {
		name       string
		ca         []byte
		serverName string
	}{
		{name: "wrong CA", ca: wrongCA, serverName: "rent-rw.dev.svc"},
		{name: "wrong name", ca: ca, serverName: "another.dev.svc"},
		{name: "invalid CA", ca: []byte("invalid"), serverName: "rent-rw.dev.svc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			startup := make(chan map[string]string, 8)
			pg := newPostgres(t, "rent", false, postgresSettings{tls: serverTLS, startup: startup})
			target := pg.target(1)
			target.Endpoint.ServerName = tc.serverName
			target.Connection = cnpgconnectgo.ConnectionParameters{Database: "rent", ServerCAPEM: string(tc.ca)}
			pool, err := Open(context.Background(), Config{
				Resolver: newResolver(target), Username: "rent", Password: "secret",
				StartupTimeout: time.Second,
			})
			if pool != nil {
				pool.Close()
			}
			if err == nil {
				t.Fatal("invalid PostgreSQL certificate accepted")
			}
			select {
			case <-startup:
				t.Fatal("credentials sent before server verification")
			default:
			}
		})
	}
}

func TestAutomaticConnectionRejectsMissingMetadata(t *testing.T) {
	pg := newPostgres(t, "old-plugin", false)
	pool, err := Open(context.Background(), Config{Resolver: newResolver(pg.target(1)), Username: "rent"})
	if pool != nil {
		pool.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "database name") {
		t.Fatalf("missing connection metadata error = %v", err)
	}
}

func TestAutomaticNetworkFallbackAndExplicitOverride(t *testing.T) {
	pgTLS, ca := testCertificate(t, "external-pg.test")
	pg := newPostgres(t, "external-primary", false, postgresSettings{tls: pgTLS})
	target := pg.target(1)
	target.FallbackEndpoint = target.Endpoint
	target.FallbackEndpoint.ServerName = "external-pg.test"
	target.Endpoint = cnpgconnectgo.Endpoint{Host: "internal-only.invalid", Port: 5432, ServerName: "internal-pg.test"}
	address, discoveryTLS := startAutomaticDiscovery(t, target, ca)
	for _, tc := range []struct {
		name      string
		network   string
		wantError bool
	}{
		{name: "automatic external fallback"},
		{name: "explicit external", network: "external"},
		{name: "explicit internal stays internal", network: "internal", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := testConfig(t)
			var internalLookups atomic.Int32
			lookup := config.ConnConfig.LookupFunc
			config.ConnConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
				if host == "internal-only.invalid" {
					internalLookups.Add(1)
					return nil, &net.DNSError{Name: host, Err: "not reachable from this network", IsNotFound: true}
				}
				return lookup(ctx, host)
			}
			pool, err := Open(context.Background(), Config{
				Address: address, Namespace: "dev", Cluster: "rent", Username: "rent", Password: "secret",
				Discovery:  cnpgconnectgo.Config{TLSConfig: discoveryTLS, Network: tc.network},
				ConnConfig: config,
			})
			if pool != nil {
				defer pool.Close()
			}
			if (err != nil) != tc.wantError {
				t.Fatalf("Open error = %v; wantError %v", err, tc.wantError)
			}
			if tc.network == "external" && internalLookups.Load() != 0 {
				t.Fatal("explicit external selection tried the internal address")
			}
			if tc.network != "external" && internalLookups.Load() == 0 {
				t.Fatal("internal address was not attempted")
			}
			if !tc.wantError {
				var identity string
				if err := pool.QueryRow(context.Background(), "SELECT identity").Scan(&identity); err != nil || identity != "external-primary" {
					t.Fatalf("external query = %q, %v", identity, err)
				}
			}
		})
	}
}

func TestNetworkFallbackBoundsUnresponsiveInternalAddress(t *testing.T) {
	pgTLS, ca := testCertificate(t, "external-pg.test")
	pg := newPostgres(t, "external", false, postgresSettings{tls: pgTLS})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn // Deliberately never complete SSL negotiation.
		}
		close(accepted)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		for conn := range accepted {
			_ = conn.Close()
		}
	})
	target := pg.target(1)
	target.FallbackEndpoint = target.Endpoint
	target.FallbackEndpoint.ServerName = "external-pg.test"
	target.Endpoint = cnpgconnectgo.Endpoint{Host: "127.0.0.1", Port: uint16(listener.Addr().(*net.TCPAddr).Port), ServerName: "internal-pg.test"}
	target.Connection = cnpgconnectgo.ConnectionParameters{Database: "rent", ServerCAPEM: string(ca)}
	config := testConfig(t)
	config.ConnConfig.ConnectTimeout = 50 * time.Millisecond
	start := time.Now()
	pool, err := Open(context.Background(), Config{Resolver: newResolver(target), Username: "rent", ConnConfig: config, StartupTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if time.Since(start) > 750*time.Millisecond {
		t.Fatal("unresponsive internal address exceeded the configured per-address timeout")
	}
}

func TestNetworkFallbackChecksRoleAndPreservesAuthenticationErrors(t *testing.T) {
	pgTLS, ca := testCertificate(t, "pg.test")
	for _, tc := range []struct {
		name      string
		recovery  bool
		authError string
	}{
		{name: "obsolete role tries external", recovery: true},
		{name: "invalid password is returned", authError: "28P01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			internal := newPostgres(t, "internal", tc.recovery, postgresSettings{tls: pgTLS, authError: tc.authError})
			externalStartup := make(chan map[string]string, 8)
			external := newPostgres(t, "external", false, postgresSettings{tls: pgTLS, startup: externalStartup})
			target := internal.target(1)
			target.Endpoint.ServerName = "pg.test"
			target.FallbackEndpoint = external.target(1).Endpoint
			target.FallbackEndpoint.ServerName = "pg.test"
			target.Connection = cnpgconnectgo.ConnectionParameters{Database: "rent", ServerCAPEM: string(ca)}
			pool, err := Open(context.Background(), Config{Resolver: newResolver(target), Username: "rent", Password: "secret"})
			if pool != nil {
				defer pool.Close()
			}
			if tc.authError != "" {
				var pgError *pgconn.PgError
				if !errors.As(err, &pgError) || pgError.Code != tc.authError {
					t.Fatalf("authentication error = %v", err)
				}
				select {
				case <-externalStartup:
					t.Fatal("authentication failure retried against another address")
				default:
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var identity string
			if err := pool.QueryRow(context.Background(), "SELECT identity", pgx.QueryExecModeSimpleProtocol).Scan(&identity); err != nil || identity != "external" {
				t.Fatalf("role-safe fallback query = %q, %v", identity, err)
			}
		})
	}
}

func testCertificate(t *testing.T, name string) (*tls.Config, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test root " + name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), DNSNames: []string{name},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER, caDER}, PrivateKey: key}}},
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}
