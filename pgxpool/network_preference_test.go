package pgxpool

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/nakiner/cnpgconnect-go"
)

func TestSuccessfulFallbackPreferenceIsIdentityScopedAndRecovers(t *testing.T) {
	tlsConfig, ca := testCertificate(t, "pg.test")
	internal := newPostgres(t, "internal", false, postgresSettings{tls: tlsConfig})
	external := newPostgres(t, "external", false, postgresSettings{tls: tlsConfig})
	target := internal.target(1)
	target.Endpoint.ServerName = "pg.test"
	target.FallbackEndpoint = external.target(1).Endpoint
	target.FallbackEndpoint.ServerName = "pg.test"
	target.Connection = cnpgconnectgo.ConnectionParameters{Database: "test", ServerCAPEM: string(ca)}
	resolver := newResolver(target)
	cfg := testConfig(t)
	cfg.MaxConns = 1
	var internalCalls, externalCalls atomic.Int64
	var internalUp, externalDown atomic.Bool
	dial := cfg.ConnConfig.DialFunc
	internalAddr := net.JoinHostPort(target.Endpoint.Host, strconv.Itoa(int(target.Endpoint.Port)))
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == internalAddr {
			internalCalls.Add(1)
			if !internalUp.Load() {
				return nil, errors.New("internal route unavailable")
			}
		} else {
			externalCalls.Add(1)
			if externalDown.Load() {
				return nil, errors.New("external route unavailable")
			}
		}
		return dial(ctx, network, address)
	}
	pool, err := Open(context.Background(), Config{Resolver: resolver, Username: "test", ConnConfig: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	reconnect := func() {
		t.Helper()
		pool.Reset()
		if err := pool.Ping(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	first := internalCalls.Load()
	if first == 0 {
		t.Fatal("initial connection did not attempt internal")
	}
	reconnect()
	if internalCalls.Load() != first {
		t.Fatal("subsequent physical connection repeated failed internal path")
	}
	target.Generation++
	resolver.set(target)
	reconnect()
	if internalCalls.Load() == first {
		t.Fatal("preference survived changed member generation")
	}
	internalUp.Store(true)
	externalDown.Store(true)
	reconnect()
	// Native fallback must still retry this same member's internal address if the
	// remembered external path fails, then learn the newly verified internal path.
	beforeExternal := externalCalls.Load()
	reconnect()
	if externalCalls.Load() != beforeExternal {
		t.Fatal("recovery did not return to internal preference")
	}
}
