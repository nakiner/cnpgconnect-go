package pgxpool

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestStartupPreservesHookFailureDuringTopologyChange(t *testing.T) {
	for _, hook := range []string{"after_net_connect", "validate_connect"} {
		for _, trigger := range []string{"topology_change", "connect_timeout", "endpoint_fallback"} {
			t.Run(hook+"/"+trigger, func(t *testing.T) {
				a, b := newPostgres(t, "a", false), newPostgres(t, "b", false)
				target := a.target(1)
				if trigger == "endpoint_fallback" {
					target.FallbackEndpoint = b.target(2).Endpoint
				}
				resolver := newResolver(target)
				cfg := testConfig(t)
				cfg.MaxConns = 1
				cfg.ConnConfig.ConnectTimeout = time.Second
				if trigger == "connect_timeout" {
					cfg.ConnConfig.ConnectTimeout = 100 * time.Millisecond
				}
				entered := make(chan struct{})
				var calls, constructors atomic.Int32
				cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
					constructors.Add(1)
					return nil
				}
				sentinel := errors.New("application hook rejected connection")
				fail := func(ctx context.Context) error {
					if calls.Add(1) != 1 {
						return nil // A replay would wrongly make Open succeed.
					}
					close(entered)
					if trigger != "endpoint_fallback" {
						<-ctx.Done()
					}
					if trigger == "topology_change" && ctx.Err() != context.Canceled {
						t.Errorf("topology did not cancel blocked hook: %v", ctx.Err())
					}
					return errors.Join(sentinel, context.DeadlineExceeded)
				}
				if hook == "after_net_connect" {
					cfg.ConnConfig.AfterNetConnect = func(ctx context.Context, _ *pgconn.Config, conn net.Conn) (net.Conn, error) {
						return conn, fail(ctx)
					}
				} else {
					cfg.ConnConfig.ValidateConnect = func(ctx context.Context, _ *pgconn.PgConn) error { return fail(ctx) }
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if trigger == "topology_change" {
					go func() {
						select {
						case <-entered:
							resolver.set(b.target(2))
						case <-ctx.Done():
						}
					}()
				}
				pool, err := Open(ctx, Config{Resolver: resolver, ConnConfig: cfg})
				if pool != nil {
					pool.Close()
				}
				if pool != nil || !errors.Is(err, sentinel) || calls.Load() != 1 || constructors.Load() != 1 {
					t.Fatalf("hook failure lost or replayed: pool_opened=%t calls=%d constructors=%d error=%v", pool != nil, calls.Load(), constructors.Load(), err)
				}
				resolver.mu.Lock()
				defer resolver.mu.Unlock()
				if len(resolver.subscribers) != 0 {
					t.Fatalf("failed startup retained %d subscriptions", len(resolver.subscribers))
				}
			})
		}
	}
}
