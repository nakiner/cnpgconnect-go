package pgxpool

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestStartupFollowsTopologyReplacement(t *testing.T) {
	for _, stage := range []string{"lookup", "dial", "handshake"} {
		for _, outcome := range []string{"retry", "caller_cancel", "caller_deadline"} {
			t.Run(stage+"/"+outcome, func(t *testing.T) {
				a, b := newPostgres(t, "a", false), newPostgres(t, "b", false)
				old := a.target(1)
				old.Endpoint.Host = "old.internal"
				resolver := newResolver(old)
				cfg := testConfig(t)
				cfg.MaxConns = 1
				started, replacement := make(chan struct{}, 1), make(chan struct{}, 1)
				var active atomic.Int32
				block := func(ctx context.Context, entered chan<- struct{}) error {
					active.Add(1)
					defer active.Add(-1)
					entered <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				}
				lookup, dial := cfg.ConnConfig.LookupFunc, cfg.ConnConfig.DialFunc
				cfg.ConnConfig.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
					if stage == "lookup" {
						if host == old.Endpoint.Host {
							return nil, block(ctx, started)
						}
						if outcome != "retry" {
							return nil, block(ctx, replacement)
						}
					}
					if host == old.Endpoint.Host {
						return []string{"127.0.0.1"}, nil
					}
					return lookup(ctx, host)
				}
				cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
					if stage != "lookup" {
						if address == a.listener.Addr().String() {
							if stage == "handshake" {
								client, server := net.Pipe()
								active.Add(1)
								go func() {
									defer active.Add(-1)
									defer server.Close()
									message, err := pgproto3.NewBackend(server, server).ReceiveStartupMessage()
									if _, ok := message.(*pgproto3.StartupMessage); err != nil || !ok {
										return // pgx can send a separate CancelRequest during cleanup.
									}
									started <- struct{}{}
									_, _ = io.Copy(io.Discard, server)
								}()
								return client, nil
							}
							return nil, block(ctx, started)
						}
						if outcome != "retry" {
							return nil, block(ctx, replacement)
						}
					}
					return dial(ctx, network, address)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if outcome == "caller_deadline" {
					var stop context.CancelFunc
					ctx, stop = context.WithTimeout(ctx, 250*time.Millisecond)
					defer stop()
				}
				type result struct {
					pool *Pool
					err  error
				}
				opened := make(chan result, 1)
				go func() {
					pool, err := Open(ctx, Config{Resolver: resolver, ConnConfig: cfg, StartupTimeout: time.Second})
					opened <- result{pool, err}
				}()
				var got result
				received := false
				defer func() {
					cancel()
					if !received {
						select {
						case got = <-opened:
						case <-time.After(time.Second):
							t.Error("Open did not stop after test cancellation")
						}
					}
					if got.pool != nil {
						got.pool.Close()
					}
				}()
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("old constructor did not start")
				}
				resolver.set(b.target(2))
				if outcome != "retry" {
					select {
					case <-replacement:
					case got = <-opened:
						received = true
						t.Fatalf("Open exited before replacement constructor: %v", got.err)
					case <-time.After(time.Second):
						t.Fatal("replacement constructor did not start")
					}
					if outcome == "caller_cancel" {
						cancel()
					}
				}
				select {
				case got = <-opened:
					received = true
					if outcome == "retry" {
						if got.err != nil {
							t.Fatalf("live caller lost startup to obsolete constructor: %v (caller: %v)", got.err, ctx.Err())
						}
						var identity string
						if err := got.pool.QueryRow(ctx, "SELECT identity").Scan(&identity); err != nil || identity != "b" {
							t.Fatalf("replacement connection: identity=%q error=%v", identity, err)
						}
						if got.pool.Stat().NewConnsCount() != 2 {
							t.Fatalf("constructors=%d, want obsolete A then healthy B", got.pool.Stat().NewConnsCount())
						}
						got.pool.Close()
						got.pool.mu.RLock()
						remaining := len(got.pool.connecting)
						got.pool.mu.RUnlock()
						if remaining != 0 {
							t.Fatalf("retained %d constructor registrations", remaining)
						}
					} else if got.pool != nil || !errors.Is(got.err, ctx.Err()) {
						t.Fatalf("caller cancellation lost: pool=%v error=%v caller=%v", got.pool, got.err, ctx.Err())
					}
				case <-time.After(time.Second):
					t.Fatal("Open did not complete within the startup budget")
				}
				waitFor(t, func() bool { return active.Load() == 0 })
				resolver.mu.Lock()
				defer resolver.mu.Unlock()
				if len(resolver.subscribers) != 0 || active.Load() != 0 {
					t.Fatalf("startup retained subscriptions=%d active callbacks=%d", len(resolver.subscribers), active.Load())
				}
			})
		}
	}
}

func TestStartupDoesNotRetryUnownedCancellation(t *testing.T) {
	server := newPostgres(t, "a", false)
	cfg := testConfig(t)
	cfg.MaxConns = 1
	var attempts atomic.Int32
	cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		attempts.Add(1)
		return nil, context.Canceled
	}
	pool, err := Open(context.Background(), Config{Resolver: newResolver(server.target(1)), ConnConfig: cfg, StartupTimeout: time.Second})
	if pool != nil || !errors.Is(err, context.Canceled) || attempts.Load() != 1 {
		if pool != nil {
			pool.Close()
		}
		t.Fatalf("unowned cancellation retried: pool=%v attempts=%d error=%v", pool, attempts.Load(), err)
	}
}
