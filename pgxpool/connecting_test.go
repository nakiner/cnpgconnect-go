package pgxpool

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestUnchangedTopologyPreservesConnectionStartup(t *testing.T) {
	server := newPostgres(t, "a", false)
	target := server.target(1)
	resolver := newResolver(target)
	cfg := testConfig(t)
	cfg.MaxConns = 1
	var pause atomic.Bool
	started, resume := make(chan struct{}), make(chan struct{})
	cfg.ConnConfig.ValidateConnect = func(ctx context.Context, _ *pgconn.PgConn) error {
		if !pause.Load() {
			return nil
		}
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
			return ctx.Err()
		}
	}
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pause.Store(true)
	pool.Reset()
	type queryResult struct {
		identity string
		err      error
	}
	result := make(chan queryResult, 1)
	go func() {
		var got queryResult
		got.err = pool.QueryRow(context.Background(), "SELECT identity").Scan(&got.identity)
		result <- got
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("connection did not enter validation")
	}
	resolver.set(target)
	pool.cancelObsoleteConnections()
	close(resume)
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("unchanged topology canceled connection startup: %v", got.err)
		}
		if got.identity != "a" {
			t.Fatalf("connection after unchanged topology = %q; want a", got.identity)
		}
	case <-time.After(time.Second):
		t.Fatal("connection did not complete after validation resumed")
	}
}

func TestTopologyChangeCancelsObsoleteConnectionStartup(t *testing.T) {
	for _, stage := range []string{"dial", "handshake", "validation", "pool_after_connect"} {
		t.Run(stage, func(t *testing.T) {
			a, b := newPostgres(t, "a", false), newPostgres(t, "b", false)
			resolver := newResolver(a.target(1))
			cfg := testConfig(t)
			cfg.MaxConns = 1
			cfg.ConnConfig.ConnectTimeout = 5 * time.Second
			started := make(chan struct{}, 1)
			obsolete := b.target(2)
			if stage == "pool_after_connect" {
				cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
					if conn.PgConn().Conn().RemoteAddr().String() != b.listener.Addr().String() {
						return nil
					}
					started <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				}
			} else if stage == "validation" {
				cfg.ConnConfig.ValidateConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
					if conn.Conn().RemoteAddr().String() != b.listener.Addr().String() {
						return nil
					}
					started <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				}
			} else {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				host, port, _ := net.SplitHostPort(listener.Addr().String())
				parsedPort, _ := strconv.Atoi(port)
				obsolete.Endpoint.Host, obsolete.Endpoint.Port = host, uint16(parsedPort)
				if stage == "dial" {
					dial := cfg.ConnConfig.DialFunc
					cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
						if address != listener.Addr().String() {
							return dial(ctx, network, address)
						}
						started <- struct{}{}
						<-ctx.Done()
						return nil, ctx.Err()
					}
				} else {
					accepted := make(chan net.Conn, 1)
					go func() {
						conn, err := listener.Accept()
						if err == nil {
							accepted <- conn
							started <- struct{}{}
						}
					}()
					defer func() {
						select {
						case conn := <-accepted:
							_ = conn.Close()
						default:
						}
					}()
				}
			}
			pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			resolver.set(obsolete)
			result := make(chan error, 1)
			go func() {
				conn, err := pool.Acquire(context.Background())
				if conn != nil {
					conn.Release()
				}
				result <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("obsolete connection did not enter blocked startup stage")
			}
			// Rechecking unchanged routing must leave this attempt running.
			resolver.set(obsolete)
			pool.cancelObsoleteConnections()
			select {
			case err := <-result:
				t.Fatalf("unchanged target canceled startup: %v", err)
			default:
			}
			// Return to the original instance with a new generation. Its previous
			// idle connection is obsolete; recovery must create a fresh connection.
			resolver.set(a.target(3))
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("obsolete startup error = %v; want cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("obsolete startup occupied the only pool slot until ConnectTimeout")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var identity string
			if err := pool.QueryRow(ctx, "SELECT identity").Scan(&identity); err != nil || identity != "a" {
				t.Fatalf("replacement query = %q, %v; want a", identity, err)
			}
			pool.mu.RLock()
			remaining := len(pool.connecting)
			pool.mu.RUnlock()
			if remaining != 0 {
				t.Fatalf("completed constructors left %d cancellation registrations", remaining)
			}
		})
	}
}

func TestConnectionCancellationPreservesApplicationTracer(t *testing.T) {
	server := newPostgres(t, "a", false)
	cfg := testConfig(t)
	cfg.MaxConns = 1
	tracer := &applicationTracer{t: t}
	cfg.ConnConfig.Tracer = tracer
	cfg.ConnConfig.ValidateConnect = func(ctx context.Context, _ *pgconn.PgConn) error {
		if ctx.Value(applicationTraceKey{}) != tracer {
			t.Error("application connection tracing context was lost")
		}
		return nil
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SELECT identity")
		return err
	}
	pool, err := Open(context.Background(), Config{Resolver: newResolver(server.target(1)), ConnConfig: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// ConnectEnd and the pool's AfterConnect cleanup cancel only constructor
	// contexts. The connection remains usable with normal query tracing.
	var identity string
	if err := pool.QueryRow(context.Background(), "SELECT identity").Scan(&identity); err != nil || identity != "a" {
		t.Fatalf("query after construction = %q, %v", identity, err)
	}
	if tracer.connectStart.Load() != 1 || tracer.connectEnd.Load() != 1 || tracer.queryStart.Load() == 0 || tracer.queryStart.Load() != tracer.queryEnd.Load() {
		t.Fatalf("application trace callbacks: connect=%d/%d query=%d/%d", tracer.connectStart.Load(), tracer.connectEnd.Load(), tracer.queryStart.Load(), tracer.queryEnd.Load())
	}
	if cfg.ConnConfig.Tracer != tracer {
		t.Fatal("caller tracer configuration was mutated")
	}
}

type applicationTraceKey struct{}

type applicationTracer struct {
	t            *testing.T
	connectStart atomic.Int32
	connectEnd   atomic.Int32
	queryStart   atomic.Int32
	queryEnd     atomic.Int32
}

func (t *applicationTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	t.connectStart.Add(1)
	return context.WithValue(ctx, applicationTraceKey{}, t)
}

func (t *applicationTracer) TraceConnectEnd(ctx context.Context, _ pgx.TraceConnectEndData) {
	t.connectEnd.Add(1)
	if ctx.Value(applicationTraceKey{}) != t {
		t.t.Error("application ConnectEnd lost its context")
	}
}

func (t *applicationTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	t.queryStart.Add(1)
	return context.WithValue(ctx, applicationTraceKey{}, t)
}

func (t *applicationTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	t.queryEnd.Add(1)
	if ctx.Value(applicationTraceKey{}) != t {
		t.t.Error("application QueryEnd lost its context")
	}
}
