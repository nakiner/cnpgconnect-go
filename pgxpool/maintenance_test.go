package pgxpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestMaintenanceDoesNotRunBorrowerHooks(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepare", true: "before_acquire"}[legacy], func(t *testing.T) {
			pg := newPostgres(t, "healthy", false)
			resolver := newResolver(pg.target(1))
			cfg := testConfig(t)
			cfg.MaxConns = 1
			var prepares, releases atomic.Int64
			if legacy {
				cfg.BeforeAcquire = func(context.Context, *pgx.Conn) bool { prepares.Add(1); return true }
			} else {
				cfg.PrepareConn = func(context.Context, *pgx.Conn) (bool, error) { prepares.Add(1); return true, nil }
			}
			cfg.AfterRelease = func(*pgx.Conn) bool { releases.Add(1); return true }
			pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			waitFor(t, func() bool { return pool.Stat().IdleConns() == 1 })
			beforePrepare, beforeRelease := prepares.Load(), releases.Load()
			for range 10 {
				resolver.set(pg.target(1))
			}
			time.Sleep(100 * time.Millisecond)
			waitFor(t, func() bool { return pool.Stat().IdleConns() == 1 })
			if prepares.Load() != beforePrepare || releases.Load() != beforeRelease {
				t.Fatalf("maintenance invoked application hooks: prepare %d->%d release %d->%d", beforePrepare, prepares.Load(), beforeRelease, releases.Load())
			}
			conn, err := pool.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			conn.Release()
			waitFor(t, func() bool { return pool.Stat().IdleConns() == 1 })
			if prepares.Load() != beforePrepare+1 || releases.Load() != beforeRelease+1 {
				t.Fatal("real borrower hooks were bypassed")
			}
		})
	}
}

func TestResetRetainsHealthyBorrowedConnectionsAndTracksRoutes(t *testing.T) {
	a, b := newPostgres(t, "a", false), newPostgres(t, "b", false)
	resolver := newResolver(a.target(1))
	resolver.alsoValid = a.target(1)
	pool, err := Open(context.Background(), Config{Resolver: resolver, ConnConfig: testConfig(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	waitFor(t, func() bool { return pool.Stat().IdleConns() == 1 })
	borrowed, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer borrowed.Release()
	held := borrowed.Conn().PgConn()
	for generation := uint64(2); generation <= 12; generation++ {
		resolver.set(b.target(generation))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var identity string
		err := pool.QueryRow(ctx, "SELECT identity").Scan(&identity)
		cancel()
		if err != nil || identity != "b" {
			t.Fatalf("generation %d: query = %q, %v", generation, identity, err)
		}
		if !pool.Valid(borrowed.Conn()) {
			t.Fatal("unrelated member changes invalidated the healthy borrowed connection")
		}
		waitFor(t, func() bool {
			pool.mu.RLock()
			defer pool.mu.RUnlock()
			return len(pool.routes) == 2
		})
	}
	// Whole-pool reset does not interrupt held sessions, even when triggered
	// by another member. pgx retires them only after the caller releases them.
	var identity string
	if err := borrowed.QueryRow(context.Background(), "SELECT identity").Scan(&identity); err != nil || identity != "a" {
		t.Fatalf("borrowed query after resets = %q, %v", identity, err)
	}
	borrowed.Release()
	select {
	case <-held.CleanupDone():
	case <-time.After(time.Second):
		t.Fatal("native Reset did not retire the returned connection")
	}
}
