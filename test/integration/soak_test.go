//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nakiner/cnpgconnect-go"
	managed "github.com/nakiner/cnpgconnect-go/pgxpool"
	"github.com/nakiner/cnpgconnect-go/stdlib"
)

// TestLiveRecoverySoak keeps independent application pools alive through
// repeated promotions and discovery outages. The caller owns the kind cluster.
func TestLiveRecoverySoak(t *testing.T) {
	if os.Getenv("CNPGCONNECT_GO_E2E_SOAK") != "1" {
		t.Skip("opt in with CNPGCONNECT_GO_E2E_SOAK=1")
	}
	clients := soakInt(t, "SOAK_CLIENTS", 24, 2, 128)
	poolSize := soakInt(t, "SOAK_POOL_SIZE", 2, 1, 8)
	duration := soakDuration(t, "SOAK_DURATION", 3*time.Minute, 10*time.Minute)
	budget := soakDuration(t, "RECOVERY_SLO", time.Minute, 3*time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), duration+2*budget+5*time.Minute)
	defer cancel()
	h := &harness{kubeconfig: os.Getenv("CNPGCONNECT_GO_E2E_KUBECONFIG")}
	if h.kubeconfig == "" {
		t.Fatal("CNPGCONNECT_GO_E2E_KUBECONFIG is required")
	}
	if err := h.guard(ctx); err != nil {
		t.Fatal(err)
	}
	initial, err := h.cluster(ctx)
	if err != nil || initial.Metadata.UID == "" {
		t.Fatalf("read integration Cluster identity: %v", err)
	}
	h.uid = initial.Metadata.UID
	deployment, err := h.discoveryDeployment(ctx)
	if err != nil || deployment.Metadata.UID == "" || deployment.Spec.Replicas < 1 || len(deployment.Spec.Selector.MatchLabels) == 0 {
		t.Fatalf("read running discovery Deployment: %v", err)
	}
	ttl, err := deployment.snapshotTTL()
	if err != nil {
		t.Fatal(err)
	}
	config := h.configuration(t, ctx)
	discovery, err := cnpgconnectgo.New(ctx, config.Discovery)
	if err != nil {
		t.Fatal(err)
	}
	defer discovery.Close()
	runID := fmt.Sprintf("cnpg_soak_%d", time.Now().UnixNano())
	config.ConnConfig.ConnConfig.RuntimeParams["application_name"] = runID
	config.ConnConfig.MaxConns = int32(poolSize)
	config.ConnConfig.MinConns = int32(poolSize)
	controlConfig := config
	controlConfig.Resolver = discovery
	controlConfig.ConnConfig = config.ConnConfig.Copy()
	controlConfig.ConnConfig.MaxConns = 1
	controlConfig.ConnConfig.MinConns = 1
	controlConfig.ConnConfig.ConnConfig.RuntimeParams["application_name"] = runID + "_control"
	control, err := managed.Open(ctx, controlConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	var maximum int
	if err := control.QueryRow(ctx, "select current_setting('max_connections')::integer").Scan(&maximum); err != nil {
		t.Fatal(err)
	}
	if clients*poolSize > maximum-16 {
		t.Fatalf("%d pools x %d connections exceeds fixture max_connections=%d with 16 slots reserved", clients, poolSize, maximum)
	}
	table := pgx.Identifier{runID}.Sanitize()
	query := "select payload, inet_server_addr()::text || '@' || pg_postmaster_start_time()::text, pg_is_in_recovery() from " + table + " where id=1"
	pools := make([]soakPool, 0, clients)
	work, stop := context.WithCancel(ctx)
	var workers sync.WaitGroup
	metrics := soakMetrics{last: make([]time.Time, clients*poolSize), servers: make([]string, clients*poolSize)}
	defer func() {
		stop()
		workers.Wait()
		cleanup, finish := context.WithTimeout(context.Background(), budget+30*time.Second)
		defer finish()
		if err := h.scaleDiscovery(cleanup, deployment.Metadata.UID, deployment.Spec.Replicas); err != nil {
			t.Errorf("restore discovery replicas: %v", err)
		}
		for _, pool := range pools {
			if err := pool.close(); err != nil {
				t.Errorf("close application pool: %v", err)
			}
		}
		// Read-only probes may retry; test writes and workload queries never do.
		for control.Ping(cleanup) != nil && cleanup.Err() == nil {
			_ = pause(cleanup, 100*time.Millisecond)
		}
		if _, err := control.Exec(cleanup, "drop table if exists "+table); err != nil {
			t.Errorf("drop soak table: %v", err)
		}
		remaining := -1
		for cleanup.Err() == nil {
			if err := control.QueryRow(cleanup, "select count(*) from pg_stat_activity where application_name=$1", runID).Scan(&remaining); err != nil {
				t.Errorf("verify connection cleanup: %v", err)
				break
			}
			if remaining == 0 {
				break
			}
			_ = pause(cleanup, 100*time.Millisecond)
		}
		if remaining != 0 {
			t.Errorf("application connections remain on the primary after Close: %d", remaining)
		}
		metrics.log(t, clients, poolSize)
	}()
	if _, err := control.Exec(ctx, "create table "+table+" (id integer primary key, payload text not null)"); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(ctx, "insert into "+table+" values (1, $1)", runID); err != nil {
		t.Fatal(err)
	}
	for index := range clients {
		pool, err := openSoakPool(ctx, config, query, index%2 == 0)
		if err != nil {
			t.Fatalf("open application pool %d: %v", index, err)
		}
		pools = append(pools, pool)
		for worker := range poolSize {
			workerID := index*poolSize + worker
			workers.Go(func() {
				for work.Err() == nil {
					attempt, done := context.WithTimeout(work, 3*time.Second)
					started := time.Now()
					var payload, server string
					var recovery bool
					err := pool.scan(attempt, &payload, &server, &recovery)
					done()
					if work.Err() != nil {
						return
					}
					if (err == nil && payload != runID) || errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
						t.Errorf("pool %d returned missing or incorrect committed data", index)
						cancel()
						return
					}
					metrics.record(workerID, started, server, err, recovery)
					_ = pause(work, 100*time.Millisecond)
				}
			})
		}
	}
	workers.Go(func() {
		for work.Err() == nil {
			attempt, done := context.WithTimeout(work, 3*time.Second)
			var connections int
			err := control.QueryRow(attempt, "select count(*) from pg_stat_activity where application_name=$1", runID).Scan(&connections)
			done()
			if err == nil {
				metrics.mu.Lock()
				metrics.peak = max(metrics.peak, connections)
				metrics.samples++
				metrics.mu.Unlock()
				// Reset can overlap closing old sockets with new construction.
				if connections > 2*clients*poolSize {
					t.Errorf("application connection count exceeds bounded retirement overlap: %d", connections)
					cancel()
				}
			}
			_ = pause(work, 250*time.Millisecond)
		}
	})
	started := time.Now()
	if err := metrics.wait(ctx, started, "", budget); err != nil {
		t.Fatalf("initial fleet readiness: %v", err)
	}
	warm, finishWarm := context.WithTimeout(ctx, budget)
	defer finishWarm()
	for {
		var connections int
		if err := control.QueryRow(warm, "select count(*) from pg_stat_activity where application_name=$1", runID).Scan(&connections); err != nil {
			t.Fatalf("verify fully warm application pools: %v", err)
		}
		if connections >= clients*poolSize {
			break
		}
		if err := pause(warm, 50*time.Millisecond); err != nil {
			t.Fatalf("only %d/%d application connections became ready: %v", connections, clients*poolSize, err)
		}
	}
	finishWarm()
	started = time.Now()
	cycles := 0
	for cycles == 0 || time.Since(started) < duration {
		cycles++
		target, err := discovery.Resolve(ctx, cnpgconnectgo.Policy{Role: cnpgconnectgo.Replica})
		if err != nil {
			t.Fatal(err)
		}
		expected := soakDirectProbe(t, ctx, config, target, query, runID, true)
		promotion := time.Now()
		if err := h.promote(ctx, target); err != nil {
			t.Fatal(err)
		}
		if err := metrics.wait(ctx, promotion, expected, budget); err != nil {
			t.Fatalf("cycle %d promotion recovery: %v", cycles, err)
		}
		soakRecoveryLog(t, cycles, "promotion", promotion)
		if err := h.scaleDiscovery(ctx, deployment.Metadata.UID, 0); err != nil {
			t.Fatal(err)
		}
		if err := h.waitDiscoveryStopped(ctx, deployment); err != nil {
			t.Fatal(err)
		}
		if err := pause(ctx, ttl+2*time.Second); err != nil {
			t.Fatal(err)
		}
		soakDirectProbe(t, ctx, config, target, query, runID, false)
		var probes sync.WaitGroup
		for index, pool := range pools {
			probes.Go(func() {
				probe, done := context.WithTimeout(ctx, time.Second)
				defer done()
				var payload, server string
				var recovery bool
				if err := pool.scan(probe, &payload, &server, &recovery); !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("pool %d did not reject expired discovery with its deadline: %v", index, err)
				}
			})
		}
		probes.Wait()
		restart := time.Now()
		if err := h.scaleDiscovery(ctx, deployment.Metadata.UID, deployment.Spec.Replicas); err != nil {
			t.Fatal(err)
		}
		if err := metrics.wait(ctx, restart, expected, budget); err != nil {
			t.Fatalf("cycle %d discovery recovery: %v", cycles, err)
		}
		soakRecoveryLog(t, cycles, "discovery_restart", restart)
		if remaining := duration - time.Since(started); remaining > 0 {
			if err := pause(ctx, min(5*time.Second, remaining)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := metrics.wait(ctx, time.Now(), "", budget); err != nil {
		t.Fatalf("final fleet readiness: %v", err)
	}
	metrics.mu.Lock()
	samples := metrics.samples
	metrics.mu.Unlock()
	if samples == 0 {
		t.Fatal("no successful PostgreSQL connection samples were collected")
	}
	t.Logf("completed %d promotion/outage cycles in %s with the original %d pool handles", cycles, time.Since(started), clients)
}

type soakPool struct {
	scan  func(context.Context, ...any) error
	close func() error
}

func openSoakPool(ctx context.Context, config managed.Config, query string, pgx bool) (soakPool, error) {
	if pgx {
		pool, err := managed.Open(ctx, config)
		if err != nil {
			return soakPool{}, err
		}
		return soakPool{scan: func(ctx context.Context, dest ...any) error { return pool.QueryRow(ctx, query).Scan(dest...) }, close: func() error { pool.Close(); return nil }}, nil
	}
	db, err := stdlib.Open(ctx, config)
	if err != nil {
		return soakPool{}, err
	}
	return soakPool{scan: func(ctx context.Context, dest ...any) error { return db.QueryRowContext(ctx, query).Scan(dest...) }, close: db.Close}, nil
}

func soakDirectProbe(t *testing.T, parent context.Context, config managed.Config, target cnpgconnectgo.Target, query, payload string, recovery bool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	conn, err := connectTarget(ctx, config, target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for {
		var got, server string
		var inRecovery bool
		err := conn.QueryRow(ctx, query).Scan(&got, &server, &inRecovery)
		if err == nil && got != payload {
			t.Fatal("direct PostgreSQL probe returned incorrect committed data")
		}
		if err == nil && inRecovery == recovery {
			return server
		}
		// The first standby probe may precede replay of the sentinel transaction.
		if waitErr := pause(ctx, 50*time.Millisecond); waitErr != nil {
			t.Fatalf("direct PostgreSQL data/role check: recovery=%v expected=%v query=%v wait=%v", inRecovery, recovery, err, waitErr)
		}
	}
}

type soakMetrics struct {
	mu                                  sync.Mutex
	queries, successes, failures, stale uint64
	buckets                             [2][32]uint64 // Powers of two microseconds: all attempts, successful primary reads.
	last                                []time.Time
	servers                             []string
	peak, samples                       int
}

func (m *soakMetrics) record(index int, started time.Time, server string, err error, recovery bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries++
	bucket := min(bits.Len64(uint64(time.Since(started).Microseconds())), len(m.buckets[0])-1)
	m.buckets[0][bucket]++
	if err != nil {
		m.failures++
	} else if recovery {
		m.stale++
	} else {
		m.successes++
		m.buckets[1][bucket]++
		m.last[index], m.servers[index] = time.Now(), server
	}
}

func (m *soakMetrics) wait(parent context.Context, since time.Time, server string, budget time.Duration) error {
	ctx, cancel := context.WithDeadline(parent, since.Add(budget))
	defer cancel()
	for {
		m.mu.Lock()
		ready := 0
		for index, last := range m.last {
			if last.After(since) && (server == "" || m.servers[index] == server) {
				ready++
			}
		}
		m.mu.Unlock()
		if ready == len(m.last) && ctx.Err() == nil {
			return nil
		}
		if err := pause(ctx, 20*time.Millisecond); err != nil {
			return fmt.Errorf("%d/%d query workers recovered within %s: %w", ready, len(m.last), budget, err)
		}
	}
}

func (m *soakMetrics) log(t *testing.T, clients, poolSize int) {
	percentile := func(histogram int, samples, percent uint64) float64 {
		if samples == 0 {
			return 0
		}
		var total uint64
		for bucket, count := range m.buckets[histogram] {
			total += count
			if total*100 >= samples*percent {
				return float64(uint64(1)<<bucket) / 1000
			}
		}
		return 0
	}
	data, _ := json.Marshal(map[string]any{
		"pools": clients, "connections_and_workers_per_pool": poolSize, "query_attempts": m.queries,
		"query_errors": m.failures, "standby_results_during_transition": m.stale,
		"successful_primary_queries": m.successes,
		"all_attempts_p50_upper_ms":  percentile(0, m.queries, 50), "all_attempts_p95_upper_ms": percentile(0, m.queries, 95), "all_attempts_p99_upper_ms": percentile(0, m.queries, 99),
		"successful_primary_p50_upper_ms": percentile(1, m.successes, 50), "successful_primary_p95_upper_ms": percentile(1, m.successes, 95), "successful_primary_p99_upper_ms": percentile(1, m.successes, 99),
		"sampled_primary_application_connections_peak": m.peak, "connection_samples": m.samples,
	})
	t.Logf("CNPG_SOAK_SUMMARY %s", data)
}

func soakRecoveryLog(t *testing.T, cycle int, phase string, started time.Time) {
	data, _ := json.Marshal(map[string]any{"cycle": cycle, "phase": phase, "seconds": time.Since(started).Seconds()})
	t.Logf("CNPG_SOAK_RECOVERY %s", data)
}

func soakInt(t *testing.T, suffix string, fallback, minimum, maximum int) int {
	t.Helper()
	name := "CNPGCONNECT_GO_E2E_" + suffix
	value := fallback
	if raw := os.Getenv(name); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("%s must be an integer", name)
		}
		value = parsed
	}
	if value < minimum || value > maximum {
		t.Fatalf("%s must be in [%d,%d]", name, minimum, maximum)
	}
	return value
}

func soakDuration(t *testing.T, suffix string, fallback, maximum time.Duration) time.Duration {
	t.Helper()
	name := "CNPGCONNECT_GO_E2E_" + suffix
	value := fallback
	if raw := os.Getenv(name); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			t.Fatalf("%s must be a duration", name)
		}
		value = parsed
	}
	if value <= 0 || value > maximum {
		t.Fatalf("%s must be positive and at most %s", name, maximum)
	}
	return value
}
