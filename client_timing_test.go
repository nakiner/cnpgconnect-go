package cnpgconnectgo

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// These fakes exercise the real watch/reconnect loop while synctest controls
// time. No sockets, timing tolerances, or retries are needed in these tests.
type scriptedTopologyClient struct {
	connectv1.TopologyServiceClient
	watch func(context.Context) grpc.ServerStreamingClient[connectv1.Snapshot]
}

func (c scriptedTopologyClient) WatchTopology(ctx context.Context, _ *connectv1.WatchTopologyRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[connectv1.Snapshot], error) {
	return c.watch(ctx), nil
}

type scriptedSnapshotStream struct {
	grpc.ClientStream
	recv func() (*connectv1.Snapshot, error)
}

func (s scriptedSnapshotStream) Recv() (*connectv1.Snapshot, error) { return s.recv() }

func TestFlappingStreamsBackOffAndStableStreamResetsDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := testClient()
		c.cfg.ReconnectMin = 100 * time.Millisecond
		c.cfg.ReconnectMax = 800 * time.Millisecond
		var cancel context.CancelFunc
		c.ctx, cancel = context.WithCancel(context.Background())
		c.done = make(chan struct{})
		starts := make(chan time.Time, 1)
		attempt := 0
		c.topology = scriptedTopologyClient{watch: func(ctx context.Context) grpc.ServerStreamingClient[connectv1.Snapshot] {
			attempt++
			starts <- time.Now()
			n := attempt
			sent := false
			return scriptedSnapshotStream{recv: func() (*connectv1.Snapshot, error) {
				if !sent {
					sent = true
					return snapshot(time.Now()), nil
				}
				if n == 5 {
					// The fifth connection stays healthy long enough to reset
					// accumulated backoff; later short streams increase it again.
					time.Sleep(c.cfg.ReconnectMax)
				}
				return nil, status.Error(codes.Unavailable, "discovery restarted")
			}}
		}}
		go c.run()
		defer func() { cancel(); <-c.done }()
		previous := <-starts
		for _, interval := range []struct {
			backoff time.Duration
			open    time.Duration
		}{
			{100 * time.Millisecond, 0},
			{200 * time.Millisecond, 0},
			{400 * time.Millisecond, 0},
			{800 * time.Millisecond, 0},
			{100 * time.Millisecond, 800 * time.Millisecond},
			{200 * time.Millisecond, 0},
		} {
			next := <-starts
			delay := next.Sub(previous) - interval.open
			if delay < interval.backoff/2 || delay >= interval.backoff {
				t.Fatalf("retry delay = %s, want jitter in [%s, %s)", delay, interval.backoff/2, interval.backoff)
			}
			previous = next
		}
	})
}

func TestClientCancellationInterruptsBackoffAndReceive(t *testing.T) {
	for _, waiting := range []string{"backoff", "receive"} {
		t.Run(waiting, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := testClient()
				c.cfg.ReconnectMin, c.cfg.ReconnectMax = time.Hour, time.Hour
				var cancel context.CancelFunc
				c.ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
				c.done = make(chan struct{})
				calls := 0
				c.topology = scriptedTopologyClient{watch: func(ctx context.Context) grpc.ServerStreamingClient[connectv1.Snapshot] {
					calls++
					return scriptedSnapshotStream{recv: func() (*connectv1.Snapshot, error) {
						if waiting == "receive" {
							<-ctx.Done()
							return nil, ctx.Err()
						}
						return nil, status.Error(codes.Unavailable, "offline")
					}}
				}}
				go c.run()
				synctest.Wait()
				before := time.Now()
				cancel()
				<-c.done
				if elapsed := time.Since(before); elapsed != 0 || calls != 1 {
					t.Fatalf("cancellation took %s and opened %d streams", elapsed, calls)
				}
			})
		})
	}
}

func TestExpiryUsesUpdatedSnapshotDeadline(t *testing.T) {
	for _, tc := range []struct {
		name       string
		initialTTL time.Duration
		updatedTTL time.Duration
	}{
		{"extend", time.Second, 2 * time.Second},
		{"shorten", 2 * time.Second, 500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := testClient()
				var cancel context.CancelFunc
				c.ctx, cancel = context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { defer close(done); c.runExpiry() }()
				defer func() { cancel(); <-done }()
				acceptFor := func(ttl time.Duration) {
					s := snapshot(time.Now())
					s.ValidUntil = timestamppb.New(time.Now().Add(ttl))
					if err := c.accept(s, time.Now()); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
				}
				acceptFor(tc.initialTTL)
				updates, unsubscribe := c.Subscribe()
				defer unsubscribe()
				<-updates
				target, err := c.Resolve(context.Background(), Policy{})
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(250 * time.Millisecond)
				acceptFor(tc.updatedTTL)
				if !c.Valid(Policy{}, target) {
					t.Fatal("freshness refresh invalidated an unchanged target")
				}
				time.Sleep(tc.updatedTTL - time.Nanosecond)
				synctest.Wait()
				select {
				case <-updates:
					t.Fatal("notified pool before the current freshness deadline")
				default:
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				// Inspect state directly: Status/Valid also expire on access and
				// could hide a missing timer notification to an idle pool.
				c.mu.Lock()
				ready := c.state.Ready
				c.mu.Unlock()
				if ready {
					t.Fatal("timer did not invalidate topology at its deadline")
				}
				select {
				case <-updates:
				default:
					t.Fatal("expiry did not notify an idle subscriber")
				}
				if c.Valid(Policy{}, target) {
					t.Fatal("expired target stayed eligible")
				}
				acceptFor(time.Second)
				if c.Valid(Policy{}, target) {
					t.Fatal("fresh snapshot reused an expired connection generation")
				}
			})
		})
	}
}

func TestStartupTimeoutDefaultAndCallerDeadline(t *testing.T) {
	cfg, err := (Config{Address: "discovery:443", Namespace: "db", Cluster: "app"}).normalized()
	if err != nil || cfg.StartupTimeout != 30*time.Second {
		t.Fatalf("startup default = %s, %v; want 30s", cfg.StartupTimeout, err)
	}
	synctest.Test(t, func(t *testing.T) {
		c := testClient()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		started := time.Now()
		if err := c.waitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitReady = %v; want caller deadline", err)
		}
		if elapsed := time.Since(started); elapsed != 200*time.Millisecond {
			t.Fatalf("startup ignored caller deadline: %s", elapsed)
		}
	})
}
