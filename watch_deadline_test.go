package cnpgconnectgo

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWatchDeadlineUsesAcceptedFreshness(t *testing.T) {
	for _, source := range []string{"cached", "received"} {
		for _, tc := range []struct {
			name      string
			age       time.Duration
			available bool
			want      time.Duration
		}{
			{"initial", 0, false, 30 * time.Second},
			{"fresh", 0, true, 15 * time.Second},
			{"partly_aged", 10 * time.Second, true, 5 * time.Second},
			{"expired", 20 * time.Second, true, 30 * time.Second},
			{"unavailable", 0, false, 30 * time.Second},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := testClient()
					c.cfg.MaxSnapshotTTL = 30 * time.Second
					c.ctx = context.Background()
					var response *connectv1.Snapshot
					if tc.name != "initial" {
						response = snapshot(time.Now().Add(-tc.age))
						response.Available = tc.available
						if source == "cached" {
							acceptSnapshot(t, c, response, time.Now())
							response = nil
						}
					}
					wantResponse := response != nil
					c.topology = scriptedTopologyClient{watch: func(ctx context.Context) grpc.ServerStreamingClient[connectv1.Snapshot] {
						return scriptedSnapshotStream{recv: func() (*connectv1.Snapshot, error) {
							if response != nil {
								s := response
								response = nil
								return s, nil
							}
							<-ctx.Done()
							return nil, ctx.Err()
						}}
					}}
					started := time.Now()
					if got, err := c.watch(); got != wantResponse || err == nil {
						t.Fatalf("watch=%v,%v; want received=%v and cancellation", got, err, wantResponse)
					}
					if elapsed := time.Since(started); elapsed != tc.want {
						t.Fatalf("recovery took %v, want %v", elapsed, tc.want)
					}
				})
			})
		}
	}
}

func TestWatchReplayDoesNotExtendRecoveryDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := testClient()
		c.cfg.MaxSnapshotTTL = 30 * time.Second
		c.ctx = context.Background()
		started := time.Now()
		s := snapshot(started)
		calls := 0
		c.topology = scriptedTopologyClient{watch: func(ctx context.Context) grpc.ServerStreamingClient[connectv1.Snapshot] {
			return scriptedSnapshotStream{recv: func() (*connectv1.Snapshot, error) {
				calls++
				switch calls {
				case 1:
					return s, nil
				case 2:
					time.Sleep(10 * time.Second)
					s.ValidUntil = timestamppb.New(started.Add(time.Hour))
					return s, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
		}}
		c.watch()
		if elapsed := time.Since(started); elapsed != 15*time.Second {
			t.Fatalf("replay extended recovery to %v", elapsed)
		}
	})
}
