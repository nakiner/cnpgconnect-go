package cnpgconnectgo

import (
	"context"
	"errors"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Exercise withdrawal, replay, and recovery together. The cases differ only in
// which observer saw the change and which negative evidence it supplied.
func TestWithdrawalReplayAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
		age  time.Duration
		seed bool
	}{
		{"older/unready", "unready", -time.Second, true},
		{"older/no_endpoint", "no_endpoint", -time.Second, true},
		{"older/primary", "primary", -time.Second, true},
		{"older/unavailable", "unavailable", -time.Second, true},
		{"older/transitioning", "transitioning", -time.Second, true},
		{"equal/unready", "unready", 0, true},
		{"equal/no_endpoint", "no_endpoint", 0, true},
		{"equal/removed", "removed", 0, true},
		{"newer/unready", "unready", time.Second, true},
		{"newer/no_endpoint", "no_endpoint", time.Second, true},
		{"initial/unready", "unready", 0, false},
		{"initial/no_endpoint", "no_endpoint", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient()
			now := time.Now()
			policy := Policy{Role: SyncReplica}
			whole := tc.kind == "primary" || tc.kind == "unavailable" || tc.kind == "transitioning"
			if whole {
				policy = Policy{}
			}
			var previous, primary Target
			var before Status
			if tc.seed {
				acceptSnapshot(t, c, snapshot(now), now)
				previous = resolveTarget(t, c, policy)
				primary = resolveTarget(t, c, Policy{})
				before = c.Status()
			}
			updates, stop := c.Subscribe()
			defer stop()
			<-updates
			withdrawal := snapshot(now.Add(tc.age))
			switch tc.kind {
			case "unready":
				withdrawal.Members[1].Ready = false
			case "no_endpoint":
				member := withdrawal.Members[1]
				member.Endpoints["external"] = member.Endpoints["internal"]
				delete(member.Endpoints, "internal")
			case "removed":
				withdrawal.Members = append(withdrawal.Members[:1], withdrawal.Members[2:]...)
			case "primary":
				withdrawal.Members[0].Ready = false
				withdrawal.Members[0].Role = connectv1.Role_ROLE_STANDBY
				withdrawal.Members[1].Role = connectv1.Role_ROLE_PRIMARY
				withdrawal.PrimaryId = "s1"
			default:
				withdrawal.Available = tc.kind == "transitioning"
				withdrawal.Transitioning = tc.kind == "transitioning"
				withdrawal.Members, withdrawal.PrimaryId = nil, ""
				withdrawal.Reason = "diagnostic text is not a protocol discriminator"
			}
			if tc.age < 0 && !whole {
				withdrawal.ValidUntil = timestamppb.New(now.Add(time.Hour))
				withdrawal.Members[0].Endpoints["internal"].Host = "obsolete-primary.test"
			}
			acceptSnapshot(t, c, withdrawal, now)
			select {
			case <-updates:
			default:
				t.Fatal("withdrawal did not notify adapters")
			}
			if !tc.seed {
				primary = resolveTarget(t, c, Policy{})
			}
			after := c.Status()
			if tc.age < 0 && (!after.ObservedAt.Equal(before.ObservedAt) || after.ValidUntil.After(before.ValidUntil)) {
				t.Fatal("older withdrawal rewound observation or extended freshness")
			}
			if tc.age < 0 && !whole && !after.ValidUntil.Equal(before.ValidUntil) {
				t.Fatal("partial withdrawal changed independent primary freshness")
			}
			if !whole && (!c.Valid(Policy{}, primary) || !after.Ready) {
				t.Fatal("partial withdrawal replaced an independent healthy primary")
			}
			if whole && after.Ready {
				t.Fatal("withdrawn primary remains usable")
			}
			if tc.seed && c.Valid(policy, previous) {
				t.Fatal("withdrawn connection remains eligible")
			}
			// An old positive sample and repeated equal-time samples must never
			// undo the withdrawal, even when this was our first observation.
			if err := c.accept(snapshot(after.ObservedAt.Add(-time.Second)), now); !errors.Is(err, errOutOfOrder) {
				t.Fatalf("accepted older positive replay: %v", err)
			}
			for range 3 {
				err := c.accept(snapshot(after.ObservedAt), now)
				if (whole && !errors.Is(err, errOutOfOrder)) || (!whole && err != nil) {
					t.Fatalf("equal-time replay: %v", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				_, err = c.Resolve(ctx, policy)
				cancel()
				if !errors.Is(err, ErrUnavailable) {
					t.Fatal("positive replay restored a withdrawn route")
				}
			}
			if tc.kind == "unavailable" || tc.kind == "transitioning" {
				acceptSnapshot(t, c, withdrawal, now) // UID survives an empty routing view.
			}
			next := after.ObservedAt.Add(time.Second)
			acceptSnapshot(t, c, snapshot(next), next)
			recovered := resolveTarget(t, c, policy)
			if !c.Valid(policy, recovered) || (tc.seed && (recovered.Generation == previous.Generation || c.Valid(policy, previous))) {
				t.Fatal("new observation did not restore a fresh connection identity")
			}
			if !whole && !c.Valid(Policy{}, primary) {
				t.Fatal("replica recovery retired the healthy primary")
			}
		})
	}
}

func TestOlderSnapshotWithoutCurrentNegativeEvidenceIsIgnored(t *testing.T) {
	for _, mode := range []string{"absent", "foreign_member", "foreign_cluster", "foreign_cluster_unavailable", "eligible_metadata"} {
		t.Run(mode, func(t *testing.T) {
			c, now := testClient(), time.Now()
			acceptSnapshot(t, c, snapshot(now), now)
			primary := resolveTarget(t, c, Policy{})
			replica := resolveTarget(t, c, Policy{Role: SyncReplica})
			older := snapshot(now.Add(-time.Second))
			switch mode {
			case "absent":
				older.Members = append(older.Members[:1], older.Members[2:]...)
			case "foreign_member":
				older.Members = append(older.Members, &connectv1.Member{Id: "former-member", Name: "former-member"})
			case "foreign_cluster", "foreign_cluster_unavailable":
				older.Cluster.Uid = "another-cluster"
				older.Members[1].Ready = false
				older.Available = mode != "foreign_cluster_unavailable"
			case "eligible_metadata":
				older.Members[1].SyncState = connectv1.SyncState_SYNC_STATE_ASYNC
				older.Members[1].Endpoints["internal"].Host = "obsolete-replica.test"
			}
			if err := c.accept(older, now); !errors.Is(err, errOutOfOrder) {
				t.Fatalf("accepted older snapshot without negative evidence: %v", err)
			}
			if !c.Valid(Policy{}, primary) || !c.Valid(Policy{Role: SyncReplica}, replica) {
				t.Fatal("ignored snapshot changed current routes")
			}
		})
	}
}
