package cnpgconnectgo

import (
	"context"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Roles change without Pod replacement or endpoint changes. Exercise every
// directed transition and its return path so rejoining a former role cannot
// accidentally resurrect a connection captured before the change.
func TestEveryRoleTransitionInvalidatesOldConnection(t *testing.T) {
	states := []Role{Primary, SyncReplica, AsyncReplica, QuorumReplica, PotentialReplica}
	for _, from := range states {
		for _, to := range states {
			if from == to {
				continue
			}
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				c := testClient()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				now := time.Now()
				if err := c.accept(transitionSnapshot(now, from), now); err != nil {
					t.Fatal(err)
				}
				original, err := c.Resolve(ctx, Policy{Role: from})
				if err != nil || original.MemberID != "moving" {
					t.Fatalf("initial target: %+v, %v", original, err)
				}
				changes, stop := c.Subscribe()
				defer stop()
				<-changes
				later := now.Add(time.Millisecond)
				if err := c.accept(transitionSnapshot(later, to), later); err != nil {
					t.Fatal(err)
				}
				select {
				case <-changes:
				default:
					t.Fatal("role transition did not notify adapters")
				}
				if c.Valid(Policy{Role: from}, original) || c.Valid(Policy{Role: Any}, original) {
					t.Fatal("old connection remained eligible after role transition")
				}
				current, err := c.Resolve(ctx, Policy{Role: to})
				if err != nil || current.MemberID != original.MemberID || current.Endpoint != original.Endpoint {
					t.Fatalf("new role did not select the same instance: %+v, %v", current, err)
				}
				if !c.Valid(Policy{Role: to}, current) || current.Generation == original.Generation {
					t.Fatal("new role lacks a fresh eligible connection identity")
				}
				later = later.Add(time.Millisecond)
				if err := c.accept(transitionSnapshot(later, from), later); err != nil {
					t.Fatal(err)
				}
				rejoined, err := c.Resolve(ctx, Policy{Role: from})
				if err != nil || rejoined.MemberID != original.MemberID || !c.Valid(Policy{Role: from}, rejoined) {
					t.Fatalf("return to original role failed: %+v, %v", rejoined, err)
				}
				if c.Valid(Policy{Role: Any}, original) || c.Valid(Policy{Role: Any}, current) {
					t.Fatal("returning to a former role resurrected an old connection")
				}
			})
		}
	}
}

func transitionSnapshot(at time.Time, state Role) *connectv1.Snapshot {
	moving := &connectv1.Member{Id: "moving", Name: "db-1", Ready: true,
		Role:      connectv1.Role_ROLE_STANDBY,
		Endpoints: map[string]*connectv1.Endpoint{"internal": {Host: "10.0.0.1", Port: 5432}}}
	other := &connectv1.Member{Id: "other", Name: "db-2", Ready: true,
		Role:      connectv1.Role_ROLE_PRIMARY,
		Endpoints: map[string]*connectv1.Endpoint{"internal": {Host: "10.0.0.2", Port: 5432}}}
	primary := other.Id
	switch state {
	case Primary:
		moving.Role = connectv1.Role_ROLE_PRIMARY
		other.Role, other.SyncState = connectv1.Role_ROLE_STANDBY, connectv1.SyncState_SYNC_STATE_ASYNC
		primary = moving.Id
	case SyncReplica:
		moving.SyncState = connectv1.SyncState_SYNC_STATE_SYNC
	case AsyncReplica:
		moving.SyncState = connectv1.SyncState_SYNC_STATE_ASYNC
	case QuorumReplica:
		moving.SyncState = connectv1.SyncState_SYNC_STATE_QUORUM
	case PotentialReplica:
		moving.SyncState = connectv1.SyncState_SYNC_STATE_POTENTIAL
	}
	return &connectv1.Snapshot{
		ApiVersion: "connect.cnpg.io/v1alpha1",
		Cluster:    &connectv1.ClusterRef{Namespace: "db", Name: "app", Uid: "cluster-1"},
		Revision:   "opaque", ObservedAt: timestamppb.New(at), ValidUntil: timestamppb.New(at.Add(15 * time.Second)),
		Available: true, PrimaryId: primary, Members: []*connectv1.Member{moving, other},
	}
}
