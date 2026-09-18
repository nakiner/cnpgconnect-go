package cnpgconnectgo

import (
	"context"
	"errors"
	"testing"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testClient() *Client {
	return &Client{cfg: Config{Namespace: "db", Cluster: "app", Network: "internal", MaxSnapshotTTL: time.Minute}, changed: make(chan struct{}), subscribers: map[chan struct{}]struct{}{}}
}
func snapshot(at time.Time) *connectv1.Snapshot {
	s := &connectv1.Snapshot{ApiVersion: "connect.cnpg.io/v1alpha1", Cluster: &connectv1.ClusterRef{Namespace: "db", Name: "app", Uid: "cluster-1"}, Revision: "opaque:1", ObservedAt: timestamppb.New(at), ValidUntil: timestamppb.New(at.Add(15 * time.Second)), Available: true, PrimaryId: "p1"}
	for i, m := range []struct {
		id    string
		role  connectv1.Role
		state connectv1.SyncState
	}{{"p1", connectv1.Role_ROLE_PRIMARY, connectv1.SyncState_SYNC_STATE_UNKNOWN}, {"s1", connectv1.Role_ROLE_STANDBY, connectv1.SyncState_SYNC_STATE_SYNC}, {"a1", connectv1.Role_ROLE_STANDBY, connectv1.SyncState_SYNC_STATE_ASYNC}, {"q1", connectv1.Role_ROLE_STANDBY, connectv1.SyncState_SYNC_STATE_QUORUM}} {
		s.Members = append(s.Members, &connectv1.Member{Id: m.id, Name: m.id, Role: m.role, SyncState: m.state, Ready: true, Zone: []string{"z1", "z2", "z1", "z2"}[i], Endpoints: map[string]*connectv1.Endpoint{"internal": {Host: m.id + ".test", Port: 5432, ServerName: "app.test"}}})
	}
	return s
}

func TestPoliciesIdentityAndFreshness(t *testing.T) {
	c := testClient()
	now := time.Now()
	s := snapshot(now)
	if err := c.accept(s, now); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		p    Policy
		want string
	}{{Policy{}, "p1"}, {Policy{Role: SyncReplica}, "s1"}, {Policy{Role: AsyncReplica}, "a1"}, {Policy{Role: QuorumReplica}, "q1"}, {Policy{Role: PotentialReplica, Fallback: []Role{Primary}}, "p1"}, {Policy{Role: Replica, PreferZone: "z1"}, "a1"}} {
		got, err := c.Resolve(context.Background(), tt.p)
		if err != nil || got.MemberID != tt.want || !c.Valid(tt.p, got) {
			t.Fatalf("policy %+v => %+v %v", tt.p, got, err)
		}
	}
	target, _ := c.Resolve(context.Background(), Policy{})
	updates, stop := c.Subscribe()
	defer stop()
	<-updates
	refreshed := proto.Clone(s).(*connectv1.Snapshot)
	refreshed.ObservedAt = timestamppb.New(now.Add(time.Second))
	refreshed.ValidUntil = timestamppb.New(now.Add(16 * time.Second))
	if err := c.accept(refreshed, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if !c.Valid(Policy{}, target) {
		t.Fatal("heartbeat invalidated existing connection")
	}
	select {
	case <-updates:
		t.Fatal("heartbeat reset pools")
	default:
	}
	c.mu.Lock()
	c.expireLocked(now.Add(17 * time.Second))
	c.mu.Unlock()
	if c.Valid(Policy{}, target) {
		t.Fatal("expired connection remained eligible")
	}
	fresh := snapshot(now.Add(2 * time.Second))
	if err := c.accept(fresh, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if c.Valid(Policy{}, target) {
		t.Fatal("expiry/recovery reused old connection generation")
	}
	newTarget, _ := c.Resolve(context.Background(), Policy{})
	fresh.Members[0].Id = "p1-replacement"
	fresh.PrimaryId = "p1-replacement"
	if err := c.accept(fresh, time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.Valid(Policy{}, newTarget) {
		t.Fatal("same-address Pod replacement was ignored")
	}
}

func TestLaggingDiscoveryReplicaCannotRollBackRouting(t *testing.T) {
	c := testClient()
	now := time.Now()
	older := snapshot(now)
	newer := snapshot(now.Add(time.Second))
	newer.Members[0].Id = "new-primary"
	newer.PrimaryId = "new-primary"
	if err := c.accept(newer, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	target, _ := c.Resolve(context.Background(), Policy{})
	if err := c.accept(older, now.Add(time.Second)); !errors.Is(err, errOutOfOrder) {
		t.Fatalf("accepted old observation: %v", err)
	}
	if !c.Valid(Policy{}, target) {
		t.Fatal("lagging server invalidated newer routing")
	}
	oldUntil := c.Status().ValidUntil
	newer.ValidUntil = timestamppb.New(now.Add(time.Minute))
	if err := c.accept(newer, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if !c.Status().ValidUntil.Equal(oldUntil) {
		t.Fatal("replayed observation extended freshness")
	}
}

func TestUnrelatedReplicaChangePreservesPrimaryConnection(t *testing.T) {
	c := testClient()
	s := snapshot(time.Now())
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	primary, _ := c.Resolve(context.Background(), Policy{})
	s.Members[1].SyncState = connectv1.SyncState_SYNC_STATE_POTENTIAL
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !c.Valid(Policy{}, primary) {
		t.Fatal("unrelated replica update invalidated healthy primary")
	}
}

func TestConnectionMetadataChangesInvalidateExistingConnections(t *testing.T) {
	c := testClient()
	s := snapshot(time.Now())
	s.Connection = &connectv1.ConnectionParameters{Database: "rent", ServerCaPem: []byte("first CA")}
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	updates, stop := c.Subscribe()
	defer stop()
	<-updates
	for _, mutate := range []func(){
		func() { s.Connection.Database = "rent_data" },
		func() { s.Connection.ServerCaPem = []byte("rotated CA") },
	} {
		previous, err := c.Resolve(context.Background(), Policy{})
		if err != nil {
			t.Fatal(err)
		}
		mutate()
		if err := c.accept(s, time.Now()); err != nil {
			t.Fatal(err)
		}
		if c.Valid(Policy{}, previous) {
			t.Fatal("connection survived changed database/CA metadata")
		}
		current, err := c.Resolve(context.Background(), Policy{})
		if err != nil || current.Connection.Database != s.Connection.Database || current.Connection.ServerCAPEM != string(s.Connection.ServerCaPem) {
			t.Fatalf("new connection metadata = %+v, %v", current.Connection, err)
		}
		select {
		case <-updates:
		default:
			t.Fatal("pool not notified of connection metadata change")
		}
	}
}

func TestAutomaticNetworkTargetsAndEndpointChanges(t *testing.T) {
	c := testClient()
	c.cfg.Network = ""
	s := snapshot(time.Now())
	s.Members[0].Endpoints["external"] = &connectv1.Endpoint{Host: "primary.example.com", Port: 6432, ServerName: "app.test"}
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := c.Resolve(context.Background(), Policy{})
	if err != nil || target.Endpoint.Host != "p1.test" || target.FallbackEndpoint.Host != "primary.example.com" {
		t.Fatalf("automatic target = %+v, %v", target, err)
	}
	s.Members[0].Endpoints["external"].Host = "new-primary.example.com"
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.Valid(Policy{}, target) {
		t.Fatal("changed external endpoint did not invalidate previous target")
	}
	delete(s.Members[0].Endpoints, "internal")
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	target, err = c.Resolve(context.Background(), Policy{})
	if err != nil || target.Endpoint.Host != "new-primary.example.com" || target.FallbackEndpoint.Host != "" {
		t.Fatalf("external-only target = %+v, %v", target, err)
	}
}

func TestUnavailablePolicyWaitsAndRecovers(t *testing.T) {
	c := testClient()
	s := snapshot(time.Now())
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Resolve(ctx, Policy{Role: PotentialReplica}); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	result := make(chan Target, 1)
	go func() { target, _ := c.Resolve(ctx2, Policy{Role: PotentialReplica}); result <- target }()
	s.Members[1].SyncState = connectv1.SyncState_SYNC_STATE_POTENTIAL
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := <-result; got.MemberID != "s1" {
		t.Fatalf("did not recover: %+v", got)
	}
}

func TestMalformedSnapshotsAndNetworks(t *testing.T) {
	for _, mutate := range []func(*connectv1.Snapshot){
		func(s *connectv1.Snapshot) { s.Cluster.Uid = "" }, func(s *connectv1.Snapshot) { s.Cluster.Name = "another" }, func(s *connectv1.Snapshot) { s.ApiVersion = "future" }, func(s *connectv1.Snapshot) { s.PrimaryId = "a1" }, func(s *connectv1.Snapshot) { s.Members[1].Id = "p1" }, func(s *connectv1.Snapshot) { s.Members[1].Endpoints["internal"].Host = "p1.test" }, func(s *connectv1.Snapshot) { s.Members[1].Role = connectv1.Role_ROLE_UNKNOWN }, func(s *connectv1.Snapshot) { s.ValidUntil = nil }, func(s *connectv1.Snapshot) { s.ObservedAt = timestamppb.New(time.Now().Add(time.Hour)) }, func(s *connectv1.Snapshot) { s.Members[0].Endpoints["internal"].Port = 70000 },
	} {
		c := testClient()
		s := snapshot(time.Now())
		mutate(s)
		if err := c.accept(s, time.Now()); !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("accepted malformed snapshot: %v", err)
		}
	}
	c := testClient()
	c.cfg.Network = "external"
	if err := c.accept(snapshot(time.Now()), time.Now()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.Resolve(ctx, Policy{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("cross-network fallback: %v", err)
	}
}

func TestPolicyFallbackAndStatusAreDefensive(t *testing.T) {
	c := testClient()
	s := snapshot(time.Now())
	s.Members[1].Ready = false
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	p := Policy{Role: SyncReplica, Fallback: []Role{Primary}}
	fallback, _ := c.Resolve(context.Background(), p)
	status := c.Status()
	status.Members[0].Endpoint.Host = "attacker.test"
	if !c.Valid(p, fallback) {
		t.Fatal("mutating status changed routing")
	}
	s.Members[1].Ready = true
	if err := c.accept(s, time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.Valid(p, fallback) {
		t.Fatal("fallback persisted after preferred role recovered")
	}
}
