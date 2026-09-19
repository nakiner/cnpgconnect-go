package cnpgconnectgo

import (
	"context"
	"testing"
	"time"
)

func TestValidDoesNotAllocate(t *testing.T) {
	c := testClient()
	acceptSnapshot(t, c, snapshot(time.Now()), time.Now())
	for _, p := range []Policy{{}, {Role: Replica}, {Role: Replica, PreferZone: "z1", Fallback: []Role{Primary}}, {Role: PotentialReplica, PreferZone: "z2", Fallback: []Role{AsyncReplica, Replica, Primary}}, {Role: Any, PreferZone: "absent"}} {
		target := resolveTarget(t, c, p)
		allocs := testing.AllocsPerRun(100, func() {
			if !c.Valid(p, target) {
				t.Fatal("selected target invalid")
			}
		})
		if allocs != 0 {
			t.Errorf("policy %+v: %v allocations/check, want zero", p, allocs)
		}
	}
}

func BenchmarkValid(b *testing.B) {
	c := testClient()
	s := snapshot(time.Now())
	if err := c.accept(s, time.Now()); err != nil {
		b.Fatal(err)
	}
	c.state.ValidUntil = time.Now().Add(time.Hour)
	for _, tt := range []struct {
		name string
		p    Policy
	}{{"primary", Policy{}}, {"replica_zone_fallback", Policy{Role: Replica, PreferZone: "z1", Fallback: []Role{Primary}}}} {
		target, err := c.Resolve(context.Background(), tt.p)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if !c.Valid(tt.p, target) {
					b.Fatal("invalid")
				}
			}
		})
		b.Run(tt.name+"/parallel", func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if !c.Valid(tt.p, target) {
						b.Error("invalid")
						return
					}
				}
			})
		})
	}
}
