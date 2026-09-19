package pgxpool

import (
	"fmt"
	"github.com/nakiner/cnpgconnect-go"
	"testing"
	"time"
)

func TestNetworkPreferenceLookupPortOverrides(t *testing.T) {
	for _, tt := range []struct{ answer, dial string }{
		{"127.0.0.1:6432", "127.0.0.1:6432"},
		{"[2001:db8::1]:6432", "[2001:db8::1]:6432"},
		{"2001:db8::1", "[2001:db8::1]:5432"},
		{"127.0.0.1", "127.0.0.1:5432"},
	} {
		t.Run(tt.answer, func(t *testing.T) {
			paths := &dialPaths{endpoints: [2]cnpgconnectgo.Endpoint{{Host: "internal.test", Port: 5432}, {Host: "external.test", Port: 5432}}}
			paths.resolved("external.test", []string{tt.answer})
			paths.dialed(tt.dial)
			if got := paths.chosen(); got != 2 {
				t.Fatalf("external path=%d, want 2", got)
			}
		})
	}
}

func TestNetworkPreferenceBoundsAndReprobe(t *testing.T) {
	p := new(Pool)
	now := time.Now()
	target := cnpgconnectgo.Target{ClusterUID: "cluster", MemberID: "member", Generation: 1, Endpoint: cnpgconnectgo.Endpoint{Host: "internal", Port: 5432}, FallbackEndpoint: cnpgconnectgo.Endpoint{Host: "external", Port: 5432}}
	p.rememberPath(target, 2, now)
	if p.preferredTarget(target, now).Endpoint != target.FallbackEndpoint {
		t.Fatal("successful fallback not preferred")
	}
	p.rememberPath(target, 2, now.Add(networkPreferenceTTL/2))
	if p.preferredTarget(target, now.Add(networkPreferenceTTL)).Endpoint != target.Endpoint {
		t.Fatal("success extended re-probe deadline")
	}
	for i := range maxNetworkPreferences + 10 {
		next := target
		next.MemberID = fmt.Sprint(i)
		p.rememberPath(next, 2, now)
	}
	if len(p.preferences) > maxNetworkPreferences {
		t.Fatal("unbounded preference cache")
	}
	ambiguous := target
	ambiguous.MemberID = "ambiguous"
	p.rememberPath(ambiguous, 3, now)
	if p.preferredTarget(ambiguous, now) != ambiguous {
		t.Fatal("ambiguous address taught preference")
	}
	paths := &dialPaths{endpoints: [2]cnpgconnectgo.Endpoint{target.Endpoint, target.FallbackEndpoint}}
	paths.resolved("internal", []string{"127.0.0.1"})
	paths.resolved("external", []string{"127.0.0.1"})
	paths.dialed("127.0.0.1:5432")
	if paths.chosen() != 3 {
		t.Fatal("shared addresses should be ambiguous")
	}
}
