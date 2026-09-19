package cnpgconnectgo

import (
	"testing"
	"time"
)

func TestStatisticsCountFreshnessAndRejectedReplay(t *testing.T) {
	c := testClient()
	now := time.Now()
	acceptSnapshot(t, c, snapshot(now), now)
	if err := c.accept(snapshot(now.Add(-time.Second)), now); err == nil {
		t.Fatal("older positive observation accepted")
	}
	c.mu.Lock()
	c.expireLocked(now.Add(time.Minute))
	c.expireLocked(now.Add(time.Minute))
	c.mu.Unlock()
	stats := c.Statistics()
	if stats.SnapshotsAccepted != 1 || stats.SnapshotsRejected != 1 || stats.SnapshotExpirations != 1 {
		t.Fatalf("incorrect event counts: %+v", stats)
	}
	stats.SnapshotsAccepted = 100
	if c.Statistics().SnapshotsAccepted != 1 {
		t.Fatal("statistics are not a defensive value")
	}
}
