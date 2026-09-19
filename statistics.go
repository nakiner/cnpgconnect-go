package cnpgconnectgo

import "sync/atomic"

// Statistics contains process-local, monotonic discovery event counts. It has
// no cluster/member labels or monitoring dependency. Export deltas or counters
// through the application's existing instrumentation. WatchAttempts includes
// the initial attempt; later attempts are reconnects. Expirations count only
// transitions from a usable view, not repeated checks of an expired view.
type Statistics struct {
	WatchAttempts       uint64
	WatchTimeouts       uint64
	SnapshotsAccepted   uint64
	SnapshotsRejected   uint64
	SnapshotExpirations uint64
}

type clientCounters struct {
	watchAttempts atomic.Uint64
	watchTimeouts atomic.Uint64
	accepted      atomic.Uint64
	rejected      atomic.Uint64
	expirations   atomic.Uint64
}

// Statistics returns a race-safe value snapshot of this client's counters.
// Individual counters are atomic; the group is not a transactional snapshot.
func (c *Client) Statistics() Statistics {
	return Statistics{
		WatchAttempts:       c.counters.watchAttempts.Load(),
		WatchTimeouts:       c.counters.watchTimeouts.Load(),
		SnapshotsAccepted:   c.counters.accepted.Load(),
		SnapshotsRejected:   c.counters.rejected.Load(),
		SnapshotExpirations: c.counters.expirations.Load(),
	}
}
