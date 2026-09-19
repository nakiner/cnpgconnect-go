package cnpgconnectgo

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strings"
	"time"

	connectv1 "github.com/nakiner/cnpg-connect-plugin/api/connect/v1"
)

func (c *Client) Resolve(ctx context.Context, p Policy) (Target, error) {
	if err := p.Validate(); err != nil {
		return Target{}, err
	}
	p.Fallback = append([]Role(nil), p.Fallback...)
	for {
		if err := ctx.Err(); err != nil {
			return Target{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		c.mu.Lock()
		c.expireLocked(time.Now())
		if c.closed {
			c.mu.Unlock()
			return Target{}, ErrClosed
		}
		role, local, count := c.selectionLocked(p)
		if count > 0 {
			// Independent random selection avoids coupling different policies'
			// traffic through one shared round-robin counter.
			index := rand.IntN(count)
			for _, target := range c.state.Members {
				if !matches(role, target) || (local && target.Zone != p.PreferZone) {
					continue
				}
				if index == 0 {
					c.mu.Unlock()
					return target, nil
				}
				index--
			}
		}
		ch := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return Target{}, fmt.Errorf("%w: %w", ErrUnavailable, ctx.Err())
		case <-ch:
		}
	}
}

func (c *Client) Valid(p Policy, target Target) bool {
	if p.Validate() != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(time.Now())
	if c.closed {
		return false
	}
	role, local, count := c.selectionLocked(p)
	if count == 0 || !matches(role, target) || (local && target.Zone != p.PreferZone) {
		return false
	}
	for _, m := range c.state.Members {
		if m == target {
			return true
		}
	}
	return false
}

// selectionLocked identifies the winning role/zone without materializing a
// candidate slice. Both selection and eligibility use exactly the same policy.
func (c *Client) selectionLocked(p Policy) (role Role, local bool, count int) {
	if !c.state.Ready {
		return
	}
	for i := -1; i < len(p.Fallback); i++ {
		role = p.Role
		if i >= 0 {
			role = p.Fallback[i]
		}
		zoneCount := 0
		for _, m := range c.state.Members {
			if matches(role, m) {
				count++
				if p.PreferZone != "" && m.Zone == p.PreferZone {
					zoneCount++
				}
			}
		}
		if count == 0 {
			continue
		}
		if zoneCount > 0 {
			return role, true, zoneCount
		}
		return role, false, count
	}
	return
}

func matches(role Role, m Target) bool {
	switch role {
	case "", Primary:
		return m.Role == Primary
	case Replica:
		return m.Role == Replica
	case SyncReplica:
		return m.Role == Replica && m.SyncState == Sync
	case AsyncReplica:
		return m.Role == Replica && m.SyncState == Async
	case QuorumReplica:
		return m.Role == Replica && m.SyncState == Quorum
	case PotentialReplica:
		return m.Role == Replica && m.SyncState == Potential
	case Any:
		return m.Role == Primary || m.Role == Replica
	default:
		return false
	}
}

func (c *Client) accept(s *connectv1.Snapshot, now time.Time) (acceptErr error) {
	defer func() {
		if acceptErr != nil {
			c.counters.rejected.Add(1)
		} else {
			c.counters.accepted.Add(1)
		}
	}()
	members, until, err := c.validateSnapshot(s, now)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	at := s.ObservedAt.AsTime()
	withdrawal := !s.Available || s.Transitioning
	// Negative evidence for the current incarnation is authoritative even from
	// a lagging observer. Keep the positive-observation high-water mark: an old
	// or equal positive replay must not resurrect a withdrawn route.
	if at.Before(c.state.ObservedAt) {
		if s.Cluster.Uid != c.clusterUID {
			return errOutOfOrder
		}
		if !withdrawal {
			return c.acceptOlderWithdrawalsLocked(s, members, now)
		}
	}
	if !withdrawal && c.withdrawn && !at.After(c.state.ObservedAt) {
		return errOutOfOrder
	}
	if at.Equal(c.state.ObservedAt) && s.Cluster.Uid == c.clusterUID {
		// Remember withdrawals of previously eligible identities. Metadata
		// changes for an eligible member still invalidate its old generation,
		// but cannot undo a readiness/removal withdrawal within this sample.
		for _, previous := range c.state.Members {
			present := false
			for _, member := range members {
				if previous.MemberID == member.MemberID {
					present = true
					break
				}
			}
			if !present {
				delete(c.observationMembers, previous.MemberID)
			}
		}
		retained := members[:0]
		for _, member := range members {
			if _, allowed := c.observationMembers[member.MemberID]; allowed {
				retained = append(retained, member)
			}
		}
		members = retained
	} else if at.After(c.state.ObservedAt) || s.Cluster.Uid != c.clusterUID {
		// Bound retained identities to one complete observation. New identities
		// and recovery of withdrawn members require a newer positive sample.
		// Seed only validated eligible members: withdrawal may be the first
		// evidence received for an identity in this observation epoch.
		c.observationMembers = make(map[string]struct{}, len(members))
		for _, member := range members {
			c.observationMembers[member.MemberID] = struct{}{}
		}
	}
	if !at.After(c.state.ObservedAt) {
		// A replayed observation cannot extend its original freshness lifetime.
		until = minTime(until, c.state.ValidUntil)
		at = c.state.ObservedAt
	}
	c.expireLocked(now)
	ready := s.Available && !s.Transitioning && now.Before(until)
	// Identity/generation belongs to this process, never to an opaque server
	// revision. Full snapshots after reconnect can legitimately rewind revision.
	changed := ready != c.state.Ready || !slices.EqualFunc(c.state.Members, members, sameMember)
	wasReady := c.state.Ready
	if changed {
		c.generation++
	}
	for i := range members {
		members[i].Generation = c.generation
		// An unrelated replica update need not reconnect a healthy primary.
		// Expiry/unavailability, however, invalidates every previous generation.
		if wasReady && ready {
			for _, previous := range c.state.Members {
				if sameMember(members[i], previous) {
					members[i].Generation = previous.Generation
					break
				}
			}
		}
	}
	c.state.Ready = ready
	c.state.Connected = true
	c.state.Revision = s.Revision
	c.state.ObservedAt = at
	c.clusterUID = s.Cluster.Uid
	c.withdrawn = withdrawal
	c.state.ValidUntil = until
	c.state.Members = members
	c.state.LastError = nil
	if !ready {
		c.state.LastError = ErrUnavailable
	}
	if changed {
		c.signalLocked()
	} else {
		c.signalWaitersLocked()
	}
	return nil
}

// A lagging observer can learn that a member is no longer eligible after its
// last complete observation. Preserve this negative evidence without accepting
// any of the older snapshot's positive routing data or freshness. An absent
// member alone is not evidence: the older observation may predate its creation.
func (c *Client) acceptOlderWithdrawalsLocked(s *connectv1.Snapshot, eligible []Target, now time.Time) error {
	eligibleIDs := make(map[string]struct{}, len(eligible))
	for _, member := range eligible {
		eligibleIDs[member.MemberID] = struct{}{}
	}
	withdrawnIDs := make(map[string]struct{})
	for _, member := range s.Members {
		if _, ok := eligibleIDs[member.Id]; !ok {
			withdrawnIDs[member.Id] = struct{}{}
		}
	}

	retained := make([]Target, 0, len(c.state.Members))
	primaryWithdrawn := false
	for _, member := range c.state.Members {
		if _, withdrawn := withdrawnIDs[member.MemberID]; withdrawn {
			delete(c.observationMembers, member.MemberID)
			primaryWithdrawn = primaryWithdrawn || member.Role == Primary
			continue
		}
		retained = append(retained, member)
	}
	if len(retained) == len(c.state.Members) {
		return errOutOfOrder
	}

	c.expireLocked(now)
	c.state.Members = retained
	c.state.Connected = true
	c.state.Revision = s.Revision
	c.generation++
	if primaryWithdrawn {
		c.state.Ready = false
		c.withdrawn = true
	}
	c.state.LastError = nil
	if !c.state.Ready {
		c.state.LastError = ErrUnavailable
	}
	c.signalLocked()
	return nil
}

func (c *Client) validateSnapshot(s *connectv1.Snapshot, now time.Time) ([]Target, time.Time, error) {
	bad := func(reason string) ([]Target, time.Time, error) {
		return nil, time.Time{}, fmt.Errorf("%w: %s", ErrInvalidSnapshot, reason)
	}
	if s.ApiVersion != "connect.cnpg.io/v1alpha1" || s.Cluster == nil || s.Cluster.Namespace != c.cfg.Namespace || s.Cluster.Name != c.cfg.Cluster || s.Cluster.Uid == "" || s.Revision == "" {
		return bad("cluster identity or API version mismatch")
	}
	if s.ObservedAt == nil || s.ValidUntil == nil || s.ObservedAt.CheckValid() != nil || s.ValidUntil.CheckValid() != nil {
		return bad("invalid timestamps")
	}
	at, until := s.ObservedAt.AsTime(), s.ValidUntil.AsTime()
	if at.After(now.Add(5*time.Second)) || !until.After(at) {
		return bad("invalid observation lifetime or clock skew")
	}
	until = minTime(until, at.Add(c.cfg.MaxSnapshotTTL))
	ids := map[string]bool{}
	names := map[string]bool{}
	endpoints := map[string]string{}
	var result []Target
	primaries := 0
	for _, m := range s.Members {
		if m == nil || m.Id == "" || m.Name == "" || ids[m.Id] || names[m.Name] {
			return bad("missing or duplicate member identity")
		}
		ids[m.Id] = true
		names[m.Name] = true
		if !m.Ready {
			continue
		}
		t := Target{
			ClusterUID: s.Cluster.Uid, MemberID: m.Id, Name: m.Name,
			Zone: m.Zone, Region: m.Region, SyncState: SyncUnknown,
			Connection: ConnectionParameters{
				Database:    s.GetConnection().GetDatabase(),
				ServerCAPEM: string(s.GetConnection().GetServerCaPem()),
			},
		}
		switch m.Role {
		case connectv1.Role_ROLE_PRIMARY:
			primaries++
			if m.Id != s.PrimaryId {
				return bad("primary identity mismatch")
			}
			t.Role = Primary
		case connectv1.Role_ROLE_STANDBY:
			t.Role = Replica
			switch m.SyncState {
			case connectv1.SyncState_SYNC_STATE_SYNC:
				t.SyncState = Sync
			case connectv1.SyncState_SYNC_STATE_ASYNC:
				t.SyncState = Async
			case connectv1.SyncState_SYNC_STATE_QUORUM:
				t.SyncState = Quorum
			case connectv1.SyncState_SYNC_STATE_POTENTIAL:
				t.SyncState = Potential
			default:
				return bad("eligible replica has unknown sync state")
			}
		default:
			return bad("eligible member has unknown role")
		}
		networks := []string{c.cfg.Network}
		if c.cfg.Network == "" {
			networks = []string{"internal", "external"}
		}
		for _, network := range networks {
			ep, ok := m.Endpoints[network]
			if !ok {
				continue
			}
			if ep == nil || ep.Host == "" || ep.Port == 0 || ep.Port > 65535 || strings.ContainsAny(ep.Host, " /\\\t\r\n") || (strings.Contains(ep.Host, ":") && net.ParseIP(ep.Host) == nil) || strings.ContainsAny(ep.ServerName, " /\\\t\r\n") {
				return bad("invalid endpoint")
			}
			address := net.JoinHostPort(strings.ToLower(ep.Host), fmt.Sprint(ep.Port))
			if owner, exists := endpoints[address]; exists && owner != m.Id {
				return bad("multiple members share an endpoint")
			}
			endpoints[address] = m.Id
			endpoint := Endpoint{Host: ep.Host, Port: uint16(ep.Port), ServerName: ep.ServerName}
			if t.Endpoint.Host == "" {
				t.Endpoint = endpoint
			} else if endpoint != t.Endpoint {
				t.FallbackEndpoint = endpoint
			}
		}
		if t.Endpoint.Host == "" {
			continue
		}
		result = append(result, t)
	}
	if s.Available && !s.Transitioning && (primaries != 1 || s.PrimaryId == "") {
		return bad("available topology lacks exactly one eligible primary")
	}
	if !s.Available || s.Transitioning {
		result = nil
	}
	slices.SortFunc(result, func(a, b Target) int { return strings.Compare(a.MemberID, b.MemberID) })
	return result, until, nil
}

// Discovery describes a member; the client assigns its local connection generation.
func sameMember(a, b Target) bool {
	a.Generation, b.Generation = 0, 0
	return a == b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
