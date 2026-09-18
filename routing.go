package cnpgconnectgo

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"reflect"
	"sort"
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
		members := c.candidatesLocked(p)
		if len(members) > 0 {
			// Independent random selection avoids coupling different policies'
			// traffic through one shared round-robin counter.
			target := members[rand.IntN(len(members))]
			c.mu.Unlock()
			return target, nil
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
	for _, m := range c.candidatesLocked(p) {
		if m == target {
			return true
		}
	}
	return false
}

func (c *Client) candidatesLocked(p Policy) []Target {
	if !c.state.Ready {
		return nil
	}
	for _, role := range append([]Role{p.Role}, p.Fallback...) {
		var members []Target
		for _, m := range c.state.Members {
			if matches(role, m) {
				members = append(members, m)
			}
		}
		if len(members) == 0 {
			continue
		}
		if p.PreferZone != "" {
			var local []Target
			for _, m := range members {
				if m.Zone == p.PreferZone {
					local = append(local, m)
				}
			}
			if len(local) > 0 {
				return local
			}
		}
		return members
	}
	return nil
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

func (c *Client) accept(s *connectv1.Snapshot, now time.Time) error {
	members, until, err := c.validateSnapshot(s, now)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if s.ObservedAt.AsTime().Before(c.state.ObservedAt) {
		return errOutOfOrder
	}
	if s.ObservedAt.AsTime().Equal(c.state.ObservedAt) {
		// A replayed observation cannot extend its original freshness lifetime.
		until = minTime(until, c.state.ValidUntil)
	}
	c.expireLocked(now)
	ready := s.Available && !s.Transitioning && now.Before(until)
	// Identity/generation belongs to this process, never to an opaque server
	// revision. Full snapshots after reconnect can legitimately rewind revision.
	old := append([]Target(nil), c.state.Members...)
	for i := range old {
		old[i].Generation = 0
	}
	changed := ready != c.state.Ready || !reflect.DeepEqual(old, members)
	wasReady := c.state.Ready
	if changed {
		c.generation++
	}
	for i := range members {
		members[i].Generation = c.generation
		// An unrelated replica update need not reconnect a healthy primary.
		// Expiry/unavailability, however, invalidates every previous generation.
		if wasReady && ready {
			for j, previous := range old {
				candidate := members[i]
				candidate.Generation = 0
				if candidate == previous {
					members[i].Generation = c.state.Members[j].Generation
					break
				}
			}
		}
	}
	c.state.Ready = ready
	c.state.Connected = true
	c.state.Revision = s.Revision
	c.state.ObservedAt = s.ObservedAt.AsTime()
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
	endpoints := map[string]bool{}
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
		t := Target{ClusterUID: s.Cluster.Uid, MemberID: m.Id, Name: m.Name, Zone: m.Zone, Region: m.Region, SyncState: SyncUnknown}
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
		ep, ok := m.Endpoints[c.cfg.Network]
		if !ok {
			continue
		} // Never cross networks implicitly.
		if ep == nil || ep.Host == "" || ep.Port == 0 || ep.Port > 65535 || strings.ContainsAny(ep.Host, " /\\\t\r\n") || (strings.Contains(ep.Host, ":") && net.ParseIP(ep.Host) == nil) || strings.ContainsAny(ep.ServerName, " /\\\t\r\n") {
			return bad("invalid endpoint")
		}
		address := net.JoinHostPort(strings.ToLower(ep.Host), fmt.Sprint(ep.Port))
		if endpoints[address] {
			return bad("multiple members share an endpoint")
		}
		endpoints[address] = true
		t.Endpoint = Endpoint{Host: ep.Host, Port: uint16(ep.Port), ServerName: ep.ServerName}
		result = append(result, t)
	}
	if s.Available && !s.Transitioning && (primaries != 1 || s.PrimaryId == "") {
		return bad("available topology lacks exactly one eligible primary")
	}
	if !s.Available || s.Transitioning {
		result = nil
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MemberID < result[j].MemberID })
	return result, until, nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
