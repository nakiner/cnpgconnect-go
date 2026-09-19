// Package cnpgconnectgo discovers CloudNativePG members and supplies a shared,
// expiring routing view to database adapters. It does not execute or replay SQL.
package cnpgconnectgo

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Role selects a routing class. A zero Role means Primary.
type Role string

const (
	Primary          Role = "primary"
	Replica          Role = "replica"
	SyncReplica      Role = "sync"
	AsyncReplica     Role = "async"
	QuorumReplica    Role = "quorum"
	PotentialReplica Role = "potential"
	Any              Role = "any"
)

type SyncState string

const (
	SyncUnknown SyncState = "unknown"
	Sync        SyncState = "sync"
	Async       SyncState = "async"
	Quorum      SyncState = "quorum"
	Potential   SyncState = "potential"
)

// Policy chooses the first nonempty routing class, then optionally prefers a
// zone within that class. Fallback is explicit; strict reads never use primary
// unless it is listed. Quorum members are distinct from priority SyncReplica.
type Policy struct {
	Role       Role
	Fallback   []Role
	PreferZone string
}

func (p Policy) Validate() error {
	for i := -1; i < len(p.Fallback); i++ {
		role := p.Role
		if i >= 0 {
			role = p.Fallback[i]
		}
		switch role {
		case "", Primary, Replica, SyncReplica, AsyncReplica, QuorumReplica, PotentialReplica, Any:
		default:
			return fmt.Errorf("cnpgconnect-go: invalid routing role %q", role)
		}
	}
	return nil
}

type Endpoint struct {
	Host       string
	Port       uint16
	ServerName string
}

// ConnectionParameters contains public PostgreSQL connection settings supplied
// by discovery. Credentials belong to the application and never enter discovery.
type ConnectionParameters struct {
	Database    string
	ServerCAPEM string
}

// Target is a connection's immutable identity. Generation changes when this
// member changes or the routing view is invalidated, including an expiry followed
// by identical recovery. Unrelated member changes preserve this generation.
type Target struct {
	ClusterUID string
	MemberID   string
	Name       string
	Endpoint   Endpoint
	// FallbackEndpoint is another address for this same member. It is populated
	// for automatic network selection and remains empty for an explicit Network.
	FallbackEndpoint Endpoint
	Connection       ConnectionParameters
	Role             Role
	SyncState        SyncState
	Zone             string
	Region           string
	Generation       uint64
}

// Resolver lets adapters share a discovery client or use another implementation.
// Resolve waits for an eligible route until ctx expires. Valid must fail closed
// on stale data. Subscribe coalesces routing changes (not heartbeat refreshes);
// its cancellation function releases the subscription and is idempotent.
type Resolver interface {
	Resolve(context.Context, Policy) (Target, error)
	Valid(Policy, Target) bool
	Subscribe() (<-chan struct{}, func())
}

var (
	ErrClosed          = errors.New("cnpgconnect-go: closed")
	ErrUnavailable     = errors.New("cnpgconnect-go: no fresh eligible database member")
	ErrInvalidSnapshot = errors.New("cnpgconnect-go: invalid discovery snapshot")
)

// Status is an immutable diagnostic view. Ready means discovery currently has
// a fresh, available snapshot; an individual policy may still have no candidate.
type Status struct {
	Ready      bool
	Connected  bool
	Revision   string
	ObservedAt time.Time
	ValidUntil time.Time
	LastError  error
	Members    []Target
}
