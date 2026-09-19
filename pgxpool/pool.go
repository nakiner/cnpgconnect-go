// Package pgxpool connects native pgx pools to CloudNativePG topology discovery.
// Open returns a stable pool whose new acquisitions follow the selected role.
// Connections already acquired, including transactions, remain owned by their
// callers until release; SQL operations are never replayed by this package.
package pgxpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	native "github.com/jackc/pgx/v5/pgxpool"
	"github.com/nakiner/cnpgconnect-go"
)

// Config connects using a discovery address, cluster identity, and PostgreSQL
// credentials. Discovery supplies the database name, endpoints, and server CA.
// The zero policy follows the primary. stdlib.Config is the same type.
type Config struct {
	Address   string
	Namespace string
	Cluster   string
	Username  string
	Password  string

	// Discovery optionally customizes transport, network, and stream settings.
	// Address, Namespace, and Cluster above take precedence when provided.
	Discovery cnpgconnectgo.Config
	// Resolver optionally shares an existing discovery client across pools. The
	// caller owns this resolver and must close it after all its pools are closed.
	Resolver cnpgconnectgo.Resolver
	Policy   cnpgconnectgo.Policy
	// ConnConfig optionally supplies tracing, hooks, runtime parameters, and pool
	// sizing. It must originate from pgxpool.ParseConfig. With Username set,
	// credentials and discovered database/TLS settings override its values.
	// For legacy configurations with no Username, ConnConfig also supplies the
	// credentials, database, and TLS policy; discovery replaces only endpoints.
	// A zero ConnectTimeout uses 5 seconds; negative timeouts are invalid.
	ConnConfig *native.Config
	// StartupTimeout bounds initial connectivity checks when ctx has no deadline.
	// Zero uses 30 seconds. A negative value is invalid.
	StartupTimeout time.Duration
}

// Pool embeds a native pgx pool, preserving its query, transaction, batch, COPY,
// and instrumentation interfaces. Close also stops discovery owned by this pool.
type Pool struct {
	*native.Pool
	resolver            cnpgconnectgo.Resolver
	policy              cnpgconnectgo.Policy
	owned               *cnpgconnectgo.Client
	ctx                 context.Context
	cancel              context.CancelFunc
	stop                func()
	done                chan struct{}
	close               sync.Once
	mu                  sync.RWMutex
	routes              map[cnpgconnectgo.Target]struct{}
	connecting          map[*connectionAttempt]struct{}
	preferences         map[cnpgconnectgo.Target]time.Time
	automaticConnection bool
}

// Open waits for a usable topology and a PostgreSQL connection. Cancellation of
// ctx bounds startup only; call Close to stop a successfully opened pool.
func Open(ctx context.Context, cfg Config) (*Pool, error) {
	automaticConnection := cfg.Username != "" || cfg.ConnConfig == nil
	if automaticConnection && cfg.Username == "" {
		return nil, errors.New("cnpgconnect-go/pgxpool: Username is required")
	}
	if cfg.ConnConfig == nil {
		// ParseConfig initializes pgx's hooks and pool defaults. TLS is installed
		// from authenticated discovery before every connection is established.
		var err error
		cfg.ConnConfig, err = native.ParseConfig("sslmode=disable")
		if err != nil {
			return nil, fmt.Errorf("cnpgconnect-go/pgxpool: initialize connection: %w", err)
		}
	}
	if cfg.ConnConfig.ConnConfig == nil {
		return nil, errors.New("cnpgconnect-go/pgxpool: ConnConfig must originate from pgxpool.ParseConfig")
	}
	if cfg.StartupTimeout < 0 {
		return nil, errors.New("cnpgconnect-go/pgxpool: StartupTimeout must not be negative")
	}
	if cfg.ConnConfig.ConnConfig.ConnectTimeout < 0 {
		return nil, errors.New("cnpgconnect-go/pgxpool: ConnectTimeout must not be negative")
	}
	if err := cfg.Policy.Validate(); err != nil {
		return nil, err
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 30 * time.Second
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.StartupTimeout)
		defer cancel()
	}
	lifetime, cancel := context.WithCancel(context.Background())
	p := &Pool{
		resolver:            cfg.Resolver,
		policy:              cfg.Policy,
		ctx:                 lifetime,
		cancel:              cancel,
		done:                make(chan struct{}),
		routes:              make(map[cnpgconnectgo.Target]struct{}),
		automaticConnection: automaticConnection,
	}
	p.policy.Fallback = append([]cnpgconnectgo.Role(nil), cfg.Policy.Fallback...)
	if p.resolver == nil {
		if cfg.Address != "" {
			cfg.Discovery.Address = cfg.Address
		}
		if cfg.Namespace != "" {
			cfg.Discovery.Namespace = cfg.Namespace
		}
		if cfg.Cluster != "" {
			cfg.Discovery.Cluster = cfg.Cluster
		}
		client, err := cnpgconnectgo.New(ctx, cfg.Discovery)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("cnpgconnect-go/pgxpool: discovery: %w", err)
		}
		p.owned, p.resolver = client, client
	}
	poolConfig := cfg.ConnConfig.Copy()
	if automaticConnection {
		poolConfig.ConnConfig.User = cfg.Username
		poolConfig.ConnConfig.Password = cfg.Password
	}
	if poolConfig.ConnConfig.ConnectTimeout == 0 {
		poolConfig.ConnConfig.ConnectTimeout = 5 * time.Second
	}
	p.installHooks(poolConfig)
	updates, unsubscribe := p.resolver.Subscribe()
	p.stop = unsubscribe
	var err error
	p.Pool, err = native.NewWithConfig(lifetime, poolConfig)
	if err != nil {
		cancel()
		unsubscribe()
		if p.owned != nil {
			_ = p.owned.Close()
		}
		return nil, fmt.Errorf("cnpgconnect-go/pgxpool: create pool: %w", err)
	}
	go p.watch(updates)
	if err := p.pingStartup(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("cnpgconnect-go/pgxpool: initial connection: %w", err)
	}
	return p, nil
}

// Close stops discovery and closes the native pool. Like pgxpool.Pool.Close, it
// waits for callers to release acquired connections and finish transactions.
// Shared resolvers are left open. Close is safe to call more than once.
func (p *Pool) Close() {
	p.close.Do(func() {
		p.cancel()
		p.stop()
		if p.owned != nil {
			_ = p.owned.Close()
		}
		<-p.done
		p.Pool.Close()
		p.mu.Lock()
		clear(p.routes)
		clear(p.preferences)
		p.mu.Unlock()
	})
}

// Valid reports whether conn still belongs to an eligible, unexpired member.
// It performs no network I/O and is suitable for database/sql ResetSession hooks.
// Connections not created by this pool return false. Like other pgx connection
// methods, call Valid only while owning conn, without concurrent connection use.
func (p *Pool) Valid(conn *pgx.Conn) bool {
	if conn == nil || p.ctx.Err() != nil || conn.IsClosed() {
		return false
	}
	identity, ok := conn.PgConn().CustomData()[connectionIdentityKey].(connectionIdentity)
	return ok && identity.pool == p && p.resolver.Valid(p.policy, identity.target)
}

func (p *Pool) watch(updates <-chan struct{}) {
	defer close(p.done)
	for {
		select {
		case <-p.ctx.Done():
			return
		case _, ok := <-updates:
			if !ok {
				return
			}
			p.cancelObsoleteConnections()
			p.mu.Lock()
			reset := false
			for target := range p.routes {
				if !p.resolver.Valid(p.policy, target) {
					delete(p.routes, target)
					reset = true
				}
			}
			if reset {
				// pgx retires idle connections and closes borrowed ones on
				// release. Keep valid routes: their constructors may finish
				// after Reset and must still be tracked for future changes.
				p.Pool.Reset()
			}
			p.mu.Unlock()
		}
	}
}

const connectionIdentityKey = "github.com/nakiner/cnpgconnect-go/pgxpool.identity"

// Immutable after construction; pgx owns its lifetime, including Hijack.
type connectionIdentity struct {
	pool   *Pool
	target cnpgconnectgo.Target
}
