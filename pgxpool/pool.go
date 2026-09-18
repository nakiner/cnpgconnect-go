// Package pgxpool connects native pgx pools to CloudNativePG topology discovery.
// Open returns a stable pool whose new acquisitions follow the selected role.
// Connections already acquired, including transactions, remain owned by their
// callers until release; SQL operations are never replayed by this package.
package pgxpool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
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
	targets             map[*pgconn.PgConn]cnpgconnectgo.Target
	connecting          map[*connectionAttempt]struct{}
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
		targets:             make(map[*pgconn.PgConn]cnpgconnectgo.Target),
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
	if err := p.Pool.Ping(ctx); err != nil {
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
		clear(p.targets)
		p.mu.Unlock()
	})
}

// Valid reports whether conn still belongs to an eligible, unexpired member.
// It performs no network I/O and is suitable for database/sql ResetSession hooks.
// Connections not created by this pool return false.
func (p *Pool) Valid(conn *pgx.Conn) bool {
	if conn == nil || p.ctx.Err() != nil {
		return false
	}
	p.mu.RLock()
	target, ok := p.targets[conn.PgConn()]
	p.mu.RUnlock()
	return ok && p.resolver.Valid(p.policy, target)
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
			// AcquireAllIdle never waits for borrowed connections. Release routes
			// each idle connection through our eligibility check, preserving the
			// healthy subset and retiring obsolete connections in the background.
			for _, conn := range p.Pool.AcquireAllIdle(p.ctx) {
				conn.Release()
			}
		}
	}
}

func (p *Pool) installHooks(cfg *native.Config) {
	beforeConnect := cfg.BeforeConnect
	afterConnect := cfg.AfterConnect
	prepareConn := cfg.PrepareConn
	beforeAcquire := cfg.BeforeAcquire
	afterRelease := cfg.AfterRelease
	beforeClose := cfg.BeforeClose
	shouldPing := cfg.ShouldPing
	cfg.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
		ctx, cancel := p.operationContext(ctx)
		defer cancel()
		if beforeConnect != nil {
			if err := beforeConnect(ctx, cc); err != nil {
				return err
			}
		}
		if cc.ConnectTimeout < 0 {
			return errors.New("cnpgconnect-go/pgxpool: BeforeConnect set a negative ConnectTimeout")
		}
		if cc.ConnectTimeout == 0 {
			cc.ConnectTimeout = 5 * time.Second
		}
		target, err := p.resolver.Resolve(ctx, p.policy)
		if err != nil {
			return fmt.Errorf("cnpgconnect-go/pgxpool: resolve member: %w", err)
		}
		if p.automaticConnection {
			if err := setDiscoveredConnection(cc, target); err != nil {
				return err
			}
		} else {
			setEndpoint(cc, target.Endpoint)
			appendEndpointFallback(cc, target.FallbackEndpoint)
		}
		// pgx's connection tracer supplies the context for the entire startup,
		// including TLS/authentication between DialFunc and ValidateConnect.
		// Keep application tracing while allowing obsolete attempts to stop as
		// soon as discovery changes, freeing their pool slots for the new route.
		tracer := &multitracer.Tracer{}
		if cc.Tracer != nil {
			tracer = multitracer.New(cc.Tracer)
		}
		tracer.ConnectTracers = append(tracer.ConnectTracers, connectionTracer{pool: p, target: target})
		cc.Tracer = tracer
		// pgx's background minimum-pool constructors are not canceled by pool
		// shutdown. Bound DNS/dial and connect hooks as well as the native
		// network startup timeout, and tie cooperative callbacks to our lifetime.
		lookup := cc.LookupFunc
		cc.LookupFunc = func(ctx context.Context, host string) ([]string, error) {
			ctx, cancel := p.connectionContext(ctx, cc.ConnectTimeout)
			defer cancel()
			return lookup(ctx, host)
		}
		dial := cc.DialFunc
		cc.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			ctx, cancel := p.connectionContext(ctx, cc.ConnectTimeout)
			defer cancel()
			return dial(ctx, network, address)
		}
		pgValidateConnect := cc.Config.ValidateConnect
		cc.Config.ValidateConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
			ctx, cancel := p.connectionContext(ctx, cc.ConnectTimeout)
			defer cancel()
			// Validate per address so pgx may try the same member's other
			// advertised endpoint when one leads to an obsolete PostgreSQL role.
			if err := validateRole(ctx, conn, target); err != nil {
				return err
			}
			if pgValidateConnect != nil {
				if err := pgValidateConnect(ctx, conn); err != nil {
					return err
				}
			}
			if !p.resolver.Valid(p.policy, target) {
				return errors.New("cnpgconnect-go/pgxpool: topology changed while connecting")
			}
			return nil
		}
		pgAfterConnect := cc.Config.AfterConnect
		cc.Config.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
			ctx, cancel := p.connectionContext(ctx, cc.ConnectTimeout)
			defer cancel()
			if pgAfterConnect != nil {
				if err := pgAfterConnect(ctx, conn); err != nil {
					return err
				}
			}
			if !p.resolver.Valid(p.policy, target) {
				return errors.New("cnpgconnect-go/pgxpool: topology changed while connecting")
			}
			p.mu.Lock()
			p.targets[conn] = target
			p.mu.Unlock()
			// Hijack bypasses native BeforeClose. Release its identity when the
			// caller eventually closes that connection, or when our pool closes.
			go func() {
				select {
				case <-conn.CleanupDone():
				case <-p.ctx.Done():
				}
				p.mu.Lock()
				delete(p.targets, conn)
				p.mu.Unlock()
			}()
			return nil
		}
		return nil
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) (err error) {
		ctx, cancel := p.connectionContext(ctx, conn.Config().ConnectTimeout)
		defer cancel()
		defer func() {
			// Native pgx closes failed constructors without invoking BeforeClose.
			if err != nil {
				p.forget(conn)
			}
		}()
		if !p.Valid(conn) {
			return errors.New("cnpgconnect-go/pgxpool: topology changed while connecting")
		}
		if afterConnect != nil {
			// Native pgxpool invokes this hook after pgx's ConnectEnd. Keep
			// cooperative session initialization cancellable until the pool
			// constructor finishes, without extending it into borrowed sessions.
			p.mu.RLock()
			target, ok := p.targets[conn.PgConn()]
			p.mu.RUnlock()
			if !ok {
				return errors.New("cnpgconnect-go/pgxpool: connection closed while preparing")
			}
			ctx, attempt := p.beginConnectionAttempt(ctx, target)
			defer p.endConnectionAttempt(attempt)
			if err := afterConnect(ctx, conn); err != nil {
				return err
			}
		}
		if !p.Valid(conn) {
			return errors.New("cnpgconnect-go/pgxpool: topology changed while preparing connection")
		}
		return nil
	}
	cfg.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		if !p.Valid(conn) {
			return false, nil
		}
		if prepareConn != nil {
			ok, err := prepareConn(ctx, conn)
			if !ok || err != nil {
				return ok, err
			}
		} else if beforeAcquire != nil && !beforeAcquire(ctx, conn) {
			return false, nil
		}
		return p.Valid(conn), nil
	}
	cfg.BeforeAcquire = nil // Preserve pgx's PrepareConn-over-BeforeAcquire precedence.
	cfg.AfterRelease = func(conn *pgx.Conn) bool {
		if afterRelease != nil && !afterRelease(conn) {
			return false
		}
		return p.Valid(conn)
	}
	cfg.BeforeClose = func(conn *pgx.Conn) {
		p.forget(conn)
		if beforeClose != nil {
			beforeClose(conn)
		}
	}
	cfg.ShouldPing = func(ctx context.Context, params native.ShouldPingParams) bool {
		// pgx evaluates ShouldPing before PrepareConn. An obsolete endpoint
		// should be discarded without waiting for a network liveness probe.
		if !p.Valid(params.Conn) {
			return false
		}
		if shouldPing != nil {
			return shouldPing(ctx, params)
		}
		return params.IdleDuration > time.Second
	}
}

func (p *Pool) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(p.ctx, cancel)
	if p.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() {
		stop()
		cancel()
	}
}

func (p *Pool) connectionContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, stop := p.operationContext(parent)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	return ctx, func() {
		cancel()
		stop()
	}
}

func (p *Pool) forget(conn *pgx.Conn) {
	p.mu.Lock()
	delete(p.targets, conn.PgConn())
	p.mu.Unlock()
}

func setEndpoint(cfg *pgx.ConnConfig, endpoint cnpgconnectgo.Endpoint) {
	originalHost, originalPort := cfg.Host, cfg.Port
	var fallbacks []*pgconn.FallbackConfig
	for _, fallback := range cfg.Fallbacks {
		// pgx represents sslmode=allow/prefer using another attempt at the
		// same host and port. Keep those transport choices while discarding
		// alternate DSN hosts, which would bypass discovery's selected member.
		if fallback.Host != originalHost || fallback.Port != originalPort {
			continue
		}
		fallbacks = append(fallbacks, &pgconn.FallbackConfig{
			Host:      endpoint.Host,
			Port:      endpoint.Port,
			TLSConfig: endpointTLS(fallback.TLSConfig, endpoint),
		})
	}
	cfg.Host, cfg.Port = endpoint.Host, endpoint.Port
	cfg.Fallbacks = fallbacks
	cfg.TLSConfig = endpointTLS(cfg.TLSConfig, endpoint)
}

func setDiscoveredConnection(cfg *pgx.ConnConfig, target cnpgconnectgo.Target) error {
	if target.Connection.Database == "" {
		return errors.New("cnpgconnect-go/pgxpool: discovery did not provide a database name; upgrade/configure cnpg-connect-plugin")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(target.Connection.ServerCAPEM)) {
		return errors.New("cnpgconnect-go/pgxpool: discovery did not provide a valid PostgreSQL server CA")
	}
	cfg.Database = target.Connection.Database
	cfg.Host, cfg.Port = target.Endpoint.Host, target.Endpoint.Port
	cfg.TLSConfig = endpointTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, target.Endpoint)
	// Never retry over plaintext or an address outside the selected member.
	cfg.Fallbacks = nil
	appendEndpointFallback(cfg, target.FallbackEndpoint)
	return nil
}

func appendEndpointFallback(cfg *pgx.ConnConfig, endpoint cnpgconnectgo.Endpoint) {
	if endpoint.Host == "" {
		return
	}
	transports := append([]*pgconn.FallbackConfig(nil), cfg.Fallbacks...)
	cfg.Fallbacks = append(cfg.Fallbacks, &pgconn.FallbackConfig{
		Host: endpoint.Host, Port: endpoint.Port, TLSConfig: endpointTLS(cfg.TLSConfig, endpoint),
	})
	// Legacy explicit TLS modes retain their transport alternatives, always
	// directed at this member. Automatic connections have verified TLS only.
	for _, transport := range transports {
		cfg.Fallbacks = append(cfg.Fallbacks, &pgconn.FallbackConfig{
			Host: endpoint.Host, Port: endpoint.Port, TLSConfig: endpointTLS(transport.TLSConfig, endpoint),
		})
	}
}

func endpointTLS(config *tls.Config, endpoint cnpgconnectgo.Endpoint) *tls.Config {
	if config == nil {
		return nil
	}
	config = config.Clone()
	config.ServerName = endpoint.ServerName
	if config.ServerName == "" {
		config.ServerName = endpoint.Host
	}
	return config
}

func validateRole(ctx context.Context, conn *pgconn.PgConn, target cnpgconnectgo.Target) error {
	results, err := conn.Exec(ctx, "SELECT pg_is_in_recovery(), current_setting('transaction_read_only')").ReadAll()
	if err != nil {
		return fmt.Errorf("cnpgconnect-go/pgxpool: verify PostgreSQL role: %w", err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 2 {
		return errors.New("cnpgconnect-go/pgxpool: invalid PostgreSQL role response")
	}
	row := results[0].Rows[0]
	recovery, readOnly := string(row[0]), string(row[1])
	switch target.Role {
	case cnpgconnectgo.Primary:
		if recovery == "f" && readOnly == "off" {
			return nil
		}
	case cnpgconnectgo.Replica:
		if recovery == "t" {
			return nil
		}
	}
	return fmt.Errorf("cnpgconnect-go/pgxpool: member %q no longer has the discovered PostgreSQL role %q", target.Name, target.Role)
}
