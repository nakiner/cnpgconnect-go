package pgxpool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nakiner/cnpgconnect-go"
)

// Each constructor selects one current member. pgx retains responsibility for
// DNS, dialing, TLS, authentication, and trying that member's address fallbacks.
func (p *Pool) configureConnection(ctx context.Context, cc *pgx.ConnConfig) error {
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
	connectionTarget := p.preferredTarget(target, time.Now())
	paths := &dialPaths{endpoints: [2]cnpgconnectgo.Endpoint{target.Endpoint, target.FallbackEndpoint}}
	if p.automaticConnection {
		if err := setDiscoveredConnection(cc, connectionTarget); err != nil {
			return err
		}
	} else {
		setEndpoint(cc, connectionTarget.Endpoint)
		appendEndpointFallback(cc, connectionTarget.FallbackEndpoint)
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
		addresses, err := lookup(ctx, host)
		if err == nil {
			paths.resolved(host, addresses)
		} else if connectionTarget.FallbackEndpoint.Host != "" && connectionTarget.FallbackEndpoint.Host != connectionTarget.Endpoint.Host {
			err = &optionalLookupError{err}
		}
		return addresses, err
	}
	dial := cc.DialFunc
	cc.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		ctx, cancel := p.connectionContext(ctx, cc.ConnectTimeout)
		defer cancel()
		paths.dialed(address)
		return dial(ctx, network, address)
	}
	if afterNetConnect := cc.Config.AfterNetConnect; afterNetConnect != nil {
		cc.Config.AfterNetConnect = func(ctx context.Context, config *pgconn.Config, conn net.Conn) (net.Conn, error) {
			conn, err := afterNetConnect(ctx, config, conn)
			return conn, hookError(err)
		}
	}
	if token := cc.Config.OAuthTokenProvider; token != nil {
		cc.Config.OAuthTokenProvider = func(ctx context.Context) (string, error) {
			value, err := token(ctx)
			return value, hookError(err)
		}
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
				return &startupHookError{err}
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
				return &startupHookError{err}
			}
		}
		p.mu.Lock()
		if !p.resolver.Valid(p.policy, target) {
			p.mu.Unlock()
			return errors.New("cnpgconnect-go/pgxpool: topology changed while connecting")
		}
		p.routes[target] = struct{}{}
		p.mu.Unlock()
		conn.CustomData()[connectionIdentityKey] = connectionIdentity{pool: p, target: target}
		p.rememberPath(target, paths.chosen(), time.Now())
		return nil
	}
	return nil
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
	// TLS verification callbacks are application policy. An EOF from one must
	// not be mistaken for a transient wire failure during initial pool startup.
	if verify := config.VerifyPeerCertificate; verify != nil {
		config.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			return hookError(verify(rawCerts, verifiedChains))
		}
	}
	if verify := config.VerifyConnection; verify != nil {
		config.VerifyConnection = func(state tls.ConnectionState) error {
			return hookError(verify(state))
		}
	}
	if certificate := config.GetClientCertificate; certificate != nil {
		config.GetClientCertificate = func(request *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := certificate(request)
			return cert, hookError(err)
		}
	}
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
