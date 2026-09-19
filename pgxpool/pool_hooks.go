package pgxpool

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	native "github.com/jackc/pgx/v5/pgxpool"
)

// Pool hooks retire obsolete idle connections while preserving application
// callbacks for real borrows, releases, and connection initialization.
func (p *Pool) installHooks(cfg *native.Config) {
	beforeConnect := cfg.BeforeConnect
	afterConnect := cfg.AfterConnect
	prepareConn := cfg.PrepareConn
	beforeAcquire := cfg.BeforeAcquire
	afterRelease := cfg.AfterRelease
	shouldPing := cfg.ShouldPing
	cfg.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
		ctx, cancel := p.operationContext(ctx)
		defer cancel()
		if beforeConnect != nil {
			if err := beforeConnect(ctx, cc); err != nil {
				return &startupHookError{err}
			}
		}
		return p.configureConnection(ctx, cc)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		ctx, cancel := p.connectionContext(ctx, conn.Config().ConnectTimeout)
		defer cancel()
		if !p.Valid(conn) {
			return errors.New("cnpgconnect-go/pgxpool: topology changed while connecting")
		}
		if afterConnect != nil {
			// Native pgxpool invokes this hook after pgx's ConnectEnd. Keep
			// cooperative session initialization cancellable until the pool
			// constructor finishes, without extending it into borrowed sessions.
			identity := conn.PgConn().CustomData()[connectionIdentityKey].(connectionIdentity)
			ctx, attempt := p.beginConnectionAttempt(ctx, identity.target)
			defer p.endConnectionAttempt(attempt)
			if err := afterConnect(ctx, conn); err != nil {
				return &startupHookError{err}
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
			if err != nil {
				return ok, &startupHookError{err}
			}
			if !ok {
				return false, nil
			}
		} else if beforeAcquire != nil && !beforeAcquire(ctx, conn) {
			return false, nil
		}
		if conn.IsClosed() {
			// A successful application hook may close the connection. Return
			// an error rather than asking pgx to repeat that hook on a new one.
			return false, errors.New("cnpgconnect-go/pgxpool: acquisition hook closed the connection")
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
