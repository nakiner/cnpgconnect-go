package pgxpool

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nakiner/cnpgconnect-go"
)

var errObsoleteConnection = errors.New("cnpgconnect-go/pgxpool: connection target became obsolete")

type connectionAttempt struct {
	target cnpgconnectgo.Target
	cancel context.CancelCauseFunc
}

type connectionAttemptKey struct{}

type connectionTracer struct {
	pool    *Pool
	target  cnpgconnectgo.Target
	startup *startupRetry
}

func (t connectionTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	// pgx applies ConnectTimeout separately to each advertised address. Keep
	// that fallback behavior; this context adds only lifecycle cancellation.
	ctx, attempt := t.pool.beginConnectionAttempt(ctx, t.target)
	return context.WithValue(ctx, connectionAttemptKey{}, attempt)
}

func (t connectionTracer) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	// pgconn returns context.Canceled without its cause. Retain provenance for
	// this exact startup error before endConnectionAttempt cancels the context.
	if t.startup != nil && context.Cause(ctx) == errObsoleteConnection {
		var connectErr *pgconn.ConnectError
		if errors.As(data.Err, &connectErr) {
			t.startup.obsolete.Store(connectErr)
		}
	}
	t.pool.endConnectionAttempt(ctx.Value(connectionAttemptKey{}).(*connectionAttempt))
}

func (p *Pool) beginConnectionAttempt(ctx context.Context, target cnpgconnectgo.Target) (context.Context, *connectionAttempt) {
	ctx, stop := p.operationContext(ctx)
	ctx, cancel := context.WithCancelCause(ctx)
	attempt := &connectionAttempt{target: target, cancel: func(cause error) {
		cancel(cause)
		stop()
	}}
	p.mu.Lock()
	if p.connecting == nil {
		p.connecting = make(map[*connectionAttempt]struct{})
	}
	p.connecting[attempt] = struct{}{}
	p.mu.Unlock()
	// Check after registration so a notification racing with registration
	// cannot leave a constructor running against an already obsolete route.
	if !p.resolver.Valid(p.policy, target) {
		attempt.cancel(errObsoleteConnection)
	}
	return ctx, attempt
}

func (p *Pool) endConnectionAttempt(attempt *connectionAttempt) {
	p.mu.Lock()
	delete(p.connecting, attempt)
	p.mu.Unlock()
	attempt.cancel(nil)
}

func (p *Pool) cancelObsoleteConnections() {
	p.mu.RLock()
	attempts := make([]*connectionAttempt, 0, len(p.connecting))
	for attempt := range p.connecting {
		attempts = append(attempts, attempt)
	}
	p.mu.RUnlock()
	for _, attempt := range attempts {
		if !p.resolver.Valid(p.policy, attempt.target) {
			attempt.cancel(errObsoleteConnection)
		}
	}
}
