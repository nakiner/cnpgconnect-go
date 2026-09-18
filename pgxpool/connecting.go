package pgxpool

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/nakiner/cnpgconnect-go"
)

type connectionAttempt struct {
	target cnpgconnectgo.Target
	cancel context.CancelFunc
}

type connectionAttemptKey struct{}

type connectionTracer struct {
	pool   *Pool
	target cnpgconnectgo.Target
}

func (t connectionTracer) TraceConnectStart(ctx context.Context, _ pgx.TraceConnectStartData) context.Context {
	// pgx applies ConnectTimeout separately to each advertised address. Keep
	// that fallback behavior; this context adds only lifecycle cancellation.
	ctx, attempt := t.pool.beginConnectionAttempt(ctx, t.target)
	return context.WithValue(ctx, connectionAttemptKey{}, attempt)
}

func (t connectionTracer) TraceConnectEnd(ctx context.Context, _ pgx.TraceConnectEndData) {
	t.pool.endConnectionAttempt(ctx.Value(connectionAttemptKey{}).(*connectionAttempt))
}

func (p *Pool) beginConnectionAttempt(ctx context.Context, target cnpgconnectgo.Target) (context.Context, *connectionAttempt) {
	ctx, cancel := p.operationContext(ctx)
	attempt := &connectionAttempt{target: target, cancel: cancel}
	p.mu.Lock()
	if p.connecting == nil {
		p.connecting = make(map[*connectionAttempt]struct{})
	}
	p.connecting[attempt] = struct{}{}
	p.mu.Unlock()
	// Check after registration so a notification racing with registration
	// cannot leave a constructor running against an already obsolete route.
	if !p.resolver.Valid(p.policy, target) {
		cancel()
	}
	return ctx, attempt
}

func (p *Pool) endConnectionAttempt(attempt *connectionAttempt) {
	p.mu.Lock()
	delete(p.connecting, attempt)
	p.mu.Unlock()
	attempt.cancel()
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
			attempt.cancel()
		}
	}
}
