package pgxpool

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgx wraps some application hooks in ConnectError too. Preserve their original
// error identity while keeping their effects outside startup transport retries.
type startupHookError struct{ error }

func (e *startupHookError) Unwrap() error { return e.error }

// pgconn replaces timeout errors with the connection context's error. Stop
// that normalization at this wrapper so application failures keep their cause.
func (*startupHookError) Timeout() bool   { return false }
func (*startupHookError) Temporary() bool { return false }

// A lookup failure can belong to an optional address of the selected member.
// Preserve this context when pgx joins it with another address's connect error.
type optionalLookupError struct{ error }

func (e *optionalLookupError) Unwrap() error { return e.error }

type startupRetryKey struct{}

// Puddle preserves acquisition context values in connection constructors.
// Each Ping owns its marker, including constructors that finish after timeout.
type startupRetry struct {
	obsolete atomic.Pointer[pgconn.ConnectError]
}

// Only Open uses this retry boundary. Once a connection has been established,
// errors from Ping or application operations are not ConnectErrors and are
// returned unchanged. No application query or transaction is replayed.
func (p *Pool) pingStartup(ctx context.Context) error {
	delay := 50 * time.Millisecond
	for {
		startup := new(startupRetry)
		err := p.Pool.Ping(context.WithValue(ctx, startupRetryKey{}, startup))
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		if p.ctx.Err() != nil {
			return errors.Join(p.ctx.Err(), err)
		}
		var connectErr *pgconn.ConnectError
		if !errors.As(err, &connectErr) || !transientConnectError(err, startup.obsolete.Load() == connectErr) {
			return err
		}
		// Keep simultaneous application starts from retrying in lockstep.
		wait := delay/2 + time.Duration(rand.Int64N(int64(delay/2)))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-p.ctx.Done():
			timer.Stop()
			return errors.Join(p.ctx.Err(), err)
		case <-timer.C:
		}
		delay = min(2*delay, time.Second)
	}
}

func transientConnectError(err error, topologyCanceled bool) bool {
	retryable, permanent := classifyConnectError(err, false, topologyCanceled)
	return retryable && !permanent
}

// An optional missing hostname is neutral: it cannot veto another address's
// transient failure, but it cannot trigger retries by itself either. Every
// authentication, TLS verification, or application-hook error still vetoes.
// Cancellation is retryable only when this constructor lost its topology target.
func classifyConnectError(err error, optionalLookup, topologyCanceled bool) (retryable, permanent bool) {
	if err == context.Canceled && topologyCanceled {
		return true, false
	}
	switch err := err.(type) {
	case *optionalLookupError:
		return classifyConnectError(err.error, true, topologyCanceled)
	case *startupHookError, *tls.CertificateVerificationError:
		return false, true
	case *net.DNSError:
		if cause := err.Unwrap(); cause != nil {
			return classifyConnectError(cause, optionalLookup, topologyCanceled)
		}
		if optionalLookup && err.IsNotFound {
			return false, false
		}
	case *pgconn.PgError:
		retryable = err.Code == "57P03" // PostgreSQL cannot accept connections yet.
		return retryable, !retryable
	case interface{ Unwrap() []error }:
		causes := err.Unwrap()
		if len(causes) == 0 {
			return false, true
		}
		for _, cause := range causes {
			canRetry, mustStop := classifyConnectError(cause, optionalLookup, topologyCanceled)
			retryable = retryable || canRetry
			permanent = permanent || mustStop
		}
		return retryable, permanent
	case interface{ Unwrap() error }:
		if cause := err.Unwrap(); cause != nil {
			return classifyConnectError(cause, optionalLookup, topologyCanceled)
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) {
		return true, false
	}
	var networkError net.Error
	retryable = errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary())
	return retryable, !retryable
}
