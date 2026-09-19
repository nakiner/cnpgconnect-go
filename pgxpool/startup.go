package pgxpool

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgx wraps some application hooks in ConnectError too. Preserve their original
// error identity while keeping their effects outside startup transport retries.
type startupHookError struct{ error }

func (e *startupHookError) Unwrap() error { return e.error }

// A lookup failure can belong to an optional address of the selected member.
// Preserve this context when pgx joins it with another address's connect error.
type optionalLookupError struct{ error }

func (e *optionalLookupError) Unwrap() error { return e.error }

func hookError(err error) error {
	if err != nil {
		return &startupHookError{err}
	}
	return nil
}

// Only Open uses this retry boundary. Once a connection has been established,
// errors from Ping or application operations are not ConnectErrors and are
// returned unchanged. No application query or transaction is replayed.
func (p *Pool) pingStartup(ctx context.Context) error {
	delay := 50 * time.Millisecond
	for {
		err := p.Pool.Ping(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Join(ctx.Err(), err)
		}
		var connectErr *pgconn.ConnectError
		if !errors.As(err, &connectErr) || !transientConnectError(err) {
			return err
		}
		// Keep simultaneous application starts from retrying in lockstep.
		wait := delay/2 + time.Duration(rand.Int64N(int64(delay/2)))
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), err)
		case <-timer.C:
		}
		delay = min(2*delay, time.Second)
	}
}

func transientConnectError(err error) bool {
	retryable, permanent := classifyConnectError(err, false)
	return retryable && !permanent
}

// An optional missing hostname is neutral: it cannot veto another address's
// transient failure, but it cannot trigger retries by itself either. Every
// authentication, TLS verification, or application-hook error still vetoes.
func classifyConnectError(err error, optionalLookup bool) (retryable, permanent bool) {
	switch err := err.(type) {
	case *optionalLookupError:
		return classifyConnectError(err.error, true)
	case *startupHookError, *tls.CertificateVerificationError:
		return false, true
	case *net.DNSError:
		if cause := err.Unwrap(); cause != nil {
			return classifyConnectError(cause, optionalLookup)
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
			canRetry, mustStop := classifyConnectError(cause, optionalLookup)
			retryable = retryable || canRetry
			permanent = permanent || mustStop
		}
		return retryable, permanent
	case interface{ Unwrap() error }:
		if cause := err.Unwrap(); cause != nil {
			return classifyConnectError(cause, optionalLookup)
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
