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
	switch err := err.(type) {
	case *startupHookError, *tls.CertificateVerificationError:
		return false
	case *pgconn.PgError:
		return err.Code == "57P03" // PostgreSQL cannot accept connections yet.
	case interface{ Unwrap() []error }:
		causes := err.Unwrap()
		for _, cause := range causes {
			if !transientConnectError(cause) {
				return false
			}
		}
		return len(causes) != 0
	case interface{ Unwrap() error }:
		if cause := err.Unwrap(); cause != nil {
			return transientConnectError(cause)
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary())
}
