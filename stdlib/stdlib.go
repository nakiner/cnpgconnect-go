// Package stdlib adapts cnpgconnect-go's managed pool to database/sql. The returned
// *sql.DB is a stable handle whose new acquisitions follow discovery updates.
package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync"

	"github.com/jackc/pgx/v5"
	pgxstdlib "github.com/jackc/pgx/v5/stdlib"
	"github.com/nakiner/cnpgconnect-go/pgxpool"
)

// Config is shared with the native pgx adapter.
type Config = pgxpool.Config

// Open creates a database/sql handle backed by a managed pgx pool. Close the
// returned DB to stop its discovery subscription and release all connections.
// A caller-supplied Resolver remains owned by its caller.
//
// Configure PostgreSQL pool limits and callbacks through Config.ConnConfig.
// Leave DB's maximum idle connections at zero: pgx owns the idle connections.
// Existing transactions and explicitly acquired sql.Conn values remain pinned
// to their original server; queries and transactions are never replayed here.
func Open(ctx context.Context, config Config) (*sql.DB, error) {
	pool, err := pgxpool.Open(ctx, config)
	if err != nil {
		return nil, err
	}
	connector := pgxstdlib.GetPoolConnector(pool.Pool,
		pgxstdlib.OptionResetSession(func(_ context.Context, conn *pgx.Conn) error {
			if !pool.Valid(conn) {
				return driver.ErrBadConn
			}
			return nil
		}),
	)
	db := sql.OpenDB(&ownedConnector{Connector: connector, closePool: pool.Close})
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(int(pool.Config().MaxConns))
	return db, nil
}

// database/sql closes connectors implementing io.Closer. Upstream pgx's pool
// connector does not own its pool, so ownership must be supplied here.
type ownedConnector struct {
	driver.Connector
	closePool func()
	once      sync.Once
}

var _ io.Closer = (*ownedConnector)(nil)

func (c *ownedConnector) Close() error {
	c.once.Do(c.closePool)
	return nil
}
