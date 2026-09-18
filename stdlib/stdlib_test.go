package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
)

// Guard the ownership contract: callers receive only *sql.DB and must not need
// another handle to shut down discovery and the underlying pgx pool.
func TestDBCloseOwnsPool(t *testing.T) {
	var closed atomic.Int32
	connector := &ownedConnector{
		Connector: noConnections{},
		closePool: func() { closed.Add(1) },
	}
	db := sql.OpenDB(connector)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connector.Close(); err != nil {
		t.Fatal(err)
	}
	if got := closed.Load(); got != 1 {
		t.Fatalf("pool closed %d times, want once", got)
	}
	if err := db.PingContext(context.Background()); err == nil {
		t.Fatal("closed database accepted a new operation")
	}
}

type noConnections struct{}

func (noConnections) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("unexpected connection")
}

func (noConnections) Driver() driver.Driver { return noConnections{} }

func (noConnections) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected connection")
}
