# Failover and connection lifetime

The managed pool has a stable identity. Discovery changes which server a new
connection may use and whether an existing idle connection can be borrowed.
Applications keep calling the same pgx pool or SQL DB throughout that process.

1. The discovery client opens `WatchTopology` and accepts full snapshots for the
   configured cluster. Network selection is automatic unless explicitly pinned.
2. A new connection resolves an eligible member and records its identity. The application supplies
   PostgreSQL credentials; discovery supplies the database name, address, and
   server CA. TLS verifies the discovered server name before authentication.
   It tries the same member's advertised external address if the internal path
   fails; it does not change member or routing role to choose a reachable path.
3. Every acquisition checks whether the connection still matches the current,
   unexpired routing policy. An obsolete connection is retired before use.
4. Role changes and expiry wake pool maintenance. Borrowed connections retire
   when released if their identity is no longer eligible.
5. If the discovery stream closes, the client reconnects with bounded backoff.
   The last snapshot remains usable only until its validity deadline.

A snapshot refresh extends freshness without changing connection identity.
Replacing a Pod changes its member identity even if its address is reused.
Database name and server CA changes also invalidate the previous connections.
There is no Kubernetes watch or Kubernetes authentication in application code.

When a policy has several eligible members, each new connection chooses one
uniformly at random. Reusing a healthy connection does not rebalance every query.
An unrelated replica update also does not retire a still-eligible primary
connection.

## During an outage

If no eligible fresh member is available, acquisition waits until a route
recovers or the operation context expires. Use deadlines on application queries
so this waiting fits the request's time budget. A strict replica policy does not
use the primary unless an explicit fallback permits it.

The discovery TTL bounds how long the client can trust a snapshot after updates
stop. It cannot eliminate the interval between a PostgreSQL role change and the
plugin observing that change. Connection failures and PostgreSQL errors can
still reach callers during this interval. The next acquisition uses current
discovery information automatically.

Role labels reflect the primary's observed replication state. A synchronous
standby does not by itself guarantee read-after-write visibility for every
query. Session and transaction consistency requirements still belong to the
application and PostgreSQL configuration.

## Transactions and held connections

An active transaction has one server-side session and cannot move to a new
primary. Likewise, an explicitly acquired `pgxpool.Conn`, a `sql.Conn`, a row
stream, a batch, and a COPY operation retain their original connection until
released. Avoid holding a connection across unrelated requests.

The library performs no automatic query or transaction replay. The usual
`database/sql` handling of a driver rejection before execution still applies;
the SQL session guard uses `driver.ErrBadConn` to discard an obsolete connection
before it can be reused. An error after a write was sent can leave its outcome
unknown. Any application retry of that operation needs its own idempotency or
transaction recovery semantics.

Always close rows, release acquired connections, and commit or roll back
transactions. Pool shutdown waits for acquired connections, matching native
pgx behavior. Close the SQL DB or managed pgx pool during service shutdown; there
is no separate reconnect-loop lifecycle to manage.

## SQL pool ownership

`stdlib.Open` wraps a managed pgx pool with the upstream pgx SQL connector. pgx
owns idle connections, so the SQL idle limit is zero and its maximum open
connections initially matches pgx's `MaxConns`. Configure those limits on the
pgx configuration before opening. Increasing SQL idle connections creates a
second pooling layer that can hold the native pool's connections unnecessarily.

`DB.Close` also closes the underlying managed pool through its connector. This
differs from calling the upstream `stdlib.OpenDBFromPool` directly, which leaves
pool ownership with the caller. See the upstream
[pool connector contract](https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib#GetPoolConnector)
and [SQL connector close contract](https://pkg.go.dev/database/sql/driver#Connector).
