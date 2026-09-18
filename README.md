# cnpgconnect-go

Go database connections that follow CloudNativePG role changes automatically.
Open a pool once, keep the same handle, and use normal pgx, `database/sql`, or Bun
operations. A background gRPC stream discovers the current primary and replica
roles from [cnpg-connect-plugin](https://github.com/nakiner/cnpg-connect-plugin).

The library retires connections that no longer match their routing policy and
waits for fresh eligible members when acquiring new ones. It reconnects the
discovery stream automatically. Application code does not manage pool replacement
or subscribe to failover events.

| Package | Returned handle | Use |
| --- | --- | --- |
| `cnpgconnect-go/pgxpool` | managed pool embedding `*pgxpool.Pool` | Native pgx, sqlc, COPY, batch operations |
| `cnpgconnect-go/stdlib` | `*sql.DB` | `database/sql`, Bun, sqlx and other SQL-based libraries |
| `cnpgconnect-go` | `*Client` implementing `Resolver` | Share discovery across several pools or build another adapter |

The module is `github.com/nakiner/cnpgconnect-go`; its root Go package is
`cnpgconnectgo`. Requires Go 1.26.4 or later, matching the plugin module that
publishes the shared protobuf API. Applications need no Kubernetes API
credentials. PostgreSQL credentials remain in your application.

## Open a database

```go
import (
    "context"
    "crypto/tls"
    "os"

    native "github.com/jackc/pgx/v5/pgxpool"
    "github.com/nakiner/cnpgconnect-go"
    "github.com/nakiner/cnpgconnect-go/stdlib"
)

postgres, err := native.ParseConfig(os.Getenv("PG_DSN"))
if err != nil {
    return err
}
db, err := stdlib.Open(context.Background(), stdlib.Config{
    Discovery: cnpgconnectgo.Config{
        Address:   "dns:///discovery.example.com:443",
        Namespace: "databases",
        Cluster:   "app-db",
        Network:   "external",
        Token:     discoveryToken,
        TLSConfig: &tls.Config{RootCAs: discoveryCAs},
    },
    ConnConfig: postgres,
})
if err != nil {
    return err
}
defer db.Close()

// Keep db for the service lifetime. Normal queries use the current primary.
err = db.QueryRowContext(ctx, "select count(*) from orders").Scan(&count)
```

`Open` checks initial discovery and PostgreSQL connectivity. Its context controls
startup; cancelling it after a successful open does not close the pool. Use
`Close` at service shutdown. Query contexts bound how long operations may wait
for a usable connection during an outage.

The default policy is primary. For a strict synchronous replica pool, set
`Policy: cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica}`. To allow a fallback,
list it explicitly:

```go
Policy: cnpgconnectgo.Policy{
    Role:       cnpgconnectgo.SyncReplica,
    Fallback:   []cnpgconnectgo.Role{cnpgconnectgo.Replica, cnpgconnectgo.Primary},
    PreferZone: "zone-a",
},
```

Policies select the first available role class, then prefer the requested zone
within that class. Available classes are `Primary`, `Replica`, `SyncReplica`,
`AsyncReplica`, `QuorumReplica`, `PotentialReplica`, and `Any`.
Quorum and potential standbys retain their own PostgreSQL replication states;
they are not reported as priority synchronous standbys.

For native pgx, use `pgxpool.Open` with the same configuration. For Bun, wrap the
returned SQL handle:

```go
db := bun.NewDB(sqldb, pgdialect.New())
defer db.Close() // Also closes the managed pool and its discovery client.
```

See [runnable examples](examples/README.md), [configuration and shared
discovery](docs/usage.md), and [failover behavior](docs/behavior.md).

## Operational contract

- A fresh topology is required for each new acquisition. Expired, unavailable,
  or invalid topology cannot supply a new connection.
- The same pool or DB handle survives primary changes, Pod replacement, and
  discovery reconnects. Individual queries may still fail during a failover.
- Active transactions, acquired pgx connections, `sql.Conn`, and open rows remain
  pinned until their caller releases them. The library never replays a query or
  transaction; an interrupted write may have an unknown outcome.
- External clients need a reachable address **for each PostgreSQL member**, plus
  a reachable discovery endpoint. One shared `rw` LB cannot distinguish replicas.
- Discovery TLS and PostgreSQL TLS are configured independently. Discovery
  credentials are never sent to PostgreSQL.

Configure SQL pool size, lifetime, tracing, and callbacks on the native pgx
configuration before opening. Leave `sql.DB`'s idle limit at zero because the
underlying pgx pool owns idle connections.

## Development

```sh
make check
```

This runs formatting checks, race tests, vet, compilation of the pgx, SQL,
and Bun examples, and compilation of the opt-in integration tests. The Bun
example has its own module so importing this library
does not add an ORM dependency.

The separately guarded [live integration suite](test/integration/README.md)
checks native pgx, SQL, and prepared statements across CNPG promotion, with an
optional primary-Pod deletion phase.

The discovery client uses the canonical generated protobuf messages and gRPC
client from `github.com/nakiner/cnpg-connect-plugin/api/connect/v1`, pinned through
the plugin module's `v0.0.3` release. There is no separate protobuf copy to
regenerate here. Application adapters share the library's exported `Resolver`,
`Policy`, and `Target` types, so custom adapters do not need to implement the
wire protocol.

Importing that API package does not import the plugin's observer or Kubernetes
client packages. The plugin module itself declares Kubernetes dependencies;
applications communicate with the discovery API and PostgreSQL directly.

For local application development, use this checkout through a Go workspace or
a local module replacement. These instructions do not require a published
`cnpgconnect-go` release.

Before the rename and shared API migration, live CNPG 1.30 tests passed for
switchover, primary Pod loss, discovery outage, and recovery through unchanged
application handles. See the [validation
record](docs/validation.md) for coverage and remaining deployment checks.
