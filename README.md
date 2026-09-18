# cnpgconnect-go

Go database connections that follow CloudNativePG role changes automatically.
Open a pool once and use ordinary pgx, `database/sql`, or Bun queries. The library
uses [cnpg-connect-plugin](https://github.com/nakiner/cnpg-connect-plugin) to find
the primary and reconnect after a promotion or failover.

## Connect

Supply the discovery address, CNPG namespace/name, and your PostgreSQL login:

```go
import (
    "context"
    "os"

    "github.com/nakiner/cnpgconnect-go/pgxpool"
)

pool, err := pgxpool.Open(ctx, pgxpool.Config{
    Address:   "discovery.example.com:443",
    Namespace: "dev",
    Cluster:   "rent",
    Username:  os.Getenv("PGUSER"),
    Password:  os.Getenv("PGPASSWORD"),
})
if err != nil {
    return err
}
defer pool.Close()

var count int
err = pool.QueryRow(context.Background(), "select count(*) from orders").Scan(&count)
```

Discovery supplies the database name, member addresses, and PostgreSQL server CA.
The library verifies PostgreSQL TLS and automatically follows the current primary.
There is no application DSN, discovery token, Kubernetes credential, or CA-file
mount to configure. PostgreSQL credentials go directly to PostgreSQL, never to
the discovery plugin.

The discovery endpoint uses a certificate trusted by the operating system. Your
platform installs that endpoint once; applications use its address. PostgreSQL
member addresses must be reachable from the application network.
The library tries each member's internal address first and its advertised
external address if needed. Applications inside and outside Kubernetes use the
same five settings; the platform configures per-member external addresses once.

For `database/sql`, use the same configuration with `stdlib.Open`:

```go
import "github.com/nakiner/cnpgconnect-go/stdlib"

db, err := stdlib.Open(ctx, stdlib.Config{
    Address:   "discovery.example.com:443",
    Namespace: "dev",
    Cluster:   "rent",
    Username:  os.Getenv("PGUSER"),
    Password:  os.Getenv("PGPASSWORD"),
})
if err != nil {
    return err
}
defer db.Close()
err = db.QueryRowContext(ctx, "select count(*) from orders").Scan(&count)
```

Bun uses that same SQL handle: `bun.NewDB(db, pgdialect.New())`. SQLx and other
libraries accepting `*sql.DB` work the same way.

| Package | Returned handle |
| --- | --- |
| `cnpgconnect-go/pgxpool` | Managed pool embedding `*pgxpool.Pool` |
| `cnpgconnect-go/stdlib` | `*sql.DB` |
| `cnpgconnect-go` | Discovery `Client` and reusable `Resolver` interface |

The Go package name is `cnpgconnectgo`. Go 1.26.4 or later is required.
See the [runnable examples](examples/README.md).

## Routing and pool options

Primary is the default. Set `Policy` for a reader pool, for example
`cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica}`. Supported roles include
primary, replica, sync, async, quorum, and potential. Fallbacks are optional and
explicit.

Existing pgx pool sizing, tracing, and callbacks remain available through the
optional `ConnConfig`. Discovery transport overrides and shared resolvers are
also optional. See [configuration](docs/usage.md) for those advanced cases.

Keep the returned pool or DB for the service lifetime and close it at shutdown.
New acquisitions follow topology changes, including sync/async changes and
promotion or demotion. Open transactions stay on their original server; an
interrupted query may return an error. Queries and transactions are never
silently replayed. See [failover behavior](docs/behavior.md).

## Development

```sh
make check
```

This runs formatting, race tests, vet, example compilation, and compilation of
the [opt-in live integration suite](test/integration/README.md). The Bun example
has a separate module so applications do not acquire an ORM dependency.

Generated protobuf messages and the streaming client come directly from
`github.com/nakiner/cnpg-connect-plugin/api/connect/v1`. The new simple connection
API requires a plugin build publishing `Snapshot.connection` (database name and
server CA); older plugins continue to work with explicit advanced `ConnConfig`.
Until matching releases are published, develop using a Go workspace containing
both plugin and client checkouts. No Kubernetes client package is compiled into
applications through these API bindings.

Earlier live tests covered CNPG 1.30 switchover, primary Pod loss, and discovery
outage/recovery; see the historical [validation record](docs/validation.md).
