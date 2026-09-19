# Configuration and reuse

Both adapters share `pgxpool.Config`; `stdlib.Config` is an alias. The normal
configuration contains only five fields:

```go
pgxpool.Config{
    Address:   "discovery.example.com:443",
    Namespace: "dev",
    Cluster:   "rent",
    Username:  "rent",
    Password:  password,
}
```

Namespace and cluster identify the CNPG `Cluster`, not a Pod. The plugin supplies
the default database name, member endpoints, and public PostgreSQL server CA.
The library verifies each server certificate against that CA using the endpoint's
TLS server name, including when its dial address is a Pod IP. Database name or
CA changes retire the previous pool connections on release. Passwords are only
sent to PostgreSQL.

Discovery uses system certificate roots by default and does not require a bearer
token. Configure its publicly trusted certificate and network access once in
platform infrastructure. Applications do not need Kubernetes credentials or
certificate files.

The default network selection tries the selected member's internal address,
then its external address when needed. An external address is also sufficient
when no internal address is advertised. Each address uses its own verified TLS
server name and must pass the PostgreSQL role check. An authentication failure
is returned directly. The normal connection timeout bounds each address attempt;
the caller's context bounds the whole operation.

## Pool tuning and routing

Optional `ConnConfig` uses the usual `github.com/jackc/pgx/v5/pgxpool.ParseConfig`
configuration. It is useful for sizing, runtime parameters, query tracing, and
session callbacks:

```go
postgres, err := native.ParseConfig("")
if err != nil {
    return err
}
postgres.MaxConns = 20
postgres.AfterConnect = initializeSession

pool, err := pgxpool.Open(ctx, pgxpool.Config{
    Address: "discovery.example.com:443", Namespace: "dev", Cluster: "rent",
    Username: user, Password: password,
    ConnConfig: postgres,
})
```

With `Username` set, application credentials and discovered database/TLS settings
override connection settings in `ConnConfig`. TLS verification is always enabled
in this path and it never falls back to plaintext. The configuration is copied;
keep callbacks safe for concurrent use, as with a native pgx pool.

For compatibility, a `ConnConfig` supplied without `Username` keeps its own
credentials, database, and TLS policy. Only the member host, port, and TLS name
are replaced. This is useful for an additional database on the same CNPG cluster
or an older discovery server; ordinary applications use the simple API above.

`Policy` defaults to the current primary. A strict reader can use
`cnpgconnectgo.Policy{Role: cnpgconnectgo.SyncReplica}`. Use `Fallback` to allow
another role, for example:

```go
Policy: cnpgconnectgo.Policy{
    Role: cnpgconnectgo.SyncReplica,
    Fallback: []cnpgconnectgo.Role{cnpgconnectgo.Replica, cnpgconnectgo.Primary},
},
```

`PreferZone` prefers members in a zone within the chosen role class. Roles are
`Primary`, `Replica`, `SyncReplica`, `AsyncReplica`, `QuorumReplica`,
`PotentialReplica`, and `Any`.

For a minimal primary-and-sync connection example, see the
[role-routing example](../examples/roles/README.md). Selecting a replication role
does not itself wait for that standby to replay a write.

`Open` waits for discovery and the initial PostgreSQL connection. Its context
bounds startup only; call `Close` during shutdown. `StartupTimeout` defaults to
30 seconds and the PostgreSQL connection timeout to 5 seconds. Use normal query
context deadlines to bound waits during failover.

For `database/sql`, pgx owns idle connections. Leave the returned DB's idle limit
at zero and tune pool size through `ConnConfig`. Bun wraps the returned handle
with `bun.NewDB(sqldb, pgdialect.New())`.

## Advanced discovery settings

`Discovery` optionally overrides the stream's configuration. Direct `Address`,
`Namespace`, and `Cluster` fields take precedence when set.

| Field on `cnpgconnectgo.Config` | Default |
| --- | --- |
| `Network` | Automatic internal-then-external selection; an explicit key pins that network |
| `TLSConfig` | System roots, verified TLS |
| `Token` | Empty; only needed if the server explicitly enables bearer authentication |
| `StartupTimeout` | 30 seconds |
| `ReconnectMin`, `ReconnectMax` | 100 milliseconds, 5 seconds |
| `MaxSnapshotTTL` | 30 seconds |
| `Insecure` | False; plaintext discovery for local development only |

For a private organizational discovery CA already trusted by the operating
system, no override is needed. Specialized environments can provide
`TLSConfig.RootCAs` or a TLS server-name override. `Insecure` cannot carry a bearer
token or a TLS configuration. The simple API never disables TLS verification.
An external proxy must support HTTP/2 gRPC and long-lived streams; each published
PostgreSQL member endpoint must also be reachable.

## Share one discovery stream

A service with writer and reader pools can share a client:

```go
discovery, err := cnpgconnectgo.New(ctx, cnpgconnectgo.Config{
    Address: "discovery.example.com:443", Namespace: "dev", Cluster: "rent",
})
if err != nil {
    return err
}
defer discovery.Close()

writer, err := pgxpool.Open(ctx, pgxpool.Config{
    Resolver: discovery, Username: user, Password: password,
})
if err != nil {
    return err
}
defer writer.Close()

reader, err := stdlib.Open(ctx, stdlib.Config{
    Resolver: discovery, Username: user, Password: password,
    Policy: cnpgconnectgo.Policy{Role: cnpgconnectgo.Replica},
})
if err != nil {
    return err
}
defer reader.Close()
```

Close the pools before their shared client. Without `Resolver`, each pool owns
and closes its own client. `Client.Status()` provides a diagnostic snapshot.

Custom adapters use `Resolver.Resolve`, `Resolver.Valid`, and
`Resolver.Subscribe`. `Target` contains the endpoint, connection parameters,
member identity, and routing generation. Retain that complete identity when
checking whether an existing connection is still usable.
