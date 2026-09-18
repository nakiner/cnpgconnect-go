# Configuration and reuse

Import the root package as `github.com/nakiner/cnpgconnect-go` and refer to it as
`cnpgconnectgo` in Go code. The adapters use
`github.com/nakiner/cnpgconnect-go/pgxpool` and
`github.com/nakiner/cnpgconnect-go/stdlib`.

## Shared configuration

Both database adapters accept the same configuration type:

```go
pgxpool.Config{
    Discovery:      discoveryConfig,
    Resolver:       nil, // Optional caller-owned shared resolver.
    Policy:         cnpgconnectgo.Policy{Role: cnpgconnectgo.Primary},
    ConnConfig:     postgresConfig,
    StartupTimeout: 10 * time.Second,
}
```

Create `ConnConfig` with `github.com/jackc/pgx/v5/pgxpool.ParseConfig`. Database,
username, password, runtime parameters, tracing, pool sizing, and callbacks use
the normal pgx configuration. The adapter copies it, then changes the host, port,
and TLS server name to the selected member. Original DSN alternative hosts do
not bypass discovery. Keep this configuration immutable after opening.

Configure the PostgreSQL CA through the DSN's `sslrootcert` or the pgx TLS
configuration. Use `sslmode=verify-full` for authenticated encrypted connections.
The discovered endpoint's `serverName` permits certificate verification when the
dial address is a Pod IP or a per-member external address.

The managed adapter preserves the DSN's PostgreSQL TLS policy: `prefer` tries TLS
before plaintext, `allow` tries plaintext before TLS, `verify-full` requires
verified TLS, and `disable` uses plaintext. Transport alternatives are retained
only for the original primary DSN host and port, then all are redirected to the
same discovered member. Other DSN hosts and ports are discarded. An unset
PostgreSQL connection timeout defaults to 5 seconds; set `ConnectTimeout` on the
native connection configuration to change this bound.

An `AfterConnect` hook still runs for every new PostgreSQL connection. Use it for
session initialization that must also apply after failover. Native query tracing
and pool instrumentation remain available. The managed `Pool` embeds the native
pool, so it retains pgx's normal `Query`, `Exec`, `Begin`, `CopyFrom`, `SendBatch`,
`Acquire`, and `Stat` methods. Call the managed `Close`, which also stops its
discovery worker.

The `stdlib` adapter returns a regular `*sql.DB`. It implements no ORM-specific
abstraction, so any library accepting that type can share the same behavior.
For Bun, use `bun.NewDB(sqldb, pgdialect.New())`, as in the
[official Bun pgx integration](https://bun.uptrace.dev/postgres/#pgx).

## Discovery configuration

| Field | Meaning | Default |
| --- | --- | --- |
| `Address` | gRPC target, e.g. `dns:///discovery.example.com:443` | required |
| `Namespace`, `Cluster` | CNPG Cluster identity | required |
| `Network` | Endpoint map key published by the plugin | `internal` |
| `Token` | Discovery bearer token | required with TLS |
| `TLSConfig` | Discovery CA, server name, optional client certificate | system CA roots |
| `StartupTimeout` | Initial discovery bound, capped by an earlier context deadline | 10 seconds |
| `ReconnectMin` | Initial stream reconnect backoff | 100 milliseconds |
| `ReconnectMax` | Maximum reconnect backoff | 5 seconds |
| `MaxSnapshotTTL` | Maximum accepted age measured from server observation | 30 seconds |
| `Insecure` | Plaintext discovery for local development | false |

`Insecure` cannot be combined with a bearer token or TLS configuration. It is
intended for a plugin started explicitly in its own insecure development mode.
Normal discovery verifies the server certificate. Keep client and server clocks
synchronized because topology deadlines are absolute timestamps. Observations
more than five seconds in the future are rejected. An older observation received
after reconnect cannot replace newer state or renew its lifetime.

When using a private discovery CA, load its PEM certificates into an
`x509.CertPool` and assign `TLSConfig.RootCAs`. A `ServerName` override is useful
for a local port-forward while still verifying the real service certificate.

The plugin's application listener must be reachable through the configured
address. An external proxy must support HTTP/2 gRPC and long-lived response
streams. The PostgreSQL endpoints must also be reachable using the selected
`Network`; the client never silently falls back to a different network.

## Share one discovery stream

A service with separate writer and reader pools can share a client:

```go
discovery, err := cnpgconnectgo.New(ctx, discoveryConfig)
if err != nil {
    return err
}
defer discovery.Close()

writer, err := pgxpool.Open(ctx, pgxpool.Config{
    Resolver:   discovery,
    ConnConfig: postgresConfig,
})
if err != nil {
    return err
}
defer writer.Close()

reader, err := stdlib.Open(ctx, stdlib.Config{
    Resolver:   discovery,
    ConnConfig: postgresConfig,
    Policy:     cnpgconnectgo.Policy{Role: cnpgconnectgo.Replica},
})
if err != nil {
    return err
}
defer reader.Close()
```

Close every pool before closing the shared client. Pool `Close` releases its
subscription but leaves a caller-provided resolver open. Without `Resolver`, each
pool owns and closes its own discovery client. `Client.Status()` exposes a copy
of the current discovery state for diagnostics and health checks.

Custom adapters implement the same contract through `Resolver.Resolve`,
`Resolver.Valid`, and `Resolver.Subscribe`. `Resolve` waits with a context;
`Valid` checks an immutable target including member identity and routing
generation; `Subscribe` coalesces routing-change notifications. Adapters must
retain the selected target identity rather than infer it later from host/port.
