# cnpgconnect-go examples

The pgx, `database/sql`, and Bun examples use the same discovery and PostgreSQL
configuration. They connect, query the server address, and close cleanly.
Run them from the `cnpgconnect-go` checkout with Go 1.26.4 or later.

Supply credentials through your normal secret delivery mechanism. The examples
read these environment variables:

```sh
export PG_DSN='postgres://app@placeholder/app?sslmode=verify-full&sslrootcert=/path/to/postgres-ca.crt'
export PGPASSWORD='your-postgres-password'
export CNPG_DISCOVERY_ADDRESS='dns:///discovery.example.com:443'
export CNPG_DISCOVERY_TOKEN_FILE='/path/to/discovery-token'
export CNPG_DISCOVERY_CA='/path/to/discovery-ca.crt'
export CNPG_NAMESPACE='databases'
export CNPG_CLUSTER='app-db'
export CNPG_NETWORK='external'
```

`PGPASSWORD` is consumed by pgx's normal DSN parser. The placeholder host is
replaced by a discovered member. The discovery CA and PostgreSQL CA are separate.
If discovery uses a publicly trusted certificate, omit `CNPG_DISCOVERY_CA` to
use system trust roots. `CNPG_DISCOVERY_SERVER_NAME` optionally overrides the
discovery TLS name, for example during a port-forward.

From the repository root:

```sh
go run ./examples/pgx
go run ./examples/sql
```

For Bun:

```sh
cd examples/bun
go run .
```

Bun has a separate example module with a local replacement for this library. Its
dependencies are excluded from the main library module. The example follows
[Bun's pgx integration](https://bun.uptrace.dev/postgres/#pgx) and sets simple
protocol for its interpolated SQL.

The examples use the primary policy. To select replicas, set `Policy` on the
configuration before calling the adapter's `Open` function. Service code should
keep the returned handle for its whole lifetime and set a context deadline on
each operation.
