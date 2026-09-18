# cnpgconnect-go examples

The pgx, `database/sql`, and Bun examples share the same five inputs:

```sh
export CNPG_DISCOVERY_ADDRESS='discovery.example.com:443'
export CNPG_NAMESPACE='dev'
export CNPG_CLUSTER='rent'
export PGUSER='rent'
export PGPASSWORD='your-postgres-password'
```

The discovery endpoint has a certificate trusted by the operating system. The
plugin supplies the database name, PostgreSQL addresses, and server CA. No DSN,
discovery token, or CA file is needed. Supply the PostgreSQL password using your
usual secret delivery mechanism.
The same configuration works outside Kubernetes when the platform has published
reachable external member addresses. The library selects the reachable path.

With Go 1.26.4 or later, enter the example you want to run:

```sh
cd examples/pgx # Or examples/sql, or examples/bun, from the checkout root.
go run .
```

The examples have separate modules; Bun does not become a library dependency.
Each example opens a connection to the current primary, prints the PostgreSQL
server address, and closes cleanly. A service keeps the returned handle for its
lifetime and uses ordinary query context deadlines.

The simple API requires the plugin's new connection metadata. Before matching
releases are published, use a Go workspace with the plugin, library, and selected
example modules. Replica routing and pool tuning are optional; see
[configuration](../docs/usage.md).
