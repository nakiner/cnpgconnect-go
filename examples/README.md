# cnpgconnect-go examples

The pgx, `database/sql`, Bun, and role-routing examples share the same five inputs:

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

For an in-cluster plugin serving plaintext gRPC, use its DNS target and set the
optional transport variable. PostgreSQL TLS verification stays enabled:

```sh
export CNPG_DISCOVERY_ADDRESS='dns:///cnpg-connect-api.cnpg-system.svc.cluster.local:443'
export CNPG_DISCOVERY_INSECURE=true
```

With Go 1.26.8 or later, enter the example you want to run:

```sh
cd examples/pgx # Or examples/sql, or examples/bun, from the checkout root.
go run .
```

The pgx, SQL, and Bun examples have separate modules; Bun does not become a
library dependency. Each opens a connection to the current primary, prints the
PostgreSQL server address, and closes cleanly. A service keeps the returned handle
for its lifetime and uses ordinary query context deadlines.

For connections to primary and a synchronous replica, see the
[role-routing example](roles/README.md). It shares one discovery stream between
two pools and prints each server address and recovery state:

```sh
go run ./examples/roles # From the checkout root.
```

The simple API requires the plugin's new connection metadata. Before matching
releases are published, use a Go workspace with the plugin, library, and selected
example modules. Replica routing and pool tuning are optional; see
[configuration](../docs/usage.md).
