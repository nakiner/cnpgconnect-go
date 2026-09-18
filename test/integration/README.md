# cnpgconnect-go live CloudNativePG integration

This historical fixture exercises the advanced connection configuration against
an isolated local cluster, including optional bearer authentication. Its token,
certificate files, and forwarding setup are test infrastructure, not requirements
for the [normal application API](../../README.md#connect). `make check` also tests
the simple API over real gRPC and PostgreSQL TLS sockets without a plugin token.

This opt-in suite requires an existing isolated kind cluster named
`cnpg-connect-test`. It refuses any other current kubeconfig context. The test
does not create or delete a cluster, install an operator, or start port-forwards.

Prepare:

- CloudNativePG 1.30 and `cnpg-connect-plugin`, with a healthy three-instance
  Cluster `databases/app-db` enrolled in discovery.
- Discovery TLS and token Secrets `cnpg-system/connect-application-tls` and
  `cnpg-system/cnpg-connect-auth`, using keys `ca.crt` and `token`.
- CNPG-generated Secrets `databases/app-db-ca` (`ca.crt`) and
  `databases/app-db-app` (`username` or `user`, `password`, `dbname`).
- Plugin external endpoint mappings for `app-db-1`, `app-db-2`, `app-db-3` to
  `127.0.0.1` ports `7541`, `7542`, `7543`, each with PostgreSQL certificate name
  `app-db-rw.databases.svc`.
- A stable TCP path from these ports to each member's PostgreSQL port and from
  `127.0.0.1:7443` to the plugin discovery service. The discovery certificate
  must cover `connect-api.cnpg-system.svc`.

The validated local setup uses per-member NodePort Services and a TCP gateway
on the kind network, preserving end-to-end TLS. Each PostgreSQL Service must
select one specific instance. A Pod port-forward can also work for basic smoke
checks, but the local kubectl version stopped forwarding after a PostgreSQL
connection ended with a reset. The discovery outage test additionally needs
its gateway to survive plugin Pod replacement. Use a stable TCP gateway or
supervised forwarding for lifecycle tests.

Run from the library root:

```sh
CNPGCONNECT_GO_E2E_KUBECONFIG=/path/to/isolated-kind-kubeconfig \
  go test -tags=integration ./test/integration -v -count=1 -timeout=12m
```

`CNPGCONNECT_GO_E2E_ADDRESS` and `CNPGCONNECT_GO_E2E_SERVER_NAME` can override the
discovery address and certificate name. Secrets are read into memory; their
contents are never logged.

The test opens native pgx and SQL pools over verified TLS, performs writes and
reads, selects a synchronous standby, and asks CNPG to promote that standby.
The same pool handles and the same prepared SQL statement must then use the new
primary. SQL idle retention is deliberately enabled during this test to exercise
the session reuse guard in addition to the documented zero-idle configuration.

Set `CNPGCONNECT_GO_E2E_FAILOVER=1` to also delete the current primary Pod with a UID
precondition and check automatic recovery on another member. Its port-forward
will stop if Pod forwarding is used; surviving routes must remain active. The suite restores the
original synchronous replication policy and drops its uniquely named test table.
It leaves the new primary in place. All mutations verify the fixed Cluster UID
and selected context; Cluster patches also check the current resource version.

Only read-only readiness probes are repeated. Test writes are issued once.
PostgreSQL member identity in query results includes the server start timestamp,
because a Pod port-forward otherwise reports the same loopback address for every
member. The caller owns environment teardown after the suite exits.

## Discovery outage

Run the separately guarded outage test against the same fixture:

```sh
CNPGCONNECT_GO_E2E_KUBECONFIG=/path/to/isolated-kind-kubeconfig \
CNPGCONNECT_GO_E2E_DISCOVERY_OUTAGE=1 \
  go test -tags=integration ./test/integration \
  -run '^TestLiveDiscoveryOutage$' -v -count=1 -timeout=8m
```

It requires the plugin Deployment `cnpg-system/connect` to specify an explicit
positive `--ttl` of at most one minute. The test scales that Deployment to zero,
waits for every producer Pod to exit and for its snapshots to expire, then checks
that pgx, SQL, and prepared SQL acquisitions stop at their context deadlines.
A direct PostgreSQL control connection must remain healthy at the same time.
The test restores the original replica count and verifies that the same handles
resume automatically on the same primary. Scaling checks Deployment UID,
resource version, and replica count; cleanup restores replicas even on failure.

`make check` only compiles these tagged tests. It never runs either live test.
