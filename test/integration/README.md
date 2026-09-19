# cnpgconnect-go live CloudNativePG integration

This fixture exercises the advanced connection configuration against
an isolated local cluster, including optional bearer authentication. Its token,
certificate files, and forwarding setup are test infrastructure, not requirements
for the [normal application API](../../README.md#connect). `make check` also tests
the simple API over real gRPC and PostgreSQL TLS sockets without a plugin token.

For a complete run, the paired plugin's runner creates the isolated cluster,
installs CNPG and the fixture, runs these tests, and tears down its cluster:

```sh
cd /path/to/cnpg-connect-plugin
./scripts/run-isolated.sh /path/to/cnpgconnect-go work/isolated 3
```

To run the Go tests directly, this opt-in suite requires an existing kind cluster named
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

`make check` only compiles these tagged tests. It never runs live tests.

## Sustained recovery

The optional soak test keeps independent application pools and their discovery
subscriptions alive through repeated promotions and complete plugin outages:

```sh
CNPGCONNECT_GO_E2E_KUBECONFIG=/path/to/isolated-kind-kubeconfig \
CNPGCONNECT_GO_E2E_SOAK=1 \
CNPGCONNECT_GO_E2E_SOAK_CLIENTS=32 \
CNPGCONNECT_GO_E2E_SOAK_POOL_SIZE=2 \
CNPGCONNECT_GO_E2E_SOAK_DURATION=3m \
  go test -tags=integration ./test/integration \
  -run '^TestLiveRecoverySoak$' -v -count=1 -timeout=30m
```

Both paired lifecycle workflows run this profile weekly and expose a `soak`
checkbox on manual dispatch. Select matching `peer_ref` commits. CI requires a
passing soak result when enabled; an older runner silently ignoring the option
cannot satisfy that check.

| Variable suffix (`CNPGCONNECT_GO_E2E_`) | Default | Accepted range |
| --- | --- | --- |
| `SOAK_CLIENTS` | `24` | 2–128 independent pools |
| `SOAK_POOL_SIZE` | `2` | 1–8 connections and query workers per pool |
| `SOAK_DURATION` | `3m` | Positive duration, at most `10m` |
| `RECOVERY_SLO` | `1m` | Positive duration, at most `3m` per recovery |

Half the pools use native pgx and half use `database/sql`. The test first verifies
that every configured connection is open. It reserves 16 PostgreSQL connection
slots for the fixture and control probes; combinations exceeding that budget
fail before opening the application pools. Each worker issues one read every
100 ms with a three-second deadline, checking a unique sentinel committed once
before the faults. No writes or failed application queries are replayed.

Each cycle promotes a standby, requires every worker to observe the new primary
within the recovery budget, and scales discovery to zero until snapshots expire.
The same pool handles must reject acquisitions at their context deadlines while
a direct PostgreSQL probe remains healthy. After discovery restarts, every
worker must recover within the budget. The requested duration covers the fault
cycles after warmup; at least one complete cycle runs, and a cycle already in
progress finishes before exit. Setup and cleanup add to wall time. The separate
`TestLiveDiscoveryStall` test covers an open connection that stops delivering
discovery data; this soak test exercises complete outages instead.

`CNPG_SOAK_RECOVERY` logs action-to-all-workers recovery time for each fault.
`CNPG_SOAK_SUMMARY` reports query errors and fixed-memory latency histograms;
percentiles are powers-of-two upper bounds in milliseconds. Successful primary
reads have separate percentiles from all attempts, which include deliberate
outage timeouts. PostgreSQL connection sampling runs every 250 ms against the
current primary and counts only application pools, excluding the control pool,
short direct probes, and sessions on other instances. This is a sampled primary
peak, not a cluster-wide maximum. Counts above twice the configured application
budget fail, allowing bounded overlap while old sockets retire. Cleanup restores
the original discovery replica count, closes every pool, drops the sentinel
table, and requires the primary's application connection count to reach zero.
