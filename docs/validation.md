# Validation record

## Unreleased reconnect and expiry improvements — 2026-09-18

Current source after `v0.0.5` passed `make check`, including race tests, vet,
all adapter examples, and integration-test compilation. Deterministic clock
tests cover exponential jitter across repeatedly short-lived streams, reset
after a stable stream, cancellation during receive/backoff, and freshness
deadlines extended or shortened by a newer observation. Idle subscribers are
notified when routing expires, without a periodic 100 ms expiry ticker.

The default startup timeout is now 30 seconds in both the discovery client and
the simple pgx pool constructor; earlier caller deadlines still apply. The
public API and generated protobuf dependency are unchanged. These changes
have not been deployed for a new live failover test; the earlier live records
below remain historical validation, not acceptance of this revision.

## Rename and shared API migration — 2026-09-18

After renaming the module to `github.com/nakiner/cnpgconnect-go` and switching
to `github.com/nakiner/cnpg-connect-plugin/api/connect/v1` from plugin `v0.0.3`:

- `make check` passed: formatting, race tests, vet, example compilation, and
  compile-only validation of guarded integration tests.
- Root tests and the Bun example compiled and passed under Go 1.26.4, the new
  minimum required by the plugin module.
- The client CI workflow passed actionlint with ShellCheck enabled.
- `go list -deps ./...` includes the canonical generated API package and no
  Kubernetes client or plugin observer packages.

Live Kubernetes failover tests were not rerun for this migration. The results
below record the original implementation before the rename and API migration.

## Original live environment — 2026-09-17

Validated locally on 2026-09-17 using CloudNativePG 1.30.0, PostgreSQL
18.4-system-trixie, and the plugin built in the preceding implementation step.
The isolated `kind-cnpg-connect-test` cluster ran Kubernetes 1.37.0 and
cert-manager 1.21.2. Tests used an explicit kubeconfig and guarded fixed resource
identities. No existing Kubernetes cluster was used.

Discovery used gRPC TLS with bearer authentication. PostgreSQL used certificate
verification with the discovered server name and the CNPG CA. External addresses
were modeled by a temporary TCP gateway and per-instance NodePort Services;
the same endpoint continued to identify its member after role and Pod changes.
This replaced kubectl port-forward, which terminated on a PostgreSQL TCP reset.

## Live results

`TestLiveLifecycle` passed in 29.42 seconds:

- Native pgx, `database/sql`, and a prepared SQL statement initially reached the
  same primary and successfully wrote/read test data.
- A strict synchronous-replica policy selected a PostgreSQL standby.
- After planned promotion, the same pool, SQL DB, and prepared-statement handles
  reached the new primary. Another write succeeded and prior data was present.
- After deleting the elected primary Pod, those same handles recovered again;
  subsequent writes and row counts succeeded without rebuilding any handle.
- SQL idle reuse was deliberately enabled to exercise the session eligibility
  check as well as normal pgx acquisition checks.

`TestLiveDiscoveryOutage` passed in 43.31 seconds:

- The discovery Deployment was scaled to zero and all producer Pods stopped.
- Once the configured topology TTL expired, pgx, SQL, and prepared SQL calls
  waited for fresh routing and reached their request deadlines.
- A direct control connection confirmed PostgreSQL stayed reachable throughout.
- Restoring discovery let the same application handles resume on the same
  primary without application-managed reconnection.

The runnable pgx, SQL, and Bun examples also passed against this cluster. Bun's
example uses the returned `*sql.DB`; its dependencies live in a separate module.

## Automated checks

`make check` passed: formatting, race tests, vet, example compilation, and
compile-only validation of guarded integration tests. Unit/protocol tests cover
stream reconnection and stalls, expiry, older snapshots after reconnect, policy
fallback, endpoint/Pod identity, active transactions, hook/TLS preservation,
configuration copying, and bounded shutdown/startup behavior.

The original root test suite also passed under Go 1.25.0, its declared minimum
at the time. The current module requires Go 1.26.4 or later because it imports
the published plugin API.
The temporary cluster, forwarding container, and test credentials were removed
after validation.

The original root module had no Kubernetes or private GitLab dependencies.
The current client imports the generated API from the plugin module, which
declares Kubernetes dependencies; application packages do not import its
observer or Kubernetes client. No source from `database-old` was copied
wholesale; its pool-lifecycle behavior informed the new implementation. Run the
live checks using [the integration guide](../test/integration/README.md).

## Validation boundary

These checks establish local integration behavior, not production acceptance.
The actual load balancer, long network partitions, sustained load, certificate
and token rotation, and other PostgreSQL/CNPG versions still need deployment
testing. Primary loss here was a Kubernetes Pod deletion, not a node partition.

Automatic discovery and reconnection apply to acquiring connections. Active
transactions and explicitly held connections remain pinned, and interrupted
queries can fail. The library does not replay SQL or infer the outcome of an
interrupted commit.
