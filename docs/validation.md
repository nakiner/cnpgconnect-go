# Validation record

## Sustained pool recovery — 2026-09-19

The paired plugin/library runner passed nine selected suites without skips or
failures against Kubernetes 1.34.0, unmodified CNPG 1.30.0 and PostgreSQL 18.4,
with two plugin replicas and three database instances. The source was held
unchanged and verified by hash. The owned kind cluster was removed afterward;
no production cluster was modified.

The new soak warmed 32 persistent pools (16 pgx and 16 `database/sql`) to
64 connections, then ran 64 query workers through five promotion/discovery-outage
cycles in 3m11s. Every worker recovered using its original pool handle: promotion
recovery took 3.151–5.127 seconds, and discovery restoration took 8.637–11.878
seconds including Pod startup. Each outage verified deadline failures after
topology expiry while a direct PostgreSQL connection remained usable.

There were 68,939 successful primary queries out of 70,140 attempts, with 1,201
errors during fault injection and no observed standby results. Successful-query
p95 had a 16.384 ms histogram upper bound; all-attempt p99 was bounded by the
4,194.304 ms bucket, including outage timeouts. The sampled connection peak on
the current primary was 64, and closing the pools left none there. Sampling
does not establish a simultaneous cluster-wide connection maximum. pgx may
refill connections gradually after resets.

Existing suites also passed prepared-statement reuse, primary Pod deletion,
open-socket discovery stalls, and certificate rotation. The new test is opt-in
for local/manual runs and enabled in weekly CI; see the
[integration guide](../test/integration/README.md). These short local runs do
not establish sustained fleet capacity or external-network behavior. Queries
and transactions are never replayed. The public API and library runtime are
unchanged by this qualification work.

## pgx connection ownership and retirement — 2026-09-19

The adapter now stores immutable connection identity in pgx CustomData and uses
native Pool.Reset when an established route becomes ineligible. `make check`
passed, including the full race suite, vet, examples and integration compilation;
the pgxpool/stdlib packages also passed five race repetitions. Tests verify
borrowed-session survival, callback behavior, constructor cancellation and
bounded route history. Configuration and public signatures are unchanged.

The matching plugin/client pair then passed eight isolated suites with no
failures or skips against Kubernetes 1.34.0, CNPG 1.30.0 and PostgreSQL 18.4,
using Go 1.27.1. Three lifecycle repetitions retained the same pgx, database/sql
and prepared-statement handles through planned promotion and primary Pod loss.
Discovery outage and open-socket stall recovery passed. The plugin's CA Secret
update and certificate renewal checks passed, and the disposable cluster was
removed. No production environment was modified.

Whole-pool retirement can reconnect healthy sessions in a multi-member pool;
held connections remain usable until release. Freshness heartbeats do not reset
the pool. These smoke tests do not establish a production capacity or latency
guarantee. The paired plugin's performance guide records recovery measurements.

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
