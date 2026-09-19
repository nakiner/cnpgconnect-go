# Unreleased hardening contract

These changes are in the source checkout; they are not a claim that existing
published client/plugin versions include them. Go 1.27.1 and patched gRPC
v1.83.2 are used by this revision. Use the matching plugin/client changes.

## Freshness and withdrawal

- Positive snapshots older than the accepted observation high-water mark cannot
  refresh routing. Replaying one timestamp cannot extend its original deadline.
- A same-Cluster-UID unavailable/transitioning update revokes routes even when
  received from a replica carrying an older observation. Retain the high-water
  mark; restoring the view requires a newer positive observation.
- At an equal observation timestamp, removing an eligible member, making it
  unready, or removing its selected endpoint revokes that member. Replaying the
  earlier healthy snapshot cannot restore it. A newer observation can restore
  it. Retained member evidence is bounded to the observation's membership.
- An older same-UID snapshot can withdraw an explicitly ineligible member
  without replacing healthy routes or extending their freshness. A member
  absent from an older snapshot is left alone: that observation may predate its
  creation. Equal-time positive replays cannot restore a withdrawn member;
  recovery requires a newer observation.
- Connection metadata updates still invalidate generations. A stale target never
  becomes valid again merely because its host or member ID is reused.
- A usable snapshot bounds the watch receipt watchdog by its remaining effective
  validity, including a reconnect that has not received its first response.
  Initially unavailable/expired streams use MaxSnapshotTTL instead of a tight
  reconnection loop.

This is fail-closed discovery, not consensus or fencing. Observer clocks must be
synchronized. PostgreSQL/CNPG remain responsible for actual failover and fencing.
A reported synchronous/quorum standby does not establish linearizable reads.

## Managed pools

Initial pool startup retries transient PostgreSQL connection-establishment
failures, such as a socket/TLS EOF, connection refusal, or a server still starting
up. Retries use a short capped backoff within the caller's deadline, or
StartupTimeout when the caller has no deadline. Authentication, certificate
verification, configuration and application-hook errors fail immediately.
Established-connection errors and application SQL are never retried by this
startup mechanism; Open returns only after a successful connectivity check.

Native pgx `Reset` retires connections when a route used by the pool becomes
ineligible. It does not invoke borrower callbacks (`BeforeAcquire`, `PrepareConn`,
`AfterRelease`); those still run on real borrows/releases. Freshness refreshes and
changes outside the pool's routes leave it untouched. A reset can retire healthy
sessions in a pool using several members, but held connections remain usable
until released. Shutdown retains its cleanup barrier.

Each connection stores its immutable pool/route identity in pgx's `CustomData`.
There is no per-connection tracking map or cleanup goroutine. Application
metadata uses other keys; as with other pgx methods, `Pool.Valid` requires
exclusive ownership of its connection. The pool retains only distinct routes
and removes invalid identities on discovery updates, including endpoint churn.

With automatic network selection, a successfully verified external connection
teaches **that pool** a temporary preference for that complete target identity,
including generation/endpoints. The cache holds at most 128 entries, expires
entries after one minute, and does not renew a preference merely by reading it.
Expired preferences retry internal first. An explicit Network choice is never
changed, and ambiguous shared resolved addresses do not teach a preference.
Custom LookupFunc host:port results are normalized like pgx, including IPv6 and
port overrides; the preference cache does not change TLS or role verification.

Valid uses the routing mutex and performs no allocations for primary and
replica/zone-fallback checks. It still lazily processes expiry.
No existing query or transaction is transparently replayed.

## Monitoring

`(*Client).Statistics()` returns atomic, monotonic process-local counters for
watch attempts, watchdog timeouts, accepted/rejected snapshots, and expiry
transitions. It adds no exporter or identity labels. Construct a shared Client
and pass it as the pool's Resolver when you need to export these discovery
counters. They are event counts, not an atomic snapshot of routing state;
`Status()` supplies current topology/freshness information.

Use native `Pool.Stat()` / `sql.DB.Stats()` for pool occupancy/waiting, and pgx
connect/query tracers for individual connection failures. Do not attach
unbounded member IDs or raw errors as monitoring labels. The plugin publishes
bounded observation/admission/freshness metrics on its private health port.

## Verification and operational limits

Run `make check` and the pinned govulncheck gates. The paired plugin's
`scripts/run-isolated.sh` builds real CNPG fixtures and executes these tests,
including open-socket discovery stalls and repeated failover recovery:

```sh
cd /path/to/cnpg-connect-plugin
CNPGCONNECT_GO_E2E_RECOVERY_SLO=60s \
  ./scripts/run-isolated.sh /path/to/cnpgconnect-go work/isolated 3
```

The runner requires a disposable, unused `kind-cnpg-connect-test` context and
creates an explicit temporary kubeconfig itself; it refuses to reuse existing
clusters. The integration workflow allows an exact peer commit for paired
qualification and uploads test/measurement evidence, never credentials.

Recovery measurements count action-start through successful primary queries on
pgx, SQL and prepared SQL handles. They are not production population p95/p99,
end-to-end first-write latency, or a substitute for storage/network load tests.
Real leaf-certificate rotation is covered; root-CA migration and CNI-specific
network policy enforcement still need environment-specific staging validation.
