# Connect to primary and sync

This example opens a primary pool and a strict synchronous-replica pool sharing
one discovery client. It prints each server address and recovery state, then exits.

Set the connection environment variables in the [examples guide](../README.md).
The cluster must have a ready synchronous replica; the reader has no fallback.

From the repository root:

```sh
go run ./examples/roles
```

Example output; addresses depend on your cluster:

```text
role=primary server=10.244.0.63 recovery=false
role=sync server=10.244.1.97 recovery=true
```

Use `writer` for primary operations and `reader` for reads from sync. The library
routes each pool's new acquisitions by role; the application chooses the pool.
