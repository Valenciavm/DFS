# High availability: PostgreSQL lease, second NameNode, fencing and degraded read-only mode

**Depends on:** issue 03 (`PostgresMetadataStore` and the `leader` row).
Issue 02's HA status route may exist as a placeholder; update it here.
**Branch:** `feat/namenode-ha`, from the PostgreSQL branch.
**Unblocks:** issue 09 (full deployment) and issue 10 (HA evidence T5, T16,
T17, T20).

## Context

The assignment gives 15% to high availability and requires the service to be
highly redundant against service and data loss. Two NameNodes in front of one
PostgreSQL instance are not enough; issue 03 provides a primary/standby
metadata database locally and RDS Multi-AZ in production. This issue makes the
NameNode service fail over between two processes while preserving metadata.

The current core has `self.term`, `begin_failover_grace()`, soft-state rebuild
from block reports and a placeholder `api_server`, but no lease acquisition,
renewal, fencing or second process. `ClusterState` is intentionally memory-only.
Read `docs/ARCHITECTURE.md` Section 5.2, Section 7, Section 9 and decisions 7–8
in Section 16 before coding.

## Lease protocol

Use the PostgreSQL `leader` singleton row:

```text
holder, url, term, lease_expires_at
```

Implement store-level atomic operations:

1. **Acquire:** one SQL transaction succeeds only if the lease is expired or
   already held by this NameNode. It increments `term` when ownership changes,
   records `holder` and `url`, and returns the new term.
2. **Renew:** succeeds only when `holder == self.node_id` and
   `term == self.term`, extending `lease_expires_at`.
3. **Release/stepdown:** conditional on the same holder and term; clears or
   expires the lease so the standby can acquire it.
4. **Read current leader:** returns holder, URL, term and expiry for
   `NOT_LEADER` responses and `/v1/ha/status`.

The lease duration is `LEASE_S` (default 15 s), renewed every `LEASE_S/3`.
Use UTC wall-clock values in SQL; do not compare them to the monotonic clock.
The in-memory tests may use an injectable fake wall clock, but do not fake the
process identity or the conditional SQL semantics.

## Fencing

Every hard-state write must be guarded by `(holder, term)` in the same SQL
transaction. A stale/zombie NameNode must receive a failure and must not write
metadata even if it has not noticed that it lost the lease.

Every DataNode response carries `term`; every command carries `term`. A DataNode
ignores commands whose term is lower than the highest term it has seen. When it
sees a higher term it sends a full block report immediately. Update issue 04's
control loop if necessary.

Every client-facing mutating route must return `503 NOT_LEADER` from a standby,
with `leader_url` in the response body. The client follows it (issue 08).
Read routing must be explicit and safe: a standby should either redirect/reject
reads according to the contract, or serve only from a fully loaded cache once
that behavior is documented and tested. Do not accidentally serve an empty
cache during takeover.

## Takeover sequence

On process startup:

1. connect to PostgreSQL;
2. attempt lease acquisition;
3. if standby, load the leader URL/term and expose `/v1/ha/status`;
4. if leader, load all hard state into the in-memory store;
5. clear soft state and call `begin_failover_grace()`;
6. accept DataNode registration and block reports;
7. only after the grace period run normal liveness and replication monitoring.

A DataNode report rebuilds `ClusterState.replicas` and checksums. A pending
upload stays `PENDING`; a client may retry `commit` and receive
`INSUFFICIENT_REPLICAS` until reports arrive, then succeed without re-uploading
already stored blocks.

## Degraded read-only mode

The design intentionally does **not** turn a temporary database outage into a
total outage:

- if a leader cannot renew its lease because the database is unreachable, it
  keeps its current leadership locally and marks itself `degraded`;
- reads that are safe from the in-memory cache may continue (`ls`, `stat`,
  `get`/block map and data-plane reads);
- all metadata writes return `503` while degraded;
- `/v1/ha/status` reports `degraded: true`;
- no standby may acquire the lease without the database, so this mode does not
  create split brain;
- once the database is reachable, the leader verifies its term and exits
  degraded mode, or steps down if fencing says it lost leadership.

Be conservative: if cache validity is uncertain, reject the operation rather
than serving potentially stale metadata. The DataNode gRPC data plane does not
need the NameNode for bytes already authorized by valid block tokens.

## REST/API updates

Complete/update:

- `GET /v1/ha/status` with `node_id`, `role`, `term`, `degraded`, `leader_url`,
  and `lease_expires_at`;
- `POST /v1/ha/stepdown`, admin-only, for a planned handover and a reproducible
  demo;
- `503 NOT_LEADER` body with `leader_url` on standby requests;
- any DataNode response that currently lacks `term`;
- API documentation/changelog if the existing placeholder shape changes.

## Tests

Use two NameNode instances against a PostgreSQL test database (or a deterministic
store double that executes the same conditional operations):

- only one of two simultaneous contenders acquires a lease;
- same holder can renew; old holder/term cannot;
- a stale leader cannot write after takeover;
- term increases on ownership change;
- standby points clients to the current leader;
- planned `/stepdown` makes the other process acquire the lease;
- kill leader during a `put`, then retry `commit` after DataNode reports (T16);
- restart all NameNodes and rebuild namespace plus replicas (T17);
- failover grace prevents premature DataNode death;
- lower-term DataNode commands are ignored and higher-term commands trigger a
  report;
- database outage produces read-only degraded mode, not an unhandled crash;
- writes resume after database promotion/reconnection (T20).

## Acceptance checklist

- [ ] Two NameNodes share one metadata database and never both write.
- [ ] Lease acquisition, renewal, release and fencing are atomic in SQL.
- [ ] `term` is propagated to DataNodes and commands.
- [ ] Soft state is rebuilt from register/block-report, not persisted as SQL.
- [ ] Pending uploads survive leader failure.
- [ ] Degraded read-only behavior is observable and tested.
- [ ] Planned failover and killed-leader scenarios are reproducible.
- [ ] Full tests and ruff pass; docs and API changelog match implementation.
