# DFSha — Pending work, as GitHub issues

This folder contains the remaining implementation plan as self-contained GitHub
issue drafts. Each numbered file can be copied into GitHub as one issue; its
first heading is the proposed title. The drafts include context, dependencies,
implementation requirements, tests and an acceptance checklist so another
agent can continue without this conversation.

> **Terminology:** numbered issue files are project-work drafts; pull request
> numbers are GitHub references. Completed work is listed separately below and
> is not an outstanding issue.

## How to work from an issue

1. Read the whole issue and the referenced sections of `docs/API.md` and
   `docs/ARCHITECTURE.md` before editing code.
2. Start from the dependency branch named by the issue. The PR stack must be
   reviewed/merged in order; do not branch all the way back from `main` unless
   the dependency has already been merged.
3. Use a focused branch, normally `feat/<short-name>` or `docs/<short-name>`.
4. Use several small **Conventional Commits**, for example
   `feat(auth): add password verification` or
   `test(namenode): cover lease fencing`.
5. Add tests with the implementation. Core tests use the fake clock in
   `server/common/clock.py`, the in-memory store and seeded randomness; do not
   require a real network/database for ordinary CI.
6. Run the complete checks before opening the PR:

   ```powershell
   .\.venv\Scripts\python.exe -m pytest -q
   .\.venv\Scripts\python.exe -m ruff check .
   ```

   On Windows PowerShell, redirect long test output to a temporary file and
   inspect it with `Get-Content` if the terminal output is truncated.

## Current baseline

The current `main` baseline includes merged PRs #1–#4 and #6. It has the
NameNode core, an in-memory `MetadataStore`, weighted placement,
WORM upload/commit/abort, read maps, block reports, re-replication, garbage
collection, stats and background loops. The DataNode pipeline recovery,
ranged reads, idempotent writes and gRPC block-token interceptor are included
in PR #6.

Important known gaps at this baseline: no REST layer, no PostgreSQL store, no
leader lease/second NameNode, no TLS/mTLS or encrypted at-rest blocks, no
complete DataNode control loop, no client library/CLI and no Compose/cloud
deployment. The auth/ACL core is implemented in this branch and will be delivered by PR #7;
it is not part of the current `main` baseline until that pull request is
merged.

## Current PR stack

```text
main
 ├─ PRs #1–#4  data plane, server common and NameNode core
 └─ PR #6      DataNode pipeline recovery, ranged reads and block-token interceptor
```

PRs #1–#4 and #6 are merged. PR #5 (a proposed revert of PR #1) was closed
without merging and is not a completed deliverable. Check GitHub for current
status rather than treating old draft branch names as active work.

## Issue roadmap

| Issue | Draft | Depends on | Main deliverable | Unblocks |
|---|---|---|---|---|
| 02 | [`02-namenode-rest-api.md`](02-namenode-rest-api.md) | PR #7 | FastAPI adapter, contract errors, tracing, `mv`, locks, file/node/admin/HA routes | 04, 08 |
| 03 | [`03-postgres-metadata-store.md`](03-postgres-metadata-store.md) | PR #7 | PostgreSQL hard-state and auth stores, migrations, local primary/standby and RDS plan | 06 |
| 04 | [`04-datanode-control-loop.md`](04-datanode-control-loop.md) | 02 | DataNode registration, heartbeats, reports, commands and retries | 08, 09 |
| 06 | [`06-ha-lease-and-second-namenode.md`](06-ha-lease-and-second-namenode.md) | 03 | PostgreSQL lease, fencing, second NameNode, failover and degraded mode | 09, 10 |
| 07 | [`07-security-tls-and-encryption.md`](07-security-tls-and-encryption.md) | PR #7, 02, 04, PR #6 | TLS/mTLS, AES-256-GCM at rest, TOTP 2FA | 09, 10 |
| 08 | [`08-client-library-and-cli.md`](08-client-library-and-cli.md) | PR #7, 02, 04, PR #6 | Python client, CLI, send/receive, put/get, open/read/lock and fallback | 09, 10 |
| 09 | [`09-docker-compose-and-cloud-deploy.md`](09-docker-compose-and-cloud-deploy.md) | 06, 07, 08 | Five-DataNode Compose cluster, README, env/certs and AWS/GCP deployment | 10 |
| 10 | [`10-test-plan-execution-and-report.md`](10-test-plan-execution-and-report.md) | 09 | T1–T24 evidence, benchmarks, final report and 10–15 minute video | final |

Issues 02–07 are mostly server work and can be parallelized only after their
listed dependencies exist. Issues 08–10 are integration work and should be
done against a stable server/security baseline.

## Completed work

- Auth/ACL core: implemented in PR #7; pending merge into `main`.
- DataNode pipeline resilience and ranged reads: merged in PR #6.

## Reference documents

- [`docs/API.md`](../API.md) — wire contract and source of truth for endpoint,
  authentication, error, block and tracing semantics.
- [`docs/ARCHITECTURE.md`](../ARCHITECTURE.md) — Option 1 architecture,
  algorithms, availability/security decisions, rubric mapping, T1–T24 and
  report/video plan.
- [`docs/NAMENODE_MODELS.md`](../NAMENODE_MODELS.md) — hard/soft state and
  store model.
- [`docs/GRPC_BLOCK_SERVICE.md`](../GRPC_BLOCK_SERVICE.md) — current DataNode
  block service and known data-plane gaps.

The assignment PDF is not stored in this repository; its requirements are
reflected in `ARCHITECTURE.md` Section 14 and the closed decisions in Section
16. The final evidence issue must still distinguish what is implemented from
what is only designed.
