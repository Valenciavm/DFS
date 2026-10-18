# DFSha — Pending work, as GitHub issues

This folder contains the remaining implementation plan as self-contained GitHub
issue drafts. Each numbered file can be copied into GitHub as one issue; its
first heading is the proposed title. The drafts include context, dependencies,
implementation requirements, tests and an acceptance checklist so another
agent can continue without this conversation.

> **Terminology:** `Issue 01`–`Issue 10` below are the project-work issues in
> this folder. `PR #1`–`PR #4` are the pull requests currently open in GitHub.
> They are different numbering systems.

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

The current implementation baseline is the tip of `feat/namenode-core-rewrite`
(PR #4): the NameNode core has an in-memory `MetadataStore`, weighted placement,
WORM upload/commit/abort, read maps, block reports, re-replication, garbage
collection, stats and background loops. The integrated gRPC DataNode is in the
lower PRs. The baseline is verified at **350 passing tests** with `ruff check .`
clean.

Important known gaps at this baseline: no REST layer, no PostgreSQL store, no
leader lease/second NameNode, no enforced auth/ACL yet, no TLS/mTLS or encrypted
at-rest blocks, no complete DataNode control loop, no client library/CLI and no
Compose/cloud deployment. The issues below are ordered to close those gaps;
they are not claims that the features already exist.

## Current PR stack

```text
main
 └─ PR #1  chore/integrate-data-plane            (gRPC data plane ported)
     └─ PR #2  docs/align-with-deliverable-spec (contract/rubric alignment)
         └─ PR #3  feat/server-common           (config, errors, block tokens)
             └─ PR #4  feat/namenode-core-rewrite (NameNode core rewrite)
```

All four PRs were open at the time these drafts were created and should be
reviewed/merged in that order. If their status changes, use the actual current
branch/PR state rather than blindly copying the old stack.

## Issue roadmap

| Issue | Draft | Depends on | Main deliverable | Unblocks |
|---|---|---|---|---|
| 01 | [`01-auth-and-acl.md`](01-auth-and-acl.md) | PR #4 | Users, Argon2 passwords, JWT/API keys, groups, ACL enforcement and audit hook | 02, 03 |
| 02 | [`02-namenode-rest-api.md`](02-namenode-rest-api.md) | 01 | FastAPI adapter, contract errors, tracing, `mv`, locks, file/node/admin/HA routes | 04, 08 |
| 03 | [`03-postgres-metadata-store.md`](03-postgres-metadata-store.md) | 01 | PostgreSQL hard-state store, migrations, local primary/standby and RDS plan | 06 |
| 04 | [`04-datanode-control-loop.md`](04-datanode-control-loop.md) | 02 | DataNode registration, heartbeats, reports, commands and retries | 08, 09 |
| 05 | [`05-pipeline-resilience-and-ranged-reads.md`](05-pipeline-resilience-and-ranged-reads.md) | none; merge before 08 | Dead-hop recovery, ranged `ReadBlock`, `StatBlock`, idempotent writes | 08 |
| 06 | [`06-ha-lease-and-second-namenode.md`](06-ha-lease-and-second-namenode.md) | 03 | PostgreSQL lease, fencing, second NameNode, failover and degraded mode | 09, 10 |
| 07 | [`07-security-tls-and-encryption.md`](07-security-tls-and-encryption.md) | 01, 02, 04, 05 | TLS/mTLS, block interceptor, AES-256-GCM at rest, TOTP 2FA | 09, 10 |
| 08 | [`08-client-library-and-cli.md`](08-client-library-and-cli.md) | 01, 02, 04, 05 | Python client, CLI, send/receive, put/get, open/read/lock and fallback | 09, 10 |
| 09 | [`09-docker-compose-and-cloud-deploy.md`](09-docker-compose-and-cloud-deploy.md) | 06, 07, 08 | Five-DataNode Compose cluster, README, env/certs and AWS/GCP deployment | 10 |
| 10 | [`10-test-plan-execution-and-report.md`](10-test-plan-execution-and-report.md) | 09 | T1–T24 evidence, benchmarks, final report and 10–15 minute video | final |

Issues 01–07 are mostly server work and can be parallelized only after their
listed dependencies exist. Issues 08–10 are integration work and should be
done against a stable server/security baseline.

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
