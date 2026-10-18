# PostgreSQL MetadataStore, schema, migrations and replicated metadata

**Depends on:** the auth/ACL core pull request. Persist its authentication
state and ACL model through a PostgreSQL implementation of `AuthStore`,
separate from `MetadataStore`. This can be developed in parallel with issue 02
after that pull request, but production metadata-store wiring is needed by
issue 06.
**Branch:** `feat/postgres-metadata-store`, from the tip of the auth branch.
**Unblocks:** issue 06 (HA lease and second NameNode).

## Context

The NameNode core in PR #4 talks only to `MetadataStore`, an abstract interface
in `server/nodes/namenode/store/base.py`. The current implementation is
`InMemoryMetadataStore` in `store/memory.py`; it is deliberately used for unit
tests and local development. It stores the namespace tree, files and blocks in
memory and maintains the indexes required by the core:

- `files_by_id` equivalent (`get_file_by_id`)
- `files_by_path` equivalent through the directory tree (`get_file`, `stat`)
- `blocks_by_id` equivalent (`get_block`, `file_of_block`)
- pending uploads, committed files and aborted-retention records

The core must not know whether it is talking to memory or PostgreSQL. Implement
the same interface in `store/postgres.py`; do not add SQL to `core.py`.
Authentication persistence is a separate boundary: implement the
`AuthStore` interface for users, API keys, groups, ACLs and audit events.
These records may share the PostgreSQL database and migrations, but must not
be folded into the `MetadataStore` interface or its namespace snapshot.

Hard state must survive a NameNode restart and be visible to both NameNodes.
Soft state must **not** be persisted here: live DataNodes, replica locations,
pipelines, pending commands, in-flight copies and reserved bytes remain in the
leader's `ClusterState` and are rebuilt from registration and block reports.

## Required topology

Use PostgreSQL, not MySQL. The schema and pinned driver (`psycopg[binary]`) are
already PostgreSQL-specific.

| Environment | Metadata database |
|---|---|
| Unit tests | `InMemoryMetadataStore`; no database connection |
| Local demo | PostgreSQL primary + streaming hot standby; only primary receives writes; promote standby with `pg_promote()` |
| AWS production | Amazon RDS Multi-AZ PostgreSQL; use the stable RDS endpoint; if RDS is unavailable in AWS Academy, document the fallback of primary + replica on two EC2 instances |

The replica is a failover mechanism, not a read pool. The NameNode leader reads
from its in-memory cache for normal operations; the database is the source of
truth for hard-state writes and reloads.

## Schema

Implement migrations (plain numbered SQL files are acceptable; do not rely on
an undocumented manual `CREATE TABLE`). The namespace schema must cover the design in
`docs/ARCHITECTURE.md` Section 5.5. The authentication tables below must
implement the separate `AuthStore` contract:

```text
users
  id, username UNIQUE, password_hash, role, created_at,
  totp_secret_encrypted NULL, totp_enabled, totp_last_step
recovery_codes
  user_id, code_hash, used_at NULL
api_keys
  id, user_id, name, key_hash, created_at, revoked_at NULL
groups
  id, name UNIQUE, owner_id
group_members
  group_id, user_id, PRIMARY KEY(group_id, user_id)
inodes
  id, parent_id NULL, name, type(dir/file), owner_id,
  current_file_id NULL, created_at, modified_at,
  UNIQUE(parent_id, name)
files
  file_id UUID PRIMARY KEY, inode_id, owner_id, size, block_size,
  replication, status, checksum NULL, created_at, modified_at, expires_at NULL
blocks
  block_id PRIMARY KEY, file_id, idx, size, checksum NULL,
  UNIQUE(file_id, idx)
acls
  inode_id, subject, permission, PRIMARY KEY(inode_id, subject)
locks
  lock_id, inode_id, owner_id, mode, expires_at
pending_deletes
  block_id PRIMARY KEY, requested_at
leader
  singleton row: holder, url, term, lease_expires_at
audit_log
  id, ts, user, operation, path NULL, result, request_id NULL
```

Use foreign keys and indexes for `(parent_id, name)`, `files.file_id`,
`blocks.file_id`, `blocks.block_id`, ACL subject lookup and audit timestamps.
Use UTC timestamps. Use PostgreSQL `uuid` for `file_id`; the contract's
`block_id` remains an opaque string. Do not add `datanodes` or
`block_replicas` tables: those are soft state.

## Transaction and cache rules

- Every mutation is one transaction. The store updates the in-memory cache
  only after the transaction commits successfully.
- `create_pending_file` reserves a path atomically. Two concurrent uploads of
  the same path cannot both win; return the contract's
  `UPLOAD_IN_PROGRESS`/`ALREADY_EXISTS` error as appropriate.
- `commit_file` atomically changes `PENDING` to `COMMITTED`, links the inode,
  swaps an overwrite, and returns the replaced file's metadata for deletion
  scheduling. Readers must see the old complete version or the new complete
  version, never a mix.
- `abort_file` is idempotent and keeps `ABORTED` records until the retention
  period so late block reports are recognized as orphans.
- Every write must be guarded by the future leader lease check from issue 06:
  the transaction must reject a stale `(holder, term)`. Design the store API
  now with an optional `LeaderGuard`/transaction context so issue 06 does not
  require rewriting every method.
- On leader takeover, provide `load_snapshot()` that reconstructs only the
  `InMemoryMetadataStore` namespace cache from PostgreSQL. It must not load
  soft state. `AuthStore` must independently reload its durable records.

## Local replication acceptance

Add a local primary/standby setup and scripts or documented commands to:

1. initialize the primary and standby;
2. create a committed file and verify it exists after restarting a NameNode;
3. stop the primary;
4. promote the standby with `pg_promote()`;
5. point the NameNodes at the promoted endpoint/configuration;
6. verify namespace and file metadata are intact and writes resume.

Do not claim automatic failover here unless issue 06 has implemented the lease
and reconnection logic. This issue proves metadata persistence and promotion;
issue 06 proves service-level takeover.

## Tests

- Every `MetadataStore` contract test currently aimed at
  `InMemoryMetadataStore` must also run against PostgreSQL when
  `POSTGRES_TEST_DSN` is set; otherwise the suite stays database-free.
- Rollback: a failed insert leaves no partial inode/file/block rows.
- Unique path and unique `(file_id, index)` constraints are exercised under
  concurrent transactions.
- Pending file invisible until commit; overwrite atomic; abort idempotent.
- Restart/load snapshot restores directories, files and blocks; auth-store
  tests verify users, API keys, groups, ACLs and audit records independently.
- Replica/pending-command soft state is not in SQL.
- SQL parameters must always be bound parameters, never string interpolation.

## Acceptance checklist

- [ ] `PostgresMetadataStore` implements every method in `MetadataStore`.
- [ ] Numbered migrations create the complete schema and indexes.
- [ ] Auth, group, ACL, lock, audit and lease rows are represented.
- [ ] No DataNode liveness/replica-location table is introduced.
- [ ] Transaction rollback and atomic commit tests pass.
- [ ] `POSTGRES_TEST_DSN` integration tests are opt-in and documented.
- [ ] Local primary/standby promotion is reproducible.
- [ ] Existing in-memory tests and full `pytest`/`ruff` remain green.
- [ ] Update `docs/NAMENODE_MODELS.md`, `ARCHITECTURE.md` Section 5.5 and
  the API changelog if the wire-visible behavior changes.
