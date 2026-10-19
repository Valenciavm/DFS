# PostgreSQL persistence and local replication

`POSTGRES_DSN` opts the NameNode into `PostgresMetadataStore` and
`PostgresAuthStore`. Without it, the NameNode continues to use in-memory
stores. Numbered SQL migrations run automatically at startup. The metadata
namespace and authentication/ACL data are separate store interfaces over the
same database.

The database stores hard state only. DataNode liveness, replica locations,
pipelines and pending commands remain in the NameNode's in-memory
`ClusterState`. The local standby is for manual recovery, not read scaling.
Issue 03 does not implement automatic NameNode failover, endpoint discovery or
database reconnection; those behaviors belong to issue 06.

## Run the PostgreSQL integration tests

Use a disposable PostgreSQL database. The test fixture creates a randomly
named schema, runs migrations inside it and drops that schema afterward; do
not point `POSTGRES_TEST_DSN` at a production database.

```powershell
$env:POSTGRES_TEST_DSN = "postgresql://postgres:example@localhost:5432/dfsha_test"
python -m pytest -q server/nodes/namenode/tests/test_store.py server/nodes/namenode/tests/test_postgres_store.py
```

Without `POSTGRES_TEST_DSN`, the suite does not connect to PostgreSQL and the
PostgreSQL-specific tests are skipped. The metadata-store contract tests
continue to run against the in-memory implementation.

## Start a local primary and streaming standby

These commands use the official PostgreSQL 16 image and a dedicated Docker
network. The credentials are for local development only.

```powershell
docker network create dfs-pg
docker volume create dfs-pg-primary
docker volume create dfs-pg-standby

docker run -d --name dfs-pg-primary --network dfs-pg --network-alias primary `
  -e POSTGRES_PASSWORD=local-postgres-password `
  -p 5432:5432 `
  -v dfs-pg-primary:/var/lib/postgresql/data `
  postgres:16 -c listen_addresses=* -c wal_level=replica `
  -c max_wal_senders=10 -c wal_keep_size=64MB
```

Wait for the primary to accept connections, then create a replication role and
allow it through `pg_hba.conf`:

```powershell
docker exec dfs-pg-primary psql -U postgres -c `
  "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'local-replication-password';"
docker exec dfs-pg-primary sh -c `
  'echo "host replication replicator 172.16.0.0/12 scram-sha-256" >> "$PGDATA/pg_hba.conf"'
docker exec dfs-pg-primary psql -U postgres -c "SELECT pg_reload_conf();"
```

Create the standby's base backup in its named volume, then start PostgreSQL
from that data directory:

```powershell
docker run --rm --network dfs-pg --user postgres `
  -v dfs-pg-standby:/var/lib/postgresql/data `
  postgres:16 pg_basebackup `
  -d "host=primary user=replicator password=local-replication-password" `
  -D /var/lib/postgresql/data -Fp -Xs -P -R

docker run -d --name dfs-pg-standby --network dfs-pg `
  -p 5433:5432 `
  -v dfs-pg-standby:/var/lib/postgresql/data `
  postgres:16
```

Confirm the standby is following the primary:

```powershell
docker exec dfs-pg-standby psql -U postgres -c "SELECT pg_is_in_recovery();"
docker exec dfs-pg-primary psql -U postgres -c `
  "SELECT application_name, state, sync_state FROM pg_stat_replication;"
```

`pg_is_in_recovery()` should be true on the standby and the primary should
report one streaming replica.

## Verify restart persistence and manual promotion

First point a local process at the primary:

```powershell
$env:POSTGRES_DSN = "postgresql://postgres:local-postgres-password@localhost:5432/postgres"
```

Create and commit a file using the store, then instantiate a new store and
check the loaded snapshot. For example, from the repository root:

```python
import os
import time
import uuid

from server.nodes.namenode.models import BlockMetadata, FileMetadata
from server.nodes.namenode.store.postgres import PostgresDatabase, PostgresMetadataStore

database = PostgresDatabase(os.environ["POSTGRES_DSN"])
store = PostgresMetadataStore(database)
store.mkdir("/users/replication-check", "replication-check", time.time(), parents=True)
file_id = str(uuid.uuid4())
file = FileMetadata(
    file_id=file_id,
    name="survives-restart",
    owner="replication-check",
    size_bytes=1,
    block_size=1,
    replication=1,
    created_at=time.time(),
    modified_at=time.time(),
    expires_at=time.time() + 300,
    blocks=[BlockMetadata(f"blk_{file_id}_0", 0, 1)],
)
store.create_pending_file("/users/replication-check/survives-restart", file)
store.commit_file(file_id, time.time())
restarted = PostgresMetadataStore(database)
assert restarted.get_file("/users/replication-check/survives-restart").file_id == file_id
```

Save that snippet as a temporary local script and run it once while the
primary is active. Wait until the standby has replayed the latest WAL, then
stop the primary and promote the standby:

```powershell
docker exec dfs-pg-primary psql -U postgres -c "SELECT pg_current_wal_flush_lsn();"
docker exec dfs-pg-standby psql -U postgres -c "SELECT pg_last_wal_replay_lsn();"
docker stop dfs-pg-primary
docker exec dfs-pg-standby psql -U postgres -c "SELECT pg_promote();"
docker exec dfs-pg-standby psql -U postgres -c "SELECT pg_is_in_recovery();"
$env:POSTGRES_DSN = "postgresql://postgres:local-postgres-password@localhost:5433/postgres"
```

Compare the primary flush LSN with the standby replay LSN and wait until the
standby's replay position is equal to or beyond the captured flush position
before stopping the primary. Then use the promoted endpoint to confirm the
pre-promotion file is still present and that a fresh write succeeds:

```python
import os
import time
import uuid

from server.nodes.namenode.models import BlockMetadata, FileMetadata
from server.nodes.namenode.store.postgres import PostgresDatabase, PostgresMetadataStore

store = PostgresMetadataStore(PostgresDatabase(os.environ["POSTGRES_DSN"]))
assert store.get_file("/users/replication-check/survives-restart") is not None
file_id = str(uuid.uuid4())
store.create_pending_file(
    "/users/replication-check/after-promotion",
    FileMetadata(
        file_id=file_id,
        name="after-promotion",
        owner="replication-check",
        size_bytes=1,
        block_size=1,
        replication=1,
        created_at=time.time(),
        modified_at=time.time(),
        expires_at=time.time() + 300,
        blocks=[BlockMetadata(f"blk_{file_id}_0", 0, 1)],
    ),
)
store.commit_file(file_id, time.time())
assert store.get_file("/users/replication-check/after-promotion") is not None
```

The former primary must not be restarted as a writable primary without being
re-cloned or rejoined as a standby.

Promotion is manual and changes which database endpoint is writable. This
runbook demonstrates PostgreSQL replication and durable metadata only; it
does not demonstrate automatic NameNode service takeover.
