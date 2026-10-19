"""Opt-in PostgreSQL acceptance tests for metadata and auth hard state."""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest
from argon2 import PasswordHasher
from psycopg.errors import UniqueViolation

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.auth_models import (
    AclEntry,
    ApiKey,
    AuditEntry,
    Group,
    Permission,
    User,
)
from server.nodes.namenode.models import BlockMetadata, FileMetadata, FileStatus
from server.nodes.namenode.postgres_auth_store import PostgresAuthStore
from server.nodes.namenode.store.postgres import (
    LeaderGuard,
    PostgresDatabase,
    PostgresMetadataStore,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("POSTGRES_TEST_DSN"),
    reason="POSTGRES_TEST_DSN is not configured",
)

T0 = 1_000.0


def make_file(
    name: str,
    file_id: str,
    *,
    block_ids: tuple[str, ...] | None = None,
) -> FileMetadata:
    ids = block_ids or (f"blk_{file_id}_0",)
    return FileMetadata(
        file_id=file_id,
        name=name,
        owner="ana",
        size_bytes=len(ids) * 100,
        block_size=100,
        replication=3,
        created_at=T0,
        modified_at=T0,
        expires_at=T0 + 3600,
        blocks=[
            BlockMetadata(block_id=block_id, index=index, size_bytes=100)
            for index, block_id in enumerate(ids)
        ],
    )


def new_store(database: PostgresDatabase) -> PostgresMetadataStore:
    store = PostgresMetadataStore(database, now=T0)
    if store.stat("/users/ana") is None:
        store.mkdir("/users/ana", "ana", T0)
    return store


def test_migration_creates_hard_state_without_persisting_soft_state(postgres_database):
    with postgres_database.connect() as connection:
        tables = {
            row["table_name"]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema()"
            ).fetchall()
        }

    assert {
        "users", "recovery_codes", "api_keys", "groups", "group_members",
        "inodes", "files", "blocks", "acls", "locks", "pending_deletes",
        "leader", "audit_log",
    } <= tables
    assert "datanodes" not in tables
    assert "block_replicas" not in tables


def test_failed_block_insert_rolls_back_the_file_and_path_reservation(postgres_database):
    store = new_store(postgres_database)
    duplicate = make_file("f", "rollback", block_ids=("same_block", "same_block"))

    with pytest.raises(ValueError, match="duplicate"):
        store.create_pending_file("/users/ana/f", duplicate)

    assert store.get_pending("/users/ana/f") is None
    assert store.get_file_by_id("rollback") is None
    with postgres_database.connect() as connection:
        assert connection.execute(
            "SELECT count(*) AS count FROM files WHERE file_id_key = %s",
            ("rollback",),
        ).fetchone()["count"] == 0


def test_two_transactions_cannot_reserve_the_same_path(postgres_database):
    store = new_store(postgres_database)

    def reserve(file_id: str):
        instance = new_store(postgres_database)
        try:
            instance.create_pending_file(
                "/users/ana/race", make_file("race", file_id)
            )
            return None
        except DfshaError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, ("race-a", "race-b")))

    assert results.count(None) == 1
    assert results.count(ErrorCode.UPLOAD_IN_PROGRESS) == 1
    assert store.get_pending("/users/ana/race") is not None


def test_concurrent_duplicate_block_indexes_are_rejected(postgres_database):
    store = new_store(postgres_database)
    store.create_pending_file("/users/ana/indexed", make_file("indexed", "indexed"))
    with postgres_database.connect() as connection:
        file_uuid = connection.execute(
            "SELECT file_id FROM files WHERE file_id_key = %s", ("indexed",)
        ).fetchone()["file_id"]

    barrier = Barrier(2)

    def insert_block(suffix: str):
        barrier.wait()
        try:
            with postgres_database.connect() as connection:
                connection.execute(
                    """
                    INSERT INTO blocks (block_id, file_id, idx, size_bytes)
                    VALUES (%s, %s, 5, 1)
                    """,
                    (f"concurrent-{suffix}", file_uuid),
                )
            return "inserted"
        except UniqueViolation:
            return "duplicate"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(insert_block, ("a", "b")))

    assert results.count("inserted") == 1
    assert results.count("duplicate") == 1


def test_overwrite_is_atomic_and_returns_the_replaced_metadata(postgres_database):
    store = new_store(postgres_database)
    store.create_pending_file("/users/ana/file", make_file("file", "old"))
    assert store.commit_file("old", T0) is None
    store.create_pending_file("/users/ana/file", make_file("file", "new"))

    assert store.get_file("/users/ana/file").file_id == "old"
    replaced = store.commit_file("new", T0 + 10)

    assert replaced is not None and replaced.file_id == "old"
    assert store.get_file("/users/ana/file").file_id == "new"
    assert store.get_block("blk_old_0") is None


def test_snapshot_restores_directories_files_blocks_and_statuses(postgres_database):
    store = new_store(postgres_database)
    store.mkdir("/users/ana/docs/nested", "ana", T0, parents=True)
    store.create_pending_file(
        "/users/ana/docs/nested/committed", make_file("committed", "committed")
    )
    store.commit_file("committed", T0 + 1)
    store.create_pending_file(
        "/users/ana/docs/nested/pending", make_file("pending", "pending")
    )
    store.create_pending_file(
        "/users/ana/docs/nested/aborted", make_file("aborted", "aborted")
    )
    store.abort_file("aborted", T0 + 2)

    restarted = PostgresMetadataStore(postgres_database, now=T0 + 3)

    assert restarted.resolve_dir("/users/ana/docs/nested").path() == "/users/ana/docs/nested"
    assert restarted.get_file("/users/ana/docs/nested/committed").file_id == "committed"
    assert restarted.get_pending("/users/ana/docs/nested/pending").file_id == "pending"
    assert restarted.get_file_by_id("aborted").status is FileStatus.ABORTED
    assert restarted.file_of_block("blk_aborted_0").status is FileStatus.ABORTED


def test_authentication_records_are_durable_and_separate(postgres_database):
    metadata = new_store(postgres_database)
    hasher = PasswordHasher(time_cost=1, memory_cost=8192)
    auth = PostgresAuthStore(postgres_database, password_hasher=hasher)
    auth.create_user(User("ana", hasher.hash("correct-password"), "user", T0))
    auth.create_user(User("bruno", hasher.hash("another-password"), "user", T0))
    auth.create_api_key(ApiKey("key-1", "ana", "laptop", "digest", T0))
    auth.create_group(Group("students", "ana", {"bruno"}))
    metadata.mkdir("/users/ana/shared", "ana", T0)
    auth.set_acl(AclEntry("/users/ana/shared", "group:students", Permission.READ))
    auth.audit(AuditEntry(T0, "ana", "mkdir", "/users/ana/shared", "OK", "req-1"))

    reloaded = PostgresAuthStore(postgres_database, password_hasher=hasher)

    assert reloaded.verify_password("ana", "correct-password")
    assert not reloaded.verify_password("ana", "wrong-password")
    assert reloaded.get_api_key_by_hash("digest").user_id == "ana"
    assert reloaded.list_api_keys("ana")[0].key_id == "key-1"
    assert reloaded.get_group("students").members == {"bruno"}
    assert reloaded.get_acl("/users/ana/shared") == [
        AclEntry("/users/ana/shared", "group:students", Permission.READ)
    ]
    assert reloaded.audit_entries() == [
        AuditEntry(T0, "ana", "mkdir", "/users/ana/shared", "OK", "req-1")
    ]


def test_leader_guard_rejects_stale_writes_and_accepts_the_current_term(postgres_database):
    guarded = PostgresDatabase(
        postgres_database.dsn,
        schema=postgres_database.schema,
        leader_guard=LeaderGuard("nn-1", 7),
    )
    store = PostgresMetadataStore(guarded, now=T0)
    with pytest.raises(DfshaError) as not_leader:
        store.mkdir("/users/ana", "ana", T0)
    assert not_leader.value.code is ErrorCode.NOT_LEADER

    with guarded.connect() as connection:
        connection.execute(
            """
            UPDATE leader
            SET holder = 'nn-1', term = 7,
                lease_expires_at = clock_timestamp() + interval '1 minute'
            WHERE singleton = TRUE
            """
        )
    store.mkdir("/users/ana", "ana", T0)

    with guarded.connect() as connection:
        connection.execute("UPDATE leader SET term = 8 WHERE singleton = TRUE")
    with pytest.raises(DfshaError) as stale:
        store.mkdir("/users/ana/blocked", "ana", T0)
    assert stale.value.code is ErrorCode.NOT_LEADER
