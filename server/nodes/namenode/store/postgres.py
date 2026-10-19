"""PostgreSQL-backed hard-state store for the NameNode."""
from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import psycopg
from psycopg import Connection
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier

from server.common.errors import DfshaError, ErrorCode
from server.common.paths import join_path, split_path
from server.nodes.namenode.models import (
    BlockMetadata,
    DirectoryMetadata,
    FileMetadata,
    FileStatus,
)
from server.nodes.namenode.store.base import USERS_DIR, DirEntry, MetadataStore
from server.nodes.namenode.store.memory import InMemoryMetadataStore

T = TypeVar("T")
Row = dict[str, Any]
ROOT_INODE_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
USERS_INODE_ID = uuid.UUID("00000000-0000-0000-0000-000000000002")
_SCHEMA_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _to_timestamp(value: float | None) -> datetime | None:
    return datetime.fromtimestamp(value, UTC) if value is not None else None


def _from_timestamp(value: datetime | None) -> float | None:
    return value.timestamp() if value is not None else None


def _file_uuid(file_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(file_id)
    except (ValueError, AttributeError):
        return uuid.uuid5(uuid.NAMESPACE_URL, f"dfsha-file:{file_id}")


@dataclass(frozen=True)
class LeaderGuard:
    """Identity and fencing term a future HA lease must authorize for writes."""

    holder: str
    term: int


class PostgresDatabase:
    """Shared connection and migration boundary for metadata and auth stores."""

    def __init__(
        self,
        dsn: str,
        *,
        schema: str | None = None,
        leader_guard: LeaderGuard | None = None,
    ) -> None:
        if not dsn:
            raise ValueError("PostgreSQL DSN must be provided")
        if schema is not None and not _SCHEMA_NAME.fullmatch(schema):
            raise ValueError("schema must be a simple PostgreSQL identifier")
        self.dsn = dsn
        self.schema = schema
        self.leader_guard = leader_guard
        self.migrate()

    def connect(self) -> Connection[Row]:
        connection = psycopg.connect(self.dsn, row_factory=dict_row)
        if self.schema is not None:
            connection.execute(
                SQL("SET search_path TO {}").format(Identifier(self.schema))
            )
        return connection

    @contextmanager
    def write_transaction(self) -> Iterator[Connection[Row]]:
        with self.connect() as connection:
            if self.leader_guard is not None:
                leader = connection.execute(
                    """
                    SELECT holder, term
                    FROM leader
                    WHERE singleton = TRUE AND lease_expires_at > clock_timestamp()
                    FOR SHARE
                    """
                ).fetchone()
                if (
                    leader is None
                    or leader["holder"] != self.leader_guard.holder
                    or leader["term"] != self.leader_guard.term
                ):
                    raise DfshaError(ErrorCode.NOT_LEADER, "stale or expired leader lease")
            yield connection

    def migrate(self) -> None:
        migration_dir = Path(__file__).with_name("migrations")
        with self.connect() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(%s)", (0x44465348414D4554,)
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            applied = connection.execute(
                "SELECT version FROM schema_migrations"
            ).fetchall()
            versions = {row["version"] for row in applied}
            for migration in sorted(migration_dir.glob("[0-9][0-9][0-9][0-9]_*.sql")):
                version = int(migration.name[:4])
                if version in versions:
                    continue
                connection.execute(migration.read_text(encoding="utf-8"), prepare=False)
                connection.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s)", (version,)
                )


class PostgresMetadataStore(MetadataStore):
    """PostgreSQL hard state with an in-memory namespace cache.

    Each mutation is committed before the local cache is rebuilt. Normal reads
    use that cache; a new leader calls ``load_snapshot`` after taking its lease.
    """

    def __init__(
        self,
        database: PostgresDatabase | str,
        now: float = 0.0,
        *,
        leader_guard: LeaderGuard | None = None,
        schema: str | None = None,
    ) -> None:
        self.database = (
            database if isinstance(database, PostgresDatabase)
            else PostgresDatabase(database, schema=schema, leader_guard=leader_guard)
        )
        if leader_guard is not None:
            self.database.leader_guard = leader_guard
        self._cache = InMemoryMetadataStore(now=now)
        self.load_snapshot()

    def _mutate(self, operation: Callable[[Connection[Row]], T]) -> T:
        try:
            with self.database.write_transaction() as connection:
                result = operation(connection)
        except UniqueViolation as error:
            if error.diag.constraint_name == "files_one_pending_per_inode_idx":
                raise DfshaError(
                    ErrorCode.UPLOAD_IN_PROGRESS, "path has an upload in progress"
                ) from error
            if error.diag.constraint_name in {"files_pkey", "files_file_id_key_key"}:
                raise ValueError("duplicate file_id") from error
            if error.diag.constraint_name in {
                "blocks_pkey",
                "blocks_file_id_idx_key",
            }:
                raise ValueError("duplicate block id or index") from error
            raise DfshaError(ErrorCode.ALREADY_EXISTS, "metadata already exists") from error
        self.load_snapshot()
        return result

    def load_snapshot(self) -> InMemoryMetadataStore:
        """Reload namespace hard state without loading any DataNode soft state."""
        with self.database.connect() as connection:
            inode_rows = connection.execute(
                "SELECT id, parent_id, name, kind, owner_id, created_at, modified_at "
                "FROM inodes ORDER BY created_at, id"
            ).fetchall()
            file_rows = connection.execute(
                """
                SELECT f.file_id, f.file_id_key, f.inode_id, f.owner_id, f.size_bytes, f.block_size,
                       f.replication, f.status, f.checksum, f.created_at, f.modified_at,
                       f.expires_at, i.name
                FROM files AS f
                JOIN inodes AS i ON i.id = f.inode_id
                ORDER BY CASE f.status
                    WHEN 'COMMITTED' THEN 0 WHEN 'ABORTED' THEN 1 ELSE 2 END,
                    f.created_at, f.file_id
                """
            ).fetchall()
            block_rows = connection.execute(
                "SELECT block_id, file_id, idx, size_bytes, checksum "
                "FROM blocks ORDER BY file_id, idx"
            ).fetchall()

        paths: dict[uuid.UUID, str] = {ROOT_INODE_ID: "/"}
        by_id = {row["id"]: row for row in inode_rows}
        unresolved = set(by_id) - {ROOT_INODE_ID}
        while unresolved:
            progressed = False
            for inode_id in tuple(unresolved):
                row = by_id[inode_id]
                parent_path = paths.get(row["parent_id"])
                if parent_path is None:
                    continue
                paths[inode_id] = (
                    join_path([row["name"]])
                    if parent_path == "/"
                    else f"{parent_path}/{row['name']}"
                )
                unresolved.remove(inode_id)
                progressed = True
            if not progressed:
                raise RuntimeError("PostgreSQL inode tree contains a missing parent or cycle")

        snapshot = InMemoryMetadataStore()
        for inode_id, row in sorted(
            by_id.items(),
            key=lambda item: (len(split_path(paths[item[0]])), paths[item[0]]),
        ):
            if inode_id == ROOT_INODE_ID:
                snapshot.root.created_at = _from_timestamp(row["created_at"]) or 0
                snapshot.root.modified_at = _from_timestamp(row["modified_at"]) or 0
                continue
            if row["kind"] != "dir":
                continue
            if paths[inode_id] == USERS_DIR:
                directory = snapshot.resolve_dir(USERS_DIR)
                directory.owner = row["owner_id"]
                directory.created_at = _from_timestamp(row["created_at"]) or 0
                directory.modified_at = _from_timestamp(row["modified_at"]) or 0
                continue
            directory = snapshot.mkdir(
                paths[inode_id], row["owner_id"], _from_timestamp(row["created_at"]) or 0,
                parents=False,
            )
            directory.modified_at = _from_timestamp(row["modified_at"]) or 0

        blocks_by_file: dict[uuid.UUID, list[BlockMetadata]] = {}
        for row in block_rows:
            blocks_by_file.setdefault(row["file_id"], []).append(
                BlockMetadata(
                    block_id=row["block_id"],
                    index=row["idx"],
                    size_bytes=row["size_bytes"],
                    checksum=row["checksum"],
                )
            )

        for row in file_rows:
            file = FileMetadata(
                file_id=row["file_id_key"],
                name=row["name"],
                owner=row["owner_id"],
                size_bytes=row["size_bytes"],
                block_size=row["block_size"],
                replication=row["replication"],
                status=FileStatus(row["status"]),
                checksum=row["checksum"],
                created_at=_from_timestamp(row["created_at"]) or 0,
                modified_at=_from_timestamp(row["modified_at"]) or 0,
                expires_at=_from_timestamp(row["expires_at"]),
                blocks=blocks_by_file.get(row["file_id"], []),
            )
            path = paths[row["inode_id"]]
            if file.status is FileStatus.ABORTED and by_id[row["inode_id"]]["kind"] == "dir":
                parent_path = join_path(split_path(path)[:-1])
                parent = snapshot.resolve_dir(parent_path)
                snapshot._files[file.file_id] = file
                snapshot._file_dir[file.file_id] = parent
                for block in file.blocks:
                    snapshot._blocks[block.block_id] = block
                    snapshot._block_file[block.block_id] = file.file_id
                continue
            snapshot.create_pending_file(path, file)
            if file.status is FileStatus.COMMITTED:
                snapshot.commit_file(file.file_id, file.modified_at)
            elif file.status is FileStatus.ABORTED:
                snapshot.abort_file(file.file_id, file.modified_at)
        self._cache = snapshot
        return snapshot

    def resolve_dir(self, path: str) -> DirectoryMetadata:
        return self._cache.resolve_dir(path)

    def mkdir(
        self, path: str, owner: str, now: float, parents: bool = False
    ) -> DirectoryMetadata:
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.ALREADY_EXISTS, "/ already exists")

        def insert(connection: Connection[Row]) -> None:
            parent_id = ROOT_INODE_ID
            for index, name in enumerate(segments):
                final = index == len(segments) - 1
                existing = connection.execute(
                    "SELECT id, kind, current_file_id FROM inodes "
                    "WHERE parent_id = %s AND name = %s FOR UPDATE",
                    (parent_id, name),
                ).fetchone()
                if existing is not None:
                    if existing["kind"] != "dir":
                        active = connection.execute(
                            "SELECT 1 FROM files WHERE inode_id = %s AND status = 'PENDING'",
                            (existing["id"],),
                        ).fetchone()
                        if existing["current_file_id"] is not None or active is not None:
                            code = ErrorCode.ALREADY_EXISTS if final else ErrorCode.NOT_A_DIRECTORY
                            raise DfshaError(code, f"{join_path(segments[:index + 1])} is a file")
                        connection.execute(
                            "UPDATE inodes SET kind = 'dir', owner_id = %s WHERE id = %s",
                            (owner, existing["id"]),
                        )
                    elif final and not parents:
                        raise DfshaError(
                            ErrorCode.ALREADY_EXISTS,
                            f"{join_path(segments)} already exists",
                        )
                    parent_id = existing["id"]
                    continue
                if not final and not parents:
                    raise DfshaError(
                        ErrorCode.DIR_NOT_FOUND,
                        f"{join_path(segments[:index + 1])} does not exist",
                    )
                row = connection.execute(
                    """
                    INSERT INTO inodes (parent_id, name, kind, owner_id, created_at, modified_at)
                    VALUES (%s, %s, 'dir', %s, %s, %s)
                    RETURNING id
                    """,
                    (parent_id, name, owner, _to_timestamp(now), _to_timestamp(now)),
                ).fetchone()
                if row is None:
                    raise RuntimeError("PostgreSQL did not return the created inode")
                connection.execute(
                    "UPDATE inodes SET modified_at = %s WHERE id = %s",
                    (_to_timestamp(now), parent_id),
                )
                parent_id = row["id"]

        self._mutate(insert)
        result = self._cache.resolve_dir(path)
        return result

    def list_dir(self, path: str) -> list[DirEntry]:
        return self._cache.list_dir(path)

    def stat(self, path: str) -> DirectoryMetadata | FileMetadata | None:
        return self._cache.stat(path)

    def get_file(self, path: str) -> FileMetadata | None:
        return self._cache.get_file(path)

    def path_of(self, file_id: str) -> str:
        return self._cache.path_of(file_id)

    def remove_file(self, path: str, now: float) -> FileMetadata:
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, "/ is a directory")
        parent_path = join_path(segments[:-1])
        parent_id = self._inode_id(
            connection=None, path=parent_path, require_directory=True
        )
        name = segments[-1]
        if self._cache.get_file(path) is None:
            node = self._cache.stat(path)
            if isinstance(node, DirectoryMetadata):
                raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{path} is a directory")
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{path} does not exist")

        def delete(connection: Connection[Row]) -> FileMetadata:
            row = connection.execute(
                """
                SELECT f.file_id FROM inodes i
                JOIN files f ON f.file_id = i.current_file_id
                WHERE i.parent_id = %s AND i.name = %s
                FOR UPDATE
                """,
                (parent_id, name),
            ).fetchone()
            if row is None:
                raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{path} does not exist")
            file = self._file_from_database(connection, row["file_id"])
            connection.execute(
                "UPDATE inodes SET current_file_id = NULL, modified_at = %s WHERE parent_id = %s AND name = %s",
                (_to_timestamp(now), parent_id, name),
            )
            connection.execute("DELETE FROM files WHERE file_id = %s", (row["file_id"],))
            return file

        return self._mutate(delete)

    def remove_dir(self, path: str, recursive: bool, now: float) -> list[FileMetadata]:
        segments = split_path(path)
        canonical = join_path(segments)
        if not segments:
            raise DfshaError(ErrorCode.BAD_REQUEST, "the root cannot be removed")
        if canonical == USERS_DIR:
            raise DfshaError(ErrorCode.BAD_REQUEST, f"{USERS_DIR} cannot be removed")
        directory = self._cache.resolve_dir(canonical)
        if not recursive and not directory.is_empty():
            raise DfshaError(ErrorCode.DIR_NOT_EMPTY, f"{canonical} is not empty")
        prefix = canonical + "/"
        if any(file.status is FileStatus.PENDING and self._cache.path_of(file.file_id).startswith(prefix)
               for file in self._cache.files()):
            raise DfshaError(ErrorCode.DIR_NOT_EMPTY, f"{canonical} has uploads in progress")
        inode_id = self._inode_id(
            connection=None, path=canonical, require_directory=True
        )
        cached_removed = [
            file for file in self._cache.files(FileStatus.COMMITTED)
            if self._cache.path_of(file.file_id).startswith(prefix)
        ]

        parent_path = join_path(segments[:-1])

        def delete(connection: Connection[Row]) -> list[FileMetadata]:
            descendant_ids = connection.execute(
                """
                WITH RECURSIVE subtree(id) AS (
                    SELECT %s::uuid
                    UNION ALL
                    SELECT i.id FROM inodes i JOIN subtree s ON i.parent_id = s.id
                )
                SELECT i.id FROM inodes i JOIN subtree s ON s.id = i.id
                FOR UPDATE OF i
                """,
                (inode_id,),
            ).fetchall()
            ids = [row["id"] for row in descendant_ids]
            pending = connection.execute(
                "SELECT 1 FROM files WHERE inode_id = ANY(%s) AND status = 'PENDING' LIMIT 1",
                (ids,),
            ).fetchone()
            if pending:
                raise DfshaError(ErrorCode.DIR_NOT_EMPTY, f"{canonical} has uploads in progress")
            if not recursive:
                contents = connection.execute(
                    """
                    SELECT 1 FROM inodes child
                    WHERE child.parent_id = %s
                    AND (child.kind = 'dir' OR child.current_file_id IS NOT NULL)
                    LIMIT 1
                    """,
                    (inode_id,),
                ).fetchone()
                if contents:
                    raise DfshaError(ErrorCode.DIR_NOT_EMPTY, f"{canonical} is not empty")
            removed = [
                self._file_from_database(connection, _file_uuid(file.file_id))
                for file in cached_removed
            ]
            connection.execute(
                "DELETE FROM inodes WHERE id = ANY(%s)",
                (ids,),
            )
            connection.execute(
                "DELETE FROM files WHERE status = 'ABORTED' AND inode_id = ANY(%s)",
                (ids,),
            )
            connection.execute(
                "UPDATE inodes SET modified_at = %s WHERE id = %s",
                (
                    _to_timestamp(now),
                    self._inode_id(
                        connection, path=parent_path, require_directory=True
                    ),
                ),
            )
            return removed

        return self._mutate(delete)

    def get_pending(self, path: str) -> FileMetadata | None:
        return self._cache.get_pending(path)

    def create_pending_file(self, path: str, file: FileMetadata) -> None:
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.BAD_REQUEST, "a file needs a name")
        canonical = join_path(segments)
        if file.name != segments[-1]:
            raise ValueError(f"file name {file.name!r} does not match path {canonical!r}")
        file_uuid = _file_uuid(file.file_id)
        parent_path = join_path(segments[:-1])

        def create(connection: Connection[Row]) -> None:
            parent_id = self._inode_id(
                connection, path=parent_path, require_directory=True
            )
            connection.execute(
                "SELECT id FROM inodes WHERE id = %s FOR UPDATE", (parent_id,)
            )
            inode = connection.execute(
                "SELECT id, kind, current_file_id FROM inodes "
                "WHERE parent_id = %s AND name = %s FOR UPDATE",
                (parent_id, file.name),
            ).fetchone()
            if inode is not None and inode["kind"] == "dir":
                raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{canonical} is a directory")
            if inode is None:
                inode = connection.execute(
                    """
                    INSERT INTO inodes (parent_id, name, kind, owner_id, created_at, modified_at)
                    VALUES (%s, %s, 'file', %s, %s, %s) RETURNING id, kind, current_file_id
                    """,
                    (
                        parent_id, file.name, file.owner,
                        _to_timestamp(file.created_at), _to_timestamp(file.modified_at),
                    ),
                ).fetchone()
            if inode is None:
                raise RuntimeError(f"could not reserve inode for {canonical}")
            pending = connection.execute(
                "SELECT 1 FROM files WHERE inode_id = %s AND status = 'PENDING'",
                (inode["id"],),
            ).fetchone()
            if pending:
                raise DfshaError(ErrorCode.UPLOAD_IN_PROGRESS, f"{canonical} has an upload in progress")
            if connection.execute(
                "SELECT 1 FROM files WHERE file_id = %s", (file_uuid,)
            ).fetchone():
                raise ValueError(f"duplicate file_id {file.file_id}")
            connection.execute(
                """
                INSERT INTO files
                    (file_id, file_id_key, inode_id, owner_id, size_bytes, block_size, replication,
                     status, checksum, created_at, modified_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'PENDING', %s, %s, %s, %s)
                """,
                (
                    file_uuid, file.file_id, inode["id"], file.owner, file.size_bytes, file.block_size,
                    file.replication, file.checksum, _to_timestamp(file.created_at),
                    _to_timestamp(file.modified_at), _to_timestamp(file.expires_at),
                ),
            )
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO blocks (block_id, file_id, idx, size_bytes, checksum)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        (block.block_id, file_uuid, block.index, block.size_bytes, block.checksum)
                        for block in file.blocks
                    ],
                )

        self._mutate(create)

    def commit_file(self, file_id: str, now: float) -> FileMetadata | None:
        file_uuid = _file_uuid(file_id)

        def commit(connection: Connection[Row]) -> FileMetadata | None:
            row = connection.execute(
                "SELECT * FROM files WHERE file_id = %s FOR UPDATE", (file_uuid,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown file_id {file_id}")
            if row["status"] != FileStatus.PENDING.value:
                raise ValueError(f"{file_id} is {row['status']}, only PENDING can be committed")
            inode = connection.execute(
                "SELECT * FROM inodes WHERE id = %s FOR UPDATE", (row["inode_id"],)
            ).fetchone()
            if inode is None:
                raise RuntimeError(f"missing inode for file {file_id}")
            if inode["kind"] != "file":
                raise DfshaError(ErrorCode.IS_A_DIRECTORY, "upload path became a directory")
            replaced = (
                self._file_from_database(connection, inode["current_file_id"])
                if inode["current_file_id"] is not None
                else None
            )
            if replaced is not None:
                connection.execute(
                    "DELETE FROM files WHERE file_id = %s", (inode["current_file_id"],)
                )
            connection.execute(
                "UPDATE files SET status = 'COMMITTED', expires_at = NULL, modified_at = %s "
                "WHERE file_id = %s",
                (_to_timestamp(now), file_uuid),
            )
            connection.execute(
                "UPDATE inodes SET current_file_id = %s, modified_at = %s WHERE id = %s",
                (file_uuid, _to_timestamp(now), inode["id"]),
            )
            return replaced

        return self._mutate(commit)

    def abort_file(self, file_id: str, now: float) -> None:
        file_uuid = _file_uuid(file_id)

        def abort(connection: Connection[Row]) -> None:
            row = connection.execute(
                "SELECT status FROM files WHERE file_id = %s FOR UPDATE", (file_uuid,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown file_id {file_id}")
            if row["status"] == FileStatus.ABORTED.value:
                return
            if row["status"] != FileStatus.PENDING.value:
                raise ValueError(f"{file_id} is {row['status']}, only PENDING can be aborted")
            connection.execute(
                "UPDATE files SET status = 'ABORTED', expires_at = NULL, modified_at = %s "
                "WHERE file_id = %s",
                (_to_timestamp(now), file_uuid),
            )

        self._mutate(abort)

    def purge_aborted(self, older_than: float) -> list[FileMetadata]:
        def purge(connection: Connection[Row]) -> list[FileMetadata]:
            rows = connection.execute(
                "SELECT file_id FROM files WHERE status = 'ABORTED' AND modified_at < %s FOR UPDATE",
                (_to_timestamp(older_than),),
            ).fetchall()
            files = [self._file_from_database(connection, row["file_id"]) for row in rows]
            ids = [row["file_id"] for row in rows]
            if ids:
                connection.execute("DELETE FROM files WHERE file_id = ANY(%s)", (ids,))
                connection.execute(
                    """
                    DELETE FROM inodes i
                    WHERE i.kind = 'file' AND i.current_file_id IS NULL
                    AND NOT EXISTS (SELECT 1 FROM files f WHERE f.inode_id = i.id)
                    """
                )
            return files

        return self._mutate(purge)

    def get_file_by_id(self, file_id: str) -> FileMetadata | None:
        return self._cache.get_file_by_id(file_id)

    def get_block(self, block_id: str) -> BlockMetadata | None:
        return self._cache.get_block(block_id)

    def file_of_block(self, block_id: str) -> FileMetadata | None:
        return self._cache.file_of_block(block_id)

    def files(self, status: FileStatus | None = None) -> list[FileMetadata]:
        return self._cache.files(status)

    def _inode_id(
        self,
        connection: Connection[Row] | None = None,
        *,
        path: str | None = None,
        parent_path: str | None = None,
        require_directory: bool = False,
    ) -> uuid.UUID:
        if path is None:
            if parent_path is None:
                raise ValueError("path is required")
            path = parent_path
        segments = split_path(path)
        if not segments:
            return ROOT_INODE_ID
        if connection is None:
            connection = self.database.connect()
            try:
                return self._inode_id(
                    connection, path=path, require_directory=require_directory
                )
            finally:
                connection.close()
        parent_id = ROOT_INODE_ID
        for index, name in enumerate(segments):
            row = connection.execute(
                "SELECT id, kind FROM inodes WHERE parent_id = %s AND name = %s",
                (parent_id, name),
            ).fetchone()
            if row is None:
                raise DfshaError(ErrorCode.DIR_NOT_FOUND, f"{path} does not exist")
            if (
                row["kind"] != "dir"
                and (index < len(segments) - 1 or require_directory)
            ):
                current_path = join_path(segments[: index + 1])
                raise DfshaError(
                    ErrorCode.NOT_A_DIRECTORY,
                    f"{current_path} is not a directory",
                )
            parent_id = row["id"]
        return parent_id

    @staticmethod
    def _file_from_database(connection: Connection[Row], file_id: uuid.UUID) -> FileMetadata:
        row = connection.execute(
            """
            SELECT f.*, i.name FROM files f JOIN inodes i ON i.id = f.inode_id
            WHERE f.file_id = %s
            """,
            (file_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown file_id {file_id}")
        blocks = connection.execute(
            "SELECT block_id, idx, size_bytes, checksum FROM blocks "
            "WHERE file_id = %s ORDER BY idx",
            (file_id,),
        ).fetchall()
        return FileMetadata(
            file_id=row["file_id_key"],
            name=row["name"],
            owner=row["owner_id"],
            size_bytes=row["size_bytes"],
            block_size=row["block_size"],
            replication=row["replication"],
            status=FileStatus(row["status"]),
            checksum=row["checksum"],
            created_at=_from_timestamp(row["created_at"]) or 0,
            modified_at=_from_timestamp(row["modified_at"]) or 0,
            expires_at=_from_timestamp(row["expires_at"]),
            blocks=[
                BlockMetadata(
                    block_id=block["block_id"],
                    index=block["idx"],
                    size_bytes=block["size_bytes"],
                    checksum=block["checksum"],
                )
                for block in blocks
            ],
        )


__all__ = ["LeaderGuard", "PostgresDatabase", "PostgresMetadataStore"]
