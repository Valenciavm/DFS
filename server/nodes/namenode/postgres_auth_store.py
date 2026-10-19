"""PostgreSQL persistence for the NameNode authentication hard state."""
from __future__ import annotations

from dataclasses import replace

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from psycopg import Connection
from psycopg.errors import UniqueViolation

from server.common.errors import DfshaError, ErrorCode
from server.common.paths import split_path
from server.nodes.namenode.auth_models import (
    AclEntry,
    ApiKey,
    AuditEntry,
    Group,
    Permission,
    User,
)
from server.nodes.namenode.auth_store import AuthStore
from server.nodes.namenode.store.postgres import (
    ROOT_INODE_ID,
    PostgresDatabase,
    Row,
    _from_timestamp,
    _to_timestamp,
)


class PostgresAuthStore(AuthStore):
    """Durable auth store separate from the namespace ``MetadataStore``."""

    def __init__(
        self,
        database: PostgresDatabase | str,
        *,
        schema: str | None = None,
        password_hasher: PasswordHasher | None = None,
    ) -> None:
        self.database = (
            database if isinstance(database, PostgresDatabase)
            else PostgresDatabase(database, schema=schema)
        )
        self._password_hasher = password_hasher or PasswordHasher()

    def create_user(self, user: User) -> None:
        try:
            with self.database.write_transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO users (username, password_hash, role, created_at, totp_enabled)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        user.username, user.password_hash, user.role,
                        _to_timestamp(user.created_at), user.totp_enabled,
                    ),
                )
        except UniqueViolation as error:
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"user {user.username} already exists") from error

    def get_user(self, username: str) -> User | None:
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT username, password_hash, role, created_at, totp_enabled "
                "FROM users WHERE username = %s",
                (username,),
            ).fetchone()
        return self._user_from_row(row) if row is not None else None

    def verify_password(self, username: str, password: str) -> bool:
        user = self.get_user(username)
        if user is None:
            return False
        try:
            return self._password_hasher.verify(user.password_hash, password)
        except (VerifyMismatchError, InvalidHashError):
            return False

    def users(self) -> list[User]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT username, password_hash, role, created_at, totp_enabled "
                "FROM users ORDER BY username"
            ).fetchall()
        return [self._user_from_row(row) for row in rows]

    def create_api_key(self, api_key: ApiKey) -> None:
        try:
            with self.database.write_transaction() as connection:
                user_id = self._user_id(connection, api_key.user_id)
                connection.execute(
                    """
                    INSERT INTO api_keys (id, user_id, name, key_hash, created_at, revoked_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        api_key.key_id, user_id, api_key.name, api_key.key_hash,
                        _to_timestamp(api_key.created_at), _to_timestamp(api_key.revoked_at),
                    ),
                )
        except UniqueViolation as error:
            raise DfshaError(
                ErrorCode.ALREADY_EXISTS, f"API key {api_key.key_id} already exists"
            ) from error

    def get_api_key(self, key_id: str) -> ApiKey | None:
        with self.database.connect() as connection:
            row = connection.execute(
                """
                SELECT k.id, u.username, k.name, k.key_hash, k.created_at, k.revoked_at
                FROM api_keys k JOIN users u ON u.id = k.user_id WHERE k.id = %s
                """,
                (key_id,),
            ).fetchone()
        return self._api_key_from_row(row) if row is not None else None

    def get_api_key_by_hash(self, key_hash: str) -> ApiKey | None:
        with self.database.connect() as connection:
            row = connection.execute(
                """
                SELECT k.id, u.username, k.name, k.key_hash, k.created_at, k.revoked_at
                FROM api_keys k JOIN users u ON u.id = k.user_id WHERE k.key_hash = %s
                """,
                (key_hash,),
            ).fetchone()
        return self._api_key_from_row(row) if row is not None else None

    def api_keys(self) -> list[ApiKey]:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT k.id, u.username, k.name, k.key_hash, k.created_at, k.revoked_at
                FROM api_keys k JOIN users u ON u.id = k.user_id ORDER BY k.created_at, k.id
                """
            ).fetchall()
        return [self._api_key_from_row(row) for row in rows]

    def groups(self) -> list[Group]:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT g.name, owner.username AS owner, member.username AS member
                FROM groups g
                JOIN users owner ON owner.id = g.owner_id
                LEFT JOIN group_members gm ON gm.group_id = g.id
                LEFT JOIN users member ON member.id = gm.user_id
                ORDER BY g.name, member.username
                """
            ).fetchall()
        grouped: dict[str, Group] = {}
        for row in rows:
            group = grouped.setdefault(row["name"], Group(row["name"], row["owner"]))
            if row["member"] is not None:
                group.members.add(row["member"])
        return [replace(group, members=set(group.members)) for group in grouped.values()]

    def revoke_api_key(self, key_id: str, now: float) -> ApiKey | None:
        with self.database.write_transaction() as connection:
            row = connection.execute(
                """
                UPDATE api_keys SET revoked_at = COALESCE(revoked_at, %s)
                WHERE id = %s
                RETURNING id, user_id, name, key_hash, created_at, revoked_at
                """,
                (_to_timestamp(now), key_id),
            ).fetchone()
            if row is None:
                return None
            user_row = connection.execute(
                "SELECT username FROM users WHERE id = %s", (row["user_id"],)
            ).fetchone()
            if user_row is None:
                raise RuntimeError("API key refers to a missing user")
            username = user_row["username"]
        return ApiKey(
            key_id=row["id"],
            user_id=username,
            name=row["name"],
            key_hash=row["key_hash"],
            created_at=_from_timestamp(row["created_at"]) or 0,
            revoked_at=_from_timestamp(row["revoked_at"]),
        )

    def list_api_keys(self, username: str) -> list[ApiKey]:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT k.id, u.username, k.name, k.key_hash, k.created_at, k.revoked_at
                FROM api_keys k JOIN users u ON u.id = k.user_id
                WHERE u.username = %s ORDER BY k.created_at, k.id
                """,
                (username,),
            ).fetchall()
        return [self._api_key_from_row(row) for row in rows]

    def create_group(self, group: Group) -> None:
        try:
            with self.database.write_transaction() as connection:
                owner_id = self._user_id(connection, group.owner)
                row = connection.execute(
                    "INSERT INTO groups (name, owner_id) VALUES (%s, %s) RETURNING id",
                    (group.name, owner_id),
                ).fetchone()
                member_ids = [self._user_id(connection, member) for member in group.members]
                if row is None:
                    raise RuntimeError(f"could not create group {group.name}")
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO group_members (group_id, user_id) VALUES (%s, %s)",
                        [(row["id"], member_id) for member_id in member_ids],
                    )
        except UniqueViolation as error:
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"group {group.name} already exists") from error

    def get_group(self, name: str) -> Group | None:
        with self.database.connect() as connection:
            rows = connection.execute(
                """
                SELECT g.name, owner.username AS owner, member.username AS member
                FROM groups g JOIN users owner ON owner.id = g.owner_id
                LEFT JOIN group_members gm ON gm.group_id = g.id
                LEFT JOIN users member ON member.id = gm.user_id
                WHERE g.name = %s ORDER BY member.username
                """,
                (name,),
            ).fetchall()
        if not rows:
            return None
        group = Group(name, rows[0]["owner"])
        group.members.update(row["member"] for row in rows if row["member"] is not None)
        return group

    def update_group_members(self, name: str, members: set[str]) -> Group:
        with self.database.write_transaction() as connection:
            row = connection.execute(
                "SELECT id FROM groups WHERE name = %s FOR UPDATE", (name,)
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown group {name}")
            member_ids = [self._user_id(connection, member) for member in members]
            connection.execute("DELETE FROM group_members WHERE group_id = %s", (row["id"],))
            with connection.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO group_members (group_id, user_id) VALUES (%s, %s)",
                    [(row["id"], member_id) for member_id in member_ids],
                )
        group = self.get_group(name)
        if group is None:
            raise RuntimeError(f"group {name} disappeared after member update")
        return group

    def set_acl(self, entry: AclEntry) -> None:
        with self.database.write_transaction() as connection:
            inode_id = self._inode_id(connection, entry.path)
            permission = {
                "NONE": "none",
                "READ": "r",
                "READ_WRITE": "rw",
            }[entry.permission.name]
            connection.execute(
                """
                INSERT INTO acls (inode_id, subject, permission)
                VALUES (%s, %s, %s)
                ON CONFLICT (inode_id, subject)
                DO UPDATE SET permission = EXCLUDED.permission
                """,
                (inode_id, entry.subject, permission),
            )

    def get_acl(self, path: str) -> list[AclEntry]:
        with self.database.connect() as connection:
            inode_id = self._inode_id(connection, path)
            rows = connection.execute(
                "SELECT subject, permission FROM acls WHERE inode_id = %s ORDER BY subject",
                (inode_id,),
            ).fetchall()
        return [
            AclEntry(path, row["subject"], Permission.parse(row["permission"]))
            for row in rows
        ]

    def remove_acl_subtree(self, path: str) -> None:
        segments = split_path(path)
        canonical = "/" + "/".join(segments) if segments else "/"
        with self.database.write_transaction() as connection:
            rows = connection.execute(
                """
                WITH RECURSIVE tree(id, path) AS (
                    SELECT id, '/'::text FROM inodes WHERE parent_id IS NULL
                    UNION ALL
                    SELECT i.id,
                           CASE WHEN tree.path = '/' THEN '/' || i.name
                                ELSE tree.path || '/' || i.name END
                    FROM inodes i JOIN tree ON i.parent_id = tree.id
                )
                SELECT id FROM tree
                WHERE path = %s OR left(path, char_length(%s)) = %s
                """,
                (canonical, canonical.rstrip("/") + "/", canonical.rstrip("/") + "/"),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                connection.execute("DELETE FROM acls WHERE inode_id = ANY(%s)", (ids,))

    def audit(self, entry: AuditEntry) -> None:
        with self.database.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO audit_log (ts, username, operation, path, result, request_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    _to_timestamp(entry.timestamp), entry.user, entry.operation,
                    entry.path, entry.result, entry.request_id,
                ),
            )

    def audit_entries(self) -> list[AuditEntry]:
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT ts, username, operation, path, result, request_id "
                "FROM audit_log ORDER BY id"
            ).fetchall()
        return [
            AuditEntry(
                timestamp=_from_timestamp(row["ts"]) or 0,
                user=row["username"],
                operation=row["operation"],
                path=row["path"],
                result=row["result"],
                request_id=row["request_id"],
            )
            for row in rows
        ]

    @staticmethod
    def _user_id(connection: Connection[Row], username: str):
        row = connection.execute(
            "SELECT id FROM users WHERE username = %s", (username,)
        ).fetchone()
        if row is None:
            raise DfshaError(ErrorCode.USER_NOT_FOUND, f"user {username} does not exist")
        return row["id"]

    @staticmethod
    def _inode_id(connection: Connection[Row], path: str):
        parent_id = ROOT_INODE_ID
        for name in split_path(path):
            row = connection.execute(
                "SELECT id FROM inodes WHERE parent_id = %s AND name = %s",
                (parent_id, name),
            ).fetchone()
            if row is None:
                raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{path} does not exist")
            parent_id = row["id"]
        return parent_id

    @staticmethod
    def _user_from_row(row) -> User:
        return User(
            username=row["username"],
            password_hash=row["password_hash"],
            role=row["role"],
            created_at=_from_timestamp(row["created_at"]) or 0,
            totp_enabled=row["totp_enabled"],
        )

    @staticmethod
    def _api_key_from_row(row) -> ApiKey:
        return ApiKey(
            key_id=row["id"],
            user_id=row["username"],
            name=row["name"],
            key_hash=row["key_hash"],
            created_at=_from_timestamp(row["created_at"]) or 0,
            revoked_at=_from_timestamp(row["revoked_at"]),
        )


__all__ = ["PostgresAuthStore"]
