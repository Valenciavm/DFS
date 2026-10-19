"""NameNode core: the rules of the control plane, with no web code.

A plain class so it can be unit tested with a fake clock and an in-memory store
(``ARCHITECTURE.md`` Section 4.1). The REST layer is a thin adapter on top.

Concurrency (Section 4.4): every public method is synchronous and runs to
completion without yielding, so when the REST handlers and the background loops
share one event loop each call is atomic. Background tasks iterate over
snapshots (``list(...)``), never over a dictionary another call may be changing.

Time (Section 4.4): liveness uses the monotonic clock, anything stored or
compared across processes uses the wall clock, and the two are never subtracted
from each other.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import secrets
import uuid
from collections.abc import Callable
from functools import wraps
from inspect import signature
from typing import Concatenate, ParamSpec, TypeVar

from argon2 import PasswordHasher

from server.common.clock import Clock, SystemClock
from server.common.config import ConfigError, Settings
from server.common.config import settings as default_settings
from server.common.errors import DfshaError, ErrorCode
from server.common.paths import join_path, split_path
from server.common.tokens import SYSTEM_PRINCIPAL, BlockOp, mint
from server.nodes.namenode import placement
from server.nodes.namenode.auth_context import current_principal
from server.nodes.namenode.auth_models import (
    AclEntry,
    ApiKey,
    AuditEntry,
    Group,
    Permission,
    User,
)
from server.nodes.namenode.auth_store import AuthStore, InMemoryAuthStore
from server.nodes.namenode.cluster_state import ClusterState, Command, DataNodeInfo
from server.nodes.namenode.jwt_auth import issue_jwt, verify_jwt
from server.nodes.namenode.models import (
    BlockMetadata,
    DirectoryMetadata,
    FileMetadata,
    FileStatus,
)
from server.nodes.namenode.postgres_auth_store import PostgresAuthStore
from server.nodes.namenode.results import (
    BlockLocation,
    ClusterStats,
    CommittedBlock,
    FileLocation,
    GcResult,
    PlannedBlock,
    ReplicationScan,
    ReportedBlock,
    UploadPlan,
)
from server.nodes.namenode.store import (
    DirEntry,
    InMemoryMetadataStore,
    MetadataStore,
)
from server.nodes.namenode.store.postgres import PostgresDatabase, PostgresMetadataStore

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")


def _audited_fs_operation(
    method: Callable[Concatenate[Namenode, P], R],
) -> Callable[Concatenate[Namenode, P], R]:
    """Record every filesystem operation outcome without hiding its exception."""
    method_signature = signature(method)

    @wraps(method)
    def wrapped(self: Namenode, *args: P.args, **kwargs: P.kwargs) -> R:
        bound = method_signature.bind(self, *args, **kwargs)
        bound.apply_defaults()
        requested_user = bound.arguments.get("user")
        username = requested_user if isinstance(requested_user, str) else "unknown"
        if requested_user is None:
            try:
                username = current_principal()
            except DfshaError:
                pass
        path = bound.arguments.get("path", bound.arguments.get("file_id"))
        audit_path = path if isinstance(path, str) else None
        if audit_path is not None and "file_id" in bound.arguments:
            file = self.store.get_file_by_id(audit_path)
            audit_path = self.store.path_of(file.file_id) if file is not None else None
        try:
            result = method(self, *args, **kwargs)
        except DfshaError as error:
            self._audit(username, method.__name__, audit_path, error.code.value)
            raise
        except Exception:
            self._audit(username, method.__name__, audit_path, "INTERNAL")
            raise
        self._audit(username, method.__name__, audit_path, "OK")
        return result

    return wrapped


_SHA256 = re.compile(r"[0-9a-f]{64}")

#: A copy that has not been confirmed after this long is given up on, so one lost
#: command cannot block the repair of a block forever. Monotonic seconds.
IN_FLIGHT_TIMEOUT_S = 120

#: Periods of the background tasks (``ARCHITECTURE.md`` Section 4.2), in seconds.
LIVENESS_PERIOD_S = 5
REPLICATION_PERIOD_S = 10
GC_PERIOD_S = 30


class Namenode:
    """The leader's view of the namespace and of the cluster."""

    def __init__(
        self,
        settings: Settings | None = None,
        store: MetadataStore | None = None,
        clock: Clock | None = None,
        rng: random.Random | None = None,
        auth_store: AuthStore | None = None,
    ) -> None:
        self.settings = settings if settings is not None else default_settings
        if not self.settings.block_token_secret:
            # fail at startup: a NameNode without it would issue tokens nobody can verify
            raise ConfigError("BLOCK_TOKEN_SECRET must be set: block tokens are signed with it")
        if not self.settings.jwt_secret:
            raise ConfigError("JWT_SECRET must be set: access tokens are signed with it")
        self.clock: Clock = clock if clock is not None else SystemClock()
        self.rng = rng if rng is not None else random.Random()
        database = None
        if store is None and self.settings.postgres_dsn:
            database = PostgresDatabase(self.settings.postgres_dsn)
            store = PostgresMetadataStore(database, now=self.clock.time())
        self.store: MetadataStore = (
            store if store is not None else InMemoryMetadataStore(now=self.clock.time())
        )
        if auth_store is None:
            if database is None and isinstance(self.store, PostgresMetadataStore):
                database = self.store.database
            auth_store = (
                PostgresAuthStore(database)
                if database is not None
                else InMemoryAuthStore()
            )
        self.auth_store: AuthStore = auth_store
        self._password_hasher = PasswordHasher()
        self.cluster = ClusterState()
        #: leadership generation; fixed at 1 until the lease is implemented
        self.term = 1
        #: monotonic instant until which nobody is declared dead after a failover
        self._grace_until = 0.0
        if bool(self.settings.admin_user) != bool(self.settings.admin_password):
            raise ConfigError("ADMIN_USER and ADMIN_PASSWORD must be configured together")
        if self.settings.admin_user and not self.auth_store.users():
            if len(self.settings.admin_password) < 8:
                raise ConfigError("ADMIN_PASSWORD must contain at least 8 characters")
            admin_home = f"/users/{self.settings.admin_user}"
            try:
                valid_admin_path = len(split_path(admin_home)) == 2
            except DfshaError:
                valid_admin_path = False
            if not valid_admin_path:
                raise ConfigError("ADMIN_USER must be a valid single path segment")
            self.auth_store.create_user(
                User(
                    username=self.settings.admin_user,
                    password_hash=self._password_hasher.hash(self.settings.admin_password),
                    role="admin",
                    created_at=self.clock.time(),
                )
            )
            self.store.mkdir(admin_home, self.settings.admin_user, self.clock.time(), parents=True)
        self._locks: dict[str, list[dict[str, object]]] = {}
        self._locks_by_id: dict[str, dict[str, object]] = {}

    # ---- authentication, groups and ACLs --------------------------------

    def _actor(self, user: str | None) -> User:
        try:
            username = user if user is not None else current_principal()
        except DfshaError:
            self._audit("unknown", "authenticate_principal", None, "UNAUTHORIZED")
            raise
        account = self.auth_store.get_user(username)
        if account is None:
            self._audit(username, "authenticate_principal", None, "UNAUTHORIZED")
            raise DfshaError(ErrorCode.UNAUTHORIZED, "unknown authenticated user")
        return account

    def _audit(
        self,
        user: str,
        operation: str,
        path: str | None,
        result: str,
        request_id: str | None = None,
    ) -> None:
        self.auth_store.audit(
            AuditEntry(self.clock.time(), user, operation, path, result, request_id)
        )

    def register_user(self, username: str, password: str) -> dict[str, str]:
        """Create a password-authenticated account with the default user role."""
        if not isinstance(username, str) or not username or username.strip() != username:
            self._audit("unknown", "register", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "username must be a non-empty trimmed string")
        try:
            username_parts = split_path(f"/{username}")
        except DfshaError:
            self._audit(username, "register", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "username must be a valid path segment") from None
        if username_parts != [username]:
            self._audit(username, "register", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "username must be a valid path segment")
        if not isinstance(password, str) or len(password) < 8:
            self._audit(username, "register", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "password must contain at least 8 characters")
        if self.auth_store.get_user(username) is not None:
            self._audit(username, "register", None, "ALREADY_EXISTS")
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"user {username} already exists")
        home = f"/users/{username}"
        if self.store.stat(home) is not None or self.store.get_pending(home) is not None:
            self._audit(username, "register", home, "ALREADY_EXISTS")
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"home path for {username} already exists")
        user = User(
            username=username,
            password_hash=self._password_hasher.hash(password),
            role="user",
            created_at=self.clock.time(),
        )
        self.auth_store.create_user(user)
        self.store.mkdir(home, username, self.clock.time(), parents=True)
        self._audit(username, "register", home, "OK")
        return {"username": username}

    def verify_password(self, username: str, password: str) -> bool:
        """Verify credentials without revealing whether the account exists."""
        if not isinstance(password, str):
            return False
        return self.auth_store.verify_password(username, password)

    def login(self, username: str, password: str) -> dict[str, object]:
        """Verify a password and return either an access token or the MFA branch."""
        if not isinstance(username, str) or not isinstance(password, str):
            self._audit(username if isinstance(username, str) else "unknown", "login", None, "UNAUTHORIZED")
            raise DfshaError(ErrorCode.UNAUTHORIZED, "invalid username or password")
        user = self.auth_store.get_user(username)
        if user is None or not self.auth_store.verify_password(username, password):
            self._audit(username, "login", None, "UNAUTHORIZED")
            raise DfshaError(ErrorCode.UNAUTHORIZED, "invalid username or password")
        if user.totp_enabled:
            self._audit(username, "login", None, "MFA_REQUIRED")
            raise NotImplementedError(
                "TOTP verification is deferred to the security issue (ARCHITECTURE.md Section 9.1)"
            )
        token = issue_jwt(
            username, self.settings.jwt_secret, self.settings.jwt_ttl_s, self.clock.time()
        )
        self._audit(username, "login", None, "OK")
        return {
            "access_token": token,
            "token_type": "bearer",
            "expires_in": self.settings.jwt_ttl_s,
        }

    def authenticate_access_token(self, token: str) -> User:
        try:
            claims = verify_jwt(token, self.settings.jwt_secret, self.clock.time())
        except DfshaError:
            self._audit("unknown", "authenticate_access_token", None, "UNAUTHORIZED")
            raise
        user = self.auth_store.get_user(str(claims["sub"]))
        if user is None:
            self._audit(str(claims["sub"]), "authenticate_access_token", None, "UNAUTHORIZED")
            raise DfshaError(ErrorCode.UNAUTHORIZED, "access token subject no longer exists")
        self._audit(user.username, "authenticate_access_token", None, "OK")
        return user

    def authenticate_api_key(self, raw_key: str) -> User:
        """Validate a high-entropy API key through its stored SHA-256 digest."""
        digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest() if isinstance(raw_key, str) else ""
        matched = self.auth_store.get_api_key_by_hash(digest) if digest else None
        if matched is not None and matched.revoked_at is not None:
            matched = None
        if matched is None:
            self._audit("unknown", "authenticate_api_key", None, "UNAUTHORIZED")
            raise DfshaError(ErrorCode.UNAUTHORIZED, "invalid or revoked API key")
        user = self.auth_store.get_user(matched.user_id)
        if user is None:
            raise DfshaError(ErrorCode.UNAUTHORIZED, "API key owner no longer exists")
        self._audit(user.username, "authenticate_api_key", None, "OK")
        return user

    def create_api_key(self, name: str, user: str | None = None) -> dict[str, object]:
        actor = self._actor(user)
        if not isinstance(name, str) or not name.strip():
            self._audit(actor.username, "create_api_key", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "API key name is required")
        key_id = f"k_{uuid.uuid4().hex[:12]}"
        raw_key = f"dfsha_live_{key_id}.{secrets.token_urlsafe(32)}"
        key = ApiKey(
            key_id=key_id,
            user_id=actor.username,
            name=name.strip(),
            key_hash=hashlib.sha256(raw_key.encode("utf-8")).hexdigest(),
            created_at=self.clock.time(),
        )
        self.auth_store.create_api_key(key)
        self._audit(actor.username, "create_api_key", None, "OK")
        return {
            "key_id": key.key_id,
            "name": key.name,
            "api_key": raw_key,
            "created_at": key.created_at,
        }

    def list_api_keys(self, user: str | None = None) -> list[dict[str, object]]:
        actor = self._actor(user)
        self._audit(actor.username, "list_api_keys", None, "OK")
        return [
            {
                "key_id": key.key_id,
                "name": key.name,
                "created_at": key.created_at,
                "revoked_at": key.revoked_at,
            }
            for key in self.auth_store.list_api_keys(actor.username)
        ]

    def revoke_api_key(self, key_id: str, user: str | None = None) -> None:
        actor = self._actor(user)
        key = self.auth_store.get_api_key(key_id)
        if key is None or key.user_id != actor.username:
            self._audit(actor.username, "revoke_api_key", None, "FORBIDDEN")
            raise DfshaError(ErrorCode.FORBIDDEN, "API key does not belong to this user")
        self.auth_store.revoke_api_key(key_id, self.clock.time())
        self._audit(actor.username, "revoke_api_key", None, "OK")

    def create_group(self, name: str, user: str | None = None) -> Group:
        actor = self._actor(user)
        if not isinstance(name, str) or not name or name.strip() != name:
            self._audit(actor.username, "create_group", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "group name must be non-empty")
        if self.auth_store.get_group(name) is not None:
            self._audit(actor.username, "create_group", None, "ALREADY_EXISTS")
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"group {name} already exists")
        group = Group(name=name, owner=actor.username, members={actor.username})
        self.auth_store.create_group(group)
        self._audit(actor.username, "create_group", None, "OK")
        return group

    def add_group_members(
        self,
        name: str,
        add: list[str],
        remove: list[str],
        user: str | None = None,
    ) -> Group:
        actor = self._actor(user)
        group = self.auth_store.get_group(name)
        if group is None:
            self._audit(actor.username, "update_group_members", None, "GROUP_NOT_FOUND")
            raise DfshaError(ErrorCode.GROUP_NOT_FOUND, f"group {name} does not exist")
        if group.owner != actor.username:
            self._audit(actor.username, "update_group_members", None, "FORBIDDEN")
            raise DfshaError(ErrorCode.FORBIDDEN, "only the group owner may change membership")
        if (
            not isinstance(add, list)
            or not isinstance(remove, list)
            or any(not isinstance(username, str) or not username for username in [*add, *remove])
        ):
            self._audit(actor.username, "update_group_members", None, "BAD_REQUEST")
            raise DfshaError(ErrorCode.BAD_REQUEST, "add and remove must be lists of usernames")
        members = set(group.members)
        for username in add:
            if self.auth_store.get_user(username) is None:
                self._audit(actor.username, "update_group_members", None, "USER_NOT_FOUND")
                raise DfshaError(ErrorCode.USER_NOT_FOUND, f"user {username} does not exist")
            members.add(username)
        members.difference_update(remove)
        updated = self.auth_store.update_group_members(name, members)
        self._audit(actor.username, "update_group_members", None, "OK")
        return updated

    def get_group(self, name: str, user: str | None = None) -> Group:
        actor = self._actor(user)
        group = self.auth_store.get_group(name)
        if group is None:
            self._audit(actor.username, "get_group", None, "GROUP_NOT_FOUND")
            raise DfshaError(ErrorCode.GROUP_NOT_FOUND, f"group {name} does not exist")
        if group.owner != actor.username and actor.username not in group.members:
            self._audit(actor.username, "get_group", None, "FORBIDDEN")
            raise DfshaError(ErrorCode.FORBIDDEN, "only group members may view group details")
        self._audit(actor.username, "get_group", None, "OK")
        return group

    def effective_permission(self, username: str, path: str) -> Permission:
        """Union grants along ancestors; a closer `none` masks inherited grants."""
        canonical = join_path(split_path(path))
        node = self.store.stat(canonical)
        if node is not None and node.owner == username:
            return Permission.READ_WRITE
        memberships = {group.name for group in self.auth_store.groups() if username in group.members}
        permission = Permission.NONE
        for candidate in self._path_ancestors(canonical):
            matches = [
                entry
                for entry in self.auth_store.get_acl(candidate)
                if (
                    entry.subject == f"user:{username}"
                    or entry.subject in {f"group:{name}" for name in memberships}
                )
            ]
            if matches:
                if any(entry.permission is Permission.NONE for entry in matches):
                    return permission
                permission = max(permission, *(entry.permission for entry in matches))
        return permission

    @staticmethod
    def _path_ancestors(path: str) -> list[str]:
        parts = split_path(path)
        return [join_path(parts[:index]) for index in range(len(parts), -1, -1)]

    @_audited_fs_operation
    def set_acl(
        self,
        path: str,
        subject: str,
        permission: str,
        user: str | None = None,
    ) -> AclEntry:
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        node = self.store.stat(canonical)
        if node is None:
            parent_path = join_path(split_path(canonical)[:-1])
            self._require_permission(actor.username, parent_path, Permission.READ)
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{canonical} does not exist")
        if node.owner != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "only the path owner may change its ACL")
        if not isinstance(subject, str):
            raise DfshaError(ErrorCode.BAD_REQUEST, "subject must be user:<name> or group:<name>")
        kind, separator, name = subject.partition(":")
        if not separator or kind not in {"user", "group"} or not name:
            raise DfshaError(ErrorCode.BAD_REQUEST, "subject must be user:<name> or group:<name>")
        if kind == "user" and self.auth_store.get_user(name) is None:
            raise DfshaError(ErrorCode.USER_NOT_FOUND, f"user {name} does not exist")
        if kind == "group" and self.auth_store.get_group(name) is None:
            raise DfshaError(ErrorCode.GROUP_NOT_FOUND, f"group {name} does not exist")
        try:
            parsed = Permission.parse(permission)
        except (TypeError, ValueError) as error:
            raise DfshaError(ErrorCode.BAD_REQUEST, str(error)) from None
        entry = AclEntry(canonical, subject, parsed)
        self.auth_store.set_acl(entry)
        return entry

    @_audited_fs_operation
    def get_acl(self, path: str, user: str | None = None) -> list[AclEntry]:
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        if self.store.stat(canonical) is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{canonical} does not exist")
        self._require_permission(actor.username, canonical, Permission.READ)
        return self.auth_store.get_acl(canonical)

    def _require_permission(
        self,
        username: str,
        path: str,
        required: Permission,
    ) -> None:
        if self.effective_permission(username, path) < required:
            raise DfshaError(ErrorCode.FORBIDDEN, f"{username} lacks permission on {path}")

    # ---- DataNode registry and liveness ----------------------------------

    def register_datanode(
        self,
        node_id: str,
        internal_addr: str,
        public_addr: str,
        capacity_bytes: int,
        used_bytes: int = 0,
    ) -> DataNodeInfo:
        """Creates or refreshes a DataNode. Replica locations already known survive.

        Called on startup, after a restart, and whenever a heartbeat was answered
        with ``NODE_NOT_REGISTERED`` (``ARCHITECTURE.md`` Section 16, decision 19).
        """
        if not node_id or not isinstance(node_id, str):
            raise DfshaError(ErrorCode.BAD_REQUEST, "node_id must be a non-empty string")
        if not internal_addr or not public_addr:
            raise DfshaError(ErrorCode.BAD_REQUEST, "internal_addr and public_addr are required")
        if capacity_bytes < 0 or used_bytes < 0:
            raise DfshaError(ErrorCode.BAD_REQUEST, "capacity and usage cannot be negative")

        node = self.cluster.datanodes.get(node_id)
        if node is None:
            node = DataNodeInfo(node_id, internal_addr, public_addr, capacity_bytes, used_bytes)
            self.cluster.datanodes[node_id] = node
        else:
            node.internal_addr = internal_addr
            node.public_addr = public_addr
            node.capacity_bytes = capacity_bytes
            node.used_bytes = used_bytes
        self._mark_seen(node)
        return node

    def record_heartbeat(
        self,
        node_id: str,
        free_bytes: int,
        used_bytes: int,
        active_transfers: int = 0,
    ) -> list[Command]:
        """Refreshes liveness and free space; returns the node's pending commands.

        ``NODE_NOT_REGISTERED`` for an unknown node. That is the normal path after
        a failover or a NameNode restart, not a fault: the DataNode registers
        again and sends a full block report.
        """
        node = self._known(node_id)
        if free_bytes < 0 or used_bytes < 0:
            raise DfshaError(ErrorCode.BAD_REQUEST, "free and used bytes cannot be negative")
        node.used_bytes = used_bytes
        node.capacity_bytes = used_bytes + free_bytes  # the node reports the truth
        self._mark_seen(node)
        return self._take_commands(node_id)

    def check_liveness(self) -> list[str]:
        """Marks silent DataNodes dead and returns the ones that just died.

        Does nothing during the grace period after a failover, so a new leader
        does not declare the whole cluster dead before the DataNodes have had a
        chance to report (``API.md`` Section 9).
        """
        now = self.clock.monotonic()
        if now < self._grace_until:
            return []
        died: list[str] = []
        for node in list(self.cluster.datanodes.values()):
            if node.is_alive and now - node.last_seen_monotonic > self.settings.dead_node_timeout_s:
                node.is_alive = False
                died.append(node.node_id)
                logger.warning("DataNode %s is dead (no heartbeat)", node.node_id)
        return died

    def begin_failover_grace(self) -> None:
        """Starts the grace period a new leader waits before declaring anyone dead."""
        self._grace_until = self.clock.monotonic() + self.settings.failover_grace_s

    # ---- helpers ---------------------------------------------------------

    def _mark_seen(self, node: DataNodeInfo) -> None:
        """Records a sign of life. Revives a node that had been declared dead."""
        node.last_heartbeat = self.clock.time()
        node.last_seen_monotonic = self.clock.monotonic()
        node.is_alive = True

    def _known(self, node_id: str) -> DataNodeInfo:
        node = self.cluster.datanodes.get(node_id)
        if node is None:
            raise DfshaError(ErrorCode.NODE_NOT_REGISTERED, f"{node_id} is not registered")
        return node

    def _live_nodes(self) -> list[DataNodeInfo]:
        return [n for n in self.cluster.datanodes.values() if n.is_alive]

    def _enqueue(self, node_id: str, command: Command) -> None:
        """Queues a command for the node's next heartbeat, dropping exact duplicates."""
        queue = self.cluster.pending_commands.setdefault(node_id, [])
        for queued in queue:
            if (queued.cmd, queued.block_id, queued.targets) == (
                command.cmd, command.block_id, command.targets,
            ):
                return
        queue.append(command)

    def _take_commands(self, node_id: str) -> list[Command]:
        return self.cluster.pending_commands.pop(node_id, [])

    # ---- uploads ---------------------------------------------------------

    @_audited_fs_operation
    def prepare_file_upload(
        self,
        path: str,
        size_bytes: int,
        owner: str,
        overwrite: bool = False,
        user: str | None = None,
    ) -> UploadPlan:
        """Step 1 of ``put`` (``ARCHITECTURE.md`` Section 4.3).

        Validates, reserves the path as a `PENDING` file, splits it into blocks
        and chooses a pipeline for **each** block. The pipeline is a plan: nothing
        counts as a replica until a DataNode confirms it.
        """
        s = self.settings
        actor = self._actor(user)
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, "/ is a directory")
        path = join_path(segments)
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise DfshaError(ErrorCode.BAD_REQUEST, "size must be a non-negative integer")
        if size_bytes > s.max_file_size_bytes:
            raise DfshaError(
                ErrorCode.FILE_TOO_LARGE,
                f"{size_bytes} bytes exceeds the limit of {s.max_file_size_bytes}",
            )
        if not owner:
            raise DfshaError(ErrorCode.BAD_REQUEST, "an upload needs an owner")
        if owner != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "an upload owner must be the authenticated user")
        parent_path = join_path(segments[:-1])
        self.store.resolve_dir(parent_path)  # DIR_NOT_FOUND / NOT_A_DIRECTORY
        self._require_permission(actor.username, parent_path, Permission.READ_WRITE)

        now = self.clock.time()
        pending = self.store.get_pending(path)
        if pending is not None:
            if pending.expires_at is not None and pending.expires_at > now:
                raise DfshaError(ErrorCode.UPLOAD_IN_PROGRESS, f"{path} has an upload in progress")
            self._abort(pending, now)  # the previous upload expired: reclaim the path now

        existing = self.store.stat(path)
        if isinstance(existing, DirectoryMetadata):
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{path} is a directory")
        if existing is not None and not overwrite:
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"{path} already exists")

        live = self._live_nodes()
        if len(live) < s.replication_factor:
            raise DfshaError(
                ErrorCode.NOT_ENOUGH_DATANODES,
                f"need {s.replication_factor} live DataNodes, have {len(live)}",
            )

        file_id, blocks = self._new_blocks(size_bytes)
        pipelines = self._place(blocks, live)  # raises before anything is reserved
        file = FileMetadata(
            file_id=file_id,
            name=segments[-1],
            owner=owner,
            size_bytes=size_bytes,
            block_size=s.block_size,
            replication=s.replication_factor,
            status=FileStatus.PENDING,
            created_at=now,
            modified_at=now,
            expires_at=now + s.upload_ttl_s,
            blocks=blocks,
        )
        self.store.create_pending_file(path, file)

        planned: list[PlannedBlock] = []
        for block, chosen in zip(blocks, pipelines, strict=True):
            self.cluster.pipelines[block.block_id] = [n.node_id for n in chosen]
            for node in chosen:
                self._reserve(block.block_id, node.node_id, block.size_bytes)
            token = mint(
                block.block_id, BlockOp.WRITE, owner, s.block_token_secret, s.token_ttl_s, now=now
            )
            planned.append(PlannedBlock(block=block, pipeline=chosen, token=token))
        return UploadPlan(file=file, path=path, blocks=planned)

    @_audited_fs_operation
    def commit_file_upload(
        self,
        file_id: str,
        blocks: list[CommittedBlock],
        file_checksum: str | None = None,
        user: str | None = None,
    ) -> FileMetadata:
        """Step 3 of ``put`` (``ARCHITECTURE.md`` Section 4.3, ``API.md`` Section 5.2).

        Accepted only if every block is listed with the right size and every block
        has at least ``MIN_REPLICAS_COMMIT`` replicas **confirmed by DataNodes** on
        live nodes whose checksum equals the client's. That is the write quorum, and
        it is checked against confirmations, never against the planned pipeline.
        """
        s = self.settings
        actor = self._actor(user)
        file = self.store.get_file_by_id(file_id)
        if file is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"no upload with id {file_id}")
        if file.owner != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "only the upload owner may commit it")
        now = self.clock.time()

        if file.status is FileStatus.COMMITTED:
            if self._same_body(file, blocks, file_checksum):
                return file  # a retried commit is a success, not an error
            raise DfshaError(ErrorCode.ALREADY_EXISTS, "file already committed with other contents")
        if file.status is FileStatus.ABORTED:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, "the upload was aborted or expired")
        if file.expires_at is not None and file.expires_at <= now:
            self._abort(file, now)
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, "the upload expired")

        reported = self._validate_report(file, blocks, file_checksum)

        live = {n.node_id for n in self._live_nodes()}
        corrupt: dict[str, list[str]] = {}
        for block in file.blocks:
            claimed = reported[block.index].checksum
            checksums = self.cluster.replica_checksums.get(block.block_id, {})
            holders = sorted(self.cluster.replicas.get(block.block_id, set()) & live)
            good = [nid for nid in holders if checksums.get(nid) == claimed]
            if len(good) < s.min_replicas_commit:
                if len(holders) > len(good):
                    raise DfshaError(
                        ErrorCode.CHECKSUM_MISMATCH,
                        f"block {block.index}: a DataNode stored different bytes than the client sent",
                    )
                raise DfshaError(
                    ErrorCode.INSUFFICIENT_REPLICAS,
                    f"block {block.index} has {len(good)} confirmed replicas, "
                    f"{s.min_replicas_commit} required",
                )
            corrupt[block.block_id] = [nid for nid in holders if nid not in good]

        # the quorum holds: commit as one step
        for block in file.blocks:
            block.checksum = reported[block.index].checksum
        file.checksum = file_checksum
        replaced = self.store.commit_file(file_id, now)

        for block in file.blocks:
            self._release_block_reservations(block.block_id)  # unconfirmed hops never arrived
            self.cluster.pipelines.pop(block.block_id, None)
            for nid in corrupt[block.block_id]:  # a copy that disagrees is not a replica
                self._discard_replica(nid, block.block_id)
                self._enqueue(nid, Command("delete", block.block_id))
        if replaced is not None:
            self._schedule_deletion(replaced.blocks)
        return file

    @_audited_fs_operation
    def abort_file_upload(self, file_id: str, user: str | None = None) -> None:
        """Cancels a pending upload. Idempotent (``API.md`` Section 5.6)."""
        actor = self._actor(user)
        file = self.store.get_file_by_id(file_id)
        if file is None:
            return
        if file.owner != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "only the upload owner may abort it")
        if file.status is FileStatus.ABORTED:
            return
        if file.status is FileStatus.COMMITTED:
            raise DfshaError(ErrorCode.BAD_REQUEST, "the file is already committed; remove it instead")
        self._abort(file, self.clock.time())

    def record_block_received(
        self, node_id: str, block_id: str, size: int, checksum: str
    ) -> None:
        """A DataNode confirms it stored a block (``API.md`` Section 7).

        This is what turns an allocated pipeline slot into a replica. A block
        nobody asked for, or one that is the wrong size, is told to delete itself.
        """
        self._known(node_id)
        block = self.store.get_block(block_id)
        file = self.store.file_of_block(block_id)
        if block is None or file is None or file.status is FileStatus.ABORTED:
            self._enqueue(node_id, Command("delete", block_id))  # an orphan
            return
        if size != block.size_bytes:
            logger.warning("%s stored %s with %d bytes, expected %d", node_id, block_id, size, block.size_bytes)
            self._enqueue(node_id, Command("delete", block_id))
            return
        self._confirm_replica(node_id, block, checksum)

    # ---- upload internals ------------------------------------------------

    def _new_blocks(self, size_bytes: int) -> tuple[str, list[BlockMetadata]]:
        """A fresh file id and its blocks. Ids are re-drawn on the (remote) chance of a clash."""
        count = self.settings.blocks_for(size_bytes)
        while True:
            file_id = str(uuid.uuid4())
            short = file_id.replace("-", "")[:8]
            ids = [f"blk_{short}_{i:06d}" for i in range(count)]
            if self.store.get_file_by_id(file_id) is None and all(
                self.store.get_block(b) is None for b in ids
            ):
                break
        blocks: list[BlockMetadata] = []
        remaining = size_bytes
        for index, block_id in enumerate(ids):
            size = min(self.settings.block_size, remaining)
            blocks.append(BlockMetadata(block_id=block_id, index=index, size_bytes=size))
            remaining -= size
        return file_id, blocks

    def _place(self, blocks: list[BlockMetadata], live: list[DataNodeInfo]) -> list[list[DataNodeInfo]]:
        """A pipeline per block, accounting for space the earlier blocks will take.

        Works on a scratch copy of the reservations, so if a later block cannot be
        placed nothing has been reserved and there is nothing to roll back.
        """
        scratch = dict(self.cluster.reserved_bytes)
        result: list[list[DataNodeInfo]] = []
        for block in blocks:
            chosen = placement.choose_pipeline(
                live, scratch, block.size_bytes, self.settings.replication_factor, self.rng
            )
            for node in chosen:
                scratch[node.node_id] = scratch.get(node.node_id, 0) + block.size_bytes
            result.append(chosen)
        return result

    def _validate_report(
        self, file: FileMetadata, blocks: list[CommittedBlock], file_checksum: str | None
    ) -> dict[int, CommittedBlock]:
        """Checks the shape of a commit body and returns it indexed by block index."""
        by_index: dict[int, CommittedBlock] = {}
        for item in blocks:
            if item.index in by_index:
                raise DfshaError(ErrorCode.BAD_REQUEST, f"block {item.index} is listed twice")
            by_index[item.index] = item
        if set(by_index) != set(range(len(file.blocks))):
            raise DfshaError(
                ErrorCode.BAD_REQUEST, f"all {len(file.blocks)} blocks must be listed exactly once"
            )
        total = 0
        for block in file.blocks:
            item = by_index[block.index]
            if item.block_id != block.block_id:
                raise DfshaError(ErrorCode.BAD_REQUEST, f"block {block.index} has the wrong block_id")
            if item.size != block.size_bytes:
                raise DfshaError(
                    ErrorCode.BAD_REQUEST,
                    f"block {block.index} is {item.size} bytes, expected {block.size_bytes}",
                )
            if not _SHA256.fullmatch(item.checksum):
                raise DfshaError(ErrorCode.BAD_REQUEST, f"block {block.index} checksum is not SHA-256 hex")
            total += item.size
        if total != file.size_bytes:
            raise DfshaError(ErrorCode.BAD_REQUEST, "block sizes do not add up to the declared size")
        if file_checksum is not None and not _SHA256.fullmatch(file_checksum):
            raise DfshaError(ErrorCode.BAD_REQUEST, "file_checksum is not SHA-256 hex")
        return by_index

    @staticmethod
    def _same_body(file: FileMetadata, blocks: list[CommittedBlock], file_checksum: str | None) -> bool:
        """Whether a retried commit describes the file as it was committed."""
        by_index = {b.index: b for b in blocks}
        if len(blocks) != len(file.blocks) or len(by_index) != len(blocks):
            return False
        for block in file.blocks:
            item = by_index.get(block.index)
            if item is None or item.block_id != block.block_id or item.checksum != block.checksum:
                return False
        return file_checksum is None or file_checksum == file.checksum

    def _abort(self, file: FileMetadata, now: float) -> None:
        """`PENDING` to `ABORTED`, freeing the path and telling holders to delete."""
        self.store.abort_file(file.file_id, now)
        self._schedule_deletion(file.blocks)

    def _confirm_replica(self, node_id: str, block: BlockMetadata, checksum: str) -> None:
        """Records a confirmed copy and releases the space that was held for it."""
        self.cluster.replicas.setdefault(block.block_id, set()).add(node_id)
        self.cluster.replica_checksums.setdefault(block.block_id, {})[node_id] = checksum
        self._release_reservation(block.block_id, node_id)
        flight = self.cluster.in_flight_replications.get(block.block_id)
        if flight is not None:
            flight.pop(node_id, None)
            if not flight:
                del self.cluster.in_flight_replications[block.block_id]

    def _schedule_deletion(self, blocks: list[BlockMetadata]) -> None:
        """Forgets blocks and tells every node that may hold them to delete them.

        Nodes that are down are asked too; they receive the command when they
        return. Anything that still slips through is an orphan, which the next
        block report removes.
        """
        for block in blocks:
            block_id = block.block_id
            holders = set(self.cluster.replicas.pop(block_id, ()))
            holders |= set(self.cluster.pipelines.pop(block_id, ()))
            holders |= set(self.cluster.in_flight_replications.pop(block_id, {}))
            holders |= set(self.cluster.reservations.get(block_id, {}))
            self._release_block_reservations(block_id)
            self.cluster.replica_checksums.pop(block_id, None)
            for node_id in sorted(holders):
                if node_id in self.cluster.datanodes:
                    self._enqueue(node_id, Command("delete", block_id))

    def _reserve(self, block_id: str, node_id: str, nbytes: int) -> None:
        """Claims ``nbytes`` on a node for a block, once per (block, node)."""
        held = self.cluster.reservations.setdefault(block_id, {})
        if node_id in held:
            return
        held[node_id] = nbytes
        self.cluster.reserved_bytes[node_id] = self.cluster.reserved_bytes.get(node_id, 0) + nbytes

    def _release_reservation(self, block_id: str, node_id: str) -> None:
        """Gives back what ``_reserve`` claimed. Safe to call twice: the second is a no-op."""
        held = self.cluster.reservations.get(block_id)
        if held is None or node_id not in held:
            return
        nbytes = held.pop(node_id)
        if not held:
            del self.cluster.reservations[block_id]
        remaining = self.cluster.reserved_bytes.get(node_id, 0) - nbytes
        if remaining > 0:
            self.cluster.reserved_bytes[node_id] = remaining
        else:
            self.cluster.reserved_bytes.pop(node_id, None)

    def _release_block_reservations(self, block_id: str) -> None:
        for node_id in list(self.cluster.reservations.get(block_id, {})):
            self._release_reservation(block_id, node_id)

    # ---- reading ---------------------------------------------------------

    @_audited_fs_operation
    def get_file_blocks(self, path: str, user: str) -> FileLocation:
        """Step 1 of ``get`` (``API.md`` Section 5.3): the block map of a committed file.

        Only **live** replicas are listed, in random order so that readers spread
        over the replicas instead of all hitting the first one. A pending upload
        is invisible here, so a reader sees the complete old version or the
        complete new one, never a mix.
        """
        if not user:
            raise DfshaError(ErrorCode.BAD_REQUEST, "a read needs a user")
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        node = self.store.stat(canonical)
        if node is None:
            parent_path = join_path(split_path(canonical)[:-1])
            self._require_permission(actor.username, parent_path, Permission.READ)
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{canonical} does not exist")
        self._require_permission(actor.username, canonical, Permission.READ)
        if isinstance(node, DirectoryMetadata):
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{canonical} is a directory")

        now = self.clock.time()
        located: list[BlockLocation] = []
        for block in node.blocks:
            replicas = [self.cluster.datanodes[nid] for nid in self._live_holders(block.block_id)]
            self.rng.shuffle(replicas)
            token = mint(
                block.block_id, BlockOp.READ, user,
                self.settings.block_token_secret, self.settings.token_ttl_s, now=now,
            )
            located.append(BlockLocation(block=block, token=token, replicas=replicas))
        return FileLocation(file=node, path=canonical, blocks=located)

    # ---- file system operations (RF1) -------------------------------------

    @_audited_fs_operation
    def mkdir(
        self,
        path: str,
        owner: str,
        parents: bool = False,
        user: str | None = None,
    ) -> DirectoryMetadata:
        actor = self._actor(user)
        if not owner:
            raise DfshaError(ErrorCode.BAD_REQUEST, "a directory needs an owner")
        if owner != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "a directory owner must be the authenticated user")
        segments = split_path(path)
        if not segments:
            return self.store.mkdir(path, actor.username, self.clock.time(), parents=parents)
        parent_path = join_path(segments[:-1])
        if parents:
            while self.store.stat(parent_path) is None and parent_path != "/":
                parent_path = join_path(split_path(parent_path)[:-1])
        self._require_permission(actor.username, parent_path, Permission.READ_WRITE)
        return self.store.mkdir(path, actor.username, self.clock.time(), parents=parents)

    @_audited_fs_operation
    def ls(self, path: str, user: str | None = None) -> list[DirEntry]:
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        self._require_permission(actor.username, canonical, Permission.READ)
        return self.store.list_dir(canonical)

    @_audited_fs_operation
    def stat(self, path: str, user: str | None = None) -> DirectoryMetadata | FileMetadata:
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        node = self.store.stat(canonical)
        if node is None:
            parent_path = join_path(split_path(canonical)[:-1])
            self._require_permission(actor.username, parent_path, Permission.READ)
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{canonical} does not exist")
        self._require_permission(actor.username, canonical, Permission.READ)
        return node

    @_audited_fs_operation
    def rm(self, path: str, user: str | None = None) -> FileMetadata:
        """Removes a file. It vanishes from ``ls`` at once; its blocks are deleted
        afterwards through ``delete`` commands (``API.md`` Section 4.3)."""
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        parent_path = join_path(split_path(canonical)[:-1])
        self._require_permission(actor.username, parent_path, Permission.READ_WRITE)
        file = self.store.remove_file(canonical, self.clock.time())
        self.auth_store.remove_acl_subtree(canonical)
        self._schedule_deletion(file.blocks)
        return file

    @_audited_fs_operation
    def rmdir(
        self, path: str, recursive: bool = False, user: str | None = None
    ) -> int:
        """Removes a directory and returns how many files went with it."""
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        parent_path = join_path(split_path(canonical)[:-1])
        self._require_permission(actor.username, parent_path, Permission.READ_WRITE)
        removed = self.store.remove_dir(canonical, recursive, self.clock.time())
        self.auth_store.remove_acl_subtree(canonical)
        for file in removed:
            self._schedule_deletion(file.blocks)
        return len(removed)

    @_audited_fs_operation
    def mv(
        self, src: str, dst: str, user: str | None = None
    ) -> FileMetadata | DirectoryMetadata:
        """Moves a committed file or directory (API.md Section 4.3)."""
        actor = self._actor(user)
        src_canonical = join_path(split_path(src))
        dst_canonical = join_path(split_path(dst))

        src_parent = join_path(split_path(src_canonical)[:-1])
        dst_parent = join_path(split_path(dst_canonical)[:-1])

        # Require write on both parents
        self._require_permission(actor.username, src_parent, Permission.READ_WRITE)
        self._require_permission(actor.username, dst_parent, Permission.READ_WRITE)

        # Check conflicting locks
        self.check_lock(src_canonical, actor.username, "exclusive")
        self.check_lock(dst_canonical, actor.username, "exclusive")

        moved = self.store.mv(src_canonical, dst_canonical, self.clock.time())
        return moved

    # ---- file locks (API.md Section 4.4) ----------------------------------

    def lock_path(
        self,
        path: str,
        user: str,
        mode: str = "exclusive",
        ttl_s: int | None = None,
    ) -> dict[str, object]:
        actor = self._actor(user)
        canonical = join_path(split_path(path))
        ttl = ttl_s if ttl_s is not None else self.settings.lock_default_ttl_s
        now = self.clock.time()
        self._expire_locks(now)

        if mode not in {"shared", "exclusive"}:
            raise DfshaError(ErrorCode.BAD_REQUEST, "mode must be 'shared' or 'exclusive'")

        held = self._locks.get(canonical, [])
        for lk in held:
            if lk["user"] != actor.username:
                if mode == "exclusive" or lk["mode"] == "exclusive":
                    raise DfshaError(ErrorCode.LOCKED, f"path {canonical} is currently locked")

        lock_id = f"lk_{uuid.uuid4().hex[:8]}"
        expires_at = now + ttl
        lock_entry = {
            "lock_id": lock_id,
            "path": canonical,
            "user": actor.username,
            "mode": mode,
            "expires_at": expires_at,
        }
        self._locks.setdefault(canonical, []).append(lock_entry)
        self._locks_by_id[lock_id] = lock_entry

        return {
            "lock_id": lock_id,
            "path": canonical,
            "type": mode,
            "expires_at": expires_at,
        }

    def renew_lock(
        self,
        lock_id: str,
        user: str,
        ttl_s: int | None = None,
    ) -> dict[str, object]:
        actor = self._actor(user)
        now = self.clock.time()
        self._expire_locks(now)
        lock_entry = self._locks_by_id.get(lock_id)
        if lock_entry is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"lock {lock_id} not found or expired")
        if lock_entry["user"] != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "only the lock owner may renew it")

        ttl = ttl_s if ttl_s is not None else self.settings.lock_default_ttl_s
        lock_entry["expires_at"] = now + ttl
        return {
            "lock_id": lock_id,
            "expires_at": lock_entry["expires_at"],
        }

    def unlock_path(self, lock_id: str, user: str) -> None:
        actor = self._actor(user)
        now = self.clock.time()
        self._expire_locks(now)
        lock_entry = self._locks_by_id.get(lock_id)
        if lock_entry is None:
            return  # Idempotent release
        if lock_entry["user"] != actor.username:
            raise DfshaError(ErrorCode.FORBIDDEN, "only the lock owner may unlock it")

        path = lock_entry["path"]
        del self._locks_by_id[lock_id]
        if path in self._locks:
            self._locks[path] = [lk for lk in self._locks[path] if lk["lock_id"] != lock_id]
            if not self._locks[path]:
                del self._locks[path]

    def check_lock(self, path: str, username: str, required_access: str = "shared") -> None:
        """Raises LOCKED (423) if path has a conflicting lock held by another user."""
        self._expire_locks(self.clock.time())
        held = self._locks.get(path, [])
        for lk in held:
            if lk["user"] != username:
                if required_access == "exclusive" or lk["mode"] == "exclusive":
                    raise DfshaError(ErrorCode.LOCKED, f"path {path} is locked by another user")

    def _expire_locks(self, now: float) -> None:
        expired_ids = [lid for lid, lk in self._locks_by_id.items() if lk["expires_at"] <= now]
        for lid in expired_ids:
            lk = self._locks_by_id.pop(lid, None)
            if lk is not None:
                p = lk["path"]
                if p in self._locks:
                    self._locks[p] = [item for item in self._locks[p] if item["lock_id"] != lid]
                    if not self._locks[p]:
                        del self._locks[p]

    def _live_holders(self, block_id: str) -> list[str]:
        """Ids of live nodes that have confirmed a copy of the block, sorted."""
        holders = self.cluster.replicas.get(block_id, set())
        return sorted(
            nid for nid in holders
            if (node := self.cluster.datanodes.get(nid)) is not None and node.is_alive
        )

    # ---- block reports -----------------------------------------------------

    def process_block_report(self, node_id: str, blocks: list[ReportedBlock]) -> list[Command]:
        """Reconciles a DataNode's full inventory with the metadata (``API.md`` Section 7).

        This is how replica locations are rebuilt after a failover, since they are
        soft state. It also finds orphans (blocks nobody owns, or that belong to an
        aborted upload) and damaged copies, and notices blocks the node used to hold
        and no longer reports. Returns the node's pending commands.
        """
        self._known(node_id)
        reported: set[str] = set()
        for item in blocks:
            reported.add(item.block_id)
            block = self.store.get_block(item.block_id)
            file = self.store.file_of_block(item.block_id)
            if block is None or file is None or file.status is FileStatus.ABORTED:
                self._enqueue(node_id, Command("delete", item.block_id))  # an orphan
                continue
            damaged = item.size != block.size_bytes or (
                block.checksum is not None and item.checksum != block.checksum
            )
            if damaged:
                self._discard_replica(node_id, item.block_id)
                self._enqueue(node_id, Command("delete", item.block_id))
                continue
            self._confirm_replica(node_id, block, item.checksum)

        # blocks this node was believed to hold but did not report are gone
        for block_id, holders in list(self.cluster.replicas.items()):
            if node_id in holders and block_id not in reported:
                self._discard_replica(node_id, block_id)
        return self._take_commands(node_id)

    def record_block_corrupt(self, node_id: str, block_id: str) -> None:
        """A DataNode found a damaged block and deleted it (``API.md`` Section 7).

        The replica stops counting; the next replication scan notices the deficit
        and schedules a fresh copy.
        """
        self._known(node_id)
        self._discard_replica(node_id, block_id)

    def _discard_replica(self, node_id: str, block_id: str) -> None:
        """Stops counting ``node_id`` as a holder of ``block_id``."""
        holders = self.cluster.replicas.get(block_id)
        if holders is not None:
            holders.discard(node_id)
            if not holders:
                del self.cluster.replicas[block_id]
        checksums = self.cluster.replica_checksums.get(block_id)
        if checksums is not None:
            checksums.pop(node_id, None)
            if not checksums:
                del self.cluster.replica_checksums[block_id]

    # ---- re-replication ----------------------------------------------------

    def scan_replication(self) -> ReplicationScan:
        """Restores the target replica count (``ARCHITECTURE.md`` Section 6.6).

        * Only blocks of `COMMITTED` files are examined. A `PENDING` upload is
          legitimately short of replicas while it is being written.
        * The most endangered blocks go first: one with a single live replica
          before one with two.
        * The ``replicate`` command goes to a node that already holds the block (the
          **source**); the NameNode never moves bytes itself.
        * A copy already in flight is not scheduled twice, but gives up after
          ``IN_FLIGHT_TIMEOUT_S`` so a lost command cannot block a repair forever.
        * Surplus replicas, for example when a dead node comes back after its blocks
          were re-created elsewhere, are ordered deleted from the fullest nodes.
        """
        mono = self.clock.monotonic()
        self._expire_in_flight(mono)
        live = self._live_nodes()

        deficits: list[tuple[int, str, int, FileMetadata, BlockMetadata, list[str]]] = []
        deleted = 0
        for file in self.store.files(FileStatus.COMMITTED):
            for block in file.blocks:
                holders = self._live_holders(block.block_id)
                if len(holders) > file.replication:
                    deleted += self._trim_surplus(block, file.replication, holders)
                elif len(holders) < file.replication:
                    deficits.append((len(holders), file.file_id, block.index, file, block, holders))
        deficits.sort(key=lambda d: d[:3])  # fewest live replicas first, then deterministic

        scheduled = unrecoverable = 0
        reserved = self.cluster.reserved_bytes
        for live_count, _, _, file, block, holders in deficits:
            if live_count == 0:
                unrecoverable += 1  # nothing left to copy from
                continue
            block_id = block.block_id
            in_flight = self.cluster.in_flight_replications.get(block_id, {})
            needed = file.replication - live_count - len(in_flight)
            if needed <= 0:
                continue
            exclude = set(self.cluster.replicas.get(block_id, ())) | set(in_flight)
            eligible = placement.eligible_nodes(live, reserved, block.size_bytes, exclude)
            targets = placement.weighted_sample(eligible, needed, self.rng, reserved)
            if not targets:
                continue  # no node can take it yet; try again next scan
            source = self.rng.choice(holders)
            token = mint(
                block_id, BlockOp.WRITE, SYSTEM_PRINCIPAL,
                self.settings.block_token_secret, self.settings.token_ttl_s, now=self.clock.time(),
            )
            self._enqueue(source, Command("replicate", block_id, [t.node_id for t in targets], token))
            for target in targets:
                self.cluster.in_flight_replications.setdefault(block_id, {})[target.node_id] = mono
                self._reserve(block_id, target.node_id, block.size_bytes)
            scheduled += 1

        return ReplicationScan(
            under_replicated=len(deficits),
            unrecoverable=unrecoverable,
            scheduled=scheduled,
            deleted=deleted,
        )

    def _trim_surplus(self, block: BlockMetadata, keep: int, holders: list[str]) -> int:
        """Orders the fullest nodes to drop copies until ``keep`` remain."""
        surplus = len(holders) - keep
        fullest_first = sorted(
            holders, key=lambda nid: (self.cluster.datanodes[nid].free_bytes(), nid)
        )
        for node_id in fullest_first[:surplus]:
            self._discard_replica(node_id, block.block_id)
            self._enqueue(node_id, Command("delete", block.block_id))
        return surplus

    def _expire_in_flight(self, mono: float) -> None:
        """Drops copies that timed out or whose target died, releasing their space."""
        for block_id, targets in list(self.cluster.in_flight_replications.items()):
            for node_id, started in list(targets.items()):
                node = self.cluster.datanodes.get(node_id)
                if node is None or not node.is_alive or mono - started > IN_FLIGHT_TIMEOUT_S:
                    del targets[node_id]
                    self._release_reservation(block_id, node_id)
            if not targets:
                del self.cluster.in_flight_replications[block_id]

    # ---- garbage collection ------------------------------------------------

    def collect_garbage(self) -> GcResult:
        """Expires stale uploads and forgets old aborted records (``ARCHITECTURE.md`` Section 4.2).

        A `PENDING` upload past its ``expires_at`` becomes `ABORTED`: its path is
        freed, its reserved space released, and every node that may hold a piece is
        told to delete it (test plan T18). The `ABORTED` record is kept for one more
        ``UPLOAD_TTL_S`` so a late commit or block report can still be recognised
        instead of looking like an unknown id, and is then purged.
        """
        now = self.clock.time()
        expired = [
            f for f in self.store.files(FileStatus.PENDING)
            if f.expires_at is not None and f.expires_at <= now
        ]
        for file in expired:
            self._abort(file, now)
        purged = self.store.purge_aborted(older_than=now - self.settings.upload_ttl_s)
        return GcResult(expired=len(expired), purged=len(purged))

    # ---- observability -----------------------------------------------------

    def stats(self) -> ClusterStats:
        """Cluster totals, including the under-replicated count the demos watch."""
        committed = self.store.files(FileStatus.COMMITTED)
        under = sum(
            1
            for file in committed
            for block in file.blocks
            if len(self._live_holders(block.block_id)) < file.replication
        )
        nodes = list(self.cluster.datanodes.values())
        return ClusterStats(
            files=len(committed),
            blocks=sum(len(f.blocks) for f in committed),
            under_replicated_blocks=under,
            live_nodes=sum(1 for n in nodes if n.is_alive),
            dead_nodes=sum(1 for n in nodes if not n.is_alive),
            bytes_stored=sum(f.size_bytes for f in committed),
            term=self.term,
        )

    # ---- background tasks --------------------------------------------------

    async def run(self) -> None:
        """Runs the REST server and the background tasks until cancelled."""
        await asyncio.gather(
            self.api_server(),
            self.monitor_datanodes(),
            self.monitor_replication(),
            self.garbage_collector(),
        )

    async def api_server(self) -> None:
        """Serves the REST API using uvicorn and FastAPI."""
        import uvicorn
        from server.nodes.namenode.api import create_app

        app = create_app(self)
        config = uvicorn.Config(
            app=app,
            host=self.settings.namenode_host,
            port=self.settings.namenode_port,
            log_level="info",
        )
        server = uvicorn.Server(config)
        await server.serve()

    async def monitor_datanodes(self) -> None:
        await self._every(LIVENESS_PERIOD_S, self.check_liveness)

    async def monitor_replication(self) -> None:
        await self._every(REPLICATION_PERIOD_S, self.scan_replication)

    async def garbage_collector(self) -> None:
        await self._every(GC_PERIOD_S, self.collect_garbage)

    async def _every(self, period_s: float, task: Callable[[], object]) -> None:
        """Calls ``task`` forever, ``period_s`` apart.

        A failure is logged and the loop carries on. These tasks are what notice a
        dead DataNode and repair its blocks, so one bad iteration must never stop them.
        """
        while True:
            try:
                task()
            except Exception:  # a background task must outlive any single failure
                logger.exception("background task %s failed; will retry", getattr(task, "__name__", task))
            await asyncio.sleep(period_s)
