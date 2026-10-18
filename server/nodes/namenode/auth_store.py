"""Hard-state interface and in-memory implementation for auth and ACL data."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import replace

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

from server.nodes.namenode.auth_models import (
    AclEntry,
    ApiKey,
    AuditEntry,
    Group,
    User,
)


class AuthStore(ABC):
    """Separate hard-state boundary for accounts, groups, ACLs and audit rows."""

    @abstractmethod
    def create_user(self, user: User) -> None: ...

    @abstractmethod
    def get_user(self, username: str) -> User | None: ...

    @abstractmethod
    def verify_password(self, username: str, password: str) -> bool: ...

    @abstractmethod
    def users(self) -> list[User]: ...

    @abstractmethod
    def create_api_key(self, api_key: ApiKey) -> None: ...

    @abstractmethod
    def get_api_key(self, key_id: str) -> ApiKey | None: ...

    @abstractmethod
    def get_api_key_by_hash(self, key_hash: str) -> ApiKey | None: ...

    @abstractmethod
    def api_keys(self) -> list[ApiKey]: ...

    @abstractmethod
    def groups(self) -> list[Group]: ...

    @abstractmethod
    def revoke_api_key(self, key_id: str, now: float) -> ApiKey | None: ...

    @abstractmethod
    def list_api_keys(self, username: str) -> list[ApiKey]: ...

    @abstractmethod
    def create_group(self, group: Group) -> None: ...

    @abstractmethod
    def get_group(self, name: str) -> Group | None: ...

    @abstractmethod
    def update_group_members(self, name: str, members: set[str]) -> Group: ...

    @abstractmethod
    def set_acl(self, entry: AclEntry) -> None: ...

    @abstractmethod
    def get_acl(self, path: str) -> list[AclEntry]: ...

    @abstractmethod
    def remove_acl_subtree(self, path: str) -> None: ...

    @abstractmethod
    def audit(self, entry: AuditEntry) -> None: ...

    @abstractmethod
    def audit_entries(self) -> list[AuditEntry]: ...


class InMemoryAuthStore(AuthStore):
    """Synchronous in-memory auth storage, suitable for tests and development."""

    def __init__(self) -> None:
        self._users: dict[str, User] = {}
        self._password_hasher = PasswordHasher()
        self._api_keys: dict[str, ApiKey] = {}
        self._api_keys_by_hash: dict[str, str] = {}
        self._groups: dict[str, Group] = {}
        self._acls: dict[tuple[str, str], AclEntry] = {}
        self._audit: list[AuditEntry] = []

    def create_user(self, user: User) -> None:
        self._users[user.username] = replace(user)

    def get_user(self, username: str) -> User | None:
        user = self._users.get(username)
        return replace(user) if user is not None else None

    def verify_password(self, username: str, password: str) -> bool:
        user = self._users.get(username)
        if user is None:
            return False
        try:
            return self._password_hasher.verify(user.password_hash, password)
        except (VerifyMismatchError, InvalidHashError):
            return False

    def users(self) -> list[User]:
        return [replace(user) for user in self._users.values()]

    def create_api_key(self, api_key: ApiKey) -> None:
        self._api_keys[api_key.key_id] = replace(api_key)
        self._api_keys_by_hash[api_key.key_hash] = api_key.key_id

    def get_api_key(self, key_id: str) -> ApiKey | None:
        key = self._api_keys.get(key_id)
        return replace(key) if key is not None else None

    def get_api_key_by_hash(self, key_hash: str) -> ApiKey | None:
        key_id = self._api_keys_by_hash.get(key_hash)
        key = self._api_keys.get(key_id) if key_id is not None else None
        return replace(key) if key is not None else None

    def api_keys(self) -> list[ApiKey]:
        return [replace(key) for key in self._api_keys.values()]

    def revoke_api_key(self, key_id: str, now: float) -> ApiKey | None:
        key = self._api_keys.get(key_id)
        if key is not None and key.revoked_at is None:
            key.revoked_at = now
        return replace(key) if key is not None else None

    def list_api_keys(self, username: str) -> list[ApiKey]:
        return [
            replace(key) for key in self._api_keys.values() if key.user_id == username
        ]

    def create_group(self, group: Group) -> None:
        self._groups[group.name] = replace(group, members=set(group.members))

    def get_group(self, name: str) -> Group | None:
        group = self._groups.get(name)
        return replace(group, members=set(group.members)) if group is not None else None

    def groups(self) -> list[Group]:
        return [replace(group, members=set(group.members)) for group in self._groups.values()]

    def update_group_members(self, name: str, members: set[str]) -> Group:
        group = self._groups[name]
        group.members = set(members)
        return replace(group, members=set(group.members))

    def set_acl(self, entry: AclEntry) -> None:
        self._acls[(entry.path, entry.subject)] = entry

    def get_acl(self, path: str) -> list[AclEntry]:
        return [entry for entry in self._acls.values() if entry.path == path]

    def remove_acl_subtree(self, path: str) -> None:
        for key in list(self._acls):
            acl_path = key[0]
            if acl_path == path or path == "/" or acl_path.startswith(path + "/"):
                del self._acls[key]

    def audit(self, entry: AuditEntry) -> None:
        self._audit.append(entry)

    def audit_entries(self) -> list[AuditEntry]:
        return list(self._audit)


__all__ = ["AuthStore", "InMemoryAuthStore"]
