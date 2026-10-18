"""Authentication and access-control models owned by the NameNode."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class Permission(IntEnum):
    """Ordered permissions; a higher value grants every lower capability."""

    NONE = 0
    READ = 1
    READ_WRITE = 2

    @classmethod
    def parse(cls, value: str) -> Permission:
        if not isinstance(value, str):
            raise TypeError("permission must be 'r', 'rw' or 'none'")
        try:
            return {"none": cls.NONE, "r": cls.READ, "rw": cls.READ_WRITE}[value]
        except KeyError:
            raise ValueError("permission must be 'r', 'rw' or 'none'") from None


@dataclass
class User:
    username: str
    password_hash: str = field(repr=False)
    role: str
    created_at: float
    totp_enabled: bool = False


@dataclass
class ApiKey:
    key_id: str
    user_id: str
    name: str
    key_hash: str = field(repr=False)
    created_at: float
    revoked_at: float | None = None


@dataclass
class Group:
    name: str
    owner: str
    members: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class AclEntry:
    path: str
    subject: str
    permission: Permission


@dataclass(frozen=True)
class AuditEntry:
    timestamp: float
    user: str
    operation: str
    path: str | None
    result: str
    request_id: str | None = None
