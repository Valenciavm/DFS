"""Cluster configuration, read from the environment exactly once.

Every tunable in ``API.md`` Section 1 lives here with its contract default, so
the NameNode, the DataNode and the tests cannot drift apart. Sizes are
**binary** throughout (MiB, GiB) — ``ARCHITECTURE.md`` Section 16, decision 17.

Usage::

    from server.common.config import settings
    settings.block_size            # 67108864

Tests override by building their own instance::

    Settings(block_size=1 << 20)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB


class ConfigError(ValueError):
    """An environment variable is present but unusable."""


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    """Reads a positive integer, failing loudly instead of silently defaulting."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name}={raw!r} is not an integer") from None
    if value < minimum:
        raise ConfigError(f"{name}={value} must be >= {minimum}")
    return value


def _env_str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return default if raw is None or raw.strip() == "" else raw.strip()


@dataclass(frozen=True)
class Settings:
    """Resolved configuration. Frozen, so nothing can mutate it at runtime."""

    # --- blocks and replication (API.md Section 1) ---
    block_size: int = 64 * MiB
    block_chunk_size: int = 1 * MiB
    replication_factor: int = 3
    min_replicas_commit: int = 2
    max_file_size_bytes: int = 10 * GiB

    # --- liveness and reporting ---
    heartbeat_interval_s: int = 5
    dead_node_timeout_s: int = 30
    block_report_interval_s: int = 60

    # --- lifetimes ---
    token_ttl_s: int = 900
    upload_ttl_s: int = 3600
    lock_default_ttl_s: int = 60
    mfa_challenge_ttl_s: int = 300

    # --- high availability (API.md Section 9) ---
    lease_s: int = 15
    failover_grace_s: int = 30

    # --- paging ---
    ls_default_limit: int = 1000

    # --- authentication (API.md Section 4.1) ---
    #: JWT access-token lifetime. Intentionally distinct from token_ttl_s,
    #: which is for gRPC block tokens.
    jwt_ttl_s: int = 3600

    # --- secrets (never defaulted to a usable value) ---
    block_token_secret: str = field(default="", repr=False)
    node_key: str = field(default="", repr=False)
    #: Signs JWT access tokens (HS256). Must be set before the REST layer starts.
    jwt_secret: str = field(default="", repr=False)
    #: First admin bootstrapped on first start (ARCHITECTURE.md Section 9).
    admin_user: str = ""
    admin_password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Checks the invariants that make the rest of the system meaningful."""
        if self.min_replicas_commit > self.replication_factor:
            raise ConfigError(
                f"MIN_REPLICAS_COMMIT ({self.min_replicas_commit}) cannot exceed "
                f"REPLICATION_FACTOR ({self.replication_factor}): no commit could ever succeed"
            )
        if self.block_chunk_size > self.block_size:
            raise ConfigError(
                f"BLOCK_CHUNK_SIZE ({self.block_chunk_size}) cannot exceed "
                f"BLOCK_SIZE ({self.block_size})"
            )
        if self.block_chunk_size > 2 * MiB:
            # a DataNode rejects chunks above 2 MiB (API.md Section 6.1)
            raise ConfigError(
                f"BLOCK_CHUNK_SIZE ({self.block_chunk_size}) exceeds the 2 MiB limit "
                "a DataNode will accept"
            )
        if self.dead_node_timeout_s <= self.heartbeat_interval_s:
            raise ConfigError(
                f"DEAD_NODE_TIMEOUT_S ({self.dead_node_timeout_s}) must be greater than "
                f"HEARTBEAT_INTERVAL_S ({self.heartbeat_interval_s}), otherwise live nodes "
                "are declared dead between beats"
            )
        if self.failover_grace_s < self.heartbeat_interval_s:
            raise ConfigError(
                f"FAILOVER_GRACE_S ({self.failover_grace_s}) must be at least one heartbeat "
                f"interval ({self.heartbeat_interval_s}), otherwise a new leader declares every "
                "DataNode dead before any of them can report"
            )
        if self.lease_s <= 1:
            raise ConfigError(f"LEASE_S ({self.lease_s}) is too short to be renewed reliably")
        if self.jwt_ttl_s <= 0:
            raise ConfigError("JWT_TTL_S must be a positive number of seconds")

    @property
    def lease_renew_interval_s(self) -> float:
        """The leader renews every `LEASE_S / 3` (ARCHITECTURE.md Section 4.2)."""
        return self.lease_s / 3

    def blocks_for(self, size_bytes: int) -> int:
        """Number of blocks a file of ``size_bytes`` needs.

        ``ARCHITECTURE.md`` Section 6.1: ceil, so the remainder is not lost, and
        at least one block, so an empty file still has one block of 0 bytes.
        """
        if size_bytes < 0:
            raise ValueError("size_bytes cannot be negative")
        return max(1, -(-size_bytes // self.block_size))  # ceil without floats


def load_settings() -> Settings:
    """Builds `Settings` from the environment, applying the contract defaults."""
    return Settings(
        block_size=_env_int("BLOCK_SIZE", 64 * MiB),
        block_chunk_size=_env_int("BLOCK_CHUNK_SIZE", 1 * MiB),
        replication_factor=_env_int("REPLICATION_FACTOR", 3),
        min_replicas_commit=_env_int("MIN_REPLICAS_COMMIT", 2),
        max_file_size_bytes=_env_int("MAX_FILE_SIZE_BYTES", 10 * GiB),
        heartbeat_interval_s=_env_int("HEARTBEAT_INTERVAL_S", 5),
        dead_node_timeout_s=_env_int("DEAD_NODE_TIMEOUT_S", 30),
        block_report_interval_s=_env_int("BLOCK_REPORT_INTERVAL_S", 60),
        token_ttl_s=_env_int("TOKEN_TTL_S", 900),
        upload_ttl_s=_env_int("UPLOAD_TTL_S", 3600),
        lock_default_ttl_s=_env_int("LOCK_DEFAULT_TTL_S", 60),
        mfa_challenge_ttl_s=_env_int("MFA_CHALLENGE_TTL_S", 300),
        lease_s=_env_int("LEASE_S", 15),
        failover_grace_s=_env_int("FAILOVER_GRACE_S", 30),
        ls_default_limit=_env_int("LS_DEFAULT_LIMIT", 1000),
        block_token_secret=_env_str("BLOCK_TOKEN_SECRET", ""),
        node_key=_env_str("NODE_KEY", ""),
        jwt_ttl_s=_env_int("JWT_TTL_S", 3600),
        jwt_secret=_env_str("JWT_SECRET", ""),
        admin_user=_env_str("ADMIN_USER", ""),
        admin_password=_env_str("ADMIN_PASSWORD", ""),
    )


#: Process-wide configuration. Import this, do not call `load_settings` again.
settings = load_settings()

__all__ = ["ConfigError", "GiB", "KiB", "MiB", "Settings", "load_settings", "settings"]
