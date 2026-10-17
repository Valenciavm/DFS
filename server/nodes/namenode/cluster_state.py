"""Soft-state models for the NameNode.

Everything the leader learns from the DataNodes and loses on failover: which
nodes exist and are alive, where replicas are, upload pipelines, pending
commands and reserved space. Rebuilt from register, heartbeats and block
reports.

Reference: docs/NAMENODE_MODELS.md section 6.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DataNodeInfo:
    """A DataNode as the leader currently sees it (soft state)."""

    node_id: str
    internal_addr: str  # host:port used by other DataNodes (pipeline, re-replication)
    public_addr: str    # host:port given to clients
    capacity_bytes: int = 0
    used_bytes: int = 0
    last_heartbeat: float = field(default_factory=time.time)  # UTC wall clock, informational
    last_seen_monotonic: float = 0.0  # time.monotonic() at the last heartbeat; used for liveness
    is_alive: bool = True

    def free_bytes(self) -> int:
        """Free space derived from capacity and usage."""
        return max(0, self.capacity_bytes - self.used_bytes)


@dataclass
class Command:
    """A command delivered to a DataNode in a heartbeat or block-report response."""

    cmd: str  # "replicate" | "delete"
    block_id: str
    targets: list[str] = field(default_factory=list)  # target node_ids (replicate)
    token: Optional[str] = None  # block token for the target write (replicate)


@dataclass
class ClusterState:
    """All the soft state the leader keeps in memory."""

    datanodes: dict[str, DataNodeInfo] = field(default_factory=dict)
    replicas: dict[str, set[str]] = field(default_factory=dict)
    pipelines: dict[str, list[str]] = field(default_factory=dict)
    pending_commands: dict[str, list[Command]] = field(default_factory=dict)
    in_flight_replications: dict[str, set[str]] = field(default_factory=dict)
    reserved_bytes: dict[str, int] = field(default_factory=dict)
