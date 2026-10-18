"""Hard-state metadata models for the NameNode.

Only what must survive a NameNode restart and is persisted in PostgreSQL lives
here: the namespace tree, files and blocks. Soft state (live DataNodes, replica
locations, pending commands) lives in ``cluster_state.py``.

Reference: docs/NAMENODE_MODELS.md.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from server.nodes.namenode.cluster_state import DataNodeInfo


class FileStatus(str, Enum):
    """Lifecycle of a file entry (see ARCHITECTURE.md section 3)."""

    PENDING = "PENDING"      # upload reserved; invisible to ls/get; has expires_at
    COMMITTED = "COMMITTED"  # visible and immutable (WORM)
    ABORTED = "ABORTED"      # cancelled or expired; kept briefly, then collected


@dataclass
class BlockMetadata:
    """One fragment of a file (hard state)."""

    block_id: str
    index: int
    size_bytes: int
    checksum: Optional[str] = None  # SHA-256 hex of the plaintext block; set at commit


@dataclass
class FileMetadata:
    """The whole logical file (hard state).

    The full path is computed from the parent chain, never stored here.
    """

    file_id: str
    name: str
    owner: str
    size_bytes: int
    block_size: int
    replication: int
    status: FileStatus = FileStatus.PENDING
    checksum: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None  # only for PENDING uploads
    blocks: list[BlockMetadata] = field(default_factory=list)


@dataclass
class DirectoryMetadata:
    """A node of the namespace tree (hard state)."""

    name: str  # "" for the root, otherwise a single path segment
    owner: str
    parent: Optional[DirectoryMetadata] = None
    children: dict[str, DirectoryMetadata] = field(default_factory=dict)
    files: dict[str, FileMetadata] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)

    def path(self) -> str:
        """Absolute path computed from the parent chain; the root is "/"."""
        if self.parent is None:
            return "/"
        parts: list[str] = []
        node: Optional[DirectoryMetadata] = self
        while node is not None and node.parent is not None:
            parts.append(node.name)
            node = node.parent
        return "/" + "/".join(reversed(parts))


class MetadataStore:
    """In-memory holder of the hard-state indexes.

    Temporary home: moves to ``store/`` with an interface and a PostgreSQL
    implementation in a following change.
    """

    def __init__(self) -> None:
        self.root: DirectoryMetadata = DirectoryMetadata(name="", owner="system")
        self.files_by_id: dict[str, FileMetadata] = {}
        self.files_by_path: dict[str, FileMetadata] = {}
        self.blocks_by_id: dict[str, BlockMetadata] = {}
        self.datanodes: dict[str, DataNodeInfo] = {}
