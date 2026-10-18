"""Hard-state metadata models for the NameNode.

Only what must survive a NameNode restart and is persisted in PostgreSQL lives
here: the namespace tree, files and blocks. Soft state (live DataNodes, replica
locations, pending commands) lives in ``cluster_state.py``. The persisted
interface over these models is ``store/base.py``.

Reference: docs/NAMENODE_MODELS.md.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class FileStatus(str, Enum):
    """Lifecycle of a file entry (see ARCHITECTURE.md Section 3)."""

    PENDING = "PENDING"      # upload reserved; invisible to ls/get; has expires_at
    COMMITTED = "COMMITTED"  # visible and immutable (WORM)
    ABORTED = "ABORTED"      # cancelled or expired; kept briefly, then collected


@dataclass
class BlockMetadata:
    """One fragment of a file (hard state)."""

    block_id: str
    index: int
    size_bytes: int
    checksum: str | None = None  # SHA-256 hex of the plaintext block; set at commit


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
    checksum: str | None = None
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)
    expires_at: float | None = None  # only for PENDING uploads
    blocks: list[BlockMetadata] = field(default_factory=list)


@dataclass(eq=False)
class DirectoryMetadata:
    """A node of the namespace tree (hard state).

    ``eq=False`` keeps identity comparison: the default dataclass ``__eq__``
    would walk ``parent`` and ``children`` into each other forever.
    """

    name: str  # "" for the root, otherwise a single path segment
    owner: str
    parent: DirectoryMetadata | None = None
    children: dict[str, DirectoryMetadata] = field(default_factory=dict)
    files: dict[str, FileMetadata] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)

    def path(self) -> str:
        """Absolute path computed from the parent chain; the root is "/"."""
        if self.parent is None:
            return "/"
        parts: list[str] = []
        node: DirectoryMetadata | None = self
        while node is not None and node.parent is not None:
            parts.append(node.name)
            node = node.parent
        return "/" + "/".join(reversed(parts))

    def is_empty(self) -> bool:
        """True when it holds neither subdirectories nor committed files."""
        return not self.children and not self.files
