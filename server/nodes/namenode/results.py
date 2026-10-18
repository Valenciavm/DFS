"""Value types the NameNode core returns.

Plain data, no behaviour. The REST layer turns them into the JSON of ``API.md``;
keeping them out of ``core.py`` stops that file from becoming a catalogue of
dataclasses.
"""
from __future__ import annotations

from dataclasses import dataclass

from server.nodes.namenode.cluster_state import DataNodeInfo
from server.nodes.namenode.models import BlockMetadata, FileMetadata


@dataclass(frozen=True)
class PlannedBlock:
    """One block of an upload: where to write it and the token that allows it."""

    block: BlockMetadata
    pipeline: list[DataNodeInfo]  # ordered; the client writes to pipeline[0]
    token: str                    # write token bound to this block


@dataclass(frozen=True)
class UploadPlan:
    """What ``put`` returns (``API.md`` Section 5.1)."""

    file: FileMetadata
    path: str
    blocks: list[PlannedBlock]


@dataclass(frozen=True)
class CommittedBlock:
    """What the client says it uploaded for one block (``API.md`` Section 5.2)."""

    index: int
    block_id: str
    size: int
    checksum: str


@dataclass(frozen=True)
class BlockLocation:
    """A block of a committed file, with where to read it from."""

    block: BlockMetadata
    token: str                   # read token bound to this block
    replicas: list[DataNodeInfo]  # live nodes only, in the order the client should try them


@dataclass(frozen=True)
class FileLocation:
    """What ``get`` returns (``API.md`` Section 5.3)."""

    file: FileMetadata
    path: str
    blocks: list[BlockLocation]
