from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time

class FileStatus(str, Enum):
    PENDING = "pending"
    COMMITTED = "committed"
    ABORTED = "aborted"

@dataclass
class BlockMetadata:
    block_id: str
    index: int
    size_bytes: int
    checksum: Optional[str] = None  # sha-256 for integrity verification
    replicas: list[str] = field(default_factory=list)  # list of node_ids storing this block

@dataclass
class FileMetadata:
    file_id: str
    path: str
    size_bytes: int
    owner: str = "system"
    status: FileStatus = FileStatus.PENDING
    blocks: list[BlockMetadata] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None  # Upload timeout expiration

@dataclass
class DataNodeInfo:
    node_id: str
    address: str  # host:port for gRPC block operations
    capacity_bytes: int = 0
    used_bytes: int = 0
    last_heartbeat: float = field(default_factory=time.time)
    is_alive: bool = True

    @property
    def free_bytes(self) -> int:
        return max(0, self.capacity_bytes - self.used_bytes)

class DirectoryMetadata:
    def __init__(self, path: str, owner: str = "system", parent: Optional[DirectoryMetadata] = None):
        self.path: str = path
        self.owner: str = owner
        self.parent: Optional[DirectoryMetadata] = parent
        self.children: list[DirectoryMetadata] = []
        self.files: list[FileMetadata] = []

class MetadataStore: 
    def __init__(self):
        self.root: DirectoryMetadata = DirectoryMetadata(path="/", owner="system")
        self.files_by_id: dict[str, FileMetadata] = {}
        self.files_by_path: dict[str, FileMetadata] = {}
        self.blocks_by_id: dict[str, BlockMetadata] = {}
        self.datanodes: dict[str, DataNodeInfo] = {}


    
    