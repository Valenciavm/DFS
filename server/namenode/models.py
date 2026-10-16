from __future__ import annotations
from typing import Optional

class BlockMetadata:
    block_id: str
    index: int
    size_bytes: int
    checksum: Optional[str] = None #sha-256 for integrity verification
    replicas: list[str] = [] # datanodes

class FileMetadata:
    file_id: str
    path:str
    size_bytes:int
    blocks: list[BlockMetadata]

class DirectoryMetadata:
    def __init__(self,path: str):
        self.path: str = path
        self.owner: str
        self.parent: Optional[DirectoryMetadata] = None
        self.children: list[DirectoryMetadata] = []
        self.files: list[FileMetadata] = []

class MetadataStore: 
    def __init__(self):
        self.root: DirectoryMetadata = DirectoryMetadata(path="/")
        self.root.owner = "system"

    
    