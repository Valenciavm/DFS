"""In-memory `MetadataStore`.

Used by unit tests (fake time, no database) and by a single NameNode in
development. It is also the shape of the leader's cache over PostgreSQL
(``docs/NAMENODE_MODELS.md`` Section 4.4), so the indexes below are the same
ones the production store will keep.
"""
from __future__ import annotations

from server.common.errors import DfshaError, ErrorCode
from server.common.paths import join_path, split_path
from server.nodes.namenode.models import (
    BlockMetadata,
    DirectoryMetadata,
    FileMetadata,
    FileStatus,
)
from server.nodes.namenode.store.base import USERS_DIR, DirEntry, MetadataStore


class InMemoryMetadataStore(MetadataStore):
    """Hard state held in dictionaries. Not thread safe: callers run on one loop."""

    def __init__(self, now: float = 0.0) -> None:
        self.root = DirectoryMetadata(name="", owner="system", created_at=now, modified_at=now)
        self._files: dict[str, FileMetadata] = {}
        self._blocks: dict[str, BlockMetadata] = {}
        self._block_file: dict[str, str] = {}
        #: directory each file lives in. PENDING files are not linked into
        #: ``DirectoryMetadata.files``, so the path is computed from this
        self._file_dir: dict[str, DirectoryMetadata] = {}
        #: canonical path -> file_id of the PENDING upload reserving it
        self._pending: dict[str, str] = {}
        self.mkdir(USERS_DIR, "system", now)

    # ---- namespace -------------------------------------------------------

    def resolve_dir(self, path: str) -> DirectoryMetadata:
        node = self.root
        walked: list[str] = []
        for segment in split_path(path):
            walked.append(segment)
            if segment in node.children:
                node = node.children[segment]
            elif segment in node.files:
                raise DfshaError(ErrorCode.NOT_A_DIRECTORY, f"{join_path(walked)} is a file")
            else:
                raise DfshaError(ErrorCode.DIR_NOT_FOUND, f"{join_path(walked)} does not exist")
        return node

    def mkdir(
        self, path: str, owner: str, now: float, parents: bool = False
    ) -> DirectoryMetadata:
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.ALREADY_EXISTS, "/ already exists")
        node = self.root
        for i, segment in enumerate(segments):
            is_last = i == len(segments) - 1
            here = join_path(segments[: i + 1])
            if segment in node.children:
                node = node.children[segment]
                if is_last and not parents:
                    raise DfshaError(ErrorCode.ALREADY_EXISTS, f"{here} already exists")
                continue
            if segment in node.files or here in self._pending:
                # a file, or an upload that has reserved the name
                code = ErrorCode.ALREADY_EXISTS if is_last else ErrorCode.NOT_A_DIRECTORY
                raise DfshaError(code, f"{here} is already taken by a file")
            if not is_last and not parents:
                raise DfshaError(ErrorCode.DIR_NOT_FOUND, f"{here} does not exist")
            child = DirectoryMetadata(
                name=segment, owner=owner, parent=node, created_at=now, modified_at=now
            )
            node.children[segment] = child
            node.modified_at = now
            node = child
        return node

    def list_dir(self, path: str) -> list[DirEntry]:
        directory = self.resolve_dir(path)
        entries = [
            DirEntry(d.name, "dir", 0, d.modified_at, d.owner) for d in directory.children.values()
        ]
        entries += [
            DirEntry(f.name, "file", f.size_bytes, f.modified_at, f.owner)
            for f in directory.files.values()
        ]
        return sorted(entries, key=lambda e: e.name)

    def stat(self, path: str) -> DirectoryMetadata | FileMetadata | None:
        segments = split_path(path)
        if not segments:
            return self.root
        parent = self._find_dir(segments[:-1])
        if parent is None:
            return None
        name = segments[-1]
        return parent.children.get(name) or parent.files.get(name)

    def get_file(self, path: str) -> FileMetadata | None:
        node = self.stat(path)
        return node if isinstance(node, FileMetadata) else None

    def path_of(self, file_id: str) -> str:
        file = self._require(file_id)
        directory = self._file_dir[file_id]
        base = directory.path()
        return join_path([file.name]) if base == "/" else f"{base}/{file.name}"

    def remove_file(self, path: str, now: float) -> FileMetadata:
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, "/ is a directory")
        parent = self.resolve_dir(join_path(segments[:-1]))
        name = segments[-1]
        if name in parent.children:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{join_path(segments)} is a directory")
        file = parent.files.pop(name, None)
        if file is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{join_path(segments)} does not exist")
        parent.modified_at = now
        self._forget(file)
        return file

    def remove_dir(self, path: str, recursive: bool, now: float) -> list[FileMetadata]:
        segments = split_path(path)
        canonical = join_path(segments)
        if not segments:
            raise DfshaError(ErrorCode.BAD_REQUEST, "the root cannot be removed")
        if canonical == USERS_DIR:
            raise DfshaError(ErrorCode.BAD_REQUEST, f"{USERS_DIR} cannot be removed")
        directory = self.resolve_dir(canonical)
        prefix = canonical + "/"
        if any(pending.startswith(prefix) for pending in self._pending):
            raise DfshaError(ErrorCode.DIR_NOT_EMPTY, f"{canonical} has uploads in progress")
        if not directory.is_empty() and not recursive:
            raise DfshaError(ErrorCode.DIR_NOT_EMPTY, f"{canonical} is not empty")

        removed: list[FileMetadata] = []
        stack = [directory]
        while stack:
            current = stack.pop()
            stack.extend(current.children.values())
            removed.extend(current.files.values())
        for file in removed:
            self._forget(file)

        assert directory.parent is not None  # the root was rejected above
        del directory.parent.children[directory.name]
        directory.parent.modified_at = now
        return removed

    # ---- uploads ---------------------------------------------------------

    def get_pending(self, path: str) -> FileMetadata | None:
        file_id = self._pending.get(join_path(split_path(path)))
        return self._files[file_id] if file_id is not None else None

    def create_pending_file(self, path: str, file: FileMetadata) -> None:
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.BAD_REQUEST, "a file needs a name")
        canonical = join_path(segments)
        parent = self.resolve_dir(join_path(segments[:-1]))
        if file.name != segments[-1]:
            raise ValueError(f"file name {file.name!r} does not match path {canonical!r}")
        if segments[-1] in parent.children:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{canonical} is a directory")
        if canonical in self._pending:
            raise DfshaError(ErrorCode.UPLOAD_IN_PROGRESS, f"{canonical} has an upload in progress")
        if file.file_id in self._files:
            raise ValueError(f"duplicate file_id {file.file_id}")
        for block in file.blocks:
            if block.block_id in self._blocks:
                raise ValueError(f"duplicate block_id {block.block_id}")

        self._files[file.file_id] = file
        self._file_dir[file.file_id] = parent
        self._pending[canonical] = file.file_id
        for block in file.blocks:
            self._blocks[block.block_id] = block
            self._block_file[block.block_id] = file.file_id

    def commit_file(self, file_id: str, now: float) -> FileMetadata | None:
        file = self._require(file_id)
        if file.status is not FileStatus.PENDING:
            raise ValueError(f"{file_id} is {file.status.value}, only PENDING can be committed")
        parent = self._file_dir[file_id]
        path = self.path_of(file_id)
        if file.name in parent.children:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{path} became a directory")

        replaced = parent.files.get(file.name)
        parent.files[file.name] = file
        file.status = FileStatus.COMMITTED
        file.expires_at = None
        file.modified_at = now
        parent.modified_at = now
        self._pending.pop(path, None)
        if replaced is not None:
            self._forget(replaced)
        return replaced

    def abort_file(self, file_id: str, now: float) -> None:
        file = self._require(file_id)
        if file.status is FileStatus.ABORTED:
            return
        if file.status is not FileStatus.PENDING:
            raise ValueError(f"{file_id} is {file.status.value}, only PENDING can be aborted")
        self._pending.pop(self.path_of(file_id), None)
        file.status = FileStatus.ABORTED
        file.expires_at = None
        file.modified_at = now

    def purge_aborted(self, older_than: float) -> list[FileMetadata]:
        stale = [
            f for f in self._files.values()
            if f.status is FileStatus.ABORTED and f.modified_at < older_than
        ]
        for file in stale:
            self._forget(file)
        return stale

    # ---- lookups ---------------------------------------------------------

    def get_file_by_id(self, file_id: str) -> FileMetadata | None:
        return self._files.get(file_id)

    def get_block(self, block_id: str) -> BlockMetadata | None:
        return self._blocks.get(block_id)

    def file_of_block(self, block_id: str) -> FileMetadata | None:
        file_id = self._block_file.get(block_id)
        return self._files.get(file_id) if file_id is not None else None

    def files(self, status: FileStatus | None = None) -> list[FileMetadata]:
        return [f for f in self._files.values() if status is None or f.status is status]

    # ---- internals -------------------------------------------------------

    def _find_dir(self, segments: list[str]) -> DirectoryMetadata | None:
        """Like `resolve_dir` but returns ``None`` instead of raising."""
        node = self.root
        for segment in segments:
            child = node.children.get(segment)
            if child is None:
                return None
            node = child
        return node

    def _require(self, file_id: str) -> FileMetadata:
        file = self._files.get(file_id)
        if file is None:
            raise KeyError(f"unknown file_id {file_id}")
        return file

    def _forget(self, file: FileMetadata) -> None:
        """Drops a file and its blocks from every index."""
        self._files.pop(file.file_id, None)
        self._file_dir.pop(file.file_id, None)
        for block in file.blocks:
            self._blocks.pop(block.block_id, None)
            self._block_file.pop(block.block_id, None)
