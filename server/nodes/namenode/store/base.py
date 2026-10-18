"""The interface through which the NameNode core reads and writes hard state.

Two implementations are planned (``docs/NAMENODE_MODELS.md`` Section 4.4): an
in-memory one, used by unit tests and by a single NameNode in development, and
a PostgreSQL one for production. The core is written against this interface
only, so swapping the storage never touches the rules.

Time is passed in (``now``) rather than read, so a store has no clock of its own
and tests stay deterministic.

Contract notes that every implementation must keep:

* A `PENDING` upload is **not visible** to `list_dir`, `stat` or `get_file`.
  It is reachable only through `get_pending` and `get_file_by_id`.
* Committing over an existing file replaces it atomically and **returns the old
  file**, so the caller can schedule its blocks for deletion.
* A name is unique within a directory across files and subdirectories.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from server.nodes.namenode.models import (
    BlockMetadata,
    DirectoryMetadata,
    FileMetadata,
    FileStatus,
)

#: Parent of every user's home (``ARCHITECTURE.md`` Section 16, decision 20).
USERS_DIR = "/users"


@dataclass(frozen=True)
class DirEntry:
    """One line of a directory listing."""

    name: str
    type: str  # "file" | "dir"
    size_bytes: int
    modified_at: float
    owner: str


class MetadataStore(ABC):
    """Hard-state storage: the namespace tree, files and blocks."""

    # ---- namespace -------------------------------------------------------

    @abstractmethod
    def resolve_dir(self, path: str) -> DirectoryMetadata:
        """The directory at ``path``.

        Raises ``DIR_NOT_FOUND`` if any segment is missing and
        ``NOT_A_DIRECTORY`` if a segment is a file.
        """

    @abstractmethod
    def mkdir(
        self, path: str, owner: str, now: float, parents: bool = False
    ) -> DirectoryMetadata:
        """Creates a directory.

        Without ``parents`` the parent must exist and the path must be new
        (``ALREADY_EXISTS``). With ``parents`` missing ancestors are created and
        an existing directory is returned unchanged, like ``mkdir -p``.
        """

    @abstractmethod
    def list_dir(self, path: str) -> list[DirEntry]:
        """Entries of a directory sorted by name. Pending uploads are hidden."""

    @abstractmethod
    def stat(self, path: str) -> DirectoryMetadata | FileMetadata | None:
        """The committed file or directory at ``path``, or ``None``."""

    @abstractmethod
    def get_file(self, path: str) -> FileMetadata | None:
        """The committed file at ``path``, or ``None`` (also for directories)."""

    @abstractmethod
    def path_of(self, file_id: str) -> str:
        """Absolute path of a file, committed or not, computed from its parent."""

    @abstractmethod
    def remove_file(self, path: str, now: float) -> FileMetadata:
        """Removes a committed file and returns it so its blocks can be deleted."""

    @abstractmethod
    def remove_dir(self, path: str, recursive: bool, now: float) -> list[FileMetadata]:
        """Removes a directory and returns every committed file that was inside.

        ``DIR_NOT_EMPTY`` without ``recursive`` on a non-empty directory, and also
        whenever an upload is in progress anywhere below it.
        """

    # ---- uploads ---------------------------------------------------------

    @abstractmethod
    def get_pending(self, path: str) -> FileMetadata | None:
        """The `PENDING` upload that reserves ``path``, if any."""

    @abstractmethod
    def create_pending_file(self, path: str, file: FileMetadata) -> None:
        """Registers a `PENDING` file and its blocks, reserving ``path``."""

    @abstractmethod
    def commit_file(self, file_id: str, now: float) -> FileMetadata | None:
        """`PENDING` to `COMMITTED` in one step, linking the file into its directory.

        Returns the committed file it replaced, or ``None``.
        """

    @abstractmethod
    def abort_file(self, file_id: str, now: float) -> None:
        """`PENDING` to `ABORTED`, releasing the path. The record is kept briefly."""

    @abstractmethod
    def purge_aborted(self, older_than: float) -> list[FileMetadata]:
        """Forgets `ABORTED` files last modified before ``older_than``."""

    # ---- lookups ---------------------------------------------------------

    @abstractmethod
    def get_file_by_id(self, file_id: str) -> FileMetadata | None:
        """Any file by id, whatever its status."""

    @abstractmethod
    def get_block(self, block_id: str) -> BlockMetadata | None:
        """Any block by id, including those of pending and aborted files."""

    @abstractmethod
    def file_of_block(self, block_id: str) -> FileMetadata | None:
        """The file a block belongs to."""

    @abstractmethod
    def files(self, status: FileStatus | None = None) -> list[FileMetadata]:
        """A snapshot of files, optionally filtered by status.

        A list, never a live view: background tasks iterate over it while other
        calls mutate the store (``ARCHITECTURE.md`` Section 4.4).
        """
