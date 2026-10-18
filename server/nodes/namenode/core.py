"""NameNode core: the rules of the control plane, with no web code.

A plain class so it can be unit tested with a fake clock and an in-memory store
(``ARCHITECTURE.md`` Section 4.1). The REST layer is a thin adapter on top.

Concurrency (Section 4.4): every public method is synchronous and runs to
completion without yielding, so when the REST handlers and the background loops
share one event loop each call is atomic. Background tasks iterate over
snapshots (``list(...)``), never over a dictionary another call may be changing.

Time (Section 4.4): liveness uses the monotonic clock, anything stored or
compared across processes uses the wall clock, and the two are never subtracted
from each other.
"""
from __future__ import annotations

import logging
import random
import re
import uuid

from server.common.clock import Clock, SystemClock
from server.common.config import ConfigError, Settings
from server.common.config import settings as default_settings
from server.common.errors import DfshaError, ErrorCode
from server.common.paths import join_path, split_path
from server.common.tokens import BlockOp, mint
from server.nodes.namenode import placement
from server.nodes.namenode.cluster_state import ClusterState, Command, DataNodeInfo
from server.nodes.namenode.models import (
    BlockMetadata,
    DirectoryMetadata,
    FileMetadata,
    FileStatus,
)
from server.nodes.namenode.results import (
    BlockLocation,
    CommittedBlock,
    FileLocation,
    PlannedBlock,
    UploadPlan,
)
from server.nodes.namenode.store import (
    DirEntry,
    InMemoryMetadataStore,
    MetadataStore,
)

logger = logging.getLogger(__name__)

_SHA256 = re.compile(r"[0-9a-f]{64}")


class Namenode:
    """The leader's view of the namespace and of the cluster."""

    def __init__(
        self,
        settings: Settings | None = None,
        store: MetadataStore | None = None,
        clock: Clock | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.settings = settings if settings is not None else default_settings
        if not self.settings.block_token_secret:
            # fail at startup: a NameNode without it would issue tokens nobody can verify
            raise ConfigError("BLOCK_TOKEN_SECRET must be set: block tokens are signed with it")
        self.clock: Clock = clock if clock is not None else SystemClock()
        self.rng = rng if rng is not None else random.Random()
        self.store: MetadataStore = (
            store if store is not None else InMemoryMetadataStore(now=self.clock.time())
        )
        self.cluster = ClusterState()
        #: leadership generation; fixed at 1 until the lease is implemented
        self.term = 1
        #: monotonic instant until which nobody is declared dead after a failover
        self._grace_until = 0.0

    # ---- DataNode registry and liveness ----------------------------------

    def register_datanode(
        self,
        node_id: str,
        internal_addr: str,
        public_addr: str,
        capacity_bytes: int,
        used_bytes: int = 0,
    ) -> DataNodeInfo:
        """Creates or refreshes a DataNode. Replica locations already known survive.

        Called on startup, after a restart, and whenever a heartbeat was answered
        with ``NODE_NOT_REGISTERED`` (``ARCHITECTURE.md`` Section 16, decision 19).
        """
        if not node_id or not isinstance(node_id, str):
            raise DfshaError(ErrorCode.BAD_REQUEST, "node_id must be a non-empty string")
        if not internal_addr or not public_addr:
            raise DfshaError(ErrorCode.BAD_REQUEST, "internal_addr and public_addr are required")
        if capacity_bytes < 0 or used_bytes < 0:
            raise DfshaError(ErrorCode.BAD_REQUEST, "capacity and usage cannot be negative")

        node = self.cluster.datanodes.get(node_id)
        if node is None:
            node = DataNodeInfo(node_id, internal_addr, public_addr, capacity_bytes, used_bytes)
            self.cluster.datanodes[node_id] = node
        else:
            node.internal_addr = internal_addr
            node.public_addr = public_addr
            node.capacity_bytes = capacity_bytes
            node.used_bytes = used_bytes
        self._mark_seen(node)
        return node

    def record_heartbeat(
        self,
        node_id: str,
        free_bytes: int,
        used_bytes: int,
        active_transfers: int = 0,
    ) -> list[Command]:
        """Refreshes liveness and free space; returns the node's pending commands.

        ``NODE_NOT_REGISTERED`` for an unknown node. That is the normal path after
        a failover or a NameNode restart, not a fault: the DataNode registers
        again and sends a full block report.
        """
        node = self._known(node_id)
        if free_bytes < 0 or used_bytes < 0:
            raise DfshaError(ErrorCode.BAD_REQUEST, "free and used bytes cannot be negative")
        node.used_bytes = used_bytes
        node.capacity_bytes = used_bytes + free_bytes  # the node reports the truth
        self._mark_seen(node)
        return self._take_commands(node_id)

    def check_liveness(self) -> list[str]:
        """Marks silent DataNodes dead and returns the ones that just died.

        Does nothing during the grace period after a failover, so a new leader
        does not declare the whole cluster dead before the DataNodes have had a
        chance to report (``API.md`` Section 9).
        """
        now = self.clock.monotonic()
        if now < self._grace_until:
            return []
        died: list[str] = []
        for node in list(self.cluster.datanodes.values()):
            if node.is_alive and now - node.last_seen_monotonic > self.settings.dead_node_timeout_s:
                node.is_alive = False
                died.append(node.node_id)
                logger.warning("DataNode %s is dead (no heartbeat)", node.node_id)
        return died

    def begin_failover_grace(self) -> None:
        """Starts the grace period a new leader waits before declaring anyone dead."""
        self._grace_until = self.clock.monotonic() + self.settings.failover_grace_s

    # ---- helpers ---------------------------------------------------------

    def _mark_seen(self, node: DataNodeInfo) -> None:
        """Records a sign of life. Revives a node that had been declared dead."""
        node.last_heartbeat = self.clock.time()
        node.last_seen_monotonic = self.clock.monotonic()
        node.is_alive = True

    def _known(self, node_id: str) -> DataNodeInfo:
        node = self.cluster.datanodes.get(node_id)
        if node is None:
            raise DfshaError(ErrorCode.NODE_NOT_REGISTERED, f"{node_id} is not registered")
        return node

    def _live_nodes(self) -> list[DataNodeInfo]:
        return [n for n in self.cluster.datanodes.values() if n.is_alive]

    def _enqueue(self, node_id: str, command: Command) -> None:
        """Queues a command for the node's next heartbeat, dropping exact duplicates."""
        queue = self.cluster.pending_commands.setdefault(node_id, [])
        for queued in queue:
            if (queued.cmd, queued.block_id, queued.targets) == (
                command.cmd, command.block_id, command.targets,
            ):
                return
        queue.append(command)

    def _take_commands(self, node_id: str) -> list[Command]:
        return self.cluster.pending_commands.pop(node_id, [])

    # ---- uploads ---------------------------------------------------------

    def prepare_file_upload(
        self, path: str, size_bytes: int, owner: str, overwrite: bool = False
    ) -> UploadPlan:
        """Step 1 of ``put`` (``ARCHITECTURE.md`` Section 4.3).

        Validates, reserves the path as a `PENDING` file, splits it into blocks
        and chooses a pipeline for **each** block. The pipeline is a plan: nothing
        counts as a replica until a DataNode confirms it.
        """
        s = self.settings
        segments = split_path(path)
        if not segments:
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, "/ is a directory")
        path = join_path(segments)
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
            raise DfshaError(ErrorCode.BAD_REQUEST, "size must be a non-negative integer")
        if size_bytes > s.max_file_size_bytes:
            raise DfshaError(
                ErrorCode.FILE_TOO_LARGE,
                f"{size_bytes} bytes exceeds the limit of {s.max_file_size_bytes}",
            )
        if not owner:
            raise DfshaError(ErrorCode.BAD_REQUEST, "an upload needs an owner")
        self.store.resolve_dir(join_path(segments[:-1]))  # DIR_NOT_FOUND / NOT_A_DIRECTORY

        now = self.clock.time()
        pending = self.store.get_pending(path)
        if pending is not None:
            if pending.expires_at is not None and pending.expires_at > now:
                raise DfshaError(ErrorCode.UPLOAD_IN_PROGRESS, f"{path} has an upload in progress")
            self._abort(pending, now)  # the previous upload expired: reclaim the path now

        existing = self.store.stat(path)
        if isinstance(existing, DirectoryMetadata):
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{path} is a directory")
        if existing is not None and not overwrite:
            raise DfshaError(ErrorCode.ALREADY_EXISTS, f"{path} already exists")

        live = self._live_nodes()
        if len(live) < s.replication_factor:
            raise DfshaError(
                ErrorCode.NOT_ENOUGH_DATANODES,
                f"need {s.replication_factor} live DataNodes, have {len(live)}",
            )

        file_id, blocks = self._new_blocks(size_bytes)
        pipelines = self._place(blocks, live)  # raises before anything is reserved
        file = FileMetadata(
            file_id=file_id,
            name=segments[-1],
            owner=owner,
            size_bytes=size_bytes,
            block_size=s.block_size,
            replication=s.replication_factor,
            status=FileStatus.PENDING,
            created_at=now,
            modified_at=now,
            expires_at=now + s.upload_ttl_s,
            blocks=blocks,
        )
        self.store.create_pending_file(path, file)

        planned: list[PlannedBlock] = []
        for block, chosen in zip(blocks, pipelines, strict=True):
            self.cluster.pipelines[block.block_id] = [n.node_id for n in chosen]
            for node in chosen:
                self._reserve(block.block_id, node.node_id, block.size_bytes)
            token = mint(
                block.block_id, BlockOp.WRITE, owner, s.block_token_secret, s.token_ttl_s, now=now
            )
            planned.append(PlannedBlock(block=block, pipeline=chosen, token=token))
        return UploadPlan(file=file, path=path, blocks=planned)

    def commit_file_upload(
        self,
        file_id: str,
        blocks: list[CommittedBlock],
        file_checksum: str | None = None,
    ) -> FileMetadata:
        """Step 3 of ``put`` (``ARCHITECTURE.md`` Section 4.3, ``API.md`` Section 5.2).

        Accepted only if every block is listed with the right size and every block
        has at least ``MIN_REPLICAS_COMMIT`` replicas **confirmed by DataNodes** on
        live nodes whose checksum equals the client's. That is the write quorum, and
        it is checked against confirmations, never against the planned pipeline.
        """
        s = self.settings
        file = self.store.get_file_by_id(file_id)
        if file is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"no upload with id {file_id}")
        now = self.clock.time()

        if file.status is FileStatus.COMMITTED:
            if self._same_body(file, blocks, file_checksum):
                return file  # a retried commit is a success, not an error
            raise DfshaError(ErrorCode.ALREADY_EXISTS, "file already committed with other contents")
        if file.status is FileStatus.ABORTED:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, "the upload was aborted or expired")
        if file.expires_at is not None and file.expires_at <= now:
            self._abort(file, now)
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, "the upload expired")

        reported = self._validate_report(file, blocks, file_checksum)

        live = {n.node_id for n in self._live_nodes()}
        corrupt: dict[str, list[str]] = {}
        for block in file.blocks:
            claimed = reported[block.index].checksum
            checksums = self.cluster.replica_checksums.get(block.block_id, {})
            holders = sorted(self.cluster.replicas.get(block.block_id, set()) & live)
            good = [nid for nid in holders if checksums.get(nid) == claimed]
            if len(good) < s.min_replicas_commit:
                if len(holders) > len(good):
                    raise DfshaError(
                        ErrorCode.CHECKSUM_MISMATCH,
                        f"block {block.index}: a DataNode stored different bytes than the client sent",
                    )
                raise DfshaError(
                    ErrorCode.INSUFFICIENT_REPLICAS,
                    f"block {block.index} has {len(good)} confirmed replicas, "
                    f"{s.min_replicas_commit} required",
                )
            corrupt[block.block_id] = [nid for nid in holders if nid not in good]

        # the quorum holds: commit as one step
        for block in file.blocks:
            block.checksum = reported[block.index].checksum
        file.checksum = file_checksum
        replaced = self.store.commit_file(file_id, now)

        for block in file.blocks:
            self._release_block_reservations(block.block_id)  # unconfirmed hops never arrived
            self.cluster.pipelines.pop(block.block_id, None)
            for nid in corrupt[block.block_id]:  # a copy that disagrees is not a replica
                self.cluster.replicas[block.block_id].discard(nid)
                self.cluster.replica_checksums.get(block.block_id, {}).pop(nid, None)
                self._enqueue(nid, Command("delete", block.block_id))
        if replaced is not None:
            self._schedule_deletion(replaced.blocks)
        return file

    def abort_file_upload(self, file_id: str) -> None:
        """Cancels a pending upload. Idempotent (``API.md`` Section 5.6)."""
        file = self.store.get_file_by_id(file_id)
        if file is None or file.status is FileStatus.ABORTED:
            return
        if file.status is FileStatus.COMMITTED:
            raise DfshaError(ErrorCode.BAD_REQUEST, "the file is already committed; remove it instead")
        self._abort(file, self.clock.time())

    def record_block_received(
        self, node_id: str, block_id: str, size: int, checksum: str
    ) -> None:
        """A DataNode confirms it stored a block (``API.md`` Section 7).

        This is what turns an allocated pipeline slot into a replica. A block
        nobody asked for, or one that is the wrong size, is told to delete itself.
        """
        self._known(node_id)
        block = self.store.get_block(block_id)
        file = self.store.file_of_block(block_id)
        if block is None or file is None or file.status is FileStatus.ABORTED:
            self._enqueue(node_id, Command("delete", block_id))  # an orphan
            return
        if size != block.size_bytes:
            logger.warning("%s stored %s with %d bytes, expected %d", node_id, block_id, size, block.size_bytes)
            self._enqueue(node_id, Command("delete", block_id))
            return
        self._confirm_replica(node_id, block, checksum)

    # ---- upload internals ------------------------------------------------

    def _new_blocks(self, size_bytes: int) -> tuple[str, list[BlockMetadata]]:
        """A fresh file id and its blocks. Ids are re-drawn on the (remote) chance of a clash."""
        count = self.settings.blocks_for(size_bytes)
        while True:
            file_id = str(uuid.uuid4())
            short = file_id.replace("-", "")[:8]
            ids = [f"blk_{short}_{i:06d}" for i in range(count)]
            if self.store.get_file_by_id(file_id) is None and all(
                self.store.get_block(b) is None for b in ids
            ):
                break
        blocks: list[BlockMetadata] = []
        remaining = size_bytes
        for index, block_id in enumerate(ids):
            size = min(self.settings.block_size, remaining)
            blocks.append(BlockMetadata(block_id=block_id, index=index, size_bytes=size))
            remaining -= size
        return file_id, blocks

    def _place(self, blocks: list[BlockMetadata], live: list[DataNodeInfo]) -> list[list[DataNodeInfo]]:
        """A pipeline per block, accounting for space the earlier blocks will take.

        Works on a scratch copy of the reservations, so if a later block cannot be
        placed nothing has been reserved and there is nothing to roll back.
        """
        scratch = dict(self.cluster.reserved_bytes)
        result: list[list[DataNodeInfo]] = []
        for block in blocks:
            chosen = placement.choose_pipeline(
                live, scratch, block.size_bytes, self.settings.replication_factor, self.rng
            )
            for node in chosen:
                scratch[node.node_id] = scratch.get(node.node_id, 0) + block.size_bytes
            result.append(chosen)
        return result

    def _validate_report(
        self, file: FileMetadata, blocks: list[CommittedBlock], file_checksum: str | None
    ) -> dict[int, CommittedBlock]:
        """Checks the shape of a commit body and returns it indexed by block index."""
        by_index: dict[int, CommittedBlock] = {}
        for item in blocks:
            if item.index in by_index:
                raise DfshaError(ErrorCode.BAD_REQUEST, f"block {item.index} is listed twice")
            by_index[item.index] = item
        if set(by_index) != set(range(len(file.blocks))):
            raise DfshaError(
                ErrorCode.BAD_REQUEST, f"all {len(file.blocks)} blocks must be listed exactly once"
            )
        total = 0
        for block in file.blocks:
            item = by_index[block.index]
            if item.block_id != block.block_id:
                raise DfshaError(ErrorCode.BAD_REQUEST, f"block {block.index} has the wrong block_id")
            if item.size != block.size_bytes:
                raise DfshaError(
                    ErrorCode.BAD_REQUEST,
                    f"block {block.index} is {item.size} bytes, expected {block.size_bytes}",
                )
            if not _SHA256.fullmatch(item.checksum):
                raise DfshaError(ErrorCode.BAD_REQUEST, f"block {block.index} checksum is not SHA-256 hex")
            total += item.size
        if total != file.size_bytes:
            raise DfshaError(ErrorCode.BAD_REQUEST, "block sizes do not add up to the declared size")
        if file_checksum is not None and not _SHA256.fullmatch(file_checksum):
            raise DfshaError(ErrorCode.BAD_REQUEST, "file_checksum is not SHA-256 hex")
        return by_index

    @staticmethod
    def _same_body(file: FileMetadata, blocks: list[CommittedBlock], file_checksum: str | None) -> bool:
        """Whether a retried commit describes the file as it was committed."""
        by_index = {b.index: b for b in blocks}
        if len(blocks) != len(file.blocks) or len(by_index) != len(blocks):
            return False
        for block in file.blocks:
            item = by_index.get(block.index)
            if item is None or item.block_id != block.block_id or item.checksum != block.checksum:
                return False
        return file_checksum is None or file_checksum == file.checksum

    def _abort(self, file: FileMetadata, now: float) -> None:
        """`PENDING` to `ABORTED`, freeing the path and telling holders to delete."""
        self.store.abort_file(file.file_id, now)
        self._schedule_deletion(file.blocks)

    def _confirm_replica(self, node_id: str, block: BlockMetadata, checksum: str) -> None:
        """Records a confirmed copy and releases the space that was held for it."""
        self.cluster.replicas.setdefault(block.block_id, set()).add(node_id)
        self.cluster.replica_checksums.setdefault(block.block_id, {})[node_id] = checksum
        self._release_reservation(block.block_id, node_id)
        flight = self.cluster.in_flight_replications.get(block.block_id)
        if flight is not None:
            flight.pop(node_id, None)
            if not flight:
                del self.cluster.in_flight_replications[block.block_id]

    def _schedule_deletion(self, blocks: list[BlockMetadata]) -> None:
        """Forgets blocks and tells every node that may hold them to delete them.

        Nodes that are down are asked too; they receive the command when they
        return. Anything that still slips through is an orphan, which the next
        block report removes.
        """
        for block in blocks:
            block_id = block.block_id
            holders = set(self.cluster.replicas.pop(block_id, ()))
            holders |= set(self.cluster.pipelines.pop(block_id, ()))
            holders |= set(self.cluster.in_flight_replications.pop(block_id, {}))
            holders |= set(self.cluster.reservations.get(block_id, {}))
            self._release_block_reservations(block_id)
            self.cluster.replica_checksums.pop(block_id, None)
            for node_id in sorted(holders):
                if node_id in self.cluster.datanodes:
                    self._enqueue(node_id, Command("delete", block_id))

    def _reserve(self, block_id: str, node_id: str, nbytes: int) -> None:
        """Claims ``nbytes`` on a node for a block, once per (block, node)."""
        held = self.cluster.reservations.setdefault(block_id, {})
        if node_id in held:
            return
        held[node_id] = nbytes
        self.cluster.reserved_bytes[node_id] = self.cluster.reserved_bytes.get(node_id, 0) + nbytes

    def _release_reservation(self, block_id: str, node_id: str) -> None:
        """Gives back what ``_reserve`` claimed. Safe to call twice: the second is a no-op."""
        held = self.cluster.reservations.get(block_id)
        if held is None or node_id not in held:
            return
        nbytes = held.pop(node_id)
        if not held:
            del self.cluster.reservations[block_id]
        remaining = self.cluster.reserved_bytes.get(node_id, 0) - nbytes
        if remaining > 0:
            self.cluster.reserved_bytes[node_id] = remaining
        else:
            self.cluster.reserved_bytes.pop(node_id, None)

    def _release_block_reservations(self, block_id: str) -> None:
        for node_id in list(self.cluster.reservations.get(block_id, {})):
            self._release_reservation(block_id, node_id)

    # ---- reading ---------------------------------------------------------

    def get_file_blocks(self, path: str, user: str) -> FileLocation:
        """Step 1 of ``get`` (``API.md`` Section 5.3): the block map of a committed file.

        Only **live** replicas are listed, in random order so that readers spread
        over the replicas instead of all hitting the first one. A pending upload
        is invisible here, so a reader sees the complete old version or the
        complete new one, never a mix.
        """
        if not user:
            raise DfshaError(ErrorCode.BAD_REQUEST, "a read needs a user")
        canonical = join_path(split_path(path))
        node = self.store.stat(canonical)
        if node is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{canonical} does not exist")
        if isinstance(node, DirectoryMetadata):
            raise DfshaError(ErrorCode.IS_A_DIRECTORY, f"{canonical} is a directory")

        now = self.clock.time()
        located: list[BlockLocation] = []
        for block in node.blocks:
            replicas = [self.cluster.datanodes[nid] for nid in self._live_holders(block.block_id)]
            self.rng.shuffle(replicas)
            token = mint(
                block.block_id, BlockOp.READ, user,
                self.settings.block_token_secret, self.settings.token_ttl_s, now=now,
            )
            located.append(BlockLocation(block=block, token=token, replicas=replicas))
        return FileLocation(file=node, path=canonical, blocks=located)

    # ---- file system operations (RF1) -------------------------------------

    def mkdir(self, path: str, owner: str, parents: bool = False) -> DirectoryMetadata:
        if not owner:
            raise DfshaError(ErrorCode.BAD_REQUEST, "a directory needs an owner")
        return self.store.mkdir(path, owner, self.clock.time(), parents=parents)

    def ls(self, path: str) -> list[DirEntry]:
        return self.store.list_dir(path)

    def stat(self, path: str) -> DirectoryMetadata | FileMetadata:
        node = self.store.stat(path)
        if node is None:
            raise DfshaError(ErrorCode.FILE_NOT_FOUND, f"{join_path(split_path(path))} does not exist")
        return node

    def rm(self, path: str) -> FileMetadata:
        """Removes a file. It vanishes from ``ls`` at once; its blocks are deleted
        afterwards through ``delete`` commands (``API.md`` Section 4.3)."""
        file = self.store.remove_file(path, self.clock.time())
        self._schedule_deletion(file.blocks)
        return file

    def rmdir(self, path: str, recursive: bool = False) -> int:
        """Removes a directory and returns how many files went with it."""
        removed = self.store.remove_dir(path, recursive, self.clock.time())
        for file in removed:
            self._schedule_deletion(file.blocks)
        return len(removed)

    def _live_holders(self, block_id: str) -> list[str]:
        """Ids of live nodes that have confirmed a copy of the block, sorted."""
        holders = self.cluster.replicas.get(block_id, set())
        return sorted(
            nid for nid in holders
            if (node := self.cluster.datanodes.get(nid)) is not None and node.is_alive
        )
