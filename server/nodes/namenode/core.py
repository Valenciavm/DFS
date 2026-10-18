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

from server.common.clock import Clock, SystemClock
from server.common.config import ConfigError, Settings
from server.common.config import settings as default_settings
from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.cluster_state import ClusterState, Command, DataNodeInfo
from server.nodes.namenode.store import InMemoryMetadataStore, MetadataStore

logger = logging.getLogger(__name__)


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
