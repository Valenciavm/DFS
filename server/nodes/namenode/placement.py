"""Choosing which DataNodes hold a block (``ARCHITECTURE.md`` Section 6.2).

Pure functions over a list of nodes and a reservation map, with the random
source passed in, so the algorithm is testable without a cluster and a seeded
run is reproducible.

The rule: for **each block separately**, pick distinct nodes by weighted random
sampling without replacement, with probability proportional to usable space.
Space favours emptier nodes; the randomness makes consecutive blocks of one file
land on different sets of nodes, which is what partitions a file across the
cluster rather than mirroring it.
"""
from __future__ import annotations

import math
import random
from collections.abc import Collection, Mapping, Sequence

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.cluster_state import DataNodeInfo


def usable_space(node: DataNodeInfo, reserved: Mapping[str, int]) -> int:
    """Free space minus what uploads and copies in flight have already claimed."""
    return max(0, node.free_bytes() - reserved.get(node.node_id, 0))


def eligible_nodes(
    nodes: Sequence[DataNodeInfo],
    reserved: Mapping[str, int],
    block_size: int,
    exclude: Collection[str] = (),
) -> list[DataNodeInfo]:
    """Live nodes outside ``exclude`` with room for a block of ``block_size``."""
    return [
        n for n in nodes
        if n.is_alive and n.node_id not in exclude and usable_space(n, reserved) >= block_size
    ]


def weighted_sample(
    candidates: Sequence[DataNodeInfo],
    count: int,
    rng: random.Random,
    reserved: Mapping[str, int],
) -> list[DataNodeInfo]:
    """Up to ``count`` distinct candidates, weighted by usable space.

    Efraimidis-Spirakis: give every item the key ``u ** (1 / weight)`` for a
    uniform ``u`` and keep the largest keys. That is equivalent to ordering by
    ``log(u) / weight``, which is what is computed here, because with weights in
    the hundreds of gigabytes ``u ** (1 / weight)`` collapses to 1.0 in floating
    point and every node would tie.
    """
    keyed: list[tuple[float, str, DataNodeInfo]] = []
    for node in candidates:
        weight = max(usable_space(node, reserved), 1)  # a zero weight would never be picked
        uniform = 1.0 - rng.random()                  # in (0, 1], so log() is defined
        keyed.append((math.log(uniform) / weight, node.node_id, node))
    keyed.sort(key=lambda item: (-item[0], item[1]))  # largest key first, id breaks ties
    return [node for _, _, node in keyed[:count]]


def choose_pipeline(
    nodes: Sequence[DataNodeInfo],
    reserved: Mapping[str, int],
    block_size: int,
    count: int,
    rng: random.Random,
) -> list[DataNodeInfo]:
    """The ordered pipeline for one new block.

    Raises ``NOT_ENOUGH_DATANODES`` when fewer than ``count`` nodes qualify.
    The node with the most usable space goes first: it receives the bytes from
    the client, so it carries the heaviest load.
    """
    eligible = eligible_nodes(nodes, reserved, block_size)
    if len(eligible) < count:
        raise DfshaError(
            ErrorCode.NOT_ENOUGH_DATANODES,
            f"need {count} live nodes with {block_size} free bytes, found {len(eligible)}",
        )
    chosen = weighted_sample(eligible, count, rng, reserved)
    chosen.sort(key=lambda n: (-usable_space(n, reserved), n.node_id))
    return chosen


__all__ = ["choose_pipeline", "eligible_nodes", "usable_space", "weighted_sample"]
