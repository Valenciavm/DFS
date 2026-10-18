"""Test helpers that play the part of DataNodes and clients."""
from __future__ import annotations

import hashlib

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.results import CommittedBlock, UploadPlan

MIB = 1 << 20
GIB = 1 << 30


def add_nodes(nn: Namenode, count: int, capacity: int = 100 * GIB, start: int = 1) -> list[str]:
    """Registers ``count`` DataNodes named dn<start>.. and returns their ids."""
    ids = []
    for i in range(start, start + count):
        nn.register_datanode(f"dn{i}", f"dn{i}:9000", f"dn{i}.example.com:9000", capacity, 0)
        ids.append(f"dn{i}")
    return ids


def checksum_of(block_id: str) -> str:
    """A deterministic stand-in for the SHA-256 a real block would have."""
    return hashlib.sha256(block_id.encode()).hexdigest()


def code_of(call) -> ErrorCode:
    """The error code a call raises; fails the test if it raises nothing."""
    try:
        call()
    except DfshaError as error:
        return error.code
    raise AssertionError("expected a DfshaError, but the call succeeded")


def confirm_all(nn: Namenode, plan: UploadPlan, replicas: int | None = None) -> None:
    """Plays the DataNodes: each pipeline node (or the first ``replicas``) reports block-received."""
    for planned in plan.blocks:
        nodes = planned.pipeline if replicas is None else planned.pipeline[:replicas]
        for node in nodes:
            nn.record_block_received(
                node.node_id,
                planned.block.block_id,
                planned.block.size_bytes,
                checksum_of(planned.block.block_id),
            )


def commit_body(plan: UploadPlan) -> list[CommittedBlock]:
    """What an honest client would send to commit ``plan``."""
    return [
        CommittedBlock(
            index=p.block.index,
            block_id=p.block.block_id,
            size=p.block.size_bytes,
            checksum=checksum_of(p.block.block_id),
        )
        for p in plan.blocks
    ]


def upload(nn: Namenode, path: str, size: int, owner: str = "ana", overwrite: bool = False):
    """prepare + confirm on every pipeline node + commit. Returns the plan."""
    plan = nn.prepare_file_upload(path, size, owner, overwrite=overwrite)
    confirm_all(nn, plan)
    nn.commit_file_upload(plan.file.file_id, commit_body(plan))
    return plan
