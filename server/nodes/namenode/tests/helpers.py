"""Test helpers that play the part of DataNodes and clients."""
from __future__ import annotations

import hashlib

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.core import Namenode

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
