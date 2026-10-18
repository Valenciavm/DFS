"""Pipeline replication over the gRPC block channel (API.md section 6).

Covers scenario T1 of ARCHITECTURE.md section 12 at block level: a block written
to the first DataNode of a pipeline must end up byte-identical on every node of
the chain, and be readable from any of them.

The nodes are started in-process on free ports, so the test needs no manual
setup. The equivalent manual procedure, used for the demo, is in
docs/grpc_block_service.md.
"""
from __future__ import annotations

import asyncio
import hashlib
import socket

import grpc
import pytest

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from server.nodes.datanode.core import DataNode

pytestmark = pytest.mark.integration

CHUNK = 16 * 1024
BLOCK = b"Hola Mundo Distribuido! Este es un bloque de prueba gRPC replicado." * 1000


def _free_port() -> int:
    """Asks the OS for an unused TCP port, so parallel runs do not collide."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _write_block(
    addr: str, block_id: str, payload: bytes, pipeline: list[str]
) -> pb2.WriteBlockResponse:
    """Streams one block to ``addr``, asking it to forward along ``pipeline``."""
    checksum = hashlib.sha256(payload).hexdigest()

    async def requests():
        yield pb2.WriteBlockRequest(
            header=pb2.WriteBlockHeader(
                block_id=block_id,
                size=len(payload),
                checksum_sha256=checksum,
                pipeline=pipeline,
            )
        )
        for i in range(0, len(payload), CHUNK):
            yield pb2.WriteBlockRequest(chunk=payload[i : i + CHUNK])

    async with grpc.aio.insecure_channel(addr) as channel:
        return await pb2_grpc.BlockServiceStub(channel).WriteBlock(requests())


async def _read_block(addr: str, block_id: str) -> bytes:
    """Downloads a whole block from ``addr`` and returns its bytes."""
    received = bytearray()
    async with grpc.aio.insecure_channel(addr) as channel:
        stream = pb2_grpc.BlockServiceStub(channel).ReadBlock(
            pb2.ReadBlockRequest(block_id=block_id, offset=0, length=0)
        )
        async for message in stream:
            if message.HasField("chunk"):
                received.extend(message.chunk)
    return bytes(received)


@pytest.fixture
async def cluster(tmp_path):
    """Two DataNodes listening on free ports, torn down after the test."""
    nodes = [
        DataNode(node_id=f"dn{i}", grpc_port=_free_port(), storage_dir=str(tmp_path / f"dn{i}"))
        for i in (1, 2)
    ]
    tasks = [asyncio.create_task(node.start_grpc_server()) for node in nodes]

    # wait until both servers accept connections instead of sleeping blindly
    for node in nodes:
        for _ in range(100):
            if node.grpc_server is not None:
                break
            await asyncio.sleep(0.05)
        async with grpc.aio.insecure_channel(node.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).Health(pb2.HealthRequest())

    yield nodes

    for node in nodes:
        if node.grpc_server is not None:
            await node.grpc_server.stop(grace=None)
    for task in tasks:
        task.cancel()


async def test_block_is_replicated_along_the_pipeline(cluster):
    """dn1 stores the block and forwards it to dn2; both copies are identical."""
    dn1, dn2 = cluster
    expected = hashlib.sha256(BLOCK).hexdigest()

    response = await _write_block(dn1.address, "blk_test_001", BLOCK, [dn2.address])

    assert response.block_id == "blk_test_001"
    assert response.size == len(BLOCK)
    assert response.checksum_sha256 == expected
    assert list(response.failed_downstream) == [], "no node of the chain should fail"

    # both nodes persisted the block under its definitive name
    for node in (dn1, dn2):
        assert hashlib.sha256(node.storage.read_block("blk_test_001")).hexdigest() == expected

    # and the replica is readable over gRPC, which is how a client would get it
    assert hashlib.sha256(await _read_block(dn2.address, "blk_test_001")).hexdigest() == expected


async def test_a_wrong_checksum_is_rejected_and_leaves_no_file(cluster):
    """A corrupted stream is refused by the node and never stored."""
    dn1, _ = cluster

    async def requests():
        yield pb2.WriteBlockRequest(
            header=pb2.WriteBlockHeader(
                block_id="blk_corrupt",
                size=3,
                checksum_sha256="deadbeef",  # does not match the bytes below
                pipeline=[],
            )
        )
        yield pb2.WriteBlockRequest(chunk=b"xyz")

    with pytest.raises(grpc.aio.AioRpcError) as error:
        async with grpc.aio.insecure_channel(dn1.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).WriteBlock(requests())

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(FileNotFoundError):
        dn1.storage.read_block("blk_corrupt")


async def test_reading_an_unknown_block_returns_not_found(cluster):
    """API.md section 3.1: an absent block maps to the gRPC NOT_FOUND status."""
    dn1, _ = cluster

    with pytest.raises(grpc.aio.AioRpcError) as error:
        await _read_block(dn1.address, "blk_does_not_exist")

    assert error.value.code() == grpc.StatusCode.NOT_FOUND


async def test_a_dead_downstream_node_is_reported_but_the_local_copy_survives(cluster):
    """API.md section 6.2: the node answers OK and names the failed hop."""
    dn1, _ = cluster
    dead_addr = f"127.0.0.1:{_free_port()}"  # nothing is listening there

    response = await _write_block(dn1.address, "blk_half", BLOCK, [dead_addr])

    assert list(response.failed_downstream) == [dead_addr]
    assert response.checksum_sha256 == hashlib.sha256(BLOCK).hexdigest()
    assert dn1.storage.read_block("blk_half") == BLOCK
