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
import os
import socket

import grpc
import pytest

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from server.common.config import Settings
from server.common.tokens import BlockOp, mint
from server.nodes.datanode.core import DataNode

pytestmark = pytest.mark.integration

CHUNK = 16 * 1024
BLOCK = b"Hola Mundo Distribuido! Este es un bloque de prueba gRPC replicado." * 1000
TOKEN_SECRET = "integration-test-secret"


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
        return await pb2_grpc.BlockServiceStub(channel).WriteBlock(
            requests(),
            metadata=(("x-block-token", _token(block_id, BlockOp.WRITE)),),
        )


def _token(block_id: str, op: BlockOp) -> str:
    return mint(block_id, op, "test", TOKEN_SECRET, 3600)


async def _read_range(addr: str, block_id: str, offset: int, length: int):
    received = bytearray()
    async with grpc.aio.insecure_channel(addr) as channel:
        stream = pb2_grpc.BlockServiceStub(channel).ReadBlock(
            pb2.ReadBlockRequest(block_id=block_id, offset=offset, length=length),
            metadata=(("x-block-token", _token(block_id, BlockOp.READ)),),
        )
        header = None
        async for message in stream:
            if message.HasField("header"):
                header = message.header
            if message.HasField("chunk"):
                assert len(message.chunk) <= Settings().block_chunk_size
                received.extend(message.chunk)
    return header, bytes(received)


async def _read_block(addr: str, block_id: str) -> bytes:
    """Downloads a whole block from ``addr`` and returns its bytes."""
    _, data = await _read_range(addr, block_id, 0, 0)
    return data


@pytest.fixture
async def cluster(tmp_path):
    """Three DataNodes listening on free ports, torn down after the test."""
    nodes = [
        DataNode(
            node_id=f"dn{i}",
            grpc_port=_free_port(),
            storage_dir=str(tmp_path / f"dn{i}"),
            config=Settings(block_token_secret=TOKEN_SECRET),
        )
        for i in (1, 2, 3)
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
    dn1, dn2, _ = cluster
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
    dn1, _, _ = cluster

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
            await pb2_grpc.BlockServiceStub(channel).WriteBlock(
                requests(),
                metadata=(("x-block-token", _token("blk_corrupt", BlockOp.WRITE)),),
            )

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "CHECKSUM_MISMATCH"
    assert not os.path.exists(dn1.storage.get_temp_block_path("blk_corrupt"))
    with pytest.raises(FileNotFoundError):
        dn1.storage.read_block("blk_corrupt")


async def test_reading_an_unknown_block_returns_not_found(cluster):
    """API.md section 3.1: an absent block maps to the gRPC NOT_FOUND status."""
    dn1, _, _ = cluster

    with pytest.raises(grpc.aio.AioRpcError) as error:
        await _read_block(dn1.address, "blk_does_not_exist")

    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "BLOCK_NOT_FOUND"


async def test_a_dead_downstream_node_is_reported_but_the_local_copy_survives(cluster):
    """API.md section 6.2: the node answers OK and names the failed hop."""
    dn1, _, _ = cluster
    dead_addr = f"127.0.0.1:{_free_port()}"  # nothing is listening there

    response = await _write_block(dn1.address, "blk_half", BLOCK, [dead_addr])

    assert list(response.failed_downstream) == [dead_addr]
    assert response.checksum_sha256 == hashlib.sha256(BLOCK).hexdigest()
    assert dn1.storage.read_block("blk_half") == BLOCK


async def test_a_dead_middle_hop_is_skipped_for_the_later_live_node(cluster):
    dn1, _, dn3 = cluster
    dead_middle = f"127.0.0.1:{_free_port()}"

    response = await _write_block(
        dn1.address, "blk_skip_middle", BLOCK, [dead_middle, dn3.address]
    )

    assert list(response.failed_downstream) == [dead_middle]
    assert dn1.storage.read_block("blk_skip_middle") == BLOCK
    assert dn3.storage.read_block("blk_skip_middle") == BLOCK


async def test_failed_first_and_last_hops_are_reported_and_local_copy_survives(cluster):
    dn1, dn2, _ = cluster
    dead_first = f"127.0.0.1:{_free_port()}"
    dead_last = f"127.0.0.1:{_free_port()}"

    response = await _write_block(
        dn1.address, "blk_failed_edges", BLOCK, [dead_first, dn2.address, dead_last]
    )

    assert list(response.failed_downstream) == [dead_first, dead_last]
    assert dn1.storage.read_block("blk_failed_edges") == BLOCK
    assert dn2.storage.read_block("blk_failed_edges") == BLOCK


async def test_all_unavailable_hops_are_reported_and_keep_the_local_copy(cluster):
    dn1, _, _ = cluster
    dead_hops = [f"127.0.0.1:{_free_port()}" for _ in range(2)]

    response = await _write_block(dn1.address, "blk_all_dead", BLOCK, dead_hops)

    assert list(response.failed_downstream) == dead_hops
    assert dn1.storage.read_block("blk_all_dead") == BLOCK


@pytest.mark.parametrize(
    ("offset", "length", "expected"),
    [
        (0, 7, BLOCK[:7]),
        (21, 13, BLOCK[21:34]),
        (len(BLOCK) - 8, 8, BLOCK[-8:]),
        (len(BLOCK), 0, b""),
        (0, 0, BLOCK),
    ],
    ids=["start", "middle", "end", "empty-at-end", "whole-block"],
)
async def test_ranged_reads_stream_the_requested_bytes_and_whole_checksum(
    cluster, offset, length, expected
):
    dn1, _, _ = cluster
    checksum = hashlib.sha256(BLOCK).hexdigest()
    await _write_block(dn1.address, "blk_ranges", BLOCK, [])

    header, data = await _read_range(dn1.address, "blk_ranges", offset, length)

    assert data == expected
    assert header.total_size == len(BLOCK)
    assert header.checksum_sha256 == checksum
    assert header.offset == offset
    assert header.length == len(expected)


@pytest.mark.parametrize(
    ("offset", "length"),
    [(-1, 1), (0, -1), (len(BLOCK) + 1, 0), (len(BLOCK) - 1, 2), (0, (1 << 63) - 1)],
)
async def test_invalid_read_ranges_return_out_of_range_metadata(
    cluster, offset, length
):
    dn1, _, _ = cluster
    await _write_block(dn1.address, "blk_bad_range", BLOCK, [])

    with pytest.raises(grpc.aio.AioRpcError) as error:
        await _read_range(dn1.address, "blk_bad_range", offset, length)

    assert error.value.code() == grpc.StatusCode.OUT_OF_RANGE
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "RANGE_NOT_SATISFIABLE"


async def test_stat_block_returns_metadata_and_not_found_error(cluster):
    dn1, _, _ = cluster
    await _write_block(dn1.address, "blk_stat", BLOCK, [])
    async with grpc.aio.insecure_channel(dn1.address) as channel:
        stub = pb2_grpc.BlockServiceStub(channel)
        response = await stub.StatBlock(
            pb2.StatBlockRequest(block_id="blk_stat"),
            metadata=(("x-block-token", _token("blk_stat", BlockOp.READ)),),
        )
        assert response.block_id == "blk_stat"
        assert response.size == len(BLOCK)
        assert response.checksum_sha256 == hashlib.sha256(BLOCK).hexdigest()

        with pytest.raises(grpc.aio.AioRpcError) as error:
            await stub.StatBlock(
                pb2.StatBlockRequest(block_id="blk_missing"),
                metadata=(("x-block-token", _token("blk_missing", BlockOp.READ)),),
            )
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "BLOCK_NOT_FOUND"


@pytest.mark.parametrize("token", [None, "bad-token"])
async def test_stat_block_rejects_missing_or_bad_tokens(cluster, token):
    dn1, _, _ = cluster
    metadata = () if token is None else (("x-block-token", token),)

    with pytest.raises(grpc.aio.AioRpcError) as error:
        async with grpc.aio.insecure_channel(dn1.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).StatBlock(
                pb2.StatBlockRequest(block_id="blk_auth"), metadata=metadata
            )

    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "UNAUTHORIZED"


async def test_stat_block_rejects_a_valid_token_for_another_block(cluster):
    dn1, _, _ = cluster
    with pytest.raises(grpc.aio.AioRpcError) as error:
        async with grpc.aio.insecure_channel(dn1.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).StatBlock(
                pb2.StatBlockRequest(block_id="blk_auth"),
                metadata=(("x-block-token", _token("different", BlockOp.READ)),),
            )
    assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "FORBIDDEN"


async def test_stat_block_rejects_an_expired_token(cluster):
    dn1, _, _ = cluster
    expired = mint("blk_auth", BlockOp.READ, "test", TOKEN_SECRET, -1)

    with pytest.raises(grpc.aio.AioRpcError) as error:
        async with grpc.aio.insecure_channel(dn1.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).StatBlock(
                pb2.StatBlockRequest(block_id="blk_auth"),
                metadata=(("x-block-token", expired),),
            )

    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "UNAUTHORIZED"


@pytest.mark.parametrize(
    ("token", "status", "error_code"),
    [
        (None, grpc.StatusCode.UNAUTHENTICATED, "UNAUTHORIZED"),
        (_token("different", BlockOp.WRITE), grpc.StatusCode.PERMISSION_DENIED, "FORBIDDEN"),
        (_token("blk_write_auth", BlockOp.READ), grpc.StatusCode.PERMISSION_DENIED, "FORBIDDEN"),
    ],
)
async def test_write_block_interceptor_rejects_unauthorized_calls(
    cluster, token, status, error_code
):
    dn1, _, _ = cluster

    async def requests():
        yield pb2.WriteBlockRequest(
            header=pb2.WriteBlockHeader(
                block_id="blk_write_auth",
                size=len(BLOCK),
                checksum_sha256=hashlib.sha256(BLOCK).hexdigest(),
            )
        )
        yield pb2.WriteBlockRequest(chunk=BLOCK)

    metadata = () if token is None else (("x-block-token", token),)
    with pytest.raises(grpc.aio.AioRpcError) as error:
        async with grpc.aio.insecure_channel(dn1.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).WriteBlock(
                requests(), metadata=metadata
            )

    assert error.value.code() == status
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == error_code
    assert not os.path.exists(dn1.storage.get_temp_block_path("blk_write_auth"))


async def test_duplicate_write_is_idempotent_and_immutable(cluster):
    dn1, _, _ = cluster
    first = await _write_block(dn1.address, "blk_idempotent", BLOCK, [])
    duplicate = await _write_block(dn1.address, "blk_idempotent", BLOCK, [])
    assert not first.already_existed
    assert duplicate.already_existed

    with pytest.raises(grpc.aio.AioRpcError) as error:
        await _write_block(dn1.address, "blk_idempotent", b"other", [])
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "CHECKSUM_MISMATCH"
    assert dn1.storage.read_block("blk_idempotent") == BLOCK


async def test_idempotent_retry_still_forwards_the_existing_block(cluster):
    dn1, dn2, _ = cluster
    await _write_block(dn1.address, "blk_retry_pipeline", BLOCK, [])

    response = await _write_block(
        dn1.address, "blk_retry_pipeline", BLOCK, [dn2.address]
    )

    assert response.already_existed
    assert list(response.failed_downstream) == []
    assert dn2.storage.read_block("blk_retry_pipeline") == BLOCK


async def test_concurrent_duplicate_writes_leave_one_valid_block(cluster):
    dn1, _, _ = cluster
    responses = await asyncio.gather(
        *(_write_block(dn1.address, "blk_concurrent", BLOCK, []) for _ in range(2))
    )
    assert sorted(response.already_existed for response in responses) == [False, True]
    assert dn1.storage.read_block("blk_concurrent") == BLOCK


async def test_oversized_chunk_is_rejected_without_temporary_garbage(cluster):
    dn1, _, _ = cluster
    payload = b"x" * (Settings().block_chunk_size + 1)
    checksum = hashlib.sha256(payload).hexdigest()

    async def requests():
        yield pb2.WriteBlockRequest(
            header=pb2.WriteBlockHeader(
                block_id="blk_oversized",
                size=len(payload),
                checksum_sha256=checksum,
            )
        )
        yield pb2.WriteBlockRequest(chunk=payload)

    with pytest.raises(grpc.aio.AioRpcError) as error:
        async with grpc.aio.insecure_channel(dn1.address) as channel:
            await pb2_grpc.BlockServiceStub(channel).WriteBlock(
                requests(),
                metadata=(("x-block-token", _token("blk_oversized", BlockOp.WRITE)),),
            )

    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "BAD_REQUEST"
    assert not os.path.exists(dn1.storage.get_temp_block_path("blk_oversized"))


async def test_a_corrupt_full_block_read_returns_data_loss_metadata(cluster):
    dn1, _, _ = cluster
    await _write_block(dn1.address, "blk_corrupt_at_rest", BLOCK, [])
    with open(  # noqa: ASYNC230 - intentional test corruption
        dn1.storage.get_block_path("blk_corrupt_at_rest"), "r+b"
    ) as block_file:
        block_file.seek(0)
        block_file.write(b"!")

    with pytest.raises(grpc.aio.AioRpcError) as error:
        await _read_block(dn1.address, "blk_corrupt_at_rest")

    assert error.value.code() == grpc.StatusCode.DATA_LOSS
    assert dict(error.value.trailing_metadata())["x-dfsha-error-code"] == "DATA_LOSS"
