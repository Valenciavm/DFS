from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from client.file import DFSFile


def manifest():
    return {
        "size": 6,
        "blocks": [
            {
                "index": 0,
                "block_id": "block-0",
                "size": 3,
                "checksum": "checksum-0",
                "token": "token-0",
                "replicas": [{"addr": "dn0:9000"}],
            },
            {
                "index": 1,
                "block_id": "block-1",
                "size": 3,
                "checksum": "checksum-1",
                "token": "token-1",
                "replicas": [{"addr": "dn1:9000"}],
            },
        ],
    }


def fake_transport():
    transport = Mock()

    def read_block(*, block_id, offset, length, **kwargs):
        data = {"block-0": b"abc", "block-1": b"def"}[block_id]
        payload = data[offset : offset + length]
        return payload, SimpleNamespace(
            offset=offset,
            length=length,
            total_size=len(data),
            checksum_sha256=f"checksum-{block_id[-1]}",
        )

    transport.read_block.side_effect = read_block
    return transport


def test_read_seek_crosses_block_boundary_using_only_requested_ranges():
    client = SimpleNamespace(block_transport=fake_transport())
    file = DFSFile(client, manifest())

    assert file.seek(2) == 2
    assert file.read(3) == b"cde"
    assert file.tell() == 5
    assert [
        (call.kwargs["block_id"], call.kwargs["offset"], call.kwargs["length"])
        for call in client.block_transport.read_block.call_args_list
    ] == [("block-0", 2, 1), ("block-1", 0, 2)]


def test_read_to_eof_seek_from_end_and_range_cache():
    client = SimpleNamespace(block_transport=fake_transport())
    file = DFSFile(client, manifest(), cache_ranges=1)

    assert file.read(2) == b"ab"
    assert file.seek(0) == 0
    assert file.read(2) == b"ab"
    assert client.block_transport.read_block.call_count == 1
    assert file.seek(-2, 2) == 4
    assert file.read() == b"ef"
    assert file.read() == b""


def test_range_read_falls_back_when_replica_returns_corrupt_metadata():
    client = SimpleNamespace(block_transport=Mock())
    client.block_transport.read_block.side_effect = [
        (
            b"abc",
            SimpleNamespace(
                offset=0,
                length=3,
                total_size=3,
                checksum_sha256="corrupt",
            ),
        ),
        (
            b"abc",
            SimpleNamespace(
                offset=0,
                length=3,
                total_size=3,
                checksum_sha256="checksum-0",
            ),
        ),
    ]
    file_manifest = manifest()
    file_manifest["blocks"][0]["replicas"].append({"addr": "backup:9000"})
    remote_file = DFSFile(client, file_manifest)

    assert remote_file.read(3) == b"abc"
    assert client.block_transport.read_block.call_count == 2


def test_close_releases_optional_lock_and_rejects_later_reads():
    client = SimpleNamespace(block_transport=fake_transport(), unlock=Mock())
    file = DFSFile(client, manifest(), lock_id="lock-1")

    file.close()
    file.close()

    client.unlock.assert_called_once_with("lock-1")
    assert file.closed
    with pytest.raises(ValueError, match="closed"):
        file.read()


def test_inconsistent_manifest_and_negative_seek_fail_clearly():
    client = SimpleNamespace(block_transport=fake_transport())
    with pytest.raises(OSError, match="file size"):
        DFSFile(client, {"size": 10, "blocks": manifest()["blocks"]})

    file = DFSFile(client, manifest())
    with pytest.raises(ValueError, match="negative"):
        file.seek(-1)
