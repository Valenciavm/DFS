"""Read-only range and sequential write handles for DFSha files."""

import logging
import os
import tempfile
from collections import OrderedDict
from pathlib import Path

import grpc

from .exceptions import IntegrityError

logger = logging.getLogger(__name__)


class DFSFile:
    """Read a DFSha file by fetching only requested block ranges."""

    def __init__(
        self,
        client,
        manifest,
        *,
        lock_id=None,
        cache_ranges=16,
    ):
        if cache_ranges < 0:
            raise ValueError("cache_ranges cannot be negative")
        self._client = client
        self._manifest = manifest
        self._lock_id = lock_id
        self._cache_ranges = cache_ranges
        self._cache = OrderedDict()
        self._position = 0
        self._closed = False
        self._size = int(manifest["size"])
        if self._size < 0:
            raise OSError("NameNode returned a negative file size")

        self._blocks = []
        offset = 0
        for block in sorted(manifest.get("blocks", []), key=lambda item: item["index"]):
            size = int(block["size"])
            if size < 0:
                raise OSError("NameNode returned a negative block size")
            self._blocks.append((offset, offset + size, block))
            offset += size
        if offset != self._size:
            raise OSError("NameNode block sizes do not match the file size")

    @property
    def closed(self):
        return self._closed

    def tell(self):
        self._check_open()
        return self._position

    def seek(self, offset, whence=0):
        self._check_open()
        if whence == 0:
            position = offset
        elif whence == 1:
            position = self._position + offset
        elif whence == 2:
            position = self._size + offset
        else:
            raise ValueError(f"invalid whence ({whence})")
        if position < 0:
            raise ValueError("negative seek position")
        self._position = position
        return position

    def read(self, size=-1):
        self._check_open()
        if size is None or size < 0:
            size = self._size - self._position
        if size == 0 or self._position >= self._size:
            return b""

        end = min(self._position + size, self._size)
        chunks = []
        for block_start, block_end, block in self._blocks:
            range_start = max(self._position, block_start)
            range_end = min(end, block_end)
            if range_start >= range_end:
                continue
            chunks.append(
                self._read_range(
                    block,
                    range_start - block_start,
                    range_end - range_start,
                )
            )

        self._position = end
        return b"".join(chunks)

    def _read_range(self, block, offset, length):
        key = (block["block_id"], offset, length)
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]

        last_error = None
        for replica in block.get("replicas", []):
            try:
                payload, header = self._client.block_transport.read_block(
                    addr=replica["addr"],
                    token=block["token"],
                    block_id=block["block_id"],
                    offset=offset,
                    length=length,
                )
                if len(payload) != length:
                    raise IntegrityError(
                        f"Incorrect range size for {block['block_id']}"
                    )
                if (
                    header.offset != offset
                    or header.length != length
                    or header.total_size != int(block["size"])
                ):
                    raise IntegrityError(
                        f"DataNode returned incorrect range metadata for {block['block_id']}"
                    )
                expected_checksum = block.get("checksum")
                if expected_checksum and header.checksum_sha256 != expected_checksum:
                    raise IntegrityError(
                        f"Block checksum metadata mismatch for {block['block_id']}"
                    )
                self._cache_payload(key, payload)
                return payload
            except (grpc.RpcError, OSError, IntegrityError) as error:
                last_error = error

        if last_error is not None:
            raise last_error
        raise OSError(f"No live replicas for {block['block_id']}")

    def _cache_payload(self, key, payload):
        if not self._cache_ranges:
            return
        self._cache[key] = payload
        self._cache.move_to_end(key)
        while len(self._cache) > self._cache_ranges:
            self._cache.popitem(last=False)

    def _check_open(self):
        if self._closed:
            raise ValueError("I/O operation on closed DFSFile")

    def close(self):
        if self._closed:
            return
        if self._lock_id is not None:
            self._client.unlock(self._lock_id)
            self._lock_id = None
        self._closed = True

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False


class DFSFileWriter:
    """Sequential WORM writer that uploads its staged file on close."""

    def __init__(
        self,
        client,
        remote_path,
        *,
        overwrite=False,
        lock_id=None,
    ):
        self._client = client
        self._remote_path = remote_path
        self._overwrite = overwrite
        self._lock_id = lock_id
        self._closed = False
        fd, temp_name = tempfile.mkstemp(
            prefix="dfsha-write-",
            suffix=".part",
        )
        self._path = Path(temp_name)
        try:
            self._file = os.fdopen(fd, "w+b")
        except OSError:
            os.close(fd)
            self._path.unlink(missing_ok=True)
            raise

    @property
    def closed(self):
        return self._closed

    def write(self, data):
        self._check_open()
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("a bytes-like object is required, not str")
        return self._file.write(data)

    def flush(self):
        self._check_open()
        self._file.flush()

    def tell(self):
        self._check_open()
        return self._file.tell()

    def seek(self, offset, whence=0):
        self._check_open()
        raise OSError("DFSha writes are sequential; random writes are not supported")

    def _check_open(self):
        if self._closed:
            raise ValueError("I/O operation on closed DFSFileWriter")

    def _cleanup(self):
        try:
            self._file.close()
        finally:
            self._path.unlink(missing_ok=True)

    def _release_lock(self, *, preserve_error):
        if self._lock_id is None:
            return
        lock_id = self._lock_id
        self._lock_id = None
        try:
            self._client.unlock(lock_id)
        except Exception:
            if not preserve_error:
                raise
            logger.exception("Unable to release lock %s after write failure", lock_id)

    def close(self):
        if self._closed:
            return
        failure = None
        try:
            self._file.flush()
            self._file.close()
            self._client.put_file(
                self._path,
                self._remote_path,
                overwrite=self._overwrite,
            )
        except Exception as error:
            failure = error
            raise
        finally:
            self._closed = True
            try:
                self._cleanup()
            finally:
                self._release_lock(preserve_error=failure is not None)

    def abort(self, *, preserve_error=False):
        if self._closed:
            return
        self._closed = True
        try:
            self._cleanup()
        finally:
            self._release_lock(preserve_error=preserve_error)

    def __enter__(self):
        self._check_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
        else:
            self.abort(preserve_error=True)
        return False
