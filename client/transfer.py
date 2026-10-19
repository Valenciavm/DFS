"""DataNode gRPC block transport adapter using generated BlockService stubs."""
from __future__ import annotations

import io
from collections.abc import Iterator
from typing import Any

import grpc

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from .exceptions import DFShaError


class GrpcBlockTransport:
    def __init__(
        self,
        *,
        ca_cert: bytes | None = None,
        client_cert: bytes | None = None,
        client_key: bytes | None = None,
        chunk_size: int = 1024 * 1024,
        timeout: float = 60.0,
    ):
        self.ca_cert = ca_cert
        self.client_cert = client_cert
        self.client_key = client_key
        self.chunk_size = chunk_size
        self.timeout = timeout

    def _get_channel(self, addr: str) -> grpc.Channel:
        """Create a gRPC channel to the target DataNode address (insecure or mTLS)."""
        if self.ca_cert:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=self.ca_cert,
                private_key=self.client_key,
                certificate_chain=self.client_cert,
            )
            return grpc.secure_channel(addr, credentials)
        return grpc.insecure_channel(addr)

    def write_block(
        self,
        *,
        addr: str,
        token: str,
        block_id: str,
        data: bytes,
        checksum_sha256: str,
        pipeline: list[str] | None = None,
    ) -> pb2.WriteBlockResponse:
        """Stream a block to a DataNode with x-block-token metadata."""
        downstream = list(pipeline or [])

        def request_generator() -> Iterator[pb2.WriteBlockRequest]:
            # Step 1: WriteBlockHeader
            header = pb2.WriteBlockHeader(
                block_id=block_id,
                size=len(data),
                checksum_sha256=checksum_sha256,
                pipeline=downstream,
            )
            yield pb2.WriteBlockRequest(header=header)

            # Step 2: Stream chunks
            stream = io.BytesIO(data)
            while True:
                chunk = stream.read(self.chunk_size)
                if not chunk:
                    break
                yield pb2.WriteBlockRequest(chunk=chunk)

        metadata = (("x-block-token", str(token)),)
        channel = self._get_channel(addr)
        try:
            stub = pb2_grpc.BlockServiceStub(channel)
            response = stub.WriteBlock(
                request_generator(),
                metadata=metadata,
                timeout=self.timeout,
            )
            return response
        except grpc.RpcError as exc:
            code = exc.code()
            details = exc.details()
            raise DFShaError(f"DataNode {addr} WriteBlock failed: {code.name} - {details}") from exc
        finally:
            channel.close()

    def read_block(
        self,
        *,
        addr: str,
        token: str,
        block_id: str,
        offset: int = 0,
        length: int = 0,
    ) -> tuple[bytes, pb2.ReadBlockHeader | None]:
        """Read a block from a DataNode with x-block-token metadata."""
        req = pb2.ReadBlockRequest(
            block_id=block_id,
            offset=offset,
            length=length,
        )
        metadata = (("x-block-token", str(token)),)
        channel = self._get_channel(addr)
        try:
            stub = pb2_grpc.BlockServiceStub(channel)
            responses = stub.ReadBlock(
                req,
                metadata=metadata,
                timeout=self.timeout,
            )
            header: pb2.ReadBlockHeader | None = None
            chunks: list[bytes] = []
            for resp in responses:
                if resp.HasField("header"):
                    header = resp.header
                elif resp.HasField("chunk"):
                    chunks.append(resp.chunk)
            return b"".join(chunks), header
        except grpc.RpcError as exc:
            code = exc.code()
            details = exc.details()
            raise DFShaError(f"DataNode {addr} ReadBlock failed: {code.name} - {details}") from exc
        finally:
            channel.close()

    def stat_block(
        self,
        *,
        addr: str,
        token: str,
        block_id: str,
    ) -> pb2.StatBlockResponse:
        """Query block status on DataNode with x-block-token metadata."""
        req = pb2.StatBlockRequest(block_id=block_id)
        metadata = (("x-block-token", str(token)),)
        channel = self._get_channel(addr)
        try:
            stub = pb2_grpc.BlockServiceStub(channel)
            return stub.StatBlock(req, metadata=metadata, timeout=self.timeout)
        except grpc.RpcError as exc:
            code = exc.code()
            details = exc.details()
            raise DFShaError(f"DataNode {addr} StatBlock failed: {code.name} - {details}") from exc
        finally:
            channel.close()

    def health(self, addr: str) -> pb2.HealthResponse:
        """Health check for DataNode (does not require block token)."""
        req = pb2.HealthRequest()
        channel = self._get_channel(addr)
        try:
            stub = pb2_grpc.BlockServiceStub(channel)
            return stub.Health(req, timeout=self.timeout)
        except grpc.RpcError as exc:
            code = exc.code()
            details = exc.details()
            raise DFShaError(f"DataNode {addr} Health failed: {code.name} - {details}") from exc
        finally:
            channel.close()
