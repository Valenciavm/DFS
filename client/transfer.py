"""Synchronous gRPC client for DataNode block transfers."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import grpc

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc


class GrpcBlockTransport:
    def __init__(
        self,
        *,
        ca_cert: str | bytes | None = None,
        client_cert: str | bytes | None = None,
        client_key: str | bytes | None = None,
        chunk_size: int = 1024 * 1024,
        timeout: float = 60,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if (client_cert is None) != (client_key is None):
            raise ValueError("client_cert and client_key must be supplied together")
        self.ca_cert = self._read_certificate(ca_cert)
        self.client_cert = self._read_certificate(client_cert)
        self.client_key = self._read_certificate(client_key)
        self.chunk_size = chunk_size
        self.timeout = timeout

    @staticmethod
    def _read_certificate(value: str | bytes | None) -> bytes | None:
        if value is None or isinstance(value, bytes):
            return value
        return Path(value).read_bytes()

    def _channel(self, addr: str):
        if self.ca_cert is None and self.client_cert is None:
            return grpc.insecure_channel(addr)
        credentials = grpc.ssl_channel_credentials(
            root_certificates=self.ca_cert,
            private_key=self.client_key,
            certificate_chain=self.client_cert,
        )
        return grpc.secure_channel(addr, credentials)

    def write_block(
        self,
        *,
        addr: str,
        token: str,
        block_id: str,
        data: bytes,
        checksum_sha256: str,
        pipeline: list[str],
    ) -> pb2.WriteBlockResponse:
        def requests() -> Iterator[pb2.WriteBlockRequest]:
            yield pb2.WriteBlockRequest(
                header=pb2.WriteBlockHeader(
                    block_id=block_id,
                    size=len(data),
                    checksum_sha256=checksum_sha256,
                    pipeline=pipeline,
                )
            )
            for offset in range(0, len(data), self.chunk_size):
                yield pb2.WriteBlockRequest(
                    chunk=data[offset : offset + self.chunk_size]
                )

        with self._channel(addr) as channel:
            stub = pb2_grpc.BlockServiceStub(channel)
            return stub.WriteBlock(
                requests(),
                timeout=self.timeout,
                metadata=(("x-block-token", token),),
            )

    def read_block(
        self,
        *,
        addr: str,
        token: str,
        block_id: str,
        offset: int,
        length: int,
    ) -> tuple[bytes, pb2.ReadBlockHeader]:
        with self._channel(addr) as channel:
            stub = pb2_grpc.BlockServiceStub(channel)
            responses = stub.ReadBlock(
                pb2.ReadBlockRequest(
                    block_id=block_id,
                    offset=offset,
                    length=length,
                ),
                timeout=self.timeout,
                metadata=(("x-block-token", token),),
            )
            header = None
            chunks: list[bytes] = []
            for response in responses:
                if response.HasField("header"):
                    if header is not None:
                        raise OSError("DataNode returned multiple ReadBlock headers")
                    header = response.header
                elif response.HasField("chunk"):
                    chunks.append(response.chunk)
            if header is None:
                raise OSError("DataNode response is missing the ReadBlock header")
            payload = b"".join(chunks)
            if len(payload) != header.length:
                raise OSError(
                    f"DataNode returned {len(payload)} bytes; expected {header.length}"
                )
            return payload, header
