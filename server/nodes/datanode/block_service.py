"""gRPC BlockService implementation (API.md section 6)."""
from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import AsyncIterator

import grpc

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from server.common.config import MiB
from server.common.errors import GRPC_ERROR_METADATA_KEY, DfshaError, ErrorCode
from server.common.tokens import BlockOp, verify

MAX_BLOCK_CHUNK_SIZE = 2 * MiB
PIPELINE_CONNECT_TIMEOUT_S = 1.0


class BlockTokenInterceptor(grpc.aio.ServerInterceptor):
    """Validate block tokens before dispatching a block RPC to its servicer."""

    def __init__(self, secret: str):
        self.secret = secret

    @staticmethod
    def _block_operation(method: str) -> BlockOp | None:
        if method.endswith("/WriteBlock"):
            return BlockOp.WRITE
        if method.endswith(("/ReadBlock", "/StatBlock")):
            return BlockOp.READ
        return None

    def _validate(
        self, metadata, block_id: str, operation: BlockOp
    ) -> None:
        token = next(
            (
                item.value
                for item in metadata or ()
                if item.key == "x-block-token" and isinstance(item.value, str)
            ),
            None,
        )
        if not self.secret:
            raise DfshaError(
                ErrorCode.UNAUTHORIZED, "block token secret is not configured"
            )
        if token is None:
            raise DfshaError(ErrorCode.UNAUTHORIZED, "missing block token")
        verify(token, self.secret, block_id=block_id, op=operation)

    async def _abort(
        self, context: grpc.aio.ServicerContext, error: DfshaError
    ) -> None:
        await context.abort(
            grpc.StatusCode[error.grpc_status],
            error.message,
            trailing_metadata=((GRPC_ERROR_METADATA_KEY, error.code.value),),
        )

    async def intercept_service(self, continuation, handler_call_details):
        handler = await continuation(handler_call_details)
        if handler is None:
            return None

        operation = self._block_operation(handler_call_details.method)
        if operation is None:
            return handler

        if handler_call_details.method.endswith("/WriteBlock"):
            async def stream_unary(request_iterator, context):
                first = await anext(request_iterator, None)
                if first is None or not first.HasField("header"):
                    await self._abort(
                        context,
                        DfshaError(ErrorCode.BAD_REQUEST, "Missing WriteBlockHeader"),
                    )
                try:
                    self._validate(
                        context.invocation_metadata(),
                        first.header.block_id,
                        operation,
                    )
                except DfshaError as error:
                    await self._abort(context, error)

                async def replay_stream():
                    yield first
                    async for request in request_iterator:
                        yield request

                return await handler.stream_unary(replay_stream(), context)

            return grpc.stream_unary_rpc_method_handler(
                stream_unary,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        async def authorize_request(request, context):
            try:
                self._validate(
                    context.invocation_metadata(), request.block_id, operation
                )
            except DfshaError as error:
                await self._abort(context, error)

        if handler.response_streaming:
            async def unary_stream(request, context):
                await authorize_request(request, context)
                async for response in handler.unary_stream(request, context):
                    yield response

            return grpc.unary_stream_rpc_method_handler(
                unary_stream,
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )

        async def unary_unary(request, context):
            await authorize_request(request, context)
            return await handler.unary_unary(request, context)

        return grpc.unary_unary_rpc_method_handler(
            unary_unary,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class BlockServiceImpl(pb2_grpc.BlockServiceServicer):
    """Streaming block operations for a single DataNode."""

    def __init__(self, datanode):
        self.dn = datanode
        self.storage = datanode.storage
        self.chunk_size = datanode.settings.block_chunk_size
        self._block_locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, block_id: str) -> asyncio.Lock:
        return self._block_locks.setdefault(block_id, asyncio.Lock())

    async def _abort(
        self,
        context: grpc.aio.ServicerContext,
        code: ErrorCode,
        message: str,
    ) -> None:
        error = DfshaError(code, message)
        await context.abort(
            grpc.StatusCode[error.grpc_status],
            error.message,
            trailing_metadata=((GRPC_ERROR_METADATA_KEY, code.value),),
        )

    async def _abort_data_loss(
        self, context: grpc.aio.ServicerContext, message: str
    ) -> None:
        await context.abort(
            grpc.StatusCode.DATA_LOSS,
            message,
            trailing_metadata=((GRPC_ERROR_METADATA_KEY, "DATA_LOSS"),),
        )

    async def Health(
        self, request: pb2.HealthRequest, context: grpc.aio.ServicerContext
    ) -> pb2.HealthResponse:
        total, used = self.storage.get_disk_usage()
        return pb2.HealthResponse(
            node_id=self.dn.node_id,
            status="ok",
            free_bytes=max(0, total - used),
            total_blocks=self.storage.count_blocks(),
        )

    async def WriteBlock(
        self,
        request_iterator: AsyncIterator[pb2.WriteBlockRequest],
        context: grpc.aio.ServicerContext,
    ) -> pb2.WriteBlockResponse:
        """Persist a block and relay it with bounded-memory backpressure."""
        header_request = await anext(request_iterator, None)
        if header_request is None or not header_request.HasField("header"):
            await self._abort(context, ErrorCode.BAD_REQUEST, "Missing WriteBlockHeader")
        header = header_request.header
        token = next(
            item.value
            for item in context.invocation_metadata()
            if item.key == "x-block-token" and isinstance(item.value, str)
        )
        if header.size < 0:
            await self._abort(context, ErrorCode.BAD_REQUEST, "Block size cannot be negative")

        async with self._lock_for(header.block_id):
            try:
                existing_size, existing_checksum = self.storage.get_block_info(header.block_id)
            except FileNotFoundError:
                existing_size = existing_checksum = None
            except OSError as error:
                await self._abort_data_loss(context, f"Cannot inspect existing block: {error}")

            if existing_size is not None:
                if existing_size != header.size or existing_checksum != header.checksum_sha256:
                    await self._abort(
                        context,
                        ErrorCode.CHECKSUM_MISMATCH,
                        f"Block {header.block_id} already exists with different content",
                    )
                return pb2.WriteBlockResponse(
                    block_id=header.block_id,
                    size=existing_size,
                    checksum_sha256=existing_checksum,
                    already_existed=True,
                    failed_downstream=await self._forward_from_file(
                        self.storage.get_block_path(header.block_id),
                        header.block_id,
                        existing_size,
                        existing_checksum,
                        list(header.pipeline),
                        token,
                    ),
                )

            temp_path = self.storage.get_temp_block_path(header.block_id)
            final_path = self.storage.get_block_path(header.block_id)
            file_handle = None
            forward_queue: asyncio.Queue[bytes | None] | None = None
            forward_task: asyncio.Task[bool] | None = None
            failed_downstream: list[str] = []
            downstream_pipeline = list(header.pipeline)
            chosen_index: int | None = None
            bytes_received = 0
            hasher = hashlib.sha256()

            try:
                if downstream_pipeline:
                    chosen_index, unavailable = await self._first_reachable(
                        downstream_pipeline
                    )
                    failed_downstream.extend(unavailable)
                    if chosen_index is not None:
                        forward_queue = asyncio.Queue(maxsize=2)
                        forward_task = asyncio.create_task(
                            self._forward_to_next_node(
                                target_addr=downstream_pipeline[chosen_index],
                                block_id=header.block_id,
                                size=header.size,
                                checksum=header.checksum_sha256,
                                pipeline=downstream_pipeline[chosen_index + 1 :],
                                token=token,
                                queue=forward_queue,
                                failed_downstream=failed_downstream,
                            )
                        )

                file_handle = open(temp_path, "wb")  # noqa: ASYNC230, SIM115
                async for chunk_request in request_iterator:
                    if not chunk_request.HasField("chunk"):
                        await self._abort(
                            context, ErrorCode.BAD_REQUEST, "Expected a WriteBlock chunk"
                        )
                    chunk = chunk_request.chunk
                    if len(chunk) > MAX_BLOCK_CHUNK_SIZE or len(chunk) > self.chunk_size:
                        await self._abort(
                            context,
                            ErrorCode.BAD_REQUEST,
                            f"Chunk exceeds the configured maximum of {self.chunk_size} bytes",
                        )

                    file_handle.write(chunk)
                    hasher.update(chunk)
                    bytes_received += len(chunk)
                    if forward_queue is not None and forward_task is not None:
                        if forward_task.done():
                            await forward_task
                            forward_task = None
                        else:
                            put_task = asyncio.create_task(forward_queue.put(chunk))
                            done, _ = await asyncio.wait(
                                (put_task, forward_task),
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if forward_task in done:
                                put_task.cancel()
                                try:
                                    await put_task
                                except asyncio.CancelledError:
                                    pass
                                await forward_task
                                forward_task = None
                            else:
                                await put_task

                file_handle.close()
                file_handle = None

                computed_checksum = hasher.hexdigest()
                if bytes_received != header.size or computed_checksum != header.checksum_sha256:
                    await self._abort(
                        context,
                        ErrorCode.CHECKSUM_MISMATCH,
                        "Block size or checksum does not match the header",
                    )

                os.replace(temp_path, final_path)
                self.storage.write_block_info(
                    header.block_id, bytes_received, computed_checksum
                )
                asyncio.create_task(
                    self.dn.notify_block_received(
                        header.block_id, bytes_received, computed_checksum
                    )
                )

                if forward_task is not None and forward_queue is not None:
                    if not forward_task.done():
                        end_task = asyncio.create_task(forward_queue.put(None))
                        done, _ = await asyncio.wait(
                            (end_task, forward_task),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if forward_task in done:
                            end_task.cancel()
                            try:
                                await end_task
                            except asyncio.CancelledError:
                                pass
                        else:
                            await end_task
                    forwarded = await forward_task
                else:
                    forwarded = forward_task is not None

                if (
                    not forwarded
                    and chosen_index is not None
                    and chosen_index + 1 < len(downstream_pipeline)
                ):
                    failed_downstream.extend(
                        await self._forward_from_file(
                            final_path,
                            header.block_id,
                            header.size,
                            computed_checksum,
                            downstream_pipeline[chosen_index + 1 :],
                            token,
                        )
                    )

                return pb2.WriteBlockResponse(
                    block_id=header.block_id,
                    size=bytes_received,
                    checksum_sha256=computed_checksum,
                    already_existed=False,
                    failed_downstream=failed_downstream,
                )
            except grpc.aio.AbortError:
                raise
            except Exception as error:  # noqa: BLE001 - clean the incomplete local copy
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
                await context.abort(
                    grpc.StatusCode.INTERNAL,
                    str(error),
                    trailing_metadata=((GRPC_ERROR_METADATA_KEY, "INTERNAL"),),
                )
            finally:
                if file_handle is not None:
                    file_handle.close()
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                if forward_task is not None and not forward_task.done():
                    forward_task.cancel()
                    try:
                        await forward_task
                    except asyncio.CancelledError:
                        pass

    async def _first_reachable(self, pipeline: list[str]) -> tuple[int | None, list[str]]:
        """Find a live hop before consuming the upstream stream."""
        failed: list[str] = []
        for index, address in enumerate(pipeline):
            channel = grpc.aio.insecure_channel(address)
            try:
                await asyncio.wait_for(
                    channel.channel_ready(), timeout=PIPELINE_CONNECT_TIMEOUT_S
                )
                return index, failed
            except (TimeoutError, grpc.aio.AioRpcError, OSError):
                failed.append(address)
            finally:
                await channel.close()
        return None, failed

    async def _forward_to_next_node(
        self,
        target_addr: str,
        block_id: str,
        size: int,
        checksum: str,
        pipeline: list[str],
        token: str,
        queue: asyncio.Queue[bytes | None],
        failed_downstream: list[str],
    ) -> bool:
        """Forward arriving chunks to one preflighted node."""
        try:
            async with grpc.aio.insecure_channel(target_addr) as channel:
                stub = pb2_grpc.BlockServiceStub(channel)

                async def requests():
                    yield pb2.WriteBlockRequest(
                        header=pb2.WriteBlockHeader(
                            block_id=block_id,
                            size=size,
                            checksum_sha256=checksum,
                            pipeline=pipeline,
                        )
                    )
                    while (chunk := await queue.get()) is not None:
                        yield pb2.WriteBlockRequest(chunk=chunk)

                response = await stub.WriteBlock(
                    requests(), metadata=(("x-block-token", token),)
                )
                failed_downstream.extend(response.failed_downstream)
                return True
        except Exception:  # noqa: BLE001 - downstream failure does not invalidate local copy
            failed_downstream.append(target_addr)
            return False

    async def _forward_from_file(
        self,
        file_path: str,
        block_id: str,
        size: int,
        checksum: str,
        pipeline: list[str],
        token: str,
    ) -> list[str]:
        """Replay a disk-backed local copy when a live hop fails mid-stream."""
        failed_downstream: list[str] = []
        remaining = list(pipeline)
        while remaining:
            index, unavailable = await self._first_reachable(remaining)
            failed_downstream.extend(unavailable)
            if index is None:
                return failed_downstream
            target = remaining[index]
            rest = remaining[index + 1 :]
            try:
                async with grpc.aio.insecure_channel(target) as channel:
                    stub = pb2_grpc.BlockServiceStub(channel)

                    async def requests(rest=rest):
                        yield pb2.WriteBlockRequest(
                            header=pb2.WriteBlockHeader(
                                block_id=block_id,
                                size=size,
                                checksum_sha256=checksum,
                                pipeline=rest,
                            )
                        )
                        with open(file_path, "rb") as block_file:  # noqa: ASYNC230
                            while chunk := block_file.read(self.chunk_size):
                                yield pb2.WriteBlockRequest(chunk=chunk)

                    response = await stub.WriteBlock(
                        requests(), metadata=(("x-block-token", token),)
                    )
                    failed_downstream.extend(response.failed_downstream)
                    return failed_downstream
            except Exception:  # noqa: BLE001 - try the next node using the local disk copy
                failed_downstream.append(target)
                remaining = rest
        return failed_downstream

    async def ReadBlock(
        self, request: pb2.ReadBlockRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[pb2.ReadBlockResponse]:
        """Stream only the requested range while reporting whole-block metadata."""
        try:
            total_size, checksum = self.storage.get_block_info(request.block_id)
        except FileNotFoundError:
            await self._abort(
                context, ErrorCode.BLOCK_NOT_FOUND, f"Block {request.block_id} not found"
            )
            return
        except OSError as error:
            asyncio.create_task(self.dn.notify_block_corrupt(request.block_id))
            await self._abort_data_loss(context, f"Cannot read block metadata: {error}")
            return

        offset = request.offset
        length = request.length
        if (
            offset < 0
            or length < 0
            or offset > total_size
            or (length > 0 and (offset + length > total_size or offset + length > (1 << 63) - 1))
        ):
            await self._abort(
                context, ErrorCode.RANGE_NOT_SATISFIABLE, "Requested block range is invalid"
            )
            return

        served_length = total_size - offset if length == 0 else length
        yield pb2.ReadBlockResponse(
            header=pb2.ReadBlockHeader(
                total_size=total_size,
                checksum_sha256=checksum,
                offset=offset,
                length=served_length,
            )
        )

        try:
            with open(  # noqa: ASYNC230
                self.storage.get_block_path(request.block_id), "rb"
            ) as block_file:
                block_file.seek(offset)
                remaining = served_length
                range_hasher = hashlib.sha256() if offset == 0 and served_length == total_size else None
                while remaining:
                    chunk = block_file.read(min(self.chunk_size, remaining))
                    if not chunk:
                        await self._abort_data_loss(context, "Block changed during ranged read")
                        return
                    remaining -= len(chunk)
                    if range_hasher is not None:
                        range_hasher.update(chunk)
                    yield pb2.ReadBlockResponse(chunk=chunk)
                if range_hasher is not None and range_hasher.hexdigest() != checksum:
                    await self._abort_data_loss(context, "Block checksum does not match metadata")
        except FileNotFoundError:
            await self._abort(
                context, ErrorCode.BLOCK_NOT_FOUND, f"Block {request.block_id} not found"
            )
        except OSError as error:
            await self._abort_data_loss(context, f"Cannot read block range: {error}")

    async def StatBlock(
        self, request: pb2.StatBlockRequest, context: grpc.aio.ServicerContext
    ) -> pb2.StatBlockResponse:
        """Return authenticated block metadata without transferring block bytes."""
        try:
            size, checksum = self.storage.get_block_info(request.block_id)
        except FileNotFoundError:
            await self._abort(
                context, ErrorCode.BLOCK_NOT_FOUND, f"Block {request.block_id} not found"
            )
        except OSError as error:
            await self._abort_data_loss(context, f"Cannot inspect block: {error}")
        return pb2.StatBlockResponse(
            block_id=request.block_id, size=size, checksum_sha256=checksum
        )
