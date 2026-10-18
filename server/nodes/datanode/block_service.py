"""gRPC BlockService implementation (API.md section 6).

This is the block-level read/write API clients and other DataNodes talk to. The
NameNode never carries block bytes.
"""
import hashlib
import os
from typing import Optional, AsyncIterator

import grpc

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc

BLOCK_CHUNK_SIZE = 64 * 1024  # 64 kb por pedazo para transmitir en streaming


class BlockServiceImpl(pb2_grpc.BlockServiceServicer):
    """Implementación del servicio gRPC BlockService."""

    def __init__(self, datanode):
        self.dn = datanode  # referencia al datanode padre
        self.storage = datanode.storage  # acceso directo al disco local

    async def Health(
        self, request: pb2.HealthRequest, context: grpc.aio.ServicerContext
    ) -> pb2.HealthResponse:
        """Devuelve el estado de salud y espacio libre del nodo."""
        total, used = self.storage.get_disk_usage()  # lee uso de disco
        free_bytes = max(0, total - used)  # calcula espacio disponible en bytes
        return pb2.HealthResponse(
            node_id=self.dn.node_id,  # id de este datanode
            status="ok",  # estado operativo
            free_bytes=free_bytes,  # espacio disponible
            total_blocks=self.storage.count_blocks(),  # cantidad total de bloques en disco
        )

    async def WriteBlock(
        self,
        request_iterator: AsyncIterator[pb2.WriteBlockRequest],
        context: grpc.aio.ServicerContext,
    ) -> pb2.WriteBlockResponse:
        """Recibe un stream de chunks de bytes para guardar un bloque en disco."""
        header: Optional[pb2.WriteBlockHeader] = None  # metadatos recibidos en el primer mensaje
        temp_path: Optional[str] = None  # ruta temporal donde se escriben los chunks
        file_handle = None  # descriptor del archivo abierto para escribir
        hasher = hashlib.sha256()  # acumulador de hash sha-256 sobre la marcha
        bytes_received = 0  # contador de bytes totales recibidos

        try:
            async for request in request_iterator:
                # 1. primer mensaje: el header con metadatos del bloque
                if request.HasField("header"):
                    header = request.header  # extrae el objeto writeblockheader
                    temp_path = self.storage.get_temp_block_path(header.block_id)  # ruta temporal .tmp
                    file_handle = open(temp_path, "wb")  # abre archivo temporal para escribir
                    print(f"[{self.dn.node_id}] Recibiendo bloque {header.block_id} ({header.size} bytes)...")

                # 2. mensajes subsecuentes: chunks de bytes del bloque
                elif request.HasField("chunk"):
                    chunk_data = request.chunk  # extrae los bytes de este pedazo
                    if file_handle:
                        file_handle.write(chunk_data)  # escribe el pedazo en el disco local
                    hasher.update(chunk_data)  # alimenta el calculo del sha-256
                    bytes_received += len(chunk_data)  # suma el tamaño recibido

            # cerramos el archivo temporal al terminar el stream
            if file_handle:
                file_handle.close()  # asegura que los datos se guarden en disco

            if header is None:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Missing WriteBlockHeader")

            # 3. validacion de integridad (compara checksum calculado contra el declarado)
            computed_checksum = hasher.hexdigest()  # hash final sobre todos los chunks recibidos
            if header.checksum_sha256 and computed_checksum != header.checksum_sha256:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)  # borra archivo corrupto
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Checksum mismatch")

            # 4. renombrar atomicamente a archivo definitivo .dat
            final_path = self.storage.get_block_path(header.block_id)  # ruta definitiva .dat
            if temp_path and os.path.exists(temp_path):
                os.replace(temp_path, final_path)  # reemplazo atomico a nivel de sistema operativo

            print(f"[{self.dn.node_id}] Bloque {header.block_id} guardado. SHA: {computed_checksum[:8]}...")

            return pb2.WriteBlockResponse(
                block_id=header.block_id,  # id confirmado
                size=bytes_received,  # tamaño total recibido
                checksum_sha256=computed_checksum,  # hash verificado
                already_existed=False,  # no existia previamente
            )

        except grpc.aio.AbortError:
            raise  # context.abort ya decidio el estado de la llamada
        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)  # limpia archivo temporal en caso de fallo
            print(f"[{self.dn.node_id}] Error escribiendo bloque: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    async def ReadBlock(
        self, request: pb2.ReadBlockRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[pb2.ReadBlockResponse]:
        """Transmite los bytes del bloque hacia el cliente en chunks."""
        try:
            data = self.storage.read_block(request.block_id)  # lee los bytes del disco local
            total_size = len(data)  # tamaño total del bloque
            checksum = hashlib.sha256(data).hexdigest()  # hash del bloque completo

            # 1. enviar header con metadatos del bloque
            yield pb2.ReadBlockResponse(
                header=pb2.ReadBlockHeader(
                    total_size=total_size,
                    checksum_sha256=checksum,
                    offset=request.offset,
                    length=total_size,
                )
            )

            # 2. enviar datos en pedazos
            for i in range(0, total_size, BLOCK_CHUNK_SIZE):
                yield pb2.ReadBlockResponse(chunk=data[i : i + BLOCK_CHUNK_SIZE])

        except FileNotFoundError:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Block {request.block_id} not found")
