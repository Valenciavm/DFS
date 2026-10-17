"""
Script de prueba para verificar la replicación directa gRPC entre DataNodes.

Flujo del test:
1. Conecta con el DataNode 1 (localhost:9001).
2. Sube un bloque de prueba con un texto dummy hacia DN1, indicando en el pipeline a DN2 (localhost:9002).
3. Verifica que DN1 guarde el archivo en ./data/dn1/{block_id}.dat.
4. Verifica que DN1 haya reenviado los chunks en pipeline a DN2, guardándolo en ./data/dn2/{block_id}.dat.
5. Lee el bloque desde DN2 con ReadBlock para comprobar que el contenido sea idéntico.
"""

import asyncio
import hashlib
import grpc
from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc


async def test_replication_pipeline():
    print("=" * 60)
    print("INICIANDO PRUEBA DE REPLICACIÓN gRPC (PIPELINE)")
    print("=" * 60)

    # 1. Crear contenido de prueba
    content = b"Hola Mundo Distribuido! Este es un bloque de prueba gRPC replicado." * 1000  # ~67 KB
    block_id = "blk_test_001"
    size = len(content)
    checksum = hashlib.sha256(content).hexdigest()

    print(f"\n[TEST] Bloque generado:")
    print(f"  - ID: {block_id}")
    print(f"  - Tamaño: {size} bytes")
    print(f"  - SHA-256: {checksum}")

    # 2. Conectar a DataNode 1 (127.0.0.1:9001)
    target_dn1 = "127.0.0.1:9001"
    target_dn2 = "127.0.0.1:9002"

    print(f"\n[TEST] Enviando bloque a {target_dn1} con pipeline hacia [{target_dn2}]...")

    async with grpc.aio.insecure_channel(target_dn1) as channel:
        stub = pb2_grpc.BlockServiceStub(channel)

        async def generate_requests():
            # Primer mensaje: Header con el pipeline hacia DN2
            header = pb2.WriteBlockHeader(
                block_id=block_id,
                size=size,
                checksum_sha256=checksum,
                pipeline=[target_dn2],  # ¡DN1 se lo reenviará a DN2 automáticamente!
            )
            yield pb2.WriteBlockRequest(header=header)

            # Siguientes mensajes: Chunks de 16 KB
            chunk_size = 16 * 1024
            for i in range(0, size, chunk_size):
                yield pb2.WriteBlockRequest(chunk=content[i : i + chunk_size])

        # Enviar stream
        response = await stub.WriteBlock(generate_requests())

    print("\n[TEST] Respuesta recibida de DN1:")
    print(f"  - Bloque: {response.block_id}")
    print(f"  - Bytes escritos: {response.size}")
    print(f"  - Checksum confirmado: {response.checksum_sha256}")
    print(f"  - Fallos en downstream: {list(response.failed_downstream)}")

    # 3. Comprobar lectura directa desde DN2 (verificar que realmente se replicó)
    print(f"\n[TEST] Verificando réplica en {target_dn2} con ReadBlock...")
    async with grpc.aio.insecure_channel(target_dn2) as channel_dn2:
        stub_dn2 = pb2_grpc.BlockServiceStub(channel_dn2)
        read_stream = stub_dn2.ReadBlock(pb2.ReadBlockRequest(block_id=block_id, offset=0, length=0))

        downloaded_bytes = bytearray()
        async for msg in read_stream:
            if msg.HasField("chunk"):
                downloaded_bytes.extend(msg.chunk)

    downloaded_checksum = hashlib.sha256(downloaded_bytes).hexdigest()
    print(f"  - Bytes leídos desde DN2: {len(downloaded_bytes)}")
    print(f"  - Checksum leído desde DN2: {downloaded_checksum}")

    if downloaded_checksum == checksum:
        print("\n" + "=" * 60)
        print(">>> PRUEBA EXITOSA: Bloque replicado y verificado en DN2! <<<")
        print("=" * 60)
    else:
        print("\n>>> ERROR: Los datos en DN2 no coinciden. <<<")


if __name__ == "__main__":
    asyncio.run(test_replication_pipeline())
