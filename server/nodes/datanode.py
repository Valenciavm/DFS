import os
import shutil
import asyncio
import hashlib
import argparse
from typing import Optional, AsyncIterator
import grpc

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc

# configuracion por defecto del datanode
DEFAULT_NAMENODE_URL = os.getenv("NAMENODE_URL", "http://localhost:8000")  # url del namenode para enviar rest
DEFAULT_HEARTBEAT_INTERVAL = 3  # segundos de espera entre latidos de vida
BLOCK_CHUNK_SIZE = 64 * 1024  # 64 kb por pedazo para transmitir en streaming


class StorageManager:
    """Gestiona el almacenamiento físico de bloques binarios en el disco local."""

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir  # ruta de la carpeta donde se guardan los bloques
        os.makedirs(self.storage_dir, exist_ok=True)  # crea la carpeta si no existe

    def get_block_path(self, block_id: str) -> str:
        """Devuelve la ruta absoluta en disco del archivo del bloque."""
        return os.path.join(self.storage_dir, f"{block_id}.dat")  # ruta del archivo definitivo .dat

    def get_temp_block_path(self, block_id: str) -> str:
        """Devuelve la ruta de un archivo temporal antes de validar su integridad."""
        return os.path.join(self.storage_dir, f"{block_id}.tmp")  # ruta temporal mientras se reciben chunks

    def write_block(self, block_id: str, data: bytes) -> str:
        """Escribe un bloque completo en el disco y retorna su hash SHA-256."""
        path = self.get_block_path(block_id)  # obtiene la ruta de destino
        with open(path, "wb") as f:  # abre en modo binario de escritura
            f.write(data)  # escribe los bytes en disco
        return hashlib.sha256(data).hexdigest()  # retorna la huella sha-256 calculada

    def read_block(self, block_id: str) -> bytes:
        """Lee y devuelve los bytes del bloque desde el disco local."""
        path = self.get_block_path(block_id)  # ruta del bloque solicitado
        if not os.path.exists(path):  # valida si el archivo existe fisicamente
            raise FileNotFoundError(f"Bloque {block_id} no existe en este nodo.")
        with open(path, "rb") as f:  # lee en modo binario
            return f.read()  # devuelve el contenido binario completo

    def delete_block(self, block_id: str) -> bool:
        """Elimina un bloque huérfano o corrupto del disco local."""
        path = self.get_block_path(block_id)  # ruta del bloque a borrar
        if os.path.exists(path):
            os.remove(path)  # elimina el archivo del sistema de archivos
            return True  # confirma que fue borrado
        return False  # el archivo no existia

    def get_disk_usage(self) -> tuple[int, int]:
        """Calcula el espacio total y usado en disco (en bytes)."""
        usage = shutil.disk_usage(self.storage_dir)  # consulta al sistema operativo
        return usage.total, usage.used  # retorna (capacidad_total, espacio_usado)


class BlockServiceImpl(pb2_grpc.BlockServiceServicer):
    """Implementación del servicio gRPC BlockService."""

    def __init__(self, datanode: "DataNode"):
        self.dn = datanode  # referencia al datanode padre
        self.storage = datanode.storage  # acceso directo al disco local

    async def Health(self, request: pb2.HealthRequest, context: grpc.aio.ServicerContext) -> pb2.HealthResponse:
        """Devuelve el estado de salud y espacio libre del nodo."""
        total, used = self.storage.get_disk_usage()  # lee uso de disco
        free_bytes = max(0, total - used)  # calcula espacio disponible en bytes
        total_blocks = len([f for f in os.listdir(self.storage.storage_dir) if f.endswith(".dat")])  # cuenta bloques guardados
        return pb2.HealthResponse(
            node_id=self.dn.node_id,  # id de este datanode
            status="ok",  # estado operativo
            free_bytes=free_bytes,  # espacio disponible
            total_blocks=total_blocks,  # cantidad total de bloques en disco
        )

    async def WriteBlock(
        self, request_iterator: AsyncIterator[pb2.WriteBlockRequest], context: grpc.aio.ServicerContext
    ) -> pb2.WriteBlockResponse:
        """
        Recibe un stream de chunks de bytes para guardar un bloque en disco.
        Si en el header viene un pipeline, retransmite los chunks al siguiente nodo.
        """
        header: Optional[pb2.WriteBlockHeader] = None  # metadatos del bloque recibidos en el primer mensaje
        temp_path: Optional[str] = None  # ruta temporal donde se escriben los chunks
        file_handle = None  # descriptor del archivo abierto para escribir
        hasher = hashlib.sha256()  # acumulador de hash sha-256 sobre la marcha
        bytes_received = 0  # contador de bytes totales recibidos
        downstream_pipeline: list[str] = []  # siguientes datanodes en la cadena de replicacion
        forward_queue: Optional[asyncio.Queue] = None  # cola para reenviar chunks en paralelo
        forward_task: Optional[asyncio.Task] = None  # tarea asincrona de reenvio hacia el siguiente nodo
        failed_downstream: list[str] = []  # almacena direcciones de nodos que hayan fallado en la cadena

        try:
            async for request in request_iterator:
                # 1. primer mensaje: el header con metadatos del bloque
                if request.HasField("header"):
                    header = request.header  # extrae el objeto writeblockheader
                    temp_path = self.storage.get_temp_block_path(header.block_id)  # ruta temporal .tmp
                    file_handle = open(temp_path, "wb")  # abre archivo temporal para escribir
                    downstream_pipeline = list(header.pipeline)  # copia la lista de siguientes nodos

                    print(f"[{self.dn.node_id}] Recibiendo bloque {header.block_id} ({header.size} bytes)...")

                    # si hay nodos siguientes en el pipeline, arranca la tarea de reenvio en caliente
                    if downstream_pipeline:
                        next_target = downstream_pipeline[0]  # primer nodo al que le toca recibir
                        remaining_pipeline = downstream_pipeline[1:]  # resto de la cadena que le pasaremos al siguiente
                        forward_queue = asyncio.Queue()  # cola asincrona para alimentar el reenvio
                        forward_task = asyncio.create_task(
                            self._forward_to_next_node(
                                next_target, header.block_id, header.size, header.checksum_sha256,
                                remaining_pipeline, forward_queue, failed_downstream
                            )
                        )

                # 2. mensajes subsecuentes: chunks de bytes del archivo
                elif request.HasField("chunk"):
                    chunk_data = request.chunk  # extrae los bytes de este pedazo
                    if file_handle:
                        file_handle.write(chunk_data)  # escribe el pedazo en el disco local
                    hasher.update(chunk_data)  # alimenta el calculo del sha-256
                    bytes_received += len(chunk_data)  # suma el tamaño recibido

                    # si hay pipeline activo, encola el pedazo para que salga de inmediato al siguiente nodo
                    if forward_queue:
                        await forward_queue.put(chunk_data)

            # cerramos el archivo temporal al terminar el stream
            if file_handle:
                file_handle.close()  # asegura que los datos se guarden en disco

            # avisar a la tarea de reenvio que la transmision termino
            if forward_queue and forward_task:
                await forward_queue.put(None)  # None es el centinela para indicar fin de transmision
                await forward_task  # espera a que el siguiente nodo confirme su parte

            # 3. validacion de integridad (compara checksum calculado contra el declarado)
            computed_checksum = hasher.hexdigest()  # hash final calculado sobre todos los chunks recibidos
            if header and header.checksum_sha256 and computed_checksum != header.checksum_sha256:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)  # borra archivo corrupto
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Checksum mismatch")  # aborta la llamada grpc

            # 4. renombrar atomicamente a archivo definitivo .dat
            if header:
                final_path = self.storage.get_block_path(header.block_id)  # ruta definitiva .dat
                if temp_path and os.path.exists(temp_path):
                    os.replace(temp_path, final_path)  # reemplazo atomico a nivel de sistema operativo

                print(f"[{self.dn.node_id}] Bloque {header.block_id} guardado exitosamente. SHA: {computed_checksum[:8]}...")

                return pb2.WriteBlockResponse(
                    block_id=header.block_id,  # id confirmado
                    size=bytes_received,  # tamaño total recibido
                    checksum_sha256=computed_checksum,  # hash verificado
                    already_existed=False,  # no existia previamente
                    failed_downstream=failed_downstream,  # lista de nodos fallidos si los hubo
                )
            else:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Missing WriteBlockHeader")

        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)  # limpia archivo temporal en caso de fallo
            print(f"[{self.dn.node_id}] Error escribiendo bloque: {e}")
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    async def _forward_to_next_node(
        self, target_addr: str, block_id: str, size: int, checksum: str,
        pipeline: list[str], queue: asyncio.Queue, failed_downstream: list[str]
    ):
        """Retransmite los chunks al siguiente nodo del pipeline mientras van llegando."""
        try:
            print(f"[{self.dn.node_id}] Conectando con siguiente nodo en pipeline: {target_addr}...")
            async with grpc.aio.insecure_channel(target_addr) as channel:  # abre conexion con el siguiente datanode
                stub = pb2_grpc.BlockServiceStub(channel)  # crea el cliente grpc

                async def generate_requests():
                    # 1. enviar header inicial con el resto de la lista de nodos
                    header = pb2.WriteBlockHeader(
                        block_id=block_id, size=size, checksum_sha256=checksum, pipeline=pipeline
                    )
                    yield pb2.WriteBlockRequest(header=header)  # inyecta el header al stream

                    # 2. enviar chunks conforme salgan de la cola
                    while True:
                        chunk = await queue.get()  # espera el siguiente pedazo que llego de la red
                        if chunk is None:  # centinela de fin
                            break
                        yield pb2.WriteBlockRequest(chunk=chunk)  # reenvia el pedazo al siguiente datanode

                resp = await stub.WriteBlock(generate_requests())  # espera confirmacion del siguiente nodo
                if resp.failed_downstream:
                    failed_downstream.extend(resp.failed_downstream)  # propaga reportes de nodos caidos
                print(f"[{self.dn.node_id}] Pipeline hacia {target_addr} completado con éxito.")
        except Exception as e:
            print(f"[{self.dn.node_id}] Error en pipeline hacia {target_addr}: {e}")
            failed_downstream.append(target_addr)  # anota el nodo caido

    async def ReadBlock(
        self, request: pb2.ReadBlockRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[pb2.ReadBlockResponse]:
        """Transmite los bytes del bloque hacia el cliente en chunks."""
        try:
            data = self.storage.read_block(request.block_id)  # lee los bytes del disco local
            total_size = len(data)  # tamaño total del bloque
            checksum = hashlib.sha256(data).hexdigest()  # hash del bloque completo

            # 1. enviar header con metadatos del bloque
            header = pb2.ReadBlockHeader(
                total_size=total_size, checksum_sha256=checksum, offset=request.offset, length=total_size
            )
            yield pb2.ReadBlockResponse(header=header)  # envia primer mensaje con metadatos

            # 2. enviar datos en pedazos de 64 kb
            for i in range(0, total_size, BLOCK_CHUNK_SIZE):
                chunk = data[i : i + BLOCK_CHUNK_SIZE]  # corta el pedazo de 64 kb
                yield pb2.ReadBlockResponse(chunk=chunk)  # envia el chunk por el stream

        except FileNotFoundError:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Block {request.block_id} not found")  # error 404 grpc


class DataNode:
    """Nodo de datos responsable de almacenar bloques y responder al NameNode."""

    def __init__(
        self,
        node_id: str,
        grpc_host: str = "127.0.0.1",
        grpc_port: int = 9001,
        storage_dir: str = "./data/blocks",
        namenode_url: str = DEFAULT_NAMENODE_URL,
    ):
        self.node_id = node_id  # identificador unico de este nodo (ej: dn1)
        self.grpc_host = grpc_host  # direccion ip o host en que escucha grpc
        self.grpc_port = grpc_port  # puerto de escucha para transferir bloques
        self.address = f"{grpc_host}:{grpc_port}"  # direccion completa entregada al cliente
        self.namenode_url = namenode_url  # url del namenode para enviar rest
        self.storage = StorageManager(storage_dir)  # gestor del disco duro local
        self.grpc_server: Optional[grpc.aio.Server] = None  # instancia del servidor grpc

    async def run(self):
        """Ciclo de vida principal del DataNode: levantamiento gRPC y tareas en paralelo."""
        await asyncio.gather(
            self.start_grpc_server(),  # servidor de bloques para clientes y otros datanodes
            self.heartbeat_loop(),  # bucle de latidos hacia el namenode
        )

    async def start_grpc_server(self):
        """Inicia el servidor gRPC asíncrono para atender peticiones WriteBlock y ReadBlock."""
        self.grpc_server = grpc.aio.server()  # crea el servidor grpc asincrono
        servicer = BlockServiceImpl(self)  # instancia la clase que atiende las peticiones
        pb2_grpc.add_BlockServiceServicer_to_server(servicer, self.grpc_server)  # registra el servicio en el servidor
        self.grpc_server.add_insecure_port(f"{self.grpc_host}:{self.grpc_port}")  # abre el puerto tcp

        await self.grpc_server.start()  # inicia la escucha en segundo plano
        print(f"[DataNode {self.node_id}] Servidor gRPC activo en {self.address} (Almacenamiento: {self.storage.storage_dir})")
        await self.grpc_server.wait_for_termination()  # mantiene vivo el servidor indefinidamente

    async def heartbeat_loop(self):
        """Envía latidos periódicos al NameNode (cuando esté activo)."""
        while True:
            await asyncio.sleep(DEFAULT_HEARTBEAT_INTERVAL)  # espera antes del proximo latido

    async def replicate_block_to_node(self, block_id: str, target_address: str):
        """
        Cliente gRPC: lee un bloque local y llama a WriteBlock del target_address.
        Esto permite la replicación dirigida por el NameNode.
        """
        data = self.storage.read_block(block_id)  # lee los bytes del disco local
        checksum = hashlib.sha256(data).hexdigest()  # calcula su hash sha-256
        size = len(data)  # tamaño en bytes

        print(f"[{self.node_id}] Replicando {block_id} ({size} bytes) -> {target_address}...")

        async with grpc.aio.insecure_channel(target_address) as channel:  # conecta con el otro datanode
            stub = pb2_grpc.BlockServiceStub(channel)  # crea el stub cliente

            async def generate_requests():
                # envia el header inicial sin mas nodos en el pipeline
                yield pb2.WriteBlockRequest(
                    header=pb2.WriteBlockHeader(
                        block_id=block_id, size=size, checksum_sha256=checksum, pipeline=[]
                    )
                )
                # envia los chunks en pedazos de 64 kb
                for i in range(0, size, BLOCK_CHUNK_SIZE):
                    yield pb2.WriteBlockRequest(chunk=data[i : i + BLOCK_CHUNK_SIZE])

            resp = await stub.WriteBlock(generate_requests())  # ejecuta la llamada remota de replicacion
            print(f"[{self.node_id}] Réplica de {block_id} en {target_address} exitosa: SHA {resp.checksum_sha256[:8]}")
            return resp


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DFS DataNode")
    parser.add_argument("--id", default="dn1", help="ID único del DataNode")
    parser.add_argument("--port", type=int, default=9001, help="Puerto gRPC")
    parser.add_argument("--storage", default="./data/dn1", help="Directorio de bloques")
    args = parser.parse_args()

    node = DataNode(node_id=args.id, grpc_port=args.port, storage_dir=args.storage)
    try:
        asyncio.run(node.run())  # arranca el bucle de eventos principal
    except KeyboardInterrupt:
        print(f"\n[DataNode {args.id}] Detenido por el usuario.")
