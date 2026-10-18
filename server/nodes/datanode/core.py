"""DataNode lifecycle: block server plus the control channel to the NameNode."""
import asyncio
import os

import grpc

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from server.common.config import Settings, settings
from server.nodes.datanode.block_service import BlockServiceImpl
from server.nodes.datanode.storage import StorageManager

# Configuración por defecto del DataNode
DEFAULT_NAMENODE_URL = os.getenv("NAMENODE_URL", "http://localhost:8000")
DEFAULT_HEARTBEAT_INTERVAL = 3  # segundos entre latidos


class DataNode:
    """Nodo de datos responsable de almacenar bloques y responder al NameNode."""

    def __init__(
        self,
        node_id: str,
        grpc_host: str = "127.0.0.1",
        grpc_port: int = 9001,
        storage_dir: str = "./data/blocks",
        namenode_url: str = DEFAULT_NAMENODE_URL,
        config: Settings | None = None,
    ):
        self.node_id = node_id  # identificador unico de este nodo (ej: dn1)
        self.grpc_host = grpc_host  # direccion ip o host en que escucha grpc
        self.grpc_port = grpc_port  # puerto de escucha para transferir bloques
        self.address = f"{grpc_host}:{grpc_port}"  # direccion completa entregada al cliente
        self.namenode_url = namenode_url  # url del namenode para enviar rest
        self.storage = StorageManager(storage_dir)  # gestor del disco duro local
        self.settings = config or settings
        self.grpc_server: grpc.aio.Server | None = None  # instancia del servidor grpc

    async def run(self):
        """Ciclo de vida principal del DataNode: registro y tareas en paralelo."""
        await self.register_with_namenode()  # paso obligatorio antes de empezar a operar
        await asyncio.gather(
            self.start_grpc_server(),  # servidor de bloques para clientes y otros datanodes
            self.heartbeat_loop(),  # bucle de latidos y recepcion de comandos del namenode
        )

    # -------------------------------------------------------------------------
    # Comunicación REST con el NameNode
    # -------------------------------------------------------------------------

    async def register_with_namenode(self):
        """Envía el registro inicial HTTP POST /v1/nodes/register al NameNode."""
        total_capacity, used_capacity = self.storage.get_disk_usage()  # lee capacidad real
        payload = {
            "node_id": self.node_id,
            "address": self.address,
            "capacity_bytes": total_capacity,
            "used_bytes": used_capacity,
        }
        # aqui se hara el request http usando httpx:
        # async with httpx.AsyncClient() as client:
        #     await client.post(f"{self.namenode_url}/v1/nodes/register", json=payload)
        print(f"[DataNode {self.node_id}] Registro pendiente de envío: {payload}")

    async def heartbeat_loop(self):
        """Envía latidos periódicos al NameNode y procesa comandos recibidos."""
        while True:
            try:
                # aqui se envia el latido http post:
                # async with httpx.AsyncClient() as client:
                #     res = await client.post(f"{self.namenode_url}/v1/nodes/heartbeat", json={"node_id": self.node_id})
                #     commands = res.json().get("commands", [])
                #     await self.handle_commands(commands)
                await asyncio.sleep(DEFAULT_HEARTBEAT_INTERVAL)  # espera antes del siguiente latido
            except Exception as e:  # noqa: BLE001 - el bucle de latidos nunca debe morir
                print(f"[DataNode {self.node_id}] Error enviando heartbeat: {e}")
                await asyncio.sleep(DEFAULT_HEARTBEAT_INTERVAL)

    async def handle_commands(self, commands: list[dict]):
        """Ejecuta las órdenes que el NameNode devuelve en el heartbeat.

        Shape of each command: API.md section 7 (``cmd``, ``block_id``,
        ``targets`` with ``internal_addr``, ``token``).
        """
        for cmd in commands:
            action = cmd.get("cmd")  # "replicate" | "delete"
            block_id = cmd.get("block_id")
            if action == "replicate":  # orden de transferir copia a otros datanodes
                for target in cmd.get("targets", []):  # un bloque puede ir a varios destinos
                    await self.replicate_block_to_node(
                        block_id, target.get("internal_addr"), token=cmd.get("token")
                    )
            elif action == "delete":  # orden del recolector de basura
                self.storage.delete_block(block_id)
            else:
                print(f"[DataNode {self.node_id}] Comando desconocido ignorado: {action}")

    # -------------------------------------------------------------------------
    # Servidor gRPC de Bloques y Pipeline
    # -------------------------------------------------------------------------

    async def start_grpc_server(self):
        """Inicia el servidor gRPC asíncrono para atender WriteBlock y ReadBlock."""
        self.grpc_server = grpc.aio.server()  # crea el servidor grpc asincrono
        pb2_grpc.add_BlockServiceServicer_to_server(
            BlockServiceImpl(self), self.grpc_server
        )  # registra el servicio en el servidor
        # TODO(security): TLS en lugar de puerto inseguro (API.md seccion 2.3)
        self.grpc_server.add_insecure_port(f"{self.grpc_host}:{self.grpc_port}")  # abre el puerto tcp

        await self.grpc_server.start()  # inicia la escucha en segundo plano
        print(
            f"[DataNode {self.node_id}] Servidor gRPC activo en {self.address} "
            f"(Almacenamiento: {self.storage.storage_dir})"
        )
        await self.grpc_server.wait_for_termination()  # mantiene vivo el servidor

    async def replicate_block_to_node(
        self, block_id: str, target_address: str, token: str | None = None
    ):
        """Envía una copia de un bloque local hacia otro DataNode por gRPC.

        Esto es el lado cliente de un comando ``replicate``: el NameNode nunca
        mueve bytes, solo le pide al nodo origen que lo haga.
        """
        size, checksum = self.storage.get_block_info(block_id)

        print(f"[DataNode {self.node_id}] Replicando {block_id} ({size} bytes) -> {target_address}...")

        # TODO(security): replace the insecure channel with TLS (API.md 2.3)
        async with grpc.aio.insecure_channel(target_address) as channel:  # conecta con el otro datanode
            stub = pb2_grpc.BlockServiceStub(channel)  # crea el stub cliente

            async def generate_requests():
                # header inicial sin mas nodos en el pipeline: es una copia dirigida
                yield pb2.WriteBlockRequest(
                    header=pb2.WriteBlockHeader(
                        block_id=block_id, size=size, checksum_sha256=checksum, pipeline=[]
                    )
                )
                with open(  # noqa: ASYNC230
                    self.storage.get_block_path(block_id), "rb"
                ) as block_file:
                    while chunk := block_file.read(self.settings.block_chunk_size):
                        yield pb2.WriteBlockRequest(chunk=chunk)

            metadata = (("x-block-token", token),) if token is not None else ()
            resp = await stub.WriteBlock(generate_requests(), metadata=metadata)
            print(
                f"[DataNode {self.node_id}] Réplica de {block_id} en {target_address} "
                f"confirmada: SHA {resp.checksum_sha256[:8]}..."
            )
            return resp
