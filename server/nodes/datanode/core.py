"""DataNode lifecycle: block server plus the control channel to the NameNode."""
import os
import asyncio

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
    ):
        self.node_id = node_id  # identificador unico de este nodo (ej: dn1)
        self.grpc_host = grpc_host  # direccion ip o host en que escucha grpc
        self.grpc_port = grpc_port  # puerto de escucha para transferir bloques
        self.address = f"{grpc_host}:{grpc_port}"  # direccion completa entregada al cliente
        self.namenode_url = namenode_url  # url del namenode para enviar rest
        self.storage = StorageManager(storage_dir)  # gestor del disco duro local

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
        print(f"[DataNode {self.node_id}] Registrado en NameNode ({self.address}).")

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
            except Exception as e:
                print(f"[DataNode {self.node_id}] Error enviando heartbeat: {e}")
                await asyncio.sleep(DEFAULT_HEARTBEAT_INTERVAL)

    async def handle_commands(self, commands: list[dict]):
        """Ejecuta órdenes devueltas por el NameNode (ej. replicar bloque o borrar)."""
        for cmd in commands:
            action = cmd.get("action")
            block_id = cmd.get("block_id")
            if action == "replicate":  # orden de transferir copia a otro datanode
                target_addr = cmd.get("target_address")
                await self.replicate_block_to_node(block_id, target_addr)
            elif action == "delete":  # orden del recolector de basura
                self.storage.delete_block(block_id)

    # -------------------------------------------------------------------------
    # Servidor gRPC de Bloques y Pipeline
    # -------------------------------------------------------------------------

    async def start_grpc_server(self):
        """Levanta el servidor gRPC que atiende peticiones WriteBlock y ReadBlock."""
        # aqui se iniciara el servidor gRPC asincrono (grpc.aio.server)
        print(f"[DataNode {self.node_id}] Servidor gRPC escuchando en {self.address}...")
        while True:
            await asyncio.sleep(3600)  # mantiene vivo el servidor de bloques

    async def replicate_block_to_node(self, block_id: str, target_address: str):
        """Envía una copia de un bloque local hacia otro DataNode por gRPC."""
        data = self.storage.read_block(block_id)  # lee los bytes del disco local
        # aqui se conecta por canal gRPC al target_address y llama a WriteBlock
        print(f"[DataNode {self.node_id}] Replicando {block_id} hacia {target_address}...")
