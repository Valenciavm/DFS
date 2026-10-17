import os
import asyncio
import math
import uuid
from typing import Optional
from server.nodes.namenode.models import (
    MetadataStore,
    FileMetadata,
    BlockMetadata,
    FileStatus,
    DataNodeInfo,
)

# Configuration defaults
DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024  # 4 MB
DEFAULT_REPLICATION_FACTOR = 3
HEARTBEAT_TIMEOUT_SECONDS = 30  # DataNode considered dead after this
UPLOAD_TTL_SECONDS = 300  # 5 minutes to complete upload before cleanup


class Namenode:
    def __init__(
        self,
        block_size: int = DEFAULT_BLOCK_SIZE,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
    ):
        self._store = MetadataStore()
        self.block_size = block_size
        self.replication_factor = replication_factor
        # Replication and cleanup queues for heartbeats
        self.pending_replications: list[dict] = []  # commands for datanodes
        self.pending_deletions: list[dict] = []

    async def run(self):
        """Main lifecycle running parallel background tasks."""
        await asyncio.gather(
            self.api_server(),               # Servidor HTTP/gRPC para atender clientes y DataNodes
            self.monitor_datanodes(),        # Revisa periódicamente si algún DataNode murió
            self.monitor_replication(),      # Detecta bloques sub-replicados y programa su copia
            self.garbage_collector(),        # Limpia uploads pendientes vencidos y bloques huérfanos
        )

    # -------------------------------------------------------------------------
    # Background Tasks (async loops)
    # -------------------------------------------------------------------------

    async def api_server(self):
        """Starts and maintains the API server alive (FastAPI / gRPC)."""
        # Aquí se inicia el servidor (ej. uvicorn o grpc.aio.server)
        while True:
            await asyncio.sleep(3600)

    async def monitor_datanodes(self):
        """Busca datanodes muertos que no hayan respondido """
        while True:
            now = asyncio.get_event_loop().time()
            # Marca como caídos los nodos que no hayan enviado heartbeat
            for node_id, node in self._store.datanodes.items():
                if node.is_alive and (now - node.last_heartbeat > HEARTBEAT_TIMEOUT_SECONDS):
                    node.is_alive = False
                    print(f"[NameNode] DataNode {node_id} dead (no heartbeat).")
            await asyncio.sleep(5)

    async def monitor_replication(self):
        """Escanea bloques y programa replicas si el numero de réplicas es menor al replication factor"""
        while True:
            # Revisa bloques de archivos confirmados
            for block_id, block in self._store.blocks_by_id.items():
                alive_replicas = [
                    nid for nid in block.replicas
                    if nid in self._store.datanodes and self._store.datanodes[nid].is_alive
                ]
                if len(alive_replicas) < self.replication_factor:
                    # Encolar orden de re-replicación hacia otro DataNode vivo
                    pass
            await asyncio.sleep(10)

    async def garbage_collector(self):
        """Limpia la cache, archivos pendientes expirados y bloques huérfanos"""
        while True:
            # Limpieza de archivos PENDING expirados
            await asyncio.sleep(30)

    # -------------------------------------------------------------------------
    # Core File Metadata Operations (Called by the API server)
    # -------------------------------------------------------------------------

    def prepare_file_upload(
        self, path: str, size_bytes: int, owner: str = "default_user"
    ) -> FileMetadata:
        """
        Step 1 of saving a file:
        Validates, calculates blocks, selects DataNode pipelines, and creates a PENDING file.
        """
        if path in self._store.files_by_path:  # verifica si la ruta ya esta registrada
            existing = self._store.files_by_path[path]
            if existing.status == FileStatus.COMMITTED:  # si ya esta confirmado no permite duplicados
                raise ValueError(f"El archivo {path} ya existe en el sistema.")

        # calcula bloques: ceil redondea hacia arriba para no perder el residuo (ej. 10mb / 4mb = 2.5 -> 3 bloques), y max(1, ...) asegura minimo 1 bloque si el archivo esta vacio (0 bytes)
        num_blocks = max(1, math.ceil(size_bytes / self.block_size))
        file_id = f"file_{uuid.uuid4().hex[:12]}"  # genera id unico para el archivo
        blocks: list[BlockMetadata] = []

        live_nodes = [dn for dn in self._store.datanodes.values() if dn.is_alive]  # filtra solo datanodes vivos
        sorted_nodes = sorted(live_nodes, key=lambda dn: dn.free_bytes, reverse=True)  # ordena por mas espacio libre
        pipeline_targets = [dn.node_id for dn in sorted_nodes[: self.replication_factor]]  # elige los n mejores nodos

        remaining = size_bytes  # bytes pendientes por asignar a bloques
        for idx in range(num_blocks):
            blk_size = min(self.block_size, remaining)  # tamaño de este bloque, evalua si lo que queda por guardar se puede guardar en un bloque, por eso min
            block_id = f"blk_{uuid.uuid4().hex[:12]}_{idx:04d}"  # genera id unico para el bloque
            block = BlockMetadata(
                block_id=block_id,
                index=idx,  # posicion del bloque para rearmar el archivo en orden
                size_bytes=blk_size,  # tamaño real de este bloque en bytes
                replicas=list(pipeline_targets),  # datanodes asignados para guardar este bloque
            )
            blocks.append(block)
            self._store.blocks_by_id[block_id] = block  # indexa el bloque globalmente en memoria
            remaining -= blk_size  # descuenta los bytes ya procesados

        file_meta = FileMetadata(
            file_id=file_id,
            path=path,
            size_bytes=size_bytes,
            owner=owner,
            status=FileStatus.PENDING,  # estado inicial pendiente hasta que el cliente haga commit
            blocks=blocks,  # lista con los metadatos de cada bloque
        )

        self._store.files_by_id[file_id] = file_meta  # indexa el archivo por su id unico
        self._store.files_by_path[path] = file_meta  # indexa el archivo por su ruta canonica
        return file_meta  # retorna el plan de bloques y nodos para que el cliente empiece a subir

    def commit_file_upload(
        self, file_id: str, block_checksums: Optional[dict[str, str]] = None
    ) -> FileMetadata:
        """
        Step 3 of saving a file:
        Called when client finishes writing blocks to DataNodes. Marks file as COMMITTED.
        """
        if file_id not in self._store.files_by_id:  # valida que la reserva del archivo exista
            raise KeyError(f"File {file_id} not found.")

        file_meta = self._store.files_by_id[file_id]  # obtiene los metadatos del archivo pendiente
        if file_meta.status != FileStatus.PENDING:  # solo permite confirmar si esta en estado pendiente
            raise ValueError(f"File {file_id} is not in PENDING state.")

        if block_checksums:  # si el cliente calculo y envio los hashes sha-256 de los bloques
            for blk in file_meta.blocks:  # recorre cada bloque del archivo
                if blk.block_id in block_checksums:  # busca el hash correspondiente a este bloque
                    blk.checksum = block_checksums[blk.block_id]  # asocia el hash al bloque para verificar integridad futura

        file_meta.status = FileStatus.COMMITTED  # cambia a committed haciendo el archivo visible y disponible
        return file_meta  # retorna los metadatos finales del archivo ya confirmado

    def register_datanode(self, node_id: str, address: str, capacity_bytes: int) -> DataNodeInfo:
        """Registers or updates a DataNode in the cluster."""
        node = DataNodeInfo(node_id=node_id, address=address, capacity_bytes=capacity_bytes)  # crea la ficha del datanode con su ip y capacidad
        self._store.datanodes[node_id] = node  # guarda el datanode en el registro del namenode para tenerlo disponible
        return node  # retorna la informacion del nodo registrado

    def record_heartbeat(self, node_id: str) -> list[dict]:
        """Updates last_heartbeat and returns pending commands for this DataNode."""
        if node_id in self._store.datanodes:
            self._store.datanodes[node_id].last_heartbeat = asyncio.get_event_loop().time()
            self._store.datanodes[node_id].is_alive = True

        # Retornar comandos pendientes para este nodo (ej. replicar bloque)
        commands = [cmd for cmd in self.pending_replications if cmd.get("source") == node_id]
        # Remover comandos despachados
        self.pending_replications = [cmd for cmd in self.pending_replications if cmd not in commands]
        return commands

