"""DataNode lifecycle: block server plus the control channel to the NameNode."""
from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import grpc
import httpx

from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from server.common.config import Settings, settings
from server.nodes.datanode.block_service import BlockServiceImpl, BlockTokenInterceptor
from server.nodes.datanode.storage import StorageManager

DEFAULT_NAMENODE_URLS = os.getenv("NAMENODE_URLS", os.getenv("NAMENODE_URL", "http://localhost:8000"))
DEFAULT_HEARTBEAT_INTERVAL = 5
DEFAULT_BLOCK_REPORT_INTERVAL = 60


class DataNode:
    """Nodo de datos responsable de almacenar bloques y responder al NameNode."""

    def __init__(
        self,
        node_id: str,
        grpc_host: str = "127.0.0.1",
        grpc_port: int = 9001,
        internal_addr: str | None = None,
        public_addr: str | None = None,
        storage_dir: str = "./data/blocks",
        namenode_urls: list[str] | str = DEFAULT_NAMENODE_URLS,
        config: Settings | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.node_id = node_id
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self.internal_addr = internal_addr or f"{grpc_host}:{grpc_port}"
        self.public_addr = public_addr or self.internal_addr
        self.address = self.internal_addr

        if isinstance(namenode_urls, str):
            self.namenode_urls = [u.strip().rstrip("/") for u in namenode_urls.split(",") if u.strip()]
        else:
            self.namenode_urls = [u.rstrip("/") for u in namenode_urls if u]
        if not self.namenode_urls:
            self.namenode_urls = ["http://localhost:8000"]

        self._current_nn_index = 0
        self.storage = StorageManager(storage_dir)
        self.settings = config or settings
        self.grpc_server: grpc.aio.Server | None = None

        # Term fencing
        self.current_term: int = 0

        # Dynamic intervals updated on registration
        self.heartbeat_interval_s: int = self.settings.heartbeat_interval_s or DEFAULT_HEARTBEAT_INTERVAL
        self.block_report_interval_s: int = self.settings.block_report_interval_s or DEFAULT_BLOCK_REPORT_INTERVAL

        # Custom or managed httpx client
        self._custom_client = http_client is not None
        self._http_client: httpx.AsyncClient = http_client or httpx.AsyncClient(timeout=10.0)

        # Control and lifecycle flags
        self.is_registered: bool = False
        self._stop_event = asyncio.Event()
        self._full_report_event = asyncio.Event()

        # Outbox for unacknowledged block receipts
        self._pending_block_receipts: asyncio.Queue[tuple[str, int, str]] = asyncio.Queue()

    @property
    def current_namenode_url(self) -> str:
        return self.namenode_urls[self._current_nn_index % len(self.namenode_urls)]

    def _rotate_namenode(self, preferred_url: str | None = None) -> None:
        """Switch active NameNode URL on failure or 503 NOT_LEADER."""
        if preferred_url:
            clean = preferred_url.rstrip("/")
            if clean in self.namenode_urls:
                self._current_nn_index = self.namenode_urls.index(clean)
                return
            # If new URL learned, add it
            self.namenode_urls.insert(0, clean)
            self._current_nn_index = 0
            return
        self._current_nn_index = (self._current_nn_index + 1) % len(self.namenode_urls)

    def _get_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "X-Request-Id": str(uuid.uuid4()),
        }
        if self.settings.node_key:
            headers["X-Node-Key"] = self.settings.node_key
        return headers

    async def _post(self, path: str, json_data: dict[str, Any], *, max_retries: int = 3) -> dict[str, Any]:
        """Send POST request to active NameNode with failover and term inspection."""
        attempt = 0
        last_exc: Exception | None = None

        while attempt < max_retries and not self._stop_event.is_set():
            attempt += 1
            url = f"{self.current_namenode_url}{path}"
            headers = self._get_headers()

            try:
                resp = await self._http_client.post(url, json=json_data, headers=headers)

                # 503 NOT_LEADER: extract leader_url or rotate
                if resp.status_code == 503:
                    try:
                        err_body = resp.json()
                        leader_url = err_body.get("leader_url")
                    except Exception:
                        leader_url = None
                    self._rotate_namenode(leader_url)
                    continue

                # 409 NODE_NOT_REGISTERED: trigger re-registration and retry
                if resp.status_code == 409 and path != "/v1/nodes/register":
                    self.is_registered = False
                    await self.register_with_namenode()
                    self._full_report_event.set()
                    continue

                resp.raise_for_status()
                data = resp.json()

                # Process term if present in response
                term = data.get("term")
                if term is not None:
                    if term > self.current_term:
                        self.current_term = term
                        self._full_report_event.set()
                    elif term < self.current_term:
                        # Ignore stale commands (fencing)
                        if "commands" in data:
                            data["commands"] = []

                return data
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_exc = exc
                self._rotate_namenode()
                await asyncio.sleep(min(0.5 * (2 ** (attempt - 1)), 2.0))

        if last_exc:
            raise last_exc
        return {}

    async def run(self):
        """Ciclo de vida principal del DataNode: registro y tareas en paralelo."""
        try:
            await self.register_with_namenode()
            await self.send_block_report()

            await asyncio.gather(
                self.start_grpc_server(),
                self.heartbeat_loop(),
                self.block_report_loop(),
                self.outbox_flush_loop(),
            )
        finally:
            await self.close()

    async def close(self):
        """Clean shutdown of background activities and servers."""
        self._stop_event.set()
        if self.grpc_server is not None:
            await self.grpc_server.stop(grace=1.0)
        if not self._custom_client:
            await self._http_client.aclose()

    # -------------------------------------------------------------------------
    # Comunicación REST con el NameNode
    # -------------------------------------------------------------------------

    async def register_with_namenode(self) -> dict[str, Any]:
        """Envía el registro inicial HTTP POST /v1/nodes/register al NameNode."""
        total_capacity, used_capacity = self.storage.get_disk_usage()
        free_bytes = max(0, total_capacity - used_capacity)
        payload = {
            "node_id": self.node_id,
            "internal_addr": self.internal_addr,
            "public_addr": self.public_addr,
            "capacity_bytes": total_capacity,
            "free_bytes": free_bytes,
        }
        res = await self._post("/v1/nodes/register", payload)
        self.is_registered = True
        if "heartbeat_interval_s" in res:
            self.heartbeat_interval_s = int(res["heartbeat_interval_s"])
        if "block_report_interval_s" in res:
            self.block_report_interval_s = int(res["block_report_interval_s"])
        return res

    async def heartbeat_loop(self):
        """Envía latidos periódicos al NameNode y procesa comandos recibidos."""
        while not self._stop_event.is_set():
            try:
                total_capacity, used_capacity = self.storage.get_disk_usage()
                free_bytes = max(0, total_capacity - used_capacity)
                payload = {
                    "node_id": self.node_id,
                    "free_bytes": free_bytes,
                    "used_bytes": used_capacity,
                }
                res = await self._post("/v1/nodes/heartbeat", payload)
                commands = res.get("commands", [])
                if commands:
                    await self.handle_commands(commands)
            except Exception as e:  # noqa: BLE001
                print(f"[DataNode {self.node_id}] Error enviando heartbeat: {e}")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.heartbeat_interval_s)
                break
            except asyncio.TimeoutError:
                pass

    async def send_block_report(self) -> dict[str, Any]:
        """Escanea bloques locales y envía un informe completo al NameNode."""
        blocks = self.storage.scan_blocks()
        payload = {
            "node_id": self.node_id,
            "blocks": blocks,
        }
        res = await self._post("/v1/nodes/block-report", payload)
        commands = res.get("commands", [])
        if commands:
            await self.handle_commands(commands)
        return res

    async def block_report_loop(self):
        """Bucle periódico de reporte de bloques o activado ante cambio de término."""
        while not self._stop_event.is_set():
            try:
                # Wait either for report interval or immediate trigger event
                trigger_task = asyncio.create_task(self._full_report_event.wait())
                stop_task = asyncio.create_task(self._stop_event.wait())
                done, pending = await asyncio.wait(
                    (trigger_task, stop_task),
                    timeout=self.block_report_interval_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()

                if self._stop_event.is_set():
                    break

                if self._full_report_event.is_set():
                    self._full_report_event.clear()

                await self.send_block_report()
            except Exception as e:  # noqa: BLE001
                print(f"[DataNode {self.node_id}] Error enviando block report: {e}")

    async def notify_block_received(self, block_id: str, size: int, checksum: str) -> None:
        """Notifica al NameNode que un bloque ha sido completado y verificado."""
        payload = {
            "node_id": self.node_id,
            "block_id": block_id,
            "size": size,
            "checksum": checksum,
        }
        try:
            await self._post("/v1/nodes/block-received", payload, max_retries=2)
        except Exception:
            # Encolar para reintentar en outbox_flush_loop
            await self._pending_block_receipts.put((block_id, size, checksum))

    async def notify_block_corrupt(self, block_id: str) -> None:
        """Elimina la copia local corrupta y notifica al NameNode."""
        self.storage.delete_block(block_id)
        payload = {
            "node_id": self.node_id,
            "block_id": block_id,
        }
        try:
            await self._post("/v1/nodes/block-corrupt", payload, max_retries=2)
        except Exception as e:
            print(f"[DataNode {self.node_id}] Error notificando bloque corrupto {block_id}: {e}")

    async def outbox_flush_loop(self):
        """Reintenta notificaciones de block-received pendientes en caso de fallos transitorios."""
        while not self._stop_event.is_set():
            try:
                block_id, size, checksum = await asyncio.wait_for(
                    self._pending_block_receipts.get(), timeout=2.0
                )
                payload = {
                    "node_id": self.node_id,
                    "block_id": block_id,
                    "size": size,
                    "checksum": checksum,
                }
                try:
                    await self._post("/v1/nodes/block-received", payload, max_retries=1)
                except Exception:
                    # Si falla, volver a encolar con breve retardo
                    await asyncio.sleep(1.0)
                    await self._pending_block_receipts.put((block_id, size, checksum))
                finally:
                    self._pending_block_receipts.task_done()
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass

    async def handle_commands(self, commands: list[dict]):
        """Ejecuta las órdenes que el NameNode devuelve en el heartbeat."""
        for cmd in commands:
            command_term = cmd.get("term", self.current_term)
            if command_term < self.current_term:
                continue
            if command_term > self.current_term:
                self.current_term = command_term
                self._full_report_event.set()
            action = cmd.get("cmd")  # "replicate" | "delete"
            block_id = cmd.get("block_id")
            if not block_id:
                continue

            if action == "replicate":
                targets = cmd.get("targets", [])
                token = cmd.get("token")
                for target in targets:
                    target_addr = target if isinstance(target, str) else target.get("internal_addr")
                    if target_addr:
                        asyncio.create_task(
                            self.replicate_block_to_node(block_id, target_addr, token=token)
                        )
            elif action == "delete":
                self.storage.delete_block(block_id)
            else:
                print(f"[DataNode {self.node_id}] Comando desconocido ignorado: {action}")

    # -------------------------------------------------------------------------
    # Servidor gRPC de Bloques y Pipeline
    # -------------------------------------------------------------------------

    async def start_grpc_server(self):
        """Inicia el servidor gRPC asíncrono para atender WriteBlock y ReadBlock."""
        self.grpc_server = grpc.aio.server(
            interceptors=[BlockTokenInterceptor(self.settings.block_token_secret)]
        )
        pb2_grpc.add_BlockServiceServicer_to_server(
            BlockServiceImpl(self), self.grpc_server
        )
        # TODO(security): TLS en lugar de puerto inseguro (API.md seccion 2.3)
        self.grpc_server.add_insecure_port(f"{self.grpc_host}:{self.grpc_port}")

        await self.grpc_server.start()
        print(
            f"[DataNode {self.node_id}] Servidor gRPC activo en {self.address} "
            f"(Almacenamiento: {self.storage.storage_dir})"
        )
        await self.grpc_server.wait_for_termination()

    async def replicate_block_to_node(
        self, block_id: str, target_address: str, token: str | None = None
    ):
        """Envía una copia de un bloque local hacia otro DataNode por gRPC."""
        size, checksum = self.storage.get_block_info(block_id)

        print(f"[DataNode {self.node_id}] Replicando {block_id} ({size} bytes) -> {target_address}...")

        # TODO(security): replace the insecure channel with TLS (API.md 2.3)
        async with grpc.aio.insecure_channel(target_address) as channel:
            stub = pb2_grpc.BlockServiceStub(channel)

            async def generate_requests():
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
