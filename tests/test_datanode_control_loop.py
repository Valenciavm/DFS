import asyncio
import httpx
import pytest

from server.common.config import Settings
from server.nodes.datanode.core import DataNode
from server.nodes.namenode.api.app import create_app
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.store.memory import InMemoryMetadataStore


@pytest.fixture
def namenode_and_app():
    settings = Settings(
        jwt_secret="test_jwt_secret",
        block_token_secret="test_token_secret",
        node_key="test_node_key",
        heartbeat_interval_s=2,
        block_report_interval_s=5,
    )
    store = InMemoryMetadataStore()
    nn = Namenode(store=store, settings=settings)
    app = create_app(nn)
    return nn, app, settings


@pytest.mark.asyncio
async def test_datanode_registration_and_heartbeat(tmp_path, namenode_and_app):
    nn, app, settings = namenode_and_app

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        dn = DataNode(
            node_id="dn1",
            grpc_host="127.0.0.1",
            grpc_port=9001,
            internal_addr="127.0.0.1:9001",
            public_addr="node1.public.dfs:9001",
            storage_dir=str(tmp_path),
            namenode_urls=["http://testserver"],
            config=settings,
            http_client=client,
        )

        # 1. Registro
        reg_res = await dn.register_with_namenode()
        assert reg_res["status"] == "registered"
        assert dn.is_registered is True
        assert dn.heartbeat_interval_s == 2
        assert dn.block_report_interval_s == 5

        # Verificar que el NameNode lo conoce en su estado
        assert "dn1" in nn.cluster.datanodes
        node_info = nn.cluster.datanodes["dn1"]
        assert node_info.internal_addr == "127.0.0.1:9001"
        assert node_info.public_addr == "node1.public.dfs:9001"

        # 2. Heartbeat con recepción de comando delete
        dn.storage.write_block("blk_junk", b"junk data")
        assert dn.storage.count_blocks() == 1

        # El NameNode encola una orden de borrado para dn1
        from server.nodes.namenode.cluster_state import Command
        nn.cluster.pending_commands.setdefault("dn1", []).append(Command(cmd="delete", block_id="blk_junk"))

        # El DataNode envía un latido
        hb_task = asyncio.create_task(dn.heartbeat_loop())
        await asyncio.sleep(0.1)
        dn._stop_event.set()
        await hb_task

        # El bloque debe haber sido eliminado por el comando delete
        assert dn.storage.count_blocks() == 0


@pytest.mark.asyncio
async def test_datanode_block_report_and_corrupt(tmp_path, namenode_and_app):
    nn, app, settings = namenode_and_app

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        dn = DataNode(
            node_id="dn2",
            storage_dir=str(tmp_path),
            namenode_urls=["http://testserver"],
            config=settings,
            http_client=client,
        )

        await dn.register_with_namenode()

        # Registrar el archivo y bloque en el NameNode para que no sea considerado huérfano
        from server.nodes.namenode.models import BlockMetadata, FileMetadata
        block_meta = BlockMetadata(block_id="blk_test", index=0, size_bytes=18, checksum=None)
        file_meta = FileMetadata(
            file_id="f1",
            name="test.txt",
            owner="admin",
            size_bytes=18,
            block_size=64 * 1024 * 1024,
            replication=1,
            blocks=[block_meta],
        )
        nn.store.create_pending_file("/users/test.txt", file_meta)
        nn.store.commit_file("f1", now=1.0)

        # Escribir el bloque en disco del DataNode
        checksum = dn.storage.write_block("blk_test", b"content for report")

        br_res = await dn.send_block_report()
        assert br_res["status"] == "ok"

        # El NameNode debe haber registrado la réplica
        assert "dn2" in nn.cluster.replicas.get("blk_test", set())

        # Notificar bloque corrupto
        await dn.notify_block_corrupt("blk_test")
        # El archivo local debe haber sido borrado
        assert dn.storage.count_blocks() == 0
        # El NameNode ya no debe contar esa réplica
        assert "dn2" not in nn.cluster.replicas.get("blk_test", set())


@pytest.mark.asyncio
async def test_datanode_fencing_ignores_lower_term(tmp_path, namenode_and_app):
    nn, app, settings = namenode_and_app

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        dn = DataNode(
            node_id="dn3",
            storage_dir=str(tmp_path),
            namenode_urls=["http://testserver"],
            config=settings,
            http_client=client,
        )

        await dn.register_with_namenode()
        dn.storage.write_block("blk_survive", b"must not be deleted")

        # Simular que el DataNode ya conoció un term más alto (ej: term 10)
        dn.current_term = 10

        # El NameNode responde con term 1 y un comando delete
        from server.nodes.namenode.cluster_state import Command
        nn.cluster.pending_commands.setdefault("dn3", []).append(Command(cmd="delete", block_id="blk_survive"))

        # Ejecutamos una petición de heartbeat
        res = await dn._post("/v1/nodes/heartbeat", {"node_id": "dn3", "free_bytes": 1000, "used_bytes": 100})
        # Fencing: los comandos de un term menor se eliminan/ignoran
        assert res.get("commands") == []

        # El archivo NO debe haber sido borrado
        assert dn.storage.count_blocks() == 1


@pytest.mark.asyncio
async def test_datanode_reregister_on_409(tmp_path, namenode_and_app):
    nn, app, settings = namenode_and_app

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        dn = DataNode(
            node_id="dn_ghost",
            storage_dir=str(tmp_path),
            namenode_urls=["http://testserver"],
            config=settings,
            http_client=client,
        )

        # No nos registramos inicialmente. Enviamos directamente un heartbeat (NameNode responderá 409 NODE_NOT_REGISTERED)
        assert dn.is_registered is False

        res = await dn._post("/v1/nodes/heartbeat", {"node_id": "dn_ghost", "free_bytes": 1000, "used_bytes": 100})
        # Al recibir 409, el cliente intercepta, llama a register_with_namenode() y reintenta
        assert dn.is_registered is True
        assert "dn_ghost" in nn.cluster.datanodes
        assert "commands" in res


@pytest.mark.asyncio
async def test_write_block_triggers_block_received(tmp_path, namenode_and_app):
    import hashlib
    import socket
    import grpc
    from proto import block_service_pb2 as pb2
    from proto import block_service_pb2_grpc as pb2_grpc
    from server.common.tokens import BlockOp, mint

    nn, app, settings = namenode_and_app

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        free_port = s.getsockname()[1]

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver") as client:
        dn = DataNode(
            node_id="dn_grpc",
            grpc_host="127.0.0.1",
            grpc_port=free_port,
            storage_dir=str(tmp_path),
            namenode_urls=["http://testserver"],
            config=settings,
            http_client=client,
        )

        await dn.register_with_namenode()

        # Iniciar servidor gRPC en segundo plano
        dn.grpc_server = grpc.aio.server()
        from server.nodes.datanode.block_service import BlockServiceImpl, BlockTokenInterceptor
        dn.grpc_server = grpc.aio.server(interceptors=[BlockTokenInterceptor(settings.block_token_secret)])
        pb2_grpc.add_BlockServiceServicer_to_server(BlockServiceImpl(dn), dn.grpc_server)
        dn.grpc_server.add_insecure_port(f"127.0.0.1:{free_port}")
        await dn.grpc_server.start()

        try:
            payload = b"Testing block reception notification end to end"
            checksum = hashlib.sha256(payload).hexdigest()
            block_id = "blk_grpc_001"

            # El NameNode debe tener registrado el bloque en un archivo para aceptarlo (si no, lo considera huérfano)
            from server.nodes.namenode.models import BlockMetadata, FileMetadata
            block_meta = BlockMetadata(block_id=block_id, index=0, size_bytes=len(payload), checksum=checksum)
            file_meta = FileMetadata(
                file_id="f_grpc",
                name="test_grpc.txt",
                owner="test_user",
                size_bytes=len(payload),
                block_size=64 * 1024 * 1024,
                replication=1,
                blocks=[block_meta],
            )
            nn.store.create_pending_file("/users/test_grpc.txt", file_meta)

            token = mint(block_id, BlockOp.WRITE, "test_user", settings.block_token_secret, ttl_s=60)

            async def generate_requests():
                yield pb2.WriteBlockRequest(
                    header=pb2.WriteBlockHeader(
                        block_id=block_id,
                        size=len(payload),
                        checksum_sha256=checksum,
                        pipeline=[],
                    )
                )
                yield pb2.WriteBlockRequest(chunk=payload)

            async with grpc.aio.insecure_channel(f"127.0.0.1:{free_port}") as channel:
                stub = pb2_grpc.BlockServiceStub(channel)
                res = await stub.WriteBlock(generate_requests(), metadata=(("x-block-token", token),))
                assert res.block_id == block_id
                assert res.size == len(payload)

            # Esperar brevemente a que la tarea asíncrona de notificación complete
            for _ in range(20):
                if "dn_grpc" in nn.cluster.replicas.get(block_id, set()):
                    break
                await asyncio.sleep(0.05)

            # Verificar que el NameNode recibió y registró la réplica de dn_grpc
            assert "dn_grpc" in nn.cluster.replicas.get(block_id, set())
            assert nn.cluster.replica_checksums[block_id]["dn_grpc"] == checksum
        finally:
            await dn.grpc_server.stop(grace=0.5)

