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
