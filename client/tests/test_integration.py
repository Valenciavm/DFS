"""Integration tests: DFShaClient with NameNode API and DataNode gRPC."""
from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
import pytest
import uvicorn
import grpc

from client import DFShaClient
from proto import block_service_pb2 as pb2
from proto import block_service_pb2_grpc as pb2_grpc
from server.common.config import Settings
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.api.app import create_app
from server.nodes.datanode.core import DataNode


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def cluster_env(tmp_path):
    token_secret = "test-cluster-token-secret-32bytes"
    jwt_secret = "test-jwt-secret-at-least-32-bytes"

    nn_settings = Settings(
        block_token_secret=token_secret,
        jwt_secret=jwt_secret,
        block_size=1024 * 1024,
        replication_factor=1,
        min_replicas_commit=1,
        admin_user="admin",
        admin_password="AdminPassword123!",
    )
    nn = Namenode(settings=nn_settings)
    app = create_app(nn)

    # Start NameNode FastAPI server
    nn_port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=nn_port, log_level="warning")
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=server.run, daemon=True)
    server_thread.start()

    # Start DataNode gRPC server in its own thread with dedicated event loop
    dn_grpc_port = _free_port()
    dn_settings = Settings(
        block_token_secret=token_secret,
        jwt_secret=jwt_secret,
        replication_factor=1,
        min_replicas_commit=1,
    )
    base_url = f"http://127.0.0.1:{nn_port}"
    dn_addr = f"127.0.0.1:{dn_grpc_port}"

    dn = DataNode(
        node_id="dn-test-01",
        grpc_port=dn_grpc_port,
        storage_dir=str(tmp_path / "datanode"),
        config=dn_settings,
        namenode_urls=[base_url],
    )

    dn_loop = asyncio.new_event_loop()
    def run_dn():
        asyncio.set_event_loop(dn_loop)
        dn_loop.run_until_complete(dn.start_grpc_server())

    dn_thread = threading.Thread(target=run_dn, daemon=True)
    dn_thread.start()

    client = DFShaClient(base_url, timeout=5)

    # Wait for NameNode
    deadline = time.time() + 10
    nn_ready = False
    while time.time() < deadline:
        try:
            r = client.http.session.get(f"{base_url}/health")
            if r.status_code == 200:
                nn_ready = True
                break
        except Exception:
            pass
        time.sleep(0.1)
    assert nn_ready, "NameNode did not start in time"

    # Wait for DataNode gRPC
    dn_ready = False
    while time.time() < deadline:
        try:
            with grpc.insecure_channel(dn_addr) as ch:
                stub = pb2_grpc.BlockServiceStub(ch)
                res = stub.Health(pb2.HealthRequest(), timeout=1)
                if res.status == "ok":
                    dn_ready = True
                    break
        except Exception:
            pass
        time.sleep(0.1)
    assert dn_ready, "DataNode did not start in time"

    # Register DataNode in NameNode cluster
    nn.register_datanode(
        node_id="dn-test-01",
        internal_addr=dn_addr,
        public_addr=dn_addr,
        capacity_bytes=100 * 1024 * 1024,
        used_bytes=0,
    )

    yield {
        "client": client,
        "namenode": nn,
        "datanode": dn,
        "base_url": base_url,
        "dn_addr": dn_addr,
    }

    server.should_exit = True
    if dn.grpc_server is not None:
        future = asyncio.run_coroutine_threadsafe(dn.grpc_server.stop(grace=None), dn_loop)
        try:
            future.result(timeout=3)
        except Exception:
            pass


def test_client_full_flow(cluster_env, tmp_path):
    c: DFShaClient = cluster_env["client"]

    # 1. Register & Login as normal user
    c.register("alice", "Password123!")
    res = c.login("alice", "Password123!")
    assert c.authenticated
    assert "access_token" in res

    # 2. Namespace operations inside alice's home directory (/users/alice)
    alice_work = "/users/alice/work"
    c.mkdir(alice_work, parents=True)
    stat_dir = c.stat(alice_work)
    assert stat_dir["type"] == "directory"

    # 3. Upload a file through NameNode planning and DataNode gRPC
    local_file = tmp_path / "sample.txt"
    payload = b"Hello DFSha World from Client HTTP & gRPC Block Transfer!"
    local_file.write_bytes(payload)

    remote_file = f"{alice_work}/sample.txt"
    put_res = c.put_file(str(local_file), remote_file)
    assert put_res["status"] == "committed"
    assert put_res["size"] == len(payload)

    # 4. List directory
    entries = c.ls(alice_work)
    names = [e["name"] for e in entries]
    assert "sample.txt" in names

    # 5. Download the file from DataNode and verify integrity
    downloaded_file = tmp_path / "downloaded.txt"
    get_res = c.get_file(remote_file, str(downloaded_file))
    assert downloaded_file.read_bytes() == payload
    assert get_res["size"] == len(payload)

    # 6. Lock and Unlock
    lock_res = c.lock(remote_file, mode="exclusive", ttl_s=30)
    assert "lock_id" in lock_res
    c.unlock(lock_res["lock_id"])

    # 7. ACL manipulation (granting read permission to registered user bob)
    c.register("bob", "PasswordBob123!")
    # Re-login as alice
    c.login("alice", "Password123!")
    acl_res = c.set_acl(remote_file, subject="user:bob", permission="r")
    assert acl_res.get("status") == "ok"
    acls = c.get_acl(remote_file)
    assert any(entry["subject"] == "user:bob" and entry["permission"] == "r" for entry in acls.get("entries", []))

    # 8. Remove file and directory
    c.rm(remote_file)
    c.rmdir(alice_work)
    alice_entries = [e["name"] for e in c.ls("/users/alice")]
    assert "work" not in alice_entries
