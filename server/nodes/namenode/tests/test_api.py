"""Integration tests for the NameNode REST API."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from server.nodes.namenode.api.app import create_app
from server.nodes.namenode.core import Namenode


@pytest.fixture
def client(nn: Namenode) -> TestClient:
    app = create_app(nn)
    return TestClient(app)


class TestAuthAndAdminEndpoints:
    def test_health_endpoint_requires_no_auth(self, client: TestClient):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "leader"
        assert "status" in data

    def test_ha_status_endpoint(self, client: TestClient):
        response = client.get("/v1/ha/status")
        assert response.status_code == 200
        data = response.json()
        assert data["role"] == "leader"
        assert data["leader_id"] == "nn1"
        assert data["node_id"] == "nn1"
        assert data["term"] == 1
        assert data["degraded"] is False

    def test_standby_rejects_service_requests_with_leader_url(
        self, client: TestClient, nn: Namenode
    ):
        nn.database = object()
        nn._ha_enabled = True
        nn._is_leader = False
        nn.term = 4
        nn.leader_id = "nn1"
        nn.leader_url = "http://nn1:8000"

        health_response = client.get("/health")
        rejected = client.get("/v1/fs/ls", params={"path": "/users/ana"})

        assert health_response.json()["role"] == "standby"
        assert health_response.json()["status"] == "not_leader"
        assert rejected.status_code == 503
        assert rejected.json()["error"]["code"] == "NOT_LEADER"
        assert rejected.json()["leader_url"] == "http://nn1:8000"

    def test_degraded_leader_keeps_reads_but_rejects_metadata_writes(
        self, client: TestClient, nn: Namenode
    ):
        login = client.post(
            "/v1/auth/login",
            json={"username": "ana", "password": "test-password"},
        )
        token = login.json()["access_token"]
        nn.database = object()
        nn._ha_enabled = True
        nn._is_leader = True
        nn._degraded = True
        nn.leader_url = "http://nn1:8000"
        nn.store.mkdir("/users/ana", "ana", nn.clock.time(), parents=True)

        rejected = client.post(
            "/v1/fs/mkdir",
            json={"path": "/users/ana/new"},
        )
        read_response = client.get(
            "/v1/fs/ls",
            params={"path": "/users/ana"},
            headers={"Authorization": f"Bearer {token}"},
        )

        assert client.get("/health").json()["status"] == "degraded"
        assert read_response.status_code == 200
        assert rejected.status_code == 503
        assert rejected.json()["error"]["code"] == "NOT_LEADER"
        assert rejected.json()["degraded"] is True

    def test_register_login_and_access_protected_endpoint(self, client: TestClient):
        # Register user
        res_reg = client.post("/v1/auth/register", json={"username": "carlos", "password": "password1234"})
        assert res_reg.status_code == 201
        assert res_reg.json() == {"username": "carlos"}

        # Login
        res_login = client.post("/v1/auth/login", json={"username": "carlos", "password": "password1234"})
        assert res_login.status_code == 200
        token = res_login.json()["access_token"]
        assert token is not None

        # Check request ID middleware
        assert "x-request-id" in res_login.headers

        # Create API Key
        headers = {"Authorization": f"Bearer {token}"}
        res_key = client.post("/v1/auth/apikeys", json={"name": "test-key"}, headers=headers)
        assert res_key.status_code == 201
        api_key = res_key.json()["api_key"]
        assert api_key.startswith("dfsha_")

        # Test authenticated with API Key
        res_list_keys = client.get("/v1/auth/apikeys", headers={"Authorization": f"ApiKey {api_key}"})
        assert res_list_keys.status_code == 200
        assert len(res_list_keys.json()) == 1


class TestFilesystemAndLocks:
    def test_mkdir_stat_and_locks(self, client: TestClient, nn: Namenode):
        # Register user
        client.post("/v1/auth/register", json={"username": "beatriz", "password": "password1234"})
        login = client.post("/v1/auth/login", json={"username": "beatriz", "password": "password1234"}).json()
        headers = {"Authorization": f"Bearer {login['access_token']}"}

        # Mkdir
        res_mkdir = client.post("/v1/fs/mkdir", json={"path": "/users/beatriz/docs"}, headers=headers)
        assert res_mkdir.status_code == 201

        # Stat
        res_stat = client.get("/v1/fs/stat?path=/users/beatriz/docs", headers=headers)
        assert res_stat.status_code == 200
        assert res_stat.json()["type"] == "directory"

        # Lock
        res_lock = client.post("/v1/fs/lock", json={"path": "/users/beatriz/docs", "type": "exclusive"}, headers=headers)
        assert res_lock.status_code == 200
        lock_id = res_lock.json()["lock_id"]

        # Unlock
        res_unlock = client.delete(f"/v1/fs/lock/{lock_id}", headers=headers)
        assert res_unlock.status_code == 200


class TestDataNodesAndFiles:
    def test_datanode_lifecycle_and_file_put(self, client: TestClient, nn: Namenode):
        # Register DataNodes
        for i in range(1, 4):
            res_reg = client.post(
                "/v1/nodes/register",
                json={
                    "node_id": f"dn{i}",
                    "internal_addr": f"dn{i}:9000",
                    "public_addr": f"localhost:900{i}",
                    "capacity_bytes": 10_000_000_000,
                    "free_bytes": 10_000_000_000,
                },
            )
            assert res_reg.status_code == 200
            assert res_reg.json()["term"] == nn.term

        # Heartbeat
        res_hb = client.post(
            "/v1/nodes/heartbeat",
            json={"node_id": "dn1", "free_bytes": 9_000_000_000, "used_bytes": 1_000_000_000},
        )
        assert res_hb.status_code == 200
        assert res_hb.json()["term"] == nn.term

        # User registers & prepares upload
        client.post("/v1/auth/register", json={"username": "uploader", "password": "password1234"})
        login = client.post("/v1/auth/login", json={"username": "uploader", "password": "password1234"}).json()
        headers = {"Authorization": f"Bearer {login['access_token']}"}

        # Put file (1000 bytes)
        res_put = client.post(
            "/v1/files/put",
            json={"path": "/users/uploader/test.txt", "size": 1000},
            headers=headers,
        )
        assert res_put.status_code == 201
        put_data = res_put.json()
        assert len(put_data["blocks"]) == 1
        block_id = put_data["blocks"][0]["block_id"]

        # DataNodes confirm block receipt
        fake_checksum = "a" * 64
        for i in range(1, 4):
            res_br = client.post(
                "/v1/nodes/block-received",
                json={"node_id": f"dn{i}", "block_id": block_id, "size": 1000, "checksum": fake_checksum},
            )
            assert res_br.status_code == 200
            assert res_br.json()["term"] == nn.term

        # Commit file
        res_commit = client.post(
            f"/v1/files/{put_data['file_id']}/commit",
            json={
                "blocks": [{"index": 0, "block_id": block_id, "size": 1000, "checksum": fake_checksum}],
                "file_checksum": fake_checksum,
            },
            headers=headers,
        )
        assert res_commit.status_code == 200
        assert res_commit.json()["status"] == "committed"

        # Get file metadata
        res_get = client.get("/v1/files/get?path=/users/uploader/test.txt", headers=headers)
        assert res_get.status_code == 200
        assert len(res_get.json()["blocks"]) == 1
        assert res_get.json()["checksum"] == fake_checksum

    def test_group_endpoints(self, client: TestClient):
        client.post("/v1/auth/register", json={"username": "group-owner", "password": "password1234"})
        owner_token = client.post(
            "/v1/auth/login",
            json={"username": "group-owner", "password": "password1234"},
        ).json()["access_token"]
        owner_headers = {"Authorization": f"Bearer {owner_token}"}

        created = client.post(
            "/v1/groups",
            json={"name": "project-team"},
            headers=owner_headers,
        )
        assert created.status_code == 201
        assert created.json()["members"] == ["group-owner"]

        client.post("/v1/auth/register", json={"username": "member", "password": "password1234"})
        updated = client.put(
            "/v1/groups/project-team/members",
            json={"add": ["member"]},
            headers=owner_headers,
        )
        assert updated.status_code == 200
        assert updated.json()["members"] == ["group-owner", "member"]

        fetched = client.get("/v1/groups/project-team", headers=owner_headers)
        assert fetched.status_code == 200
        assert fetched.json() == updated.json()
