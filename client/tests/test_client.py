import hashlib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import httpx

from client import DFShaClient, MFARequired
from client.http import NameNodeHTTP


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.client = DFShaClient("http://nn.test:8000")
        self.client.http.request = Mock()

    def test_mfa_login(self):
        self.client.http.request.return_value = {
            "mfa_required": True,
            "challenge": "mfa_test",
            "expires_in": 300,
        }

        with self.assertRaises(MFARequired) as error:
            self.client.login("ana", "secret")

        self.assertEqual(error.exception.challenge, "mfa_test")

    def test_relative_path(self):
        self.client.current_dir = "/projects"
        self.assertEqual(self.client.resolve_path("a.txt"), "/projects/a.txt")

    def test_ls_pagination(self):
        self.client.http.request.side_effect = [
            {"entries": [{"name": "a"}], "next_cursor": "x"},
            {"entries": [{"name": "b"}], "next_cursor": None},
        ]

        self.assertEqual(
            [entry["name"] for entry in self.client.ls("/")],
            ["a", "b"],
        )

    def test_mkdir_uses_query_parameter_for_parents(self):
        self.client.mkdir("data", parents=True)

        self.client.http.request.assert_called_once_with(
            "POST",
            "/v1/fs/mkdir",
            params={"parents": "true"},
            json={"path": "/data"},
        )

    def test_set_acl_uses_entries_contract(self):
        self.client.set_acl("/projects", "group:team", "rw")

        self.client.http.request.assert_called_once_with(
            "PUT",
            "/v1/fs/acl",
            json={
                "path": "/projects",
                "entries": [{"subject": "group:team", "permission": "rw"}],
            },
        )

    def test_invalid_acl(self):
        with self.assertRaises(ValueError):
            self.client.set_acl("/x", "user:a", "bad")

    def test_open_uses_read_manifest_and_releases_optional_lock(self):
        self.client.http.request.return_value = {
            "size": 0,
            "blocks": [],
        }
        self.client.lock = Mock(return_value={"lock_id": "lock-1"})
        self.client.unlock = Mock()

        with self.client.open("empty.txt", lock="shared") as remote_file:
            self.assertEqual(remote_file.read(), b"")

        self.client.lock.assert_called_once_with("/empty.txt", mode="shared", ttl_s=60)
        self.client.unlock.assert_called_once_with("lock-1")

    def test_open_rejects_write_modes_for_worm_files(self):
        with self.assertRaisesRegex(ValueError, "mode"):
            self.client.open("file.txt", mode="ab")

    def test_open_write_stages_file_until_close_and_supports_exclusive_lock(self):
        self.client.lock = Mock(return_value={"lock_id": "lock-1"})
        self.client.unlock = Mock()
        self.client.put_file = Mock()

        with self.client.open("file.txt", "wb", lock="exclusive") as remote_file:
            remote_file.write(b"new contents")
            self.client.put_file.assert_not_called()

        self.client.lock.assert_called_once_with(
            "/file.txt", mode="exclusive", ttl_s=60
        )
        self.client.put_file.assert_called_once()
        self.assertEqual(self.client.put_file.call_args.args[1], "/file.txt")
        self.client.unlock.assert_called_once_with("lock-1")

    def test_open_releases_lock_if_manifest_is_invalid(self):
        self.client.http.request.return_value = {"size": 5, "blocks": []}
        self.client.lock = Mock(return_value={"lock_id": "lock-1"})
        self.client.unlock = Mock()

        with self.assertRaisesRegex(OSError, "block sizes"):
            self.client.open("file.txt", lock="shared")

        self.client.unlock.assert_called_once_with("lock-1")

    def test_upload_uses_path_commit_and_checksum(self):
        payload = b"dfs"
        checksum = hashlib.sha256(payload).hexdigest()
        self.client.http.request.side_effect = [
            {
                "file_id": "f1",
                "blocks": [
                    {
                        "index": 0,
                        "block_id": "b1",
                        "size": len(payload),
                        "token": "write-token",
                        "pipeline": [{"addr": "dn1:9000"}],
                    }
                ],
            },
            {"status": "committed"},
        ]
        self.client.block_transport.write_block = Mock(
            return_value={
                "block_id": "b1",
                "size": len(payload),
                "checksum_sha256": checksum,
                "failed_downstream": [],
            }
        )

        with TemporaryDirectory() as directory:
            local_file = Path(directory) / "local.txt"
            local_file.write_bytes(payload)

            result = self.client.put_file(local_file, "/remote.txt")

        self.assertEqual(result, {"status": "committed"})
        self.assertEqual(
            self.client.http.request.call_args_list[1].args[:2],
            ("POST", "/v1/files/f1/commit"),
        )
        commit_body = self.client.http.request.call_args_list[1].kwargs["json"]
        self.assertEqual(commit_body["file_checksum"], checksum)
        self.assertEqual(commit_body["blocks"][0]["checksum"], checksum)


class NameNodeHTTPTests(unittest.TestCase):
    def test_write_redirects_once_to_reported_leader(self):
        requested_hosts = []

        def respond(request):
            requested_hosts.append(request.url.host)
            if request.url.host == "standby.test":
                return httpx.Response(
                    503,
                    json={
                        "error": {"code": "NOT_LEADER", "message": "standby"},
                        "leader_url": "http://leader.test:8000",
                    },
                )
            return httpx.Response(201, json={"created": True})

        transport = NameNodeHTTP(["http://standby.test:8000"])
        transport.session.close()
        transport.session = httpx.Client(transport=httpx.MockTransport(respond))

        try:
            result = transport.request(
                "POST",
                "/v1/fs/mkdir",
                json={"path": "/data"},
            )
        finally:
            transport.close()

        self.assertEqual(result, {"created": True})
        self.assertEqual(requested_hosts, ["standby.test", "leader.test"])
