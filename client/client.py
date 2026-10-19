"""High-level DFSha Python client, following docs/API.md."""
import hashlib, os
import tempfile
from pathlib import Path
from urllib.parse import quote
from .http import NameNodeHTTP
from .exceptions import MFARequired, IntegrityError
from .transfer import GrpcBlockTransport


class DFShaClient:
    """High-level DFSha Python client."""

    def __init__(
        self,
        namenode_urls,
        *,
        timeout=15,
        verify_tls=True,
        max_retries=2,
        block_transport=None,
        page_size=1000,
    ):
        self.http = NameNodeHTTP(
            namenode_urls,
            timeout,
            verify_tls,
            max_retries,
        )
        self.block_transport = block_transport or GrpcBlockTransport()
        self.page_size = page_size
        self.username = None
        self.current_dir = "/"
        self._mfa_challenge = None

    @property
    def authenticated(self):
        return bool(self.http.token)

    def register(self, username, password):
        return self.http.request(
            "POST",
            "/v1/auth/register",
            json={"username": username, "password": password},
            authenticated=False,
        )

    def login(self, username, password):
        response = self.http.request(
            "POST",
            "/v1/auth/login",
            json={"username": username, "password": password},
            authenticated=False,
        )

        if response.get("mfa_required"):
            self.username = username
            self._mfa_challenge = response["challenge"]
            raise MFARequired(
                response["challenge"],
                response.get("expires_in"),
            )

        self.username = username
        self._set_auth(response)
        return response

    def login_2fa(self, challenge, code):
        response = self.http.request(
            "POST",
            "/v1/auth/login/2fa",
            json={"challenge": challenge, "code": code},
            authenticated=False,
        )

        self._mfa_challenge = None
        self._set_auth(response)
        return response

    def login_pending_2fa(self, code):
        if not self._mfa_challenge:
            raise ValueError(
                "No pending MFA challenge; call login() first"
            )

        return self.login_2fa(self._mfa_challenge, code)

    def _set_auth(self, response):
        if not response.get("access_token"):
            raise ValueError(
                "NameNode response missing access_token"
            )

        self.http.set_token(
            response["access_token"],
            response.get("token_type", "Bearer"),
        )

        if self.username:
            self.current_dir = f"/users/{self.username}"

    def use_api_key(self, key):
        if not key or not key.startswith("dfsha_live_"):
            raise ValueError(
                "API key must begin with 'dfsha_live_'"
            )

        self.http.set_token(key)
        return self

    def logout(self):
        self.http.token = None
        self.username = None
        self._mfa_challenge = None
        self.current_dir = "/"

    def resolve_path(self, path):
        """Resolve a user-supplied path into a normalized DFSha path."""
        if not isinstance(path, str) or not path.strip():
            raise ValueError("Path cannot be empty")

        path = path.strip()

        if path.startswith("/"):
            return os.path.normpath(path)

        home = f"/users/{self.username}" if self.username else "/"
        base = self.current_dir or home

        if base == "/":
            base = home

        return os.path.normpath(os.path.join(base, path))

    def pwd(self):
        return self.current_dir

    def cd(self, path):
        resolved = self.resolve_path(path)

        if self.stat(resolved).get("type") != "dir":
            raise NotADirectoryError(resolved)

        self.current_dir = resolved
        return resolved

    def ls(self, path=".", limit=None):
        resolved = self.resolve_path(path)
        cursor = None
        entries = []

        while True:
            params = {
                "path": resolved,
                "limit": limit or self.page_size,
            }

            if cursor:
                params["cursor"] = cursor

            page = self.http.request(
                "GET",
                "/v1/fs/ls",
                params=params,
            )

            entries.extend(page.get("entries", []))
            cursor = page.get("next_cursor")

            if not cursor or limit is not None:
                return entries[:limit] if limit is not None else entries

    def stat(self, path):
        return self.http.request(
            "GET",
            "/v1/fs/stat",
            params={"path": self.resolve_path(path)},
        )

    def mkdir(self, path, parents=False):
        return self.http.request(
            "POST",
            "/v1/fs/mkdir",
            json={
                "path": self.resolve_path(path),
                "parents": bool(parents),
            },
        )

    def rmdir(self, path, recursive=False):
        return self.http.request(
            "DELETE",
            "/v1/fs/rmdir",
            params={
                "path": self.resolve_path(path),
                "recursive": str(bool(recursive)).lower(),
            },
        )

    def rm(self, path):
        return self.http.request(
            "DELETE",
            "/v1/fs/rm",
            params={"path": self.resolve_path(path)},
        )

    def mv(self, src, dst):
        return self.http.request(
            "POST",
            "/v1/fs/mv",
            json={
                "src": self.resolve_path(src),
                "dst": self.resolve_path(dst),
            },
        )

    def set_acl(self, path, subject, permission):
        if permission not in {"r", "rw", "none"}:
            raise ValueError(
                "permission must be 'r', 'rw', or 'none'"
            )

        return self.http.request(
            "PUT",
            "/v1/fs/acl",
            json={
                "path": self.resolve_path(path),
                "subject": subject,
                "permission": permission,
            },
        )

    def get_acl(self, path):
        return self.http.request(
            "GET",
            "/v1/fs/acl",
            params={"path": self.resolve_path(path)},
        )

    def lock(self, path, mode="shared", ttl_s=60):
        if mode not in {"shared", "exclusive"}:
            raise ValueError(
                "mode must be 'shared' or 'exclusive'"
            )

        return self.http.request(
            "POST",
            "/v1/fs/lock",
            json={
                "path": self.resolve_path(path),
                "mode": mode,
                "ttl_s": int(ttl_s),
            },
        )

    def renew_lock(self, lock_id, ttl_s=60):
        return self.http.request(
            "POST",
            f"/v1/fs/lock/{quote(lock_id, safe='')}/renew",
            json={"ttl_s": int(ttl_s)},
        )

    def unlock(self, lock_id):
        return self.http.request(
            "DELETE",
            f"/v1/fs/lock/{quote(lock_id, safe='')}",
        )

    def create_group(self, name):
        return self.http.request(
            "POST",
            "/v1/groups",
            json={"name": name},
        )

    def group_info(self, name):
        return self.http.request(
            "GET",
            f"/v1/groups/{quote(name, safe='')}",
        )

    def add_members(self, name, usernames, remove=None):
        return self.http.request(
            "PUT",
            f"/v1/groups/{quote(name, safe='')}/members",
            json={
                "add": list(usernames),
                "remove": list(remove or []),
            },
        )

    def create_api_key(self, name):
        return self.http.request(
            "POST",
            "/v1/auth/apikeys",
            json={"name": name},
        )

    def list_api_keys(self):
        return self.http.request(
            "GET",
            "/v1/auth/apikeys",
        )

    def revoke_api_key(self, key_id):
        return self.http.request(
            "DELETE",
            f"/v1/auth/apikeys/{quote(key_id, safe='')}",
        )

    def put_file(self, local_path, remote_path, overwrite=False):
        """Upload a local file and commit it after validation."""
        local = Path(local_path)

        if not local.is_file():
            raise FileNotFoundError(local)

        remote = self.resolve_path(remote_path)
        file_size = local.stat().st_size

        reservation = self.http.request(
            "POST",
            "/v1/files/put",
            json={
                "path": remote,
                "size": file_size,
                "overwrite": bool(overwrite),
            },
        )

        file_id = reservation.get("file_id")
        if not file_id:
            raise IOError("NameNode response is missing file_id")

        committed = False

        try:
            blocks = reservation.get("blocks")
            if not isinstance(blocks, list):
                raise IOError(
                    "Invalid reservation: blocks must be a list"
                )

            blocks = sorted(blocks, key=lambda block: block["index"])
            indexes = [block["index"] for block in blocks]

            if len(indexes) != len(set(indexes)):
                raise IOError(
                    "Invalid reservation: duplicate block indexes"
                )

            for block in blocks:
                if int(block["size"]) < 0:
                    raise IOError(
                        "Invalid reservation: negative block size"
                    )

                if not block.get("block_id"):
                    raise IOError(
                        "Invalid reservation: missing block_id"
                    )

                if not block.get("token"):
                    raise IOError(
                        f"Missing block token for {block.get('block_id')}"
                    )

            reserved_size = sum(
                int(block["size"]) for block in blocks
            )

            if reserved_size != file_size:
                raise IOError(
                    f"Reservation size mismatch: local file is "
                    f"{file_size} bytes, but blocks total "
                    f"{reserved_size} bytes"
                )

            block_checksums = []
            file_checksum = hashlib.sha256()
            bytes_uploaded = 0

            with local.open("rb") as source:
                for block in blocks:
                    block_id = block["block_id"]
                    block_size = int(block["size"])
                    pipeline = block.get("pipeline") or []

                    if not pipeline:
                        raise IOError(
                            f"No DataNode pipeline for block {block_id}"
                        )

                    address = pipeline[0].get("addr")
                    if not address:
                        raise IOError(
                            f"Missing primary DataNode address "
                            f"for {block_id}"
                        )

                    data = source.read(block_size)
                    if len(data) != block_size:
                        raise IOError(
                            f"Unexpected end of local file "
                            f"while reading {block_id}"
                        )

                    checksum = hashlib.sha256(data).hexdigest()
                    file_checksum.update(data)

                    downstream = []

                    for node in pipeline[1:]:
                        node_address = node.get("internal_addr")

                        if not node_address:
                            raise IOError(
                                f"Missing internal_addr in pipeline "
                                f"for {block_id}"
                            )

                        downstream.append(node_address)

                    response = self.block_transport.write_block(
                        addr=address,
                        token=block["token"],
                        block_id=block_id,
                        data=data,
                        checksum_sha256=checksum,
                        pipeline=downstream,
                    )

                    def response_field(name, default=None):
                        if isinstance(response, dict):
                            return response.get(name, default)
                        return getattr(response, name, default)

                    response_id = response_field("block_id")
                    response_size = response_field("size")
                    response_checksum = response_field(
                        "checksum_sha256"
                    )
                    failed_downstream = response_field(
                        "failed_downstream", []
                    )

                    if response_id and response_id != block_id:
                        raise IOError(
                            f"DataNode acknowledged unexpected block "
                            f"{response_id}"
                        )

                    if (
                        response_size is not None
                        and int(response_size) != block_size
                    ):
                        raise IOError(
                            f"DataNode reported incorrect size "
                            f"for {block_id}"
                        )

                    if (
                        response_checksum
                        and response_checksum != checksum
                    ):
                        raise IntegrityError(
                            f"DataNode checksum mismatch for {block_id}"
                        )

                    if failed_downstream:
                        raise IOError(
                            f"Replication failed for {block_id}: "
                            f"{failed_downstream}"
                        )

                    block_checksums.append({
                        "index": block["index"],
                        "block_id": block_id,
                        "size": block_size,
                        "checksum": checksum,
                    })

                    bytes_uploaded += block_size

                if source.read(1):
                    raise IOError(
                        "Local file changed during upload or "
                        "reservation was incomplete"
                    )

            if bytes_uploaded != file_size:
                raise IOError(
                    f"Uploaded size mismatch: expected {file_size}, "
                    f"read {bytes_uploaded}"
                )

            result = self.http.request(
                "POST",
                f"/v1/files/{quote(file_id, safe='')}/commit",
                json={
                    "blocks": block_checksums,
                    "file_checksum": file_checksum.hexdigest(),
                },
            )

            committed = True
            return result

        finally:
            if not committed:
                try:
                    self.http.request(
                        "POST",
                        f"/v1/files/{quote(file_id, safe='')}/abort",
                        json={},
                    )
                except Exception:
                    pass

    def get_file(self, remote_path, local_path):
        """Download a file, verify its integrity, then replace the target."""
        manifest = self.http.request(
            "GET",
            "/v1/files/get",
            params={"path": self.resolve_path(remote_path)},
        )

        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)

        fd, temp_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".dfsha-part",
            dir=str(target.parent),
        )
        os.close(fd)
        temp = Path(temp_name)
        whole = hashlib.sha256()

        try:
            with temp.open("wb") as output:
                blocks = sorted(
                    manifest.get("blocks", []),
                    key=lambda block: block["index"],
                )

                for block in blocks:
                    data = None
                    last_error = None

                    for replica in block.get("replicas", []):
                        try:
                            payload, _ = self.block_transport.read_block(
                                addr=replica["addr"],
                                token=block["token"],
                                block_id=block["block_id"],
                                offset=0,
                                length=int(block["size"]),
                            )

                            if len(payload) != int(block["size"]):
                                raise IntegrityError(
                                    f"Incorrect block size for "
                                    f"{block['block_id']}"
                                )

                            actual_checksum = hashlib.sha256(
                                payload
                            ).hexdigest()

                            if actual_checksum != block["checksum"]:
                                raise IntegrityError(
                                    f"Integrity check failed for "
                                    f"{block['block_id']}"
                                )

                            data = payload
                            break

                        except Exception as error:
                            last_error = error

                    if data is None:
                        raise last_error or IOError(
                            f"No live replicas for {block['block_id']}"
                        )

                    output.write(data)
                    whole.update(data)

            digest = whole.hexdigest()

            expected_checksum = manifest.get("checksum")
            if expected_checksum and expected_checksum != digest:
                raise IntegrityError(
                    "Whole-file SHA-256 mismatch"
                )

            os.replace(temp, target)

            return {
                "path": str(target),
                "size": target.stat().st_size,
                "checksum": digest,
            }

        except Exception:
            try:
                temp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def send(self, local_path, remote_path, overwrite=False):
        """Alias for uploading a local file to DFSha."""
        return self.put_file(
            local_path=local_path,
            remote_path=remote_path,
            overwrite=overwrite,
        )

    def receive(self, remote_path, local_path):
        """Alias for downloading a DFSha file."""
        return self.get_file(
            remote_path=remote_path,
            local_path=local_path,
        )
