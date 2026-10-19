from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

from client import DFShaClient


def main() -> None:
    urls = os.environ["NAMENODE_URLS"]
    username = os.environ["ADMIN_USER"]
    password = os.environ["ADMIN_PASSWORD"]
    payload = os.urandom((1 << 20) + 37)

    with tempfile.TemporaryDirectory() as directory:
        local_source = Path(directory) / "source.bin"
        local_download = Path(directory) / "download.bin"
        local_source.write_bytes(payload)

        client = DFShaClient(urls)
        try:
            client.login(username, password)
            remote = f"/users/{username}/compose-smoke.bin"
            client.put_file(local_source, remote, overwrite=True)
            client.get_file(remote, local_download)
            downloaded = local_download.read_bytes()
            if downloaded != payload:
                raise SystemExit("Smoke test failed: downloaded bytes differ")

            digest = hashlib.sha256(downloaded).hexdigest()
            print(f"Smoke test passed: {len(downloaded)} bytes, sha256={digest}")
            print(f"NameNodes: {', '.join(client.http.base_urls)}")
            print(f"File: {remote}")
        finally:
            client.http.close()


if __name__ == "__main__":
    main()
