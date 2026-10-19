# DFSha client starter

This package follows the current `docs/API.md` contract: REST metadata calls to the NameNode (`/v1`), gRPC/TLS block transfers directly to DataNodes, paginated `ls`, client-side relative paths, MFA-aware login, and API-key authentication.

## Important DataNode integration boundary

The API contract fixes BlockService semantics but leaves the exact `.proto` messages to the DataNode team. `client/transfer.py` therefore provides an explicit adapter boundary rather than guessing generated protobuf field names. Metadata methods work; `put_file` / `get_file` orchestrate transfer but will raise a clear error until `GrpcBlockTransport` is implemented against the merged `.proto` and generated stubs.

## Install and run

Add the repository's pinned `requests` dependency if not already present. Run from repository root:

```bash
export NAMENODE_URLS="https://nn1.example.com:8000,https://nn2.example.com:8000"
python -m client.cli --username ana ls /
python -m client.cli --api-key "$DFSHA_API_KEY" mkdir /projects/data
python -m unittest discover -s client/tests
```

`--insecure` disables TLS certificate validation for local development only. Do not use it in deployment.

## Python API

```python
from client import DFShaClient, MFARequired
c = DFShaClient(["https://nn1:8000", "https://nn2:8000"])
try:
    c.login("ana", "password")
except MFARequired as challenge:
    c.login_2fa(challenge.challenge, input("TOTP/recovery code: "))
print(c.ls("/projects"))
```

Included: auth/MFA, API keys, namespace CRUD, ACLs, locks, groups, upload reserve/commit/abort orchestration, download replica fallback and SHA-256 verification, and CLI commands. Full file transfers require the agreed gRPC schema.
