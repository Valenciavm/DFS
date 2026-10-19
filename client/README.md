# DFSha client

The client uses REST for NameNode metadata operations and the repository's gRPC `BlockService` for direct DataNode transfers. It includes paginated directory listing, client-side relative paths, MFA-aware login, API-key authentication, upload/download orchestration, and SHA-256 verification.

## Install and run

Install the repository dependencies and run from the repository root:

```bash
pip install -r requirements.txt
```

PowerShell:

```powershell
$env:NAMENODE_URLS = "http://127.0.0.1:8000"
python -m client.cli --username ana ls /
python -m client.cli --api-key $env:DFSHA_API_KEY mkdir /projects/data
python -m unittest discover -s client/tests
```

`--insecure` disables NameNode TLS verification for local development only. The default gRPC transport is insecure unless constructed with CA/client certificates; configure those for a secured DataNode deployment.

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

Included: auth/MFA, API keys, namespace CRUD, ACLs, locks, groups, upload reserve/commit/abort orchestration, download replica fallback and SHA-256 verification, and CLI commands.
