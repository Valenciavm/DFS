# DFSha

> [!IMPORTANT]
> **Technical Report:** The main document of the project's technical report is available at [DFS_Informe_Tecnico.pdf](DFSha_Informe_Tecnico_v1.0.pdf).

DFSha is a block-based distributed file system with NameNode metadata services,
DataNode block storage, and a Python client. See [the architecture](docs/ARCHITECTURE.md),
[the API contract](docs/API.md), and [issue 09](docs/issues/09-docker-compose-and-cloud-deploy.md).

## Local development demo

> **Development only, not secure.** This Compose stack uses HTTP, insecure gRPC,
> demo credentials, and does not encrypt DataNode files at rest. Do not expose it
> to an untrusted network. The missing TLS/mTLS and AES-256-GCM controls are tracked
> in [issue 07](docs/issues/07-security-tls-and-encryption.md); this Compose setup
> does not close that issue or qualify for production/cloud deployment.

Prerequisites: Docker Desktop with the Compose plugin, and Python 3.13 if running
the client from the host. The Compose client container already has the pinned
dependencies installed.

From PowerShell:

```powershell
Copy-Item .env.example .env
docker compose config
docker compose up --build -d
docker compose ps
Invoke-RestMethod http://localhost:8001/health
Invoke-RestMethod http://localhost:8002/health
docker compose exec client python scripts/smoke_test.py
```

From a POSIX shell:

```sh
cp .env.example .env
docker compose config
docker compose up --build -d
docker compose ps
curl --fail http://localhost:8001/health
curl --fail http://localhost:8002/health
docker compose exec -T client python -m scripts.smoke_test
```

The smoke test logs in with `ADMIN_USER`/`ADMIN_PASSWORD`, uploads a file larger
than one demo block, downloads it, and compares the bytes and SHA-256 checksum.
To open the interactive client, first copy any local files you want to upload
into the client container, then start a TTY session:

```sh
# Replace this path with a file that actually exists on your machine.
docker compose cp ./mi-archivo.txt client:/tmp/mi-archivo.txt
docker compose exec -it client python -m client.cli --username demo-admin shell
```

The CLI asks for the password once when the session starts, then reuses the
authenticated client until `exit` or `logout`. On terminals with Python's
`readline` support, arrow keys edit the command line and navigate this session's
history. Within the prompt, use familiar commands such as `pwd`, `ls`, `cd`,
`mkdir`, `touch`, `put`, `get`, `mv`, `rm`, `rmdir`, and `share`; relative
DFSha paths follow the current directory. For example:

```text
dfsha:/users/demo-admin$ mkdir proyectos
dfsha:/users/demo-admin$ cd proyectos
dfsha:/users/demo-admin/proyectos$ pwd
dfsha:/users/demo-admin/proyectos$ touch vacio.txt
dfsha:/users/demo-admin/proyectos$ put /tmp/mi-archivo.txt mi-archivo.txt
dfsha:/users/demo-admin/proyectos$ ls
dfsha:/users/demo-admin/proyectos$ share mi-archivo.txt user:bruno r
dfsha:/users/demo-admin/proyectos$ get mi-archivo.txt /tmp/descargado.txt
dfsha:/users/demo-admin/proyectos$ exit
```

The commands also remain available as one-shot invocations, for example:

```sh
docker compose exec client python -m client.cli --username demo-admin ls /users/demo-admin
docker compose exec client python -m client.cli --username demo-admin stat /users/demo-admin/compose-smoke.bin
```

These examples use the `.env.example` username; replace `demo-admin` if
`ADMIN_USER` was changed in `.env`.

The Python library also provides ranged read-only access to large files:

```python
from client import DFShaClient

client = DFShaClient("http://namenode1:8000")
try:
    client.login("demo-admin", password)
    with client.open("/users/demo-admin/mi-archivo.txt", lock="shared") as remote_file:
        remote_file.seek(1024)
        data = remote_file.read(256)
finally:
    client.http.close()
```

Remote files are WORM: `open()` is read-only. Create a new file or replace one
with `put`/`put_file`; random in-place writes are not supported.

The demo uses 1 MiB blocks, replication factor 3 and a two-replica commit
quorum, with five DataNodes on the private Compose network. Their advertised
addresses use Compose DNS names, so run the client inside the `client` service.
Each DataNode and each PostgreSQL instance has a separate named volume. Inspect
DataNode placement and replication through the API/admin endpoints and logs:

```sh
docker compose logs --follow namenode1 namenode2 datanode1 datanode2 datanode3 datanode4 datanode5
```

To demonstrate failure handling, stop one DataNode after the smoke test:

```sh
docker compose stop datanode1
docker compose logs --follow namenode1 namenode2
```

Restore it with `docker compose start datanode1`. Allow the NameNode liveness and
replication loops to detect the failure and schedule repair. The local topology
does not configure TLS; do not infer production failover or security from this demo.

PostgreSQL primary data streams to a hot-standby replica. Both NameNodes currently
connect to the primary DSN; automatic promotion and DSN reconfiguration are not
implemented. Stop both NameNodes before a manual replica promotion:

```sh
docker compose stop namenode1 namenode2
./scripts/promote-postgres-replica.sh
```

PowerShell:

```powershell
docker compose stop namenode1 namenode2
.\scripts\promote-postgres-replica.ps1
```

Promotion is an operator action and does not automatically point Compose at the
new primary. Update `POSTGRES_PRIMARY_DSN` in `.env` and recreate both NameNodes
before resuming the demo. Do not remove volumes if the demo data must be retained.
For a fresh disposable cluster, `docker compose down -v` deletes all database and
block data.

## Tests and lint

```sh
python -m pytest -q
python -m ruff check client scripts/smoke_test.py server/nodes/namenode/api/routes/files.py server/nodes/namenode/api/routes/groups.py server/nodes/namenode/api/schemas.py server/nodes/namenode/tests/test_api.py
python -m unittest discover -s client/tests
```

The repository-wide `python -m ruff check .` currently reports 23 diagnostics
in existing server and test files; the command above covers the client/API files
changed for this integration and is clean.

## Security and deployment status

The Compose profile is an integration/dev harness only. HTTP/gRPC TLS, mTLS,
AES-256-GCM storage encryption, cloud firewall rules, and automated PostgreSQL
failover remain open work. See [the security issue](docs/issues/07-security-tls-and-encryption.md)
and [the deployment issue](docs/issues/09-docker-compose-and-cloud-deploy.md)
before planning any non-local deployment.
