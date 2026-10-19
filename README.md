# DFSha

> [!IMPORTANT]
> **Technical Report:** The main document of the project's technical report is available at [DFS_Informe_Tecnico.pdf](DFS_Informe_Tecnico.pdf).

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
To use the CLI interactively:

```sh
docker compose exec client python -m client.cli --username demo-admin ls /users/demo-admin
docker compose exec client python -m client.cli --username demo-admin stat /users/demo-admin/compose-smoke.bin
```

The commands above use the `.env.example` username; replace `demo-admin` if
`ADMIN_USER` was changed in `.env`.

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
