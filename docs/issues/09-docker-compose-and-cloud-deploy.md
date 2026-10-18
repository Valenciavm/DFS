# Reproducible deployment: Docker Compose, five DataNodes, README and cloud

**Depends on:** issues 06 (HA), 07 (security) and 08 (client). It is the
integration/deployment issue, not the place to hide unfinished server logic.
**Branch:** `feat/deployment`, from the latest integrated server/client branch.
**Unblocks:** issue 10 (final evidence, report and video).

## Why this is required

The assignment's Option 1 is client/server with a private/virtual network, but
it also requires the project to run over the Internet on AWS Academy or GCP
academic IaaS. A localhost-only demo does not satisfy the environment
requirement. The rubric also evaluates a reproducible GitHub repository.

The final topology must show both planes:

- control plane: two NameNodes + replicated PostgreSQL metadata;
- data plane: a client plus **at least five DataNodes**;
- clients never choose DataNodes manually; addresses come from the NameNode.

Five DataNodes is intentional. With replication factor 3 and only three nodes,
every block lands on every node, so partitioning is indistinguishable from
mirroring and re-replication has nowhere to go. Five nodes make T2/T4/T13
meaningful.

## Repository files to add

Create and document:

```text
README.md
.env.example
.dockerignore
docker-compose.yml
scripts/
  generate-dev-ca.ps1 / generate-dev-ca.sh
  init-postgres-primary.ps1 / init-postgres-primary.sh
  promote-postgres-replica.ps1 / promote-postgres-replica.sh
  smoke-test.ps1 / smoke-test.sh
docker/
  namenode.Dockerfile
  datanode.Dockerfile
  client.Dockerfile
migrations/
  ... (owned by issue 03)
```

Keep the scripts cross-platform where practical; the actual cloud machines are
Linux, while the repository is also developed from Windows PowerShell. Never
commit `.env`, private keys, generated certificates or database data.

## Local Compose topology

The default demo profile must include:

- `postgres-primary`;
- `postgres-replica` in streaming/hot-standby mode;
- `namenode1`, `namenode2`;
- `datanode1` through `datanode5`;
- `client` or documented client commands from the host.

Use a private Compose network. Only expose the client-facing NameNode REST
ports and DataNode gRPC ports needed for the demo. PostgreSQL must be reachable
only from NameNodes. DataNodes need internal DNS names for pipeline addresses
and public addresses for client responses; in a local profile they may resolve
to the same host/port.

Use health checks and dependency conditions, but do not pretend readiness is
liveness: processes must retry if the dependency starts later. The compose
profile must pass secrets and certificates through environment/volumes, not
hardcode them in YAML.

Set the demo profile explicitly:

```text
BLOCK_SIZE=1 MiB
REPLICATION_FACTOR=3
MIN_REPLICAS_COMMIT=2
five DataNodes
```

The production default remains 64 MiB. Document storage volumes separately for
each DataNode so deleting/recreating a container does not silently delete its
blocks during a demo.

## Environment contract

Create `.env.example` with safe placeholders and comments for at least:

```text
NAMENODE_URLS
NODE_ID / INTERNAL_ADDR / PUBLIC_ADDR
BLOCK_SIZE / BLOCK_CHUNK_SIZE
REPLICATION_FACTOR / MIN_REPLICAS_COMMIT
BLOCK_TOKEN_SECRET / NODE_KEY
JWT_SECRET / ADMIN_USER / ADMIN_PASSWORD
DATANODE_MASTER_KEY
POSTGRES_PRIMARY_DSN / POSTGRES_REPLICA_DSN
TLS_CA_FILE / TLS_CERT_FILE / TLS_KEY_FILE
MFA_CHALLENGE_TTL_S
```

The application must fail clearly when a required production secret is absent.
The example may contain dummy values only if it says they are for local demo
use and are not safe for production.

## Cloud deployment

Provide a deployment runbook for AWS Academy (preferred) and a short GCP
mapping if possible:

| VM/service | Runs |
|---|---|
| VM/managed | RDS Multi-AZ PostgreSQL; if unavailable in the sandbox, primary + streaming replica on two EC2 VMs |
| VM1 | NameNode 1 |
| VM2 | NameNode 2 |
| VM3–VM7 | DataNodes 1–5 |
| local/VM | Client CLI/library |

Define security groups/firewall rules:

- clients → NameNode REST HTTPS;
- clients → DataNode gRPC TLS;
- DataNode ↔ DataNode internal gRPC mTLS;
- DataNode → NameNode HTTPS/mTLS control API;
- NameNode ↔ NameNode mTLS status/handover API;
- NameNodes → PostgreSQL 5432 only;
- no public PostgreSQL port and no unnecessary SSH exposure.

The runbook must include DNS/addresses, certificates/SANs, environment setup,
startup order, health checks, leader discovery, how to upload/download a test
file, how to kill a DataNode, how to promote the database replica if using
EC2, and how to tear the environment down without losing the evidence files.
Never include real credentials in documentation.

## README requirements

The README should let a new teammate go from clone to a verified demo:

1. prerequisites and Python version;
2. virtual environment and pinned dependency install;
3. protobuf stub generation;
4. local Compose startup;
5. health check and login/2FA setup;
6. `put`, `ls`, `get`, checksum comparison;
7. placement/replica inspection;
8. failure demo and re-replication;
9. test/lint commands;
10. cloud deployment link to the runbook;
11. architecture, API, issue and rubric links;
12. security warning and cleanup commands.

## Acceptance checklist

- [ ] Fresh clone can run the documented local demo without undocumented manual
  edits.
- [ ] Compose has primary/replica metadata, two NameNodes and five DataNodes.
- [ ] Health checks wait for readiness and services retry dependencies.
- [ ] Five DataNodes show real partitioning: no one node owns every block.
- [ ] TLS/mTLS and secrets are mounted/configured without committed secrets.
- [ ] PostgreSQL is not publicly exposed.
- [ ] Cloud IaaS/RDS topology and firewall rules are documented and reproduced.
- [ ] README includes exact Windows and Linux-relevant commands where needed.
- [ ] `docker compose config` passes with the example environment.
- [ ] Full tests/lint and a real local smoke test pass.
