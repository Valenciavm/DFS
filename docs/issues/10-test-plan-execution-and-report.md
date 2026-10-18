# Execute the rubric test plan, collect evidence, write the report and record the video

**Depends on:** issue 09 and all server/client/security work. This is the last
issue and should be completed from a tagged/reviewed integration commit, not
from a moving feature branch.
**Branch:** `docs/final-evidence-and-report` (or a final release branch agreed
by the team).

## Context

The assignment gives 15% each to service/architecture, communications,
scalability, high availability, replication/consistency and security, plus 10%
for the video/report/repository. `docs/ARCHITECTURE.md` Section 14 maps each
criterion to evidence; Section 12 lists T1–T24; Section 13 gives a 10–15 minute
video plan.

Do not mark a scenario complete because a unit test exists. For the final
report, distinguish:

- unit evidence (fast deterministic core/security tests);
- integration evidence (multiple processes/containers);
- failure evidence (kill/restart/promote components);
- cloud evidence (actual VMs/network/security groups).

Store raw logs, timings, checksums and configuration used for every result.
Do not store passwords, JWTs, private keys or unredacted secrets.

## Execute T1–T24

Run every scenario in `ARCHITECTURE.md` Section 12 and record status, command,
environment, expected result, observed result, artifact path and rubric
criterion. Pay particular attention to the scenarios added during design review:

- **T2:** use five DataNodes and show block sets differ; show no single node
  has every block;
- **T4:** kill a DataNode, observe under-replication, then observe return to 0;
- **T20:** kill/promote the metadata primary and show degraded read-only mode,
  then resumed writes without metadata loss;
- **T21:** enroll TOTP, test valid/invalid/reused/recovery code and API-key
  automation;
- **T22:** reject missing/wrong mTLS certificates;
- **T23:** kill the middle pipeline hop and show the later live hop receives the
  block and quorum still succeeds;
- **T24:** seek/read small ranges from a large file concurrently.

For each failure test capture before/during/after states, not only the final
success. Include `/v1/admin/stats`, node health, replica maps and relevant logs.

## Performance/scalability measurements

Run the experiments in Section 11 with a fixed environment and repeat each
measurement enough to report median and spread:

- 100 MB–1 GB upload/download with 1, 2, 4 and 8 parallel transfers;
- same file with 3, 5 and 7 DataNodes, including blocks per node;
- 1 MiB, 4 MiB and 64 MiB block sizes;
- 256 KiB, 1 MiB and 2 MiB gRPC chunks;
- replication factor 1 vs 3;
- NameNode `ls`/`stat`/`mkdir` under concurrent clients;
- recovery time after DataNode failure;
- NameNode failover time and failed request count;
- ranged reads versus full-block reads for a small request.

Record CPU/memory where possible. Explain hardware, network, file generation,
cache state, parallelism and whether encryption/TLS was enabled. Produce simple
charts/tables; do not present one unrepeatable number as a benchmark.

## Report structure

Write the technical report as PDF/DOCX/Markdown according to the course
submission rules. Minimum structure, matching the assignment:

1. objective and theoretical framework (GFS/HDFS, block vs object DFS);
2. service definition and functional/non-functional requirements RF1–RF3;
3. Option 1 architecture and component/network diagrams;
4. communication specifications for all five pairs;
5. block splitting, placement, replication, quorum and consistency algorithms;
6. execution environment: local Compose and AWS/GCP IaaS topology;
7. security: user/group/ACL/API key/TOTP, TLS/mTLS, block tokens,
   AES-GCM-at-rest and threat model;
8. implementation structure and important design decisions;
9. tests, failures, performance tables, charts and analysis;
10. limitations, deliberate scope decisions (WORM, optional FUSE, client-side
    encryption) and future work;
11. reproducibility instructions and repository/issue links.

Make the distinction between design and implementation honest: if a feature is
documented but not demonstrated, mark it as pending or limitation rather than
claiming it passed.

## Video (10–15 minutes)

Prepare one reproducible script/checklist and record:

1. problem, Option 1 and architecture (0:00–1:30);
2. login/2FA, `mkdir`, `put`, `ls`, `get`, checksum (1:30–4:30);
3. five-node placement and replica map (4:30–5:30);
4. DataNode failure, fallback read and re-replication (5:30–7:00);
5. NameNode/database failover (7:00–8:30);
6. security demo: ACL/group denial/grant, API key, block token, TLS/mTLS,
   encrypted storage (8:30–10:30);
7. concurrency, ranged reads, WORM/lock and checksum corruption (10:30–12:30);
8. scalability chart, cloud topology, limitations and repo tour (12:30–15:00).

Have a clean backup recording/screenshots for slow or cloud-dependent steps.
Never show credentials or private certificate keys.

## Acceptance checklist

- [ ] T1–T24 have a recorded status and artifact, or an explicit documented
  reason they could not run.
- [ ] Rubric Section 14 maps each criterion to a concrete artifact.
- [ ] Performance results include method/environment and charts.
- [ ] Report follows the assignment's required sections and is reproducible.
- [ ] Video is 10–15 minutes, clear, and does not expose secrets.
- [ ] README points to the report, demo script, raw evidence and issue list.
- [ ] Final repository is clean of generated data, secrets, private keys and
  temporary logs.
- [ ] Full `pytest`, `ruff`, local smoke test and cloud smoke test are recorded.
