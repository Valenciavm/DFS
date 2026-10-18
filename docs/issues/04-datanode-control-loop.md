# DataNode control loop and NameNode REST integration

**Depends on:** issue 02 (NameNode REST endpoints). Issue 03's PostgreSQL is
not required for local unit tests, but the control loop must work against a
real NameNode process.
**Branch:** `feat/datanode-control-loop` from the REST branch.
**Unblocks:** issue 08 (client integration), issue 09 (cluster deployment).

## Context

The DataNode package is `server/nodes/datanode/`:

- `core.py` owns lifecycle, storage and command handling;
- `block_service.py` owns the gRPC BlockService;
- `storage.py` owns local disk files;
- `__main__.py` provides CLI flags.

The gRPC data plane was tested independently, but the current DataNode's
`register_with_namenode()` and `heartbeat_loop()` are placeholders. It does not
register, send heartbeats, send block reports, acknowledge block reception or
process the exact command shape from `API.md` Section 7 in a real cluster.

The NameNode never initiates connections to DataNodes. DataNodes call the
NameNode over REST, and commands come back in heartbeat/block-report responses.
The DataNode talks to other DataNodes over gRPC for block movement.

## Contract to implement

Read `docs/API.md` Sections 1, 2.3, 6 and 7. Implement these calls with
`httpx.AsyncClient` and a configurable list of NameNode URLs:

- `POST /v1/nodes/register`
  `{node_id, internal_addr, public_addr, capacity_bytes, used_bytes}`
- `POST /v1/nodes/heartbeat`
  `{node_id, free_bytes, used_bytes, active_transfers}`
  → `{term, commands}`
- `POST /v1/nodes/block-report`
  `{node_id, blocks:[{block_id,size,checksum}]}` → `{term, commands}`
- `POST /v1/nodes/block-received`
  `{node_id, block_id, size, checksum}`
- `POST /v1/nodes/block-corrupt`
  `{node_id, block_id}`

Every control call carries the node credential (`X-Node-Key` for now) and
`X-Request-Id`; issue 07 adds mutual TLS. The DataNode must handle:

- `503 NOT_LEADER`: follow `leader_url` or try the next configured NameNode;
- `409 NODE_NOT_REGISTERED`: register again, then send a full block report;
- connection timeout/unavailable: retry with bounded exponential backoff;
- a higher `term`: remember it and immediately send a full block report;
- a lower `term`: ignore commands from that response;
- duplicate commands: execute idempotently where possible.

Do not put a hardcoded localhost URL in production code. Add settings for
`NAMENODE_URLS`, node key, heartbeat interval, block report interval and
request timeout. Keep local defaults usable only when explicitly configured by
the demo environment.

## Heartbeat command execution

The command format is:

```json
{
  "cmd": "replicate",
  "block_id": "blk...",
  "targets": [{"node_id":"dn3", "internal_addr":"dn3:9000"}],
  "token": "<system write token>"
}
```

or:

```json
{"cmd":"delete", "block_id":"blk..."}
```

For `replicate`:

1. read the local block;
2. send a gRPC `WriteBlock` stream to each target's `internal_addr`;
3. pass the supplied block token as `x-block-token` metadata;
4. on success, the target will call `block-received`; do not claim success
   merely because the gRPC stream was opened;
5. on failure, retain the command/reporting state so the next heartbeat can
   retry or the NameNode can time out the in-flight copy.

For `delete`, delete the block atomically/safely and make the command
idempotent if the file is already absent.

## Block reports

Implement a full report by scanning only definitive `.dat` block files in the
storage directory, reading size and SHA-256. Never report `.tmp` files as
valid blocks. Send a report at startup, after a higher term, after
`BLOCK_REPORT_INTERVAL_S`, and when the NameNode explicitly asks through its
response if that extension is added.

After local `WriteBlock` succeeds, notify `block-received` exactly once per
stored copy. If the NameNode is unavailable, queue/retry the notification;
do not lose it silently. On disk corruption detected by the DataNode, delete
the local copy and call `block-corrupt`.

## Tests

Use a fake `httpx` transport or a small ASGI NameNode test app for unit tests;
do not require a real network for normal CI. Add integration tests with two
real processes or in-process servers for:

- registration payload has internal and public addresses separately;
- heartbeat updates the NameNode and returns commands;
- unknown-node response causes register + full report;
- higher term causes an immediate report; lower-term commands are ignored;
- NameNode failover URL is followed;
- transient failure retries with a bounded delay and eventually reports a
  failure instead of hanging forever;
- block report excludes temporary files and includes correct checksums;
- a pipeline write produces `block-received` notifications;
- replicate and delete commands are idempotent;
- graceful shutdown closes the HTTP client and gRPC server.

## Acceptance checklist

- [ ] No placeholder `register_with_namenode` or `heartbeat_loop` remains.
- [ ] All five DataNode→NameNode endpoints are exercised.
- [ ] `NODE_NOT_REGISTERED`, `NOT_LEADER`, term and retry behavior match the
  contract.
- [ ] Full reports are sent on startup and term changes.
- [ ] Commands use `cmd`, `targets[].internal_addr` and `token`, not the old
  `action`/`target_address` shape.
- [ ] Existing gRPC pipeline tests and full test/lint suites pass.
- [ ] Update `docs/GRPC_BLOCK_SERVICE.md` and `docs/API.md` only if behavior
  changes; add a changelog entry for any wire change.
