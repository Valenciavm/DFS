# Python client library, CLI, transfer API and dynamic transparency

**Depends on:** issue 02 (REST), issue 04 (DataNode control loop), PR #6
(ranged/idempotent gRPC), and the auth/ACL core pull request. Issue 07 is needed
before production TLS, but the client can use explicit local CA settings first.
**Branch:** `feat/client-library-cli`, from the latest server branch.
**Unblocks:** issue 09 and the end-to-end demonstrations.

## Context

The assignment requires a client CLI/API with RF1 filesystem management, RF2
`send()`/`receive()` transfer, RF3 `open()`/`close()`/`read()`/`write()`/`lock()`,
and client block read/write APIs. The contract calls the Python library
`DFShaClient` (`docs/API.md` Section 10); the current `client/` package is
empty.

The client must hide DataNode locations from users. It talks to the NameNode
for metadata and tokens, then transfers bytes directly to DataNodes. This is
**dynamic location transparency**: nodes are resolved at runtime, never
hardcoded in the CLI. A FUSE mount is optional (decision A in
`ARCHITECTURE.md` Section 16); do not make it a prerequisite.

## Public API

Implement `client/__init__.py` exporting at least:

```python
c = DFShaClient(["https://nn1", "https://nn2"])
c.login("ana", password)             # branches on mfa_required
# or c.use_api_key(api_key)

c.ls(path); c.cd(path); c.pwd(); c.stat(path)
c.mkdir(path, parents=False); c.rmdir(path, recursive=False)
c.rm(path); c.mv(src, dst)
c.set_acl(path, subject, permission)

c.put_file(local, remote, overwrite=False)
c.get_file(remote, local)

with c.open(remote, "rb", lock="shared") as f:
    f.seek(offset)
    data = f.read(length)

allocation = c.begin_upload(remote, size)
c.write_block(allocation.blocks[0], bytes_chunk)
c.commit_upload(allocation)
```

Provide aliases named `send()`/`receive()` or document them as exact aliases of
`put_file()`/`get_file()` so the assignment's terminology is visible to a
grader. `write()` is WORM-compatible: sequential creation through a pending
upload; in-place random overwrite must fail clearly rather than pretending to
be supported.

## NameNode behavior

Create a small REST transport with:

- retry across configured NameNode URLs;
- follow `leader_url` on `503 NOT_LEADER`;
- bounded exponential backoff for connection failures;
- `X-Request-Id` propagation;
- JWT/API key authentication and 2FA challenge handling;
- consistent conversion of the contract error body into client exceptions;
- no DataNode address selected by the caller.

For upload:

1. call `/files/put` (or the batched allocation endpoint once issue 09/contract
   work changes it);
2. split local bytes using the returned per-file block size;
3. stream each block in chunks, in parallel with a bounded worker count;
4. send `x-block-token` and the pipeline header to the first DataNode;
5. retry failed write pipelines through `/tokens` when tokens/pipelines expire;
6. collect checksums and commit only after all blocks are written.

For download:

1. call `/files/get`;
2. download blocks in index order or bounded parallelism;
3. try replicas in order and fall back on `UNAVAILABLE`, `DATA_LOSS`, token
   failure or checksum mismatch;
4. verify every block checksum and optional whole-file checksum;
5. write the local destination atomically (temporary file + rename).

## File handle

`DFSFile` must support `read(size=-1)`, `seek(offset, whence=0)`,
`tell()`, `close()` and context-manager methods. Map reads to block indexes and
`ReadBlock(offset,length)`, fetching only the needed ranges. Cache a bounded
number of recently used blocks/ranges; do not load the entire file by default.
A shared lock may be acquired/released around the handle; lock expiry and
renewal must not leave a crashed client holding a lock forever.

## CLI

Create `client/cli.py` with commands matching `API.md` Section 10:

```text
login, apikey, ls, cd, pwd, mkdir, rmdir, rm, mv,
put, get, stat, lock, unlock, share, group
```

Use argparse/Typer/etc. consistently, return nonzero exit codes for contract
errors, never print passwords/tokens, and provide `--namenode`/environment
configuration. `cd` is client-side state: validate the target with `stat` and
resolve relative paths against `/users/<username>` while sending global paths
on the wire.

## Tests

- transport retries and follows `leader_url`;
- login without/with 2FA and API-key login;
- relative/home path mapping to `/users/<user>`;
- put of empty, exact-block and remainder files;
- bounded parallel upload/download;
- checksum mismatch and failed-replica fallback;
- expired write token requests fresh tokens;
- ranged `open/seek/read` across block boundaries;
- WORM write semantics and lock lifecycle;
- atomic local destination on failed download;
- CLI command parsing and exit codes;
- no DataNode address appears in user-facing selection arguments.

## Acceptance checklist

- [ ] `from client import DFShaClient` works.
- [ ] RF1, RF2, RF3 and block-level APIs from the assignment are usable.
- [ ] `send`/`receive` aliases are present or explicitly documented.
- [ ] DataNode locations are resolved dynamically and hidden from callers.
- [ ] Replica fallback and checksum verification work.
- [ ] Full tests, ruff and a real local NameNode/DataNode smoke test pass.
- [ ] CLI usage and library examples are documented in `README.md`.
