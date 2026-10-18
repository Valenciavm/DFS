# Data-plane resilience: dead pipeline hops, ranged reads, StatBlock and idempotent writes

**Depends on:** none for implementation; it can be developed in parallel with
NameNode/auth work. It must be merged before the client and deployment issues.
**Branch:** `feat/data-plane-resilience`, from the integrated data-plane branch.
**Unblocks:** issue 08 and the availability/consistency demonstrations.

## Context

The DataNode gRPC service is in `server/nodes/datanode/block_service.py`; the
DataNode lifecycle/client is in `server/nodes/datanode/core.py`; the protobuf
contract is `proto/block_service.proto`; implementation notes are
`docs/GRPC_BLOCK_SERVICE.md`; the authoritative semantics are `docs/API.md`
Section 6.

The current service proves a healthy pipeline and checksum validation, but the
real-cluster test found a serious failure: with `dn1 -> dn2 -> dn3` and `dn2`
dead, `dn1` kept its own copy and reported the failure, but `dn3` never got the
block. Three replicas became one, below the write quorum. The architecture
already promises that a pipeline write continues through remaining nodes, so
code and design disagree.

The current `ReadBlock` also ignores `offset`/`length` and loads the whole block
into memory, `StatBlock` is declared but not implemented, and `already_existed`
is always false. These are not optional polish: the assignment asks for
concurrency inside a file, and the design uses ranged reads for `open/seek/read`.

## Required changes

### 1. Skip a dead pipeline hop

When forwarding a stream, if `pipeline[0]` is unavailable:

1. record that address in `failed_downstream`;
2. try the next address in the remaining pipeline;
3. forward the same header/chunk stream to the first reachable node, with the
   rest of the pipeline after it;
4. preserve backpressure and do not buffer an entire large block in memory;
5. return success for the local copy plus the skipped addresses if at least the
   local node stored the block.

With a 3-node pipeline and one dead middle node, the result should be two
confirmed copies, enough for `MIN_REPLICAS_COMMIT=2`; the NameNode later
re-replicates the third. If no downstream hop survives, report all failures and
leave the local copy intact.

Be careful with async generator lifetimes: a failed downstream attempt must not
consume the source stream irreversibly. A practical design is a fan-out/queue
per attempt or a forwarding coordinator that can connect the next target before
consuming data. Do not solve it by reading the full block into RAM.

### 2. Implement ranged `ReadBlock`

Contract:

- `offset >= 0`;
- `length == 0` means through the end;
- otherwise serve `[offset, offset + length)`;
- invalid ranges return `OUT_OF_RANGE` plus
  `x-dfsha-error-code: RANGE_NOT_SATISFIABLE`;
- response header reports whole-block size/checksum and the actual served
  `offset`/`length`;
- read only the required byte range from disk (seek/read in chunks), verify the
  at-rest authentication tag once encryption exists, and stream chunks no larger
  than `BLOCK_CHUNK_SIZE`.

The block checksum in the header is the checksum of the whole block, not only
the range. A client that wants end-to-end verification can fetch the whole block
or use the metadata checksum supplied by the NameNode.

### 3. Implement `StatBlock`

Return `block_id`, size and SHA-256 for an existing block. Missing blocks return
`NOT_FOUND`. It needs a read block token and must use the same interceptor/error
metadata conventions as `ReadBlock`.

### 4. Make writes idempotent

If the definitive `.dat` file exists with the same checksum and size, return
`already_existed=true` without rewriting it. If it exists with a different
checksum, reject with the contract's checksum/application error — never silently
overwrite immutable committed data. A `.tmp` left by a crashed transfer must
not be treated as a valid existing block.

Use a per-block async lock or an atomic temp-file strategy so two concurrent
writes cannot turn idempotency into a race.

### 5. Error metadata and limits

Centralize the gRPC error helper:

- missing/bad/expired token → `UNAUTHENTICATED`;
- valid token for another block/op → `PERMISSION_DENIED`;
- absent block → `NOT_FOUND`;
- invalid range → `OUT_OF_RANGE`;
- checksum mismatch → `INVALID_ARGUMENT`;
- disk corruption → `DATA_LOSS`;
- every application failure adds `x-dfsha-error-code` trailing metadata.

Enforce configured `BLOCK_CHUNK_SIZE` and reject chunks over the hard 2 MiB
maximum. Move the current 64 KiB constant to `server/common/config.py`'s
contract default of 1 MiB, while keeping tests free to use a smaller value.

## Tests

Add tests that run without external nodes plus at least one two/three-node
integration test:

- middle pipeline hop dead: downstream later node still receives the block;
- first and last hop failures report all failures and preserve local storage;
- ranged read from offset 0, middle, end and exact boundary;
- zero length means to end;
- negative/out-of-range/overflow ranges map to the correct gRPC status and
  application error metadata;
- `StatBlock` success/missing/token failures;
- same block written twice with same checksum returns `already_existed`;
- same block written with different checksum is rejected;
- concurrent duplicate writes leave one valid definitive file;
- chunk-limit rejection and checksum mismatch leave no temporary garbage;
- client receives whole-block checksum while reading a partial range.

## Acceptance checklist

- [ ] Dead middle hop is skipped without buffering the complete block.
- [ ] `ReadBlock` honors offset/length and streams only the requested bytes.
- [ ] `StatBlock` is implemented and authenticated.
- [ ] Idempotency flag is truthful and immutable data is not overwritten.
- [ ] gRPC application error metadata is present consistently.
- [ ] Existing healthy pipeline tests, full tests and ruff remain green.
- [ ] `docs/GRPC_BLOCK_SERVICE.md` no longer lists these items as missing.
- [ ] Add a concise changelog entry to the relevant docs if the protobuf
  semantics or response behavior changes.
