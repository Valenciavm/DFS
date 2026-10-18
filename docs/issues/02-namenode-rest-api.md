# NameNode REST API (FastAPI layer)

**Depends on:** the auth/ACL core pull request merged. Do not expose the core
over HTTP before permissions are enforced.
**Branch:** `feat/namenode-rest-api`, from the tip of the auth branch.
**Unblocks:** issue 04 (DataNode control loop) and issue 08 (client library
needs a server to talk to).

## Context you need

The NameNode core (`server/nodes/namenode/core.py`, class `Namenode`) is a
plain Python class with **no web code**. `ARCHITECTURE.md` Section 4.1 is
explicit about this split:

> "The core is a plain Python class with no web code, so it can be
> unit-tested with fake time and an in-memory store. The REST layer is a thin
> adapter on top of it."

Your job is that thin adapter. **Do not put business logic in route
handlers.** Every route handler should: parse/validate the request, call one
`Namenode` method, translate the result (or the raised `DfshaError`) into the
HTTP response shape from `docs/API.md`.

The NameNode serves **REST only** — no gRPC (`ARCHITECTURE.md` Section 2:
"The NameNode serves REST only; gRPC exists only on the DataNodes"). FastAPI
is the pinned dependency (`requirements.txt`: `fastapi`, `uvicorn`,
`pydantic`).

Read `docs/API.md` in full — it is the literal spec for every endpoint, every
request/response shape, every error code. This issue implements Sections
2 (partially — JWT/API key extraction, not issuance, which the auth core
provides), 3 (error format), 4 (namespace: ls/stat/mkdir/rmdir/rm/mv/acl, and
locks), 5 (file put/get/commit/abort/tokens), 7 (DataNode-facing endpoints:
register/heartbeat/block-report/block-received/block-corrupt), 8 (admin/stats),
and 9's `/v1/ha/status` (the lease/second-NameNode logic itself is issue 06 —
for now this endpoint can report `role: "leader"` unconditionally, since
there's only one NameNode until issue 06).

## What already exists

- `server/nodes/namenode/core.py` — every operation you're exposing already
  exists as a method: `register_datanode`, `record_heartbeat`,
  `check_liveness` (background, not a route), `prepare_file_upload`,
  `commit_file_upload`, `abort_file_upload`, `record_block_received`,
  `get_file_blocks`, `mkdir`, `ls`, `stat`, `rm`, `rmdir`,
  `process_block_report`, `record_block_corrupt`, `scan_replication`
  (background), `collect_garbage` (background), `stats`.
- `server/nodes/namenode/results.py` — the dataclasses these methods return
  (`UploadPlan`, `FileLocation`, `ClusterStats`, etc). Your Pydantic response
  models should mirror these, not reinvent the shape.
- `server/common/errors.py` — `DfshaError` has `.http_status`, `.to_body()`
  (the exact REST error body from `API.md` Section 3) ready to use. A FastAPI
  exception handler that catches `DfshaError` and returns
  `JSONResponse(status_code=e.http_status, content=e.to_body())` is most of
  your error handling, in one place.
- The auth/ACL core gives you: user auth, JWT issuance/verification, API
  key verification, ACL enforcement already happening *inside* the core
  methods you call — so your job is to **extract the caller identity** from
  the request (JWT bearer token or API key header) and pass it as the `user`
  parameter to core methods. You do not re-implement permission checks here.
- `server/nodes/namenode/__main__.py` — current entry point just does
  `asyncio.run(Namenode().run())`, and `Namenode.run()` currently awaits
  `api_server()` which is a **placeholder** (`await asyncio.Event().wait()`)
  — see `core.py`, method `api_server`. **You are replacing that placeholder**
  with an actual `uvicorn` server running your FastAPI app, while keeping
  `monitor_datanodes`, `monitor_replication`, `garbage_collector` running
  concurrently via the existing `asyncio.gather` in `run()`.

## Where to put the code

Suggested layout (adjust if you find something cleaner, but keep the core
untouched):

```
server/nodes/namenode/
  api/
    __init__.py
    app.py          # FastAPI() app, exception handlers, router includes
    deps.py         # dependency-injected current-user extraction, shared Namenode instance
    schemas.py       # Pydantic request/response models
    routes/
      auth.py        # /v1/auth/*  (register, login, login/2fa, apikeys) — thin wrappers over the auth core
      fs.py           # /v1/fs/*   (ls, stat, mkdir, rmdir, rm, mv, acl, lock/unlock)
      files.py        # /v1/files/* (put, tokens, commit, abort, get)
      nodes.py        # /v1/nodes/* (register, heartbeat, block-report, block-received, block-corrupt)
      admin.py        # /v1/admin/*, /health
      ha.py           # /v1/ha/status
```

## Specific things the contract requires that are easy to miss

- **`X-Request-Id` header** (`API.md` Section 1, "Tracing"): every component
  echoes it in the response and includes it in logs; if absent, generate one
  (`uuid4`). Implement as FastAPI middleware, and pass it through to the
  audit log calls from the auth core (you may need to extend those method
  signatures — check what extension points the auth core provides.
- **Pagination** (`API.md` Section 4.3, `ls`): `?limit=1000&cursor=...`,
  response includes `next_cursor` (`null` when done). The core's `ls` method
  (`core.py`) currently returns a plain `list[DirEntry]` with no pagination —
  **you will need to either extend `Namenode.ls` to accept `limit`/`cursor`
  and return a cursor-paginated result, or paginate at the REST layer** over
  the full list. Prefer extending the core method (cleaner, and the admin
  endpoints might want pagination too later) — but that means touching
  `core.py` and its tests, so budget time for it, and keep it backward
  compatible with the auth core and PR #4's existing callers (default
  `limit=LS_DEFAULT_LIMIT` matches `server/common/config.py`).
- **`PUT /v1/fs/acl`, `GET /v1/fs/acl`** — these call into the auth core's
  `set_acl`/`get_acl` core methods (or store methods, depending on how issue
  01 was implemented — check).
- **Locks** (`API.md` Section 4.4: `POST /v1/fs/lock`, `POST
  /v1/fs/lock/{lock_id}/renew`, `DELETE /v1/fs/lock/{lock_id}`): **the core
  has no lock implementation yet.** This issue must add it — to the core, not
  just the REST layer, following the existing pattern (methods on `Namenode`,
  tested with a fake clock, before wiring the route). Semantics are fully
  specified in `API.md` Section 4.4: `shared` vs `exclusive`, automatic
  expiry via `ttl_s` (default `LOCK_DEFAULT_TTL_S`, already in
  `server/common/config.py`), same user's own operations are never blocked by
  their own lock, conflicting access gets `423 LOCKED`. This is a real
  sub-feature; do not shortcut it as "a dict that's never checked".
- **`mv`** (`API.md` Section 4.3): also missing from the core. `{src, dst}` →
  `200`. Only changes metadata, no block copying. Add
  `Namenode.mv(src, dst, user)` to the core first, with tests, same pattern
  as `rm`/`rmdir`. Needs a permission check (write on both the source's and
  destination's parent — write on source parent to remove the entry, write on
  destination parent to create it).
- **`POST /v1/files/{file_id}/tokens`** (`API.md` Section 5.5): renews tokens
  (and re-evaluates pipelines) for blocks not yet written, for long uploads
  where the original token expired. **The core has no method for this yet**
  — add `Namenode.renew_upload_tokens(file_id, indexes, user) -> list[PlannedBlock]`
  reusing the placement logic already in `prepare_file_upload`/`_place`
  (read `core.py` carefully — there's a private `_place` method you can
  likely reuse or adapt; don't duplicate the placement call).
- **Admin endpoints** (`API.md` Section 8) require `role: admin` — this
  depends on the auth core's role model.
- **`/health`** needs no auth (`API.md` Section 8).

## What NOT to build here

- TOTP/2FA endpoints (`/v1/auth/2fa/*`, `/v1/auth/login/2fa`'s actual code
  verification) — the auth core deliberately defers the TOTP math; this issue
  should wire the routes to the auth core's extension point, but if it raises
  `NotImplementedError`, you can either leave the
  route stubbed with a clear `501 Not Implemented` + comment, or implement
  the TOTP verification yourself if the auth core left a gap — check
  before assuming. Issue 07 (security) is the one explicitly scoped to finish
  this if it's still open.
- The leader lease / `term` fencing / second NameNode (issue 06). For now,
  `/v1/ha/status` returns a static leader report and `NOT_LEADER` never
  happens — document this as a known gap in your PR, matching
  `ARCHITECTURE.md` Appendix A.4's existing "not implemented yet" list (add
  to it if your PR doesn't close it).

## Tests

FastAPI's `TestClient` (or `httpx.AsyncClient` with `ASGITransport`) against
the app, not against a running server. Follow the existing test style:
descriptive sentence names, grouped by endpoint area. You'll want a test
fixture that builds a fresh `Namenode` with a `FakeClock` and seeded RNG per
test (mirror `server/nodes/namenode/tests/conftest.py`), with the FastAPI app
wired to that instance via dependency override (FastAPI's
`app.dependency_overrides`).

Minimum scenarios per endpoint: success path, the specific error codes
`API.md` lists for that endpoint (table in Section 3), and that the response
JSON shape matches the contract's example exactly (field names, types).

## Acceptance checklist

- [ ] Every endpoint in `API.md` Sections 4, 5, 7, 8 and `/v1/ha/status` from Section 9 is implemented, or explicitly listed as deferred in the PR description with a reason.
- [ ] `mv` and locks added to the core (with their own unit tests, core-level, following existing conventions) before being exposed via REST.
- [ ] `DfshaError` → HTTP response mapping is a single exception handler, not repeated per route.
- [ ] `X-Request-Id` handled as middleware.
- [ ] OpenAPI docs are served at `/docs` (FastAPI default — just don't disable it) per `ARCHITECTURE.md`'s "Publishes the OpenAPI generated by FastAPI at /docs".
- [ ] `Namenode.run()` / `__main__.py` actually serves HTTP now (manual smoke test: start it, `curl http://localhost:8000/health`).
- [ ] Existing NameNode core tests (~250+) still pass untouched (you should not need to modify `core.py`'s existing methods except to add `mv`, lock methods, and pagination — and those are additive).
- [ ] `ruff check .` clean.
- [ ] `docs/API.md` changelog entry if you deviate from the documented contract anywhere (should be rare — flag it loudly if so, since the contract is supposed to be the source of truth and changing it needs the "team rule" at the top of that document followed).
