# Authentication, API keys, groups and ACL enforcement in the NameNode core

**Depends on:** PR #4 (`feat/namenode-core-rewrite`) merged.
**Branch:** `feat/auth-and-acl`, from the tip of `feat/namenode-core-rewrite` (or `main` if #4 is already merged there).
**Unblocks:** issue 02 (REST API — must not be exposed without this).

## Context you need

DFSha is a block-based distributed file system (HDFS-style). A `NameNode`
(`server/nodes/namenode/core.py`) holds all metadata — the namespace tree,
files, blocks, replica locations — and several `DataNode`s hold block bytes.
The contract between every component is `docs/API.md`; the design rationale
and test plan are `docs/ARCHITECTURE.md`.

The NameNode **core** (the class `Namenode` in `server/nodes/namenode/core.py`)
was just rewritten from scratch (PR #4) and is fully unit tested (~250 tests)
with a fake clock (`server/common/clock.py`) and an in-memory store
(`server/nodes/namenode/store/memory.py`, behind the `MetadataStore` interface
in `store/base.py`). Read `core.py` and `store/base.py` before writing any code
— this issue adds to that file, it does not replace it.

**Critical gap:** the core currently records an `owner` string on every file
and directory but **never checks it**. Any caller can `rm` anyone's file. This
must be fixed before the REST layer (issue 02) is built, because REST is what
turns "a caller" into "a stranger on the internet".

The namespace layout was decided in `ARCHITECTURE.md` Section 16, decision 20:
one global tree, every user's home is `/users/<username>`, presented as `/` by
the client library. The server never rewrites paths — it is the client's job
to prefix relative paths with the home directory. This matters for ACL design:
a path like `/users/ana/projects` has to be nameable by `bruno` for an ACL
grant to mean anything.

## What already exists and what you are adding

Already built (do not redo):
- `server/common/errors.py` — `DfshaError`, `ErrorCode` enum with HTTP and
  gRPC status mappings. `ErrorCode.FORBIDDEN` (403) and `ErrorCode.UNAUTHORIZED`
  (401) already exist and are used elsewhere.
- `server/common/clock.py` — `Clock` protocol, `SystemClock`, `FakeClock`.
  Every new piece of time-dependent code must take a `Clock`, not call
  `time.time()` directly. `FakeClock.advance(seconds)` moves both wall and
  monotonic time together; `jump_wall(seconds)` moves only wall time (for
  testing NTP corrections — liveness code must never be fooled by this).
- `server/common/paths.py` — `split_path`, `join_path`, `parent_and_name`.
  Every path must go through `split_path` for validation; never hand-roll
  `.split("/")`.
- `server/common/tokens.py` — block token mint/verify (HMAC, used for the
  gRPC block channel, not for this issue, but read it for the pattern: tokens
  are self-contained and verified without a round trip).
- `server/common/config.py` — `Settings` dataclass, read from environment.
  Add any new tunables here (e.g. `TOKEN_TTL_S` is already there for block
  tokens; you'll want a JWT TTL too — check `API.md` Section 1 for the name
  `TOKEN_TTL_S` is already block-specific, so pick a distinct one, e.g.
  reuse the existing `token_ttl_s` only if semantically identical, otherwise
  add `jwt_ttl_s` — check the contract first, API.md Section 1 lists
  `TOKEN_TTL_S` only for block tokens).
- `server/nodes/namenode/core.py` — the `Namenode` class. Has `self.store`,
  `self.clock`, `self.settings`, `self.cluster` (soft state — not relevant
  here). Methods you will be modifying: `mkdir`, `ls`, `stat`, `rm`, `rmdir`,
  `prepare_file_upload`, `commit_file_upload`, `get_file_blocks`.

What this issue adds:
1. User and credential model (hard state — survives restarts, will later be
   persisted to PostgreSQL in issue 03, but for now lives in a new method on
   `MetadataStore` or a separate in-memory `UserStore` — your call, see
   "Design choice" below).
2. API keys.
3. Groups.
4. ACL entries and the permission check itself, wired into every core method
   that touches a path.

## The contract (what you must match exactly)

Read `docs/API.md` Sections 2, 2.1, 2.2, 4.1, 4.1.1, 4.2 and 4.3 in full before
starting. Key points, summarized:

### Authentication (`API.md` Section 4.1)
- `POST /v1/auth/register` — `{username, password}` → `201 {username}`.
- `POST /v1/auth/login` — `{username, password}` → either
  `{access_token, token_type, expires_in}` **or**, if the user has TOTP 2FA
  enabled, `{mfa_required: true, challenge, expires_in}`.
- `POST /v1/auth/login/2fa` — `{challenge, code}` → `{access_token, ...}`.
  **2FA itself (TOTP enrollment, verification) is NOT part of this issue** —
  it is substantial enough to be its own follow-up. For this issue, build the
  `login` flow so it is **ready** to branch on `mfa_required` (i.e., the user
  model has `totp_enabled: bool = False` and `login` checks it), but you do
  not need to implement enrollment or the TOTP code path itself. Leave a
  clearly marked `NotImplementedError` or a TODO with a reference to
  `ARCHITECTURE.md` Section 9.1 for the actual TOTP verification, so issue 07
  (security) can fill it in without redesigning the login flow.
- `POST /v1/auth/apikeys`, `GET /v1/auth/apikeys`, `DELETE /v1/auth/apikeys/{key_id}`.

### Security rules (`API.md` Section 2.2, `ARCHITECTURE.md` Section 9)
- Passwords hashed with **argon2** (the dependency `argon2-cffi` is already
  pinned in `requirements.txt` — use `argon2.PasswordHasher`).
- API keys stored **hashed** (same hasher is fine), shown only once at
  creation, prefixed `dfsha_live_...` in the response (cosmetic, but match the
  contract example in `API.md` Section 4.1).
- JWT: `HS256` is enough for now (RS256 is mentioned as an option but not
  required). The pinned dependency is `pyjwt`. Claims: at minimum `sub`
  (username), `exp`. When 2FA support lands (issue 07) the claim `amr` will
  record factors used — leave room for it but do not implement it now.
- **Roles**: `user` (default) and `admin`. First admin bootstraps from
  `ADMIN_USER` / `ADMIN_PASSWORD` environment variables on first start — add
  these to `server/common/config.py`.
- **Audit log**: `ARCHITECTURE.md` Section 9 requires one (timestamp, user,
  operation, path, result, request_id). For this issue, a simple in-memory
  list behind an interface is fine (persisting it is issue 03's job); the
  point now is to call it from the core on every auth decision and every FS
  operation, so the hook exists.

### Groups (`API.md` Section 4.2)
- `POST /v1/groups` — creates a group, creator becomes owner.
- `PUT /v1/groups/{name}/members` — `{add: [...], remove: [...]}`, owner only.
- `GET /v1/groups/{name}` — info and members.

### ACL (`API.md` Section 4.3, "Semantics" paragraph)
- `PUT /v1/fs/acl` — `{path, subject, permission}` where `subject` is
  `"user:bruno"` or `"group:students"`, and `permission` is `"r"`, `"rw"` or
  `"none"`.
- `GET /v1/fs/acl?path=...` — lists ACL entries on a path.
- **The owner always has `rw`.** An ACL entry grants `r` or `rw` to a user or
  group **on a path and everything below it** (i.e., ACL is inherited down
  the tree, not just on the exact path). The effective permission is the
  **union** of all matching entries (owner, direct ACL, group ACL, and
  anything inherited from an ancestor directory).
- This issue's job is **only** the server-side model and enforcement; the
  `/v1/fs/acl` REST endpoints themselves are issue 02's job (REST layer), but
  the core methods this issue adds (`set_acl`, `get_acl`, `create_group`,
  `add_group_members`, etc.) are what REST will call.

### Permission checks — when and how (`ARCHITECTURE.md` Section 4.3, `API.md` Section 2.2)
> "The NameNode checks permissions (owner / ACL for user or group) before
> issuing any token, and re-checks on every FS operation."

This means every one of these core methods needs a `user: str` parameter
(most already have one, e.g. `get_file_blocks(path, user)` — check each) and
must raise `DfshaError(ErrorCode.FORBIDDEN)` if the effective permission is
insufficient:

| Method | Needs |
|---|---|
| `mkdir` | write on parent |
| `ls` | read on the directory |
| `stat` | read on the path (or its parent, for existence checks — match HDFS semantics: you need read to stat) |
| `rm` | write on parent (removing a file is a write to the containing directory) |
| `rmdir` | write on parent |
| `prepare_file_upload` (`put`) | write on parent; **owner must be the authenticated user**, not a caller-supplied string (currently it trusts the `owner` parameter — this is the bug to fix) |
| `commit_file_upload` | no extra check — the file was already reserved by an authorized caller; but do re-verify the caller is the same user who started the upload (store this on `FileMetadata` if not already — check `models.py`) |
| `get_file_blocks` (`get`) | read on the path |

## Design choice you need to make (and document)

`MetadataStore` (in `store/base.py`) currently only manages the namespace
(files/directories/blocks). You have two reasonable options for where users,
groups and ACL entries live:

- **Option A**: extend `MetadataStore` with user/group/ACL methods, so the
  future PostgreSQL implementation (issue 03) has one interface to satisfy.
- **Option B**: a separate interface (e.g. `AuthStore` / `AccessStore`) that
  the `Namenode` core holds alongside `self.store`, mirroring the hard
  state / soft state split already used for `ClusterState`.

Either is defensible. **Pick one, write a paragraph in your PR description
explaining why**, and update `docs/NAMENODE_MODELS.md` to reflect it (that
document is the code-level reference for the data model and is currently
silent on auth — add a new section). Whichever you choose, provide an
in-memory implementation for unit tests, following the exact pattern of
`InMemoryMetadataStore`: a constructor that takes nothing persistent-specific,
synchronous methods, snapshots (not live views) wherever something is iterated
by a caller.

## What to build, concretely

1. **Models** (new file, e.g. `server/nodes/namenode/auth_models.py` or inside
   `models.py` — your call, but keep `BlockMetadata`/`FileMetadata`/
   `DirectoryMetadata` focused on the namespace and put auth models
   separately for readability):
   - `User(username, password_hash, role, created_at, totp_enabled=False)`
   - `ApiKey(key_id, user_id, name, key_hash, created_at, revoked_at=None)`
   - `Group(name, owner, members: set[str])`
   - `AclEntry(path, subject, permission)` — `subject` format `"user:x"` /
     `"group:y"`, `permission` is an enum `READ`/`READ_WRITE`/`NONE` or similar.

2. **Store interface + in-memory implementation** per the design choice above.
   Needs at minimum:
   - `create_user`, `get_user(username)`, `verify_password`
   - `create_api_key`, `get_api_key_by_hash` (for lookup during auth), `revoke_api_key`, `list_api_keys(user)`
   - `create_group`, `get_group`, `update_group_members`
   - `set_acl(path, subject, permission)`, `get_acl(path)` (direct entries only; inheritance resolution is a core-level concern, see below)

3. **Permission resolution** (this is core logic, belongs in `core.py` or a
   new `permissions.py` module, tested as pure logic like `placement.py` was):
   - `effective_permission(user, path) -> Permission`: walks from the path up
     to the root, collecting ACL entries that apply (direct user match, group
     match via membership, inherited from ancestors), and the owner check,
     returning the **union** (most permissive wins, except explicit `"none"`
     — read `API.md` carefully for whether `"none"` can revoke an inherited
     grant; if the contract is ambiguous, pick the stricter interpretation —
     **explicit `none` on a closer path overrides an inherited grant from a
     further ancestor** — and document the choice in your PR).
   - This needs to be efficient-ish (it's called on every operation) but
     correctness first; a few directory levels is not a hot path yet.

4. **Wire it into `core.py`**: add `user: str` parameters where missing, call
   permission checks, raise `DfshaError(ErrorCode.FORBIDDEN, ...)` on denial.
   **Do not break the existing ~250 tests** — they call these methods without
   auth context today. You will need to either:
   - add an `admin`/"system" bypass used by the existing tests (simplest:
     tests continue to pass an owner who matches, or you add a `superuser`
     concept used only in tests), or
   - update the existing test fixtures in `server/nodes/namenode/tests/` to
     set up users and pass them through.

   Prefer updating the fixtures (`conftest.py`, `helpers.py`) over adding a
   bypass — a bypass that leaks into production code is a security hole
   waiting to happen. Budget real time for this: touching ~250 existing tests
   is the bulk of the work in this issue.

5. **JWT issuance and verification** (`server/common/` is the right place —
   this is shared with the DataNode eventually for... actually no, DataNodes
   use block tokens, not JWT; JWT is NameNode-only, so put it in
   `server/nodes/namenode/` or keep it in `server/common/jwt_auth.py` if you
   think it's generically useful). Needs:
   - `issue_jwt(user, secret, ttl_s, now) -> str`
   - `verify_jwt(token, secret, now) -> claims` raising `DfshaError(UNAUTHORIZED)` on expiry/bad signature, mirroring the pattern in `server/common/tokens.py` exactly (same error codes, same "signature invalid vs claims invalid" distinction logic if applicable — JWT libraries do this for you, but the *wrapping* into `DfshaError` should match).

6. **Audit log hook**: a simple append-only in-memory list of
   `AuditEntry(timestamp, user, operation, path, result, request_id)`, with a
   method on the core (or the store) to record one, called from every
   mutating operation and every permission denial. `request_id` can be `None`
   for now (it's a REST-layer concern from `API.md` Section 1 — "Tracing" —
   that issue 02 will plumb through).

## Tests you must write

Follow the existing test style exactly: `pytest`, fixtures in `conftest.py` /
`helpers.py`, a `FakeClock`, descriptive test names as full sentences
(`test_a_user_without_write_permission_cannot_mkdir`), grouped in classes by
topic (`TestRegistration`, `TestLogin`, `TestAcl`, `TestPermissionInheritance`, etc).

Minimum scenarios:
- Register, login (success, wrong password, unknown user), API key creation
  and revocation, a revoked key stops authenticating.
- Owner always has `rw` even with no ACL entries.
- A user with no ACL entry and not the owner gets `403` on every operation.
- ACL grant on a parent directory is inherited by a file created later inside it.
- ACL grant via group membership works; removing from the group revokes it.
- Explicit `"none"` on a subdirectory overrides an inherited grant (per your
  documented interpretation).
- `admin` role bypasses ACL (check `ARCHITECTURE.md`/`API.md` for whether this
  is explicit — if not stated, assume admin is for the admin endpoints only,
  **not** a blanket ACL bypass, and note this as an open question in your PR).
- Audit log gets an entry for a denied operation and for a successful one.

## Acceptance checklist

- [ ] Design choice (Option A/B above) documented in the PR description and in `docs/NAMENODE_MODELS.md`.
- [ ] All ~250 existing NameNode tests still pass, updated to go through auth rather than bypassing it.
- [ ] New tests for every scenario above, following existing naming/style conventions.
- [ ] `ruff check .` clean.
- [ ] `docs/API.md` changelog entry added (bump to next version) if any wire-visible behavior changed (it shouldn't — this issue is server-internal plus matching the already-documented auth endpoints' *backing logic*, not new endpoints).
- [ ] PR description states clearly: what is done, what is deliberately deferred (TOTP verification itself, PostgreSQL persistence, REST endpoints), and the permission-inheritance interpretation chosen.
