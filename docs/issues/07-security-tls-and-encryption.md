# Security hardening: TLS/mTLS, AES-256-GCM at rest and TOTP 2FA

**Depends on:** issue 01 for user/auth models, issue 02 for HTTP wiring and
issue 04 for node control traffic. Issue 05's gRPC interceptor is the natural
place for block-token enforcement.
**Branch:** `feat/security-hardening`, from the latest branch containing issues
01, 02, 04 and 05, or split into smaller stacked PRs if the work is too large.
**Unblocks:** issue 09 (secure deployment) and issue 10 (T9–T12, T21–T22).

## Context and rubric reason

The assignment explicitly requires encryption in communications and storage,
access control from password/ACL/API keys up to 2FA, and security between data
and control nodes. This issue closes the security gaps; it is not a stretch
goal.

Read `docs/API.md` Sections 2, 2.1, 2.2, 2.3 and 3.1 plus
`docs/ARCHITECTURE.md` Section 9 and Section 9.1. Do not invent a new security
protocol if the contract already specifies one.

## TLS and mutual TLS

Create a reproducible certificate script/configuration (never commit private
keys):

- one cluster CA for development;
- per-NameNode and per-DataNode certificates with correct DNS/IP SANs;
- Node→NameNode HTTPS requires the node certificate and validates the cluster
  CA;
- DataNode↔DataNode gRPC uses TLS with client certificates;
- client→NameNode HTTPS and client→DataNode gRPC use server-authenticated TLS;
- NameNode↔NameNode control calls use mutual TLS.

Development may use a generated self-signed CA. Production may use managed or
public certificates where appropriate. Secrets/certificates come from mounted
files or environment/secret storage, never Git.

Test that a client without a certificate and one signed by the wrong CA are
rejected. Verify the hostname/SAN checks, not just `CERT_NONE` or an encrypted
socket. Document local certificate generation and rotation.

Keep `X-Node-Key` as a second application-level check for node REST calls,
while mTLS provides cryptographic node identity. Do not treat one shared header
secret as a replacement for mTLS.

## AES-256-GCM at rest

Implement in the DataNode storage layer, not in the NameNode:

- `DATANODE_MASTER_KEY` is 32 random bytes encoded in base64;
- derive a per-block key with HKDF-SHA256 using `block_id` as salt/info;
- encrypt plaintext bytes with AES-256-GCM;
- use a fresh random 96-bit nonce per block;
- store a versioned header containing magic/version, block id or metadata,
  nonce, plaintext size and authentication tag/ciphertext;
- verify the GCM tag on every read; on failure return `DATA_LOSS`, notify
  `block-corrupt` and delete the bad copy;
- replication decrypts at the source over mTLS and re-encrypts at the target
  with that target's key.

Do not write plaintext temporary files to the final storage directory. If a
stream must use a temporary file, ensure it is removed on every failure and
explain the exposure in the design. Use a maintained cryptography library
(`cryptography` is acceptable) and pin the dependency.

Preserve the logical checksum contract: SHA-256 is over plaintext bytes exactly
as sent by the client, not ciphertext. Existing unencrypted demo data must be
rejected or migrated explicitly; do not silently interpret old `.dat` files as
encrypted.

## Block-token interceptor

Complete the local gRPC interceptor using `server/common/tokens.py`:

- `WriteBlock` needs a valid write token bound to its block;
- `ReadBlock` and `StatBlock` need a valid read token;
- malformed/bad/expired token → `UNAUTHENTICATED`;
- valid token for another block/op → `PERMISSION_DENIED`;
- add `x-dfsha-error-code` trailing metadata;
- replication commands use the `system` principal but are still block/op-bound.

The interceptor must validate before accepting/reading a stream body.

## TOTP 2FA

Implement the contract in `API.md` Section 4.1.1 and design in
`ARCHITECTURE.md` Section 9.1:

- `pyotp` RFC 6238, six digits, 30-second step, ±1 verification window;
- `/v1/auth/2fa/enroll` creates a secret and `otpauth://` URI/QR, but stays
  disabled until the first valid `/verify` code;
- `/v1/auth/2fa/verify` enables it and returns ten single-use recovery codes
  once;
- `/v1/auth/login` returns either a JWT or `{mfa_required, challenge}`;
- `/v1/auth/login/2fa` exchanges the short-lived single-use challenge plus TOTP
  or recovery code for a JWT;
- secrets encrypted at rest, recovery codes hashed, last accepted timestep
  stored to prevent replay, attempts rate-limited;
- JWT `amr` is `["pwd","otp"]` for two-factor sessions;
- API keys deliberately bypass interactive 2FA for non-interactive scripts;
- disabling 2FA requires password and current code.

Use the already pinned `argon2-cffi`, `pyjwt`, `pyotp` and `qrcode` dependencies.

## Tests and acceptance

- [ ] TLS handshake success/failure and SAN/CA validation tests.
- [ ] mTLS rejects missing/wrong client certificates on node endpoints.
- [ ] Plaintext is not visible in stored encrypted block files.
- [ ] Correct GCM decrypts; changed ciphertext/header returns `DATA_LOSS` and
  schedules repair.
- [ ] Plaintext checksum survives encryption/replication/read.
- [ ] Token interceptor covers every RPC and status/metadata mapping.
- [ ] TOTP enrollment, wrong code, clock skew, replay, recovery-code one-time
  use, rate limit and disable flow are tested.
- [ ] JWT with `amr` is correct; API key remains usable for automation.
- [ ] No secrets/private keys are committed; `ruff` and full tests pass.
- [ ] Update API changelog and security/runbook documentation.

## Acceptance checklist

- [ ] TLS works on every external/internal channel and mTLS rejects missing or wrong certificates.
- [ ] Node certificates use valid SANs and private keys/certificates are never committed.
- [ ] Blocks are AES-256-GCM encrypted at rest with per-block derived keys and plaintext checksums preserved.
- [ ] GCM corruption maps to `DATA_LOSS`, removes the bad replica and triggers repair.
- [ ] Block-token interceptor authenticates every protected RPC and emits the contract error metadata.
- [ ] TOTP enrollment, login challenge, replay prevention, recovery codes and rate limiting pass tests.
- [ ] JWT `amr` claims and API-key non-interactive behavior match `API.md`.
- [ ] Full `pytest` and `ruff check .` pass; security docs and API changelog are updated.
