"""Block tokens: the capability that lets a client talk to a DataNode.

``API.md`` Section 2.1. The NameNode mints a token only after checking
permissions; the DataNode verifies it **locally**, in a gRPC interceptor, with
no call back to the NameNode. That is what keeps the control plane off the data
path, and it is why the token has to be self-contained and cheap to check.

Wire format::

    token = base64url(payload_json) + "." + base64url(HMAC-SHA256(secret, base64url(payload_json)))

Note the signature covers the **encoded** payload, not the raw JSON, so
verification never has to re-serialize (and therefore cannot be tricked by a
different but equivalent JSON encoding).

A token is bound to one `block_id` and one operation, so it is useless for any
other block: stealing a read token for one block does not open the file, let
alone the namespace.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from enum import Enum

from server.common.errors import DfshaError, ErrorCode


class BlockOp(str, Enum):
    """The `op` claim. `READ` also covers `StatBlock` (API.md Section 2.1)."""

    READ = "r"
    WRITE = "w"


#: Principal used for tokens the NameNode issues to itself for replication
#: commands, where no end user is involved (API.md Section 2.1).
SYSTEM_PRINCIPAL = "system"


@dataclass(frozen=True)
class BlockToken:
    """A verified token's claims."""

    block_id: str
    op: BlockOp
    user: str
    expires_at: int

    def allows(self, block_id: str, op: BlockOp) -> bool:
        """Whether these claims authorize `op` on `block_id`.

        The operations are distinct, not ordered: a write token is not also a
        read token.
        """
        return self.block_id == block_id and self.op is op


def _b64url_encode(raw: bytes) -> str:
    """base64url without padding, so the token stays safe in headers and URLs."""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _sign(secret: str, encoded_payload: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), encoded_payload.encode("ascii"), hashlib.sha256)
    return _b64url_encode(mac.digest())


def mint(
    block_id: str,
    op: BlockOp,
    user: str,
    secret: str,
    ttl_s: int,
    *,
    now: float | None = None,
) -> str:
    """Issues a token for one block and one operation.

    `now` is injectable so tests do not have to sleep.
    """
    if not secret:
        raise ValueError("refusing to mint a block token with an empty secret")
    issued_at = time.time() if now is None else now
    payload = {
        "bid": block_id,
        "op": op.value,
        "usr": user,
        "exp": int(issued_at) + ttl_s,
    }
    # separators without spaces and sorted keys keep the encoding stable
    encoded = _b64url_encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    return f"{encoded}.{_sign(secret, encoded)}"


def verify(
    token: str,
    secret: str,
    *,
    block_id: str | None = None,
    op: BlockOp | None = None,
    now: float | None = None,
) -> BlockToken:
    """Validates a token and returns its claims.

    Checks, in the order a DataNode interceptor must apply them
    (``API.md`` Section 2.1):

    1. shape and signature → `UNAUTHORIZED`
    2. expiry → `UNAUTHORIZED`
    3. bound `block_id` and `op`, when given → `FORBIDDEN`

    The split matters: a bad signature means "we do not know who you are",
    while a valid token aimed at another block means "we know who you are and
    you may not do this", which is 401 against 403 on REST and
    `UNAUTHENTICATED` against `PERMISSION_DENIED` on gRPC.
    """
    if not secret:
        raise ValueError("refusing to verify a block token with an empty secret")

    encoded, _, signature = token.partition(".")
    if not encoded or not signature:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "malformed block token")

    # constant-time comparison: a timing side channel here would leak the secret
    if not hmac.compare_digest(signature, _sign(secret, encoded)):
        raise DfshaError(ErrorCode.UNAUTHORIZED, "bad block token signature")

    try:
        payload = json.loads(_b64url_decode(encoded))
        claims = BlockToken(
            block_id=payload["bid"],
            op=BlockOp(payload["op"]),
            user=payload["usr"],
            expires_at=int(payload["exp"]),
        )
    except Exception:  # noqa: BLE001 - any malformed payload is simply not a credential
        # only reachable with a payload signed by our own secret, so this is a
        # bug or a rotated format rather than an attack
        raise DfshaError(ErrorCode.UNAUTHORIZED, "unreadable block token payload") from None

    current = time.time() if now is None else now
    if claims.expires_at <= current:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "expired block token")

    if block_id is not None and op is not None and not claims.allows(block_id, op):
        raise DfshaError(
            ErrorCode.FORBIDDEN,
            f"token is valid but not for {op.value} on {block_id}",
        )

    return claims


__all__ = ["SYSTEM_PRINCIPAL", "BlockOp", "BlockToken", "mint", "verify"]
