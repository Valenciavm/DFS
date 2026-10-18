"""JWT access-token issuance and verification for NameNode clients."""
from __future__ import annotations

import jwt

from server.common.errors import DfshaError, ErrorCode


def issue_jwt(user: str, secret: str, ttl_s: int, now: float) -> str:
    if not secret:
        raise ValueError("JWT_SECRET must be configured")
    if ttl_s <= 0:
        raise ValueError("JWT lifetime must be positive")
    return jwt.encode(
        {"sub": user, "exp": int(now) + ttl_s, "iat": int(now), "amr": ["pwd"]},
        secret,
        algorithm="HS256",
    )


def verify_jwt(token: str, secret: str, now: float) -> dict[str, object]:
    if not secret:
        raise ValueError("JWT_SECRET must be configured")
    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options={"verify_exp": False, "verify_iat": False},
        )
    except jwt.InvalidTokenError:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "invalid access token") from None
    if not isinstance(claims.get("sub"), str) or not claims["sub"]:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "access token has no subject")
    try:
        expires = int(claims["exp"])
    except (KeyError, TypeError, ValueError):
        raise DfshaError(ErrorCode.UNAUTHORIZED, "access token has invalid expiry") from None
    if expires <= now:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "access token expired")
    return claims


__all__ = ["issue_jwt", "verify_jwt"]
