"""Request-scoped authenticated principal context for core calls."""
from __future__ import annotations

from contextvars import ContextVar, Token

from server.common.errors import DfshaError, ErrorCode

_principal: ContextVar[str | None] = ContextVar("dfsha_principal", default=None)


def set_principal(username: str) -> Token[str | None]:
    """Set the principal for a request; the caller must reset the returned token."""
    return _principal.set(username)


def reset_principal(token: Token[str | None]) -> None:
    _principal.reset(token)


def current_principal() -> str:
    username = _principal.get()
    if not username:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "an authenticated principal is required")
    return username
