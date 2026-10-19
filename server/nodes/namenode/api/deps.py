"""Dependency injection helpers for FastAPI routes in NameNode."""
from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Header, Request

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.auth_context import reset_principal, set_principal
from server.nodes.namenode.auth_models import User
from server.nodes.namenode.core import Namenode

_shared_namenode: Namenode | None = None


def set_shared_namenode(nn: Namenode) -> None:
    global _shared_namenode
    _shared_namenode = nn


def get_namenode() -> Namenode:
    if _shared_namenode is None:
        raise RuntimeError("NameNode instance has not been initialized in the API layer.")
    return _shared_namenode


def get_shared_namenode() -> Namenode | None:
    return _shared_namenode


async def get_current_principal(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
    nn: Annotated[Namenode, Depends(get_namenode)] = None,
) -> str:
    """Extract and authenticate principal from Authorization Bearer JWT or X-API-Key."""
    username: str | None = None

    if authorization:
        parts = authorization.strip().split()
        if len(parts) == 2:
            scheme, token = parts
            if scheme.lower() == "bearer":
                user = nn.authenticate_access_token(token)
                username = user.username
            elif scheme.lower() == "apikey":
                user = nn.authenticate_api_key(token)
                username = user.username
            else:
                raise DfshaError(ErrorCode.UNAUTHORIZED, f"Unsupported authorization scheme: {scheme}")
        else:
            raise DfshaError(ErrorCode.UNAUTHORIZED, "Invalid Authorization header format")
    elif x_api_key:
        user = nn.authenticate_api_key(x_api_key)
        username = user.username

    if not username:
        raise DfshaError(ErrorCode.UNAUTHORIZED, "Missing authentication credentials")

    # Bind request-scoped principal to contextvars
    token = set_principal(username)
    request.state.principal_token = token
    return username


async def get_current_user(
    principal: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> User:
    user = nn.auth_store.get_user(principal)
    if not user:
        raise DfshaError(ErrorCode.USER_NOT_FOUND, f"User {principal} not found")
    return user
