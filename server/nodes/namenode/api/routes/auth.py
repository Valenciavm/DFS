"""Auth routes: /v1/auth/*."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, status

from server.nodes.namenode.api.deps import get_current_principal, get_namenode
from server.nodes.namenode.api.schemas import (
    ApiKeyItem,
    CreateApiKeyRequest,
    CreateApiKeyResponse,
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
)
from server.nodes.namenode.core import Namenode

router = APIRouter()


@router.post("/register", status_code=status.HTTP_201_CREATED, response_model=RegisterResponse)
async def register(
    req: RegisterRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> RegisterResponse:
    res = nn.register_user(username=req.username, password=req.password)
    return RegisterResponse(username=res["username"])


@router.post("/login", status_code=status.HTTP_200_OK, response_model=LoginResponse)
async def login(
    req: LoginRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> LoginResponse:
    res = nn.login(username=req.username, password=req.password)
    return LoginResponse(
        access_token=res.get("access_token"),
        token_type=res.get("token_type", "bearer"),
        expires_in=res.get("expires_in"),
        mfa_required=res.get("mfa_required"),
        challenge=res.get("challenge"),
    )


@router.post("/apikeys", status_code=status.HTTP_201_CREATED, response_model=CreateApiKeyResponse)
async def create_api_key(
    req: CreateApiKeyRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> CreateApiKeyResponse:
    res = nn.create_api_key(name=req.name, user=user)
    return CreateApiKeyResponse(
        key_id=res["key_id"],
        name=res["name"],
        api_key=res["api_key"],
        created_at=str(res["created_at"]),
    )


@router.get("/apikeys", status_code=status.HTTP_200_OK, response_model=list[ApiKeyItem])
async def list_api_keys(
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> list[ApiKeyItem]:
    keys = nn.list_api_keys(user=user)
    return [
        ApiKeyItem(
            key_id=str(k["key_id"]),
            name=str(k["name"]),
            created_at=str(k["created_at"]),
        )
        for k in keys
    ]


@router.delete("/apikeys/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_api_key(
    key_id: str,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> None:
    nn.revoke_api_key(key_id=key_id, user=user)
