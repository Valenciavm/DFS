"""Group membership routes: /v1/groups/*."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from server.nodes.namenode.api.deps import get_current_principal, get_namenode
from server.nodes.namenode.api.schemas import (
    CreateGroupRequest,
    GroupResponse,
    UpdateGroupMembersRequest,
)
from server.nodes.namenode.core import Namenode

router = APIRouter()


def _response(group) -> GroupResponse:
    return GroupResponse(
        name=group.name,
        owner=group.owner,
        members=sorted(group.members),
    )


@router.post("", status_code=status.HTTP_201_CREATED, response_model=GroupResponse)
async def create_group(
    req: CreateGroupRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> GroupResponse:
    return _response(nn.create_group(req.name, user=user))


@router.get("/{name}", response_model=GroupResponse)
async def get_group(
    name: str,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> GroupResponse:
    return _response(nn.get_group(name, user=user))


@router.put("/{name}/members", response_model=GroupResponse)
async def update_group_members(
    name: str,
    req: UpdateGroupMembersRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> GroupResponse:
    group = nn.add_group_members(
        name,
        add=req.add,
        remove=req.remove,
        user=user,
    )
    return _response(group)
