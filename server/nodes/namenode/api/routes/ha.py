"""HA Status route: /v1/ha/status."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, status

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.api.deps import get_current_user, get_namenode
from server.nodes.namenode.api.schemas import HAStatusResponse
from server.nodes.namenode.auth_models import User
from server.nodes.namenode.core import Namenode

router = APIRouter()


@router.get("/status", status_code=status.HTTP_200_OK, response_model=HAStatusResponse)
async def ha_status(
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> HAStatusResponse:
    expires_in = None
    if nn.lease_expires_at is not None:
        expires_in = max(
            0, int(nn.lease_expires_at.timestamp() - datetime.now(UTC).timestamp())
        )
    return HAStatusResponse(
        role="leader" if nn.is_leader else "standby",
        node_id=nn.node_id,
        term=nn.term,
        degraded=nn.degraded,
        leader_url=nn.leader_url,
        lease_expires_at=nn.lease_expires_at,
        leader_id=nn.leader_id,
        generation=nn.term,
        lease_expires_in_s=expires_in,
    )


@router.post("/stepdown", status_code=status.HTTP_200_OK)
async def step_down(
    user: Annotated[User, Depends(get_current_user)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> dict[str, str]:
    if user.role != "admin":
        raise DfshaError(ErrorCode.FORBIDDEN, "admin role required to step down")
    nn.step_down()
    return {"status": "stepped_down"}
