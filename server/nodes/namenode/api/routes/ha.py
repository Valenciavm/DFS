"""HA Status route: /v1/ha/status."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from server.nodes.namenode.api.deps import get_namenode
from server.nodes.namenode.api.schemas import HAStatusResponse
from server.nodes.namenode.core import Namenode

router = APIRouter()


@router.get("/status", status_code=status.HTTP_200_OK, response_model=HAStatusResponse)
async def ha_status(
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> HAStatusResponse:
    # Per Issue 02 spec, before Issue 06 lands, report leader unconditionally
    return HAStatusResponse(
        role="leader",
        leader_id="nn1",
        generation=nn.term,
        lease_expires_in_s=nn.settings.lease_s,
    )
