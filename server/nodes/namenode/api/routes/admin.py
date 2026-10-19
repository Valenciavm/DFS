"""Admin and Health routes: /health, /v1/admin/*."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from server.nodes.namenode.api.deps import get_current_user, get_namenode
from server.nodes.namenode.api.schemas import ClusterStatsResponse, HealthResponse
from server.nodes.namenode.auth_models import User
from server.nodes.namenode.core import Namenode

router = APIRouter()


@router.get("/health", status_code=status.HTTP_200_OK, response_model=HealthResponse)
async def health(
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> HealthResponse:
    # Cluster is ok if there is at least one live node (or degraded if 0 nodes)
    live_count = sum(1 for n in nn.cluster.datanodes.values() if n.is_alive)
    status_str = "ok" if live_count > 0 or not nn.cluster.datanodes else "degraded"
    return HealthResponse(
        status=status_str,
        role="leader",
        version="v1",
    )


@router.get("/v1/admin/stats", status_code=status.HTTP_200_OK, response_model=ClusterStatsResponse)
async def get_cluster_stats(
    user: Annotated[User, Depends(get_current_user)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> ClusterStatsResponse:
    from server.common.errors import DfshaError, ErrorCode

    if user.role != "admin":
        raise DfshaError(ErrorCode.FORBIDDEN, "admin role required to view stats")

    stats = nn.stats()
    nodes = list(nn.cluster.datanodes.values())
    total_capacity = sum(n.capacity_bytes for n in nodes)
    total_free = sum(n.free_bytes() for n in nodes)
    total_used = sum(n.used_bytes for n in nodes)

    return ClusterStatsResponse(
        total_capacity_bytes=total_capacity,
        total_used_bytes=total_used,
        total_free_bytes=total_free,
        live_nodes=stats.live_nodes,
        dead_nodes=stats.dead_nodes,
        total_files=stats.files,
        total_blocks=stats.blocks,
        under_replicated_blocks=stats.under_replicated_blocks,
        over_replicated_blocks=0,
        missing_blocks=0,
    )
