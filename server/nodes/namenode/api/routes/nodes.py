"""DataNode facing routes: /v1/nodes/*."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from server.nodes.namenode.api.deps import get_namenode
from server.nodes.namenode.api.schemas import (
    BlockCorruptRequest,
    BlockCorruptResponse,
    BlockReceivedRequest,
    BlockReceivedResponse,
    BlockReportRequest,
    BlockReportResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    NodeCommandSchema,
    RegisterNodeRequest,
    RegisterNodeResponse,
)
from server.nodes.namenode.cluster_state import Command
from server.nodes.namenode.core import Namenode

router = APIRouter()


def _to_command_schema(cmd: Command, term: int, nn: Namenode) -> NodeCommandSchema:
    targets = [
        nn.cluster.datanodes[target_id].internal_addr
        for target_id in cmd.targets
        if target_id in nn.cluster.datanodes
    ]
    return NodeCommandSchema(
        cmd=cmd.cmd,
        block_id=cmd.block_id,
        term=term,
        targets=targets if targets else None,
        token=cmd.token if cmd.token else None,
    )


@router.post(
    "/register", status_code=status.HTTP_200_OK, response_model=RegisterNodeResponse
)
async def register_datanode(
    req: RegisterNodeRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> RegisterNodeResponse:
    used_bytes = max(0, req.capacity_bytes - req.free_bytes)
    nn.register_datanode(
        node_id=req.node_id,
        internal_addr=req.internal_addr,
        public_addr=req.public_addr,
        capacity_bytes=req.capacity_bytes,
        used_bytes=used_bytes,
    )
    return RegisterNodeResponse(
        status="registered",
        heartbeat_interval_s=nn.settings.heartbeat_interval_s,
        block_report_interval_s=nn.settings.block_report_interval_s,
        term=nn.term,
    )


@router.post(
    "/heartbeat", status_code=status.HTTP_200_OK, response_model=HeartbeatResponse
)
async def heartbeat(
    req: HeartbeatRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> HeartbeatResponse:
    commands = nn.record_heartbeat(
        node_id=req.node_id,
        free_bytes=req.free_bytes,
        used_bytes=req.used_bytes,
    )
    return HeartbeatResponse(
        term=nn.term,
        commands=[_to_command_schema(cmd, nn.term, nn) for cmd in commands],
    )


@router.post(
    "/block-report", status_code=status.HTTP_200_OK, response_model=BlockReportResponse
)
async def block_report(
    req: BlockReportRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> BlockReportResponse:
    from server.nodes.namenode.results import ReportedBlock

    reported_blocks = [
        ReportedBlock(block_id=b.block_id, size=b.size, checksum=b.checksum)
        for b in req.blocks
    ]
    commands = nn.process_block_report(
        node_id=req.node_id,
        blocks=reported_blocks,
    )
    return BlockReportResponse(
        status="ok",
        term=nn.term,
        commands=[_to_command_schema(cmd, nn.term, nn) for cmd in commands],
    )


@router.post(
    "/block-received",
    status_code=status.HTTP_200_OK,
    response_model=BlockReceivedResponse,
)
async def block_received(
    req: BlockReceivedRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> BlockReceivedResponse:
    nn.record_block_received(
        node_id=req.node_id,
        block_id=req.block_id,
        size=req.size,
        checksum=req.checksum,
    )
    return BlockReceivedResponse(status="ok", term=nn.term)


@router.post(
    "/block-corrupt",
    status_code=status.HTTP_200_OK,
    response_model=BlockCorruptResponse,
)
async def block_corrupt(
    req: BlockCorruptRequest,
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> BlockCorruptResponse:
    nn.record_block_corrupt(
        node_id=req.node_id,
        block_id=req.block_id,
    )
    return BlockCorruptResponse(status="ok", term=nn.term)
