"""File transfer routes for clients: /v1/files/*."""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.api.deps import get_current_principal, get_namenode
from server.nodes.namenode.api.schemas import (
    AbortFileRequest,
    BlockLocationSchema,
    CommitFileRequest,
    CommitFileResponse,
    DataNodePublicSchema,
    GetFileResponse,
    PlannedBlockSchema,
    PutFileRequest,
    PutFileResponse,
    RenewTokenRequest,
    RenewTokenResponse,
)
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.results import CommittedBlock

router = APIRouter()


@router.post("/put", status_code=status.HTTP_201_CREATED, response_model=PutFileResponse)
async def put_file(
    req: PutFileRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> PutFileResponse:
    plan = nn.prepare_file_upload(
        path=req.path,
        size_bytes=req.size,
        owner=user,
        overwrite=req.overwrite,
        user=user,
    )
    blocks_schema = [
        PlannedBlockSchema(
            block_id=pb.block.block_id,
            index=pb.block.index,
            size=pb.block.size_bytes,
            pipeline=[
                DataNodePublicSchema(
                    node_id=node.node_id,
                    addr=node.public_addr,
                    internal_addr=node.internal_addr,
                )
                for node in pb.pipeline
            ],
            token=pb.token,
        )
        for pb in plan.blocks
    ]
    return PutFileResponse(
        file_id=plan.file.file_id,
        path=plan.path,
        block_size=nn.settings.block_size,
        replication=plan.file.replication,
        blocks=blocks_schema,
    )


async def _commit_file(
    req: CommitFileRequest,
    file_id: str,
    user: str,
    nn: Namenode,
) -> CommitFileResponse:
    committed_blocks = [
        CommittedBlock(
            index=b.index,
            block_id=b.block_id,
            size=b.size,
            checksum=b.checksum,
        )
        for b in req.blocks
    ]
    file_meta = nn.commit_file_upload(
        file_id=file_id,
        blocks=committed_blocks,
        file_checksum=req.file_checksum,
        user=user,
    )
    path = nn.store.path_of(file_meta.file_id)
    return CommitFileResponse(
        file_id=file_meta.file_id,
        path=path,
        status="committed",
        size=file_meta.size_bytes,
        blocks_committed=len(file_meta.blocks),
    )


@router.post("/commit", status_code=status.HTTP_200_OK, response_model=CommitFileResponse)
async def commit_file_legacy(
    req: CommitFileRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> CommitFileResponse:
    if req.file_id is None:
        raise DfshaError(ErrorCode.BAD_REQUEST, "file_id is required")
    return await _commit_file(req, req.file_id, user, nn)


@router.post("/{file_id}/commit", status_code=status.HTTP_200_OK, response_model=CommitFileResponse)
async def commit_file(
    file_id: str,
    req: CommitFileRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> CommitFileResponse:
    return await _commit_file(req, file_id, user, nn)


@router.post("/abort", status_code=status.HTTP_204_NO_CONTENT)
async def abort_file_legacy(
    req: AbortFileRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> None:
    nn.abort_file_upload(
        file_id=req.file_id,
        user=user,
    )


@router.post("/{file_id}/abort", status_code=status.HTTP_204_NO_CONTENT)
async def abort_file(
    file_id: str,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> None:
    nn.abort_file_upload(file_id=file_id, user=user)


@router.get("/get", status_code=status.HTTP_200_OK, response_model=GetFileResponse)
async def get_file(
    path: Annotated[str, Query(description="Absolute path of the file")],
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> GetFileResponse:
    file_location = nn.get_file_blocks(path=path, user=user)
    blocks_schema = [
        BlockLocationSchema(
            block_id=bl.block.block_id,
            index=bl.block.index,
            size=bl.block.size_bytes,
            checksum=bl.block.checksum or "",
            replicas=[
                DataNodePublicSchema(node_id=dn.node_id, addr=dn.public_addr)
                for dn in bl.replicas
            ],
            token=bl.token,
        )
        for bl in file_location.blocks
    ]
    return GetFileResponse(
        file_id=file_location.file.file_id,
        path=file_location.path,
        size=file_location.file.size_bytes,
        blocks=blocks_schema,
        checksum=file_location.file.checksum,
    )


@router.post("/tokens", status_code=status.HTTP_200_OK, response_model=RenewTokenResponse)
async def renew_tokens(
    req: RenewTokenRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> RenewTokenResponse:
    from server.common.tokens import BlockOp, mint

    now = nn.clock.time()
    tokens: dict[str, str] = {}
    for block_id in req.block_ids:
        # Renew token for writing or reading depending on block state
        tokens[block_id] = mint(
            block_id=block_id,
            op=BlockOp.WRITE,
            principal=user,
            secret=nn.settings.block_token_secret,
            ttl_s=nn.settings.token_ttl_s,
            now=now,
        )
    return RenewTokenResponse(tokens=tokens)
