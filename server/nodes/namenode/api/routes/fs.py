"""Filesystem and Lock routes: /v1/fs/*."""
from __future__ import annotations

import base64
import json
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, status

from server.nodes.namenode.api.deps import get_current_principal, get_namenode
from server.nodes.namenode.api.schemas import (
    AclSubject,
    DirEntrySchema,
    GetAclResponse,
    LockRequest,
    LockResponse,
    LsResponse,
    MkdirRequest,
    MoveRequest,
    SetAclRequest,
    StatResponse,
)
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.models import DirectoryMetadata, FileMetadata

router = APIRouter()


@router.get("/ls", status_code=status.HTTP_200_OK, response_model=LsResponse)
async def list_directory(
    path: Annotated[str, Query(description="Absolute path to list")],
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
    limit: Annotated[int, Query(ge=1, le=10000)] = 1000,
    cursor: Annotated[str | None, Query()] = None,
) -> LsResponse:
    entries = nn.ls(path=path, user=user)

    # Sort entries by name
    entries.sort(key=lambda e: e.name)

    start_idx = 0
    if cursor:
        try:
            decoded = json.loads(base64.b64decode(cursor).decode("utf-8"))
            after_name = decoded.get("after")
            if after_name:
                for idx, entry in enumerate(entries):
                    if entry.name == after_name:
                        start_idx = idx + 1
                        break
        except Exception:
            start_idx = 0

    page_entries = entries[start_idx : start_idx + limit]
    next_cursor = None
    if start_idx + limit < len(entries):
        last_name = page_entries[-1].name
        next_cursor = base64.b64encode(json.dumps({"after": last_name}).encode("utf-8")).decode("utf-8")

    response_items = [
        DirEntrySchema(
            name=e.name,
            type="file" if e.type == "file" else "directory",
            size=e.size_bytes,
            modified_at=str(e.modified_at),
            owner=e.owner,
        )
        for e in page_entries
    ]
    return LsResponse(entries=response_items, next_cursor=next_cursor)


@router.get("/stat", status_code=status.HTTP_200_OK, response_model=StatResponse)
async def stat_path(
    path: Annotated[str, Query(description="Absolute path to inspect")],
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> StatResponse:
    node = nn.stat(path=path, user=user)
    if isinstance(node, DirectoryMetadata):
        return StatResponse(
            path=node.path(),
            type="directory",
            size=0,
            created_at=str(node.created_at),
            modified_at=str(node.modified_at),
            owner=node.owner,
        )
    return StatResponse(
        path=path,
        type="file",
        size=node.size_bytes,
        created_at=str(node.created_at),
        modified_at=str(node.modified_at),
        owner=node.owner,
        blocks=len(node.blocks),
        replication=node.replication,
        committed=(node.status.value == "COMMITTED"),
    )


@router.post("/mkdir", status_code=status.HTTP_201_CREATED)
async def make_directory(
    req: MkdirRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
    parents: bool = False,
) -> dict[str, str]:
    nn.mkdir(path=req.path, owner=user, parents=parents, user=user)
    return {"status": "created", "path": req.path}


@router.delete("/rm", status_code=status.HTTP_200_OK)
async def remove_file(
    path: Annotated[str, Query(description="Path of file to remove")],
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> dict[str, str]:
    nn.rm(path=path, user=user)
    return {"status": "deleted", "path": path}


@router.delete("/rmdir", status_code=status.HTTP_200_OK)
async def remove_directory(
    path: Annotated[str, Query(description="Path of directory to remove")],
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
    recursive: bool = False,
) -> dict[str, object]:
    count = nn.rmdir(path=path, recursive=recursive, user=user)
    return {"status": "deleted", "path": path, "files_removed": count}


@router.post("/mv", status_code=status.HTTP_200_OK)
async def move_path(
    req: MoveRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> dict[str, str]:
    nn.mv(src=req.src, dst=req.dst, user=user)
    return {"status": "moved", "src": req.src, "dst": req.dst}


@router.get("/acl", status_code=status.HTTP_200_OK, response_model=GetAclResponse)
async def get_acl(
    path: Annotated[str, Query(description="Path of file or directory")],
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> GetAclResponse:
    node = nn.stat(path=path, user=user)
    acls = nn.get_acl(path=path, user=user)
    entries = [
        AclSubject(subject=a.subject, permission="r" if a.permission.value == 1 else "rw")
        for a in acls
    ]
    return GetAclResponse(
        path=path,
        owner=node.owner,
        entries=entries,
    )


@router.put("/acl", status_code=status.HTTP_200_OK)
async def set_acl(
    req: SetAclRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> dict[str, str]:
    for entry in req.entries:
        nn.set_acl(path=req.path, subject=entry.subject, permission=entry.permission, user=user)
    return {"status": "ok", "path": req.path}


@router.post("/lock", status_code=status.HTTP_200_OK, response_model=LockResponse)
async def acquire_lock(
    req: LockRequest,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> LockResponse:
    res = nn.lock_path(path=req.path, user=user, mode=req.type, ttl_s=req.ttl_s)
    return LockResponse(
        lock_id=res["lock_id"],
        path=res["path"],
        type=res["type"],
        expires_at=str(res["expires_at"]),
    )


@router.post("/lock/{lock_id}/renew", status_code=status.HTTP_200_OK)
async def renew_lock(
    lock_id: str,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
    ttl_s: int | None = None,
) -> dict[str, str]:
    res = nn.renew_lock(lock_id=lock_id, user=user, ttl_s=ttl_s)
    return {"lock_id": lock_id, "expires_at": str(res["expires_at"])}


@router.delete("/lock/{lock_id}", status_code=status.HTTP_200_OK)
async def release_lock(
    lock_id: str,
    user: Annotated[str, Depends(get_current_principal)],
    nn: Annotated[Namenode, Depends(get_namenode)],
) -> dict[str, str]:
    nn.unlock_path(lock_id=lock_id, user=user)
    return {"status": "unlocked", "lock_id": lock_id}
