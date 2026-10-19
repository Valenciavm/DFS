"""Pydantic schemas for the NameNode REST API, matching docs/API.md."""
from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Section 3: Error format
# ---------------------------------------------------------------------------

class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


# ---------------------------------------------------------------------------
# Section 4.1: Authentication schemas
# ---------------------------------------------------------------------------

class RegisterRequest(BaseModel):
    username: str
    password: str


class RegisterResponse(BaseModel):
    username: str


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str | None = None
    token_type: str | None = "bearer"
    expires_in: int | None = None
    mfa_required: bool | None = None
    challenge: str | None = None


class Login2FARequest(BaseModel):
    challenge: str
    code: str


class CreateApiKeyRequest(BaseModel):
    name: str


class CreateApiKeyResponse(BaseModel):
    key_id: str
    name: str
    api_key: str
    created_at: str


class ApiKeyItem(BaseModel):
    key_id: str
    name: str
    created_at: str


# ---------------------------------------------------------------------------
# Section 4.3: Namespace / Filesystem schemas
# ---------------------------------------------------------------------------

class MkdirRequest(BaseModel):
    path: str


class MoveRequest(BaseModel):
    src: str
    dst: str


class DirEntrySchema(BaseModel):
    name: str
    type: Literal["file", "directory"]
    size: int
    modified_at: str
    owner: str


class LsResponse(BaseModel):
    entries: list[DirEntrySchema]
    next_cursor: str | None = None


class StatResponse(BaseModel):
    path: str
    type: Literal["file", "directory"]
    size: int
    created_at: str
    modified_at: str
    owner: str
    group: str | None = None
    blocks: int | None = None
    replication: int | None = None
    committed: bool | None = None


class AclSubject(BaseModel):
    subject: str
    permission: Literal["none", "r", "rw"]


class GetAclResponse(BaseModel):
    path: str
    owner: str
    group: str | None = None
    entries: list[AclSubject]


class SetAclRequest(BaseModel):
    path: str
    entries: list[AclSubject]


# ---------------------------------------------------------------------------
# Section 4.4: Locks schemas
# ---------------------------------------------------------------------------

class LockRequest(BaseModel):
    path: str
    type: Literal["shared", "exclusive"] = "exclusive"
    ttl_s: int | None = None


class LockResponse(BaseModel):
    lock_id: str
    path: str
    type: Literal["shared", "exclusive"]
    expires_at: str


# ---------------------------------------------------------------------------
# Section 5: Files schemas
# ---------------------------------------------------------------------------

class PutFileRequest(BaseModel):
    path: str
    size: int
    replication: int | None = None


class DataNodePublicSchema(BaseModel):
    node_id: str
    addr: str  # public_addr reachable from the user's machine


class PlannedBlockSchema(BaseModel):
    block_id: str
    index: int
    size: int
    pipeline: list[DataNodePublicSchema]
    token: str


class PutFileResponse(BaseModel):
    file_id: str
    path: str
    block_size: int
    replication: int
    blocks: list[PlannedBlockSchema]


class CommittedBlockSchema(BaseModel):
    index: int
    block_id: str
    size: int
    checksum: str


class CommitFileRequest(BaseModel):
    file_id: str
    blocks: list[CommittedBlockSchema]


class CommitFileResponse(BaseModel):
    file_id: str
    path: str
    status: Literal["committed"]
    size: int
    blocks_committed: int


class AbortFileRequest(BaseModel):
    file_id: str
    reason: str | None = None


class BlockLocationSchema(BaseModel):
    block_id: str
    index: int
    size: int
    checksum: str
    replicas: list[DataNodePublicSchema]
    token: str


class GetFileResponse(BaseModel):
    file_id: str
    path: str
    size: int
    blocks: list[BlockLocationSchema]


class RenewTokenRequest(BaseModel):
    block_ids: list[str]


class RenewTokenResponse(BaseModel):
    tokens: dict[str, str]


# ---------------------------------------------------------------------------
# Section 7: DataNode schemas (DataNode -> NameNode)
# ---------------------------------------------------------------------------

class RegisterNodeRequest(BaseModel):
    node_id: str
    internal_addr: str
    public_addr: str
    capacity_bytes: int
    free_bytes: int


class RegisterNodeResponse(BaseModel):
    status: Literal["registered"]
    heartbeat_interval_s: int
    block_report_interval_s: int
    term: int = 1


class HeartbeatRequest(BaseModel):
    node_id: str
    free_bytes: int
    used_bytes: int


class NodeCommandSchema(BaseModel):
    cmd: Literal["replicate", "delete"]
    block_id: str
    targets: list[str] | None = None
    token: str | None = None


class HeartbeatResponse(BaseModel):
    term: int = 1
    commands: list[NodeCommandSchema]


class BlockReportBlock(BaseModel):
    block_id: str
    size: int
    checksum: str


class BlockReportRequest(BaseModel):
    node_id: str
    blocks: list[BlockReportBlock]


class BlockReportResponse(BaseModel):
    status: Literal["ok"]
    term: int = 1
    commands: list[NodeCommandSchema]


class BlockReceivedRequest(BaseModel):
    node_id: str
    block_id: str
    size: int
    checksum: str


class BlockReceivedResponse(BaseModel):
    status: Literal["ok"]


class BlockCorruptRequest(BaseModel):
    node_id: str
    block_id: str
    reason: str | None = None


class BlockCorruptResponse(BaseModel):
    status: Literal["ok"]


# ---------------------------------------------------------------------------
# Section 8 & 9: Admin, Health & HA schemas
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "not_leader"]
    role: Literal["leader", "standby"] = "leader"
    version: str = "v1"


class ClusterStatsResponse(BaseModel):
    total_capacity_bytes: int
    total_used_bytes: int
    total_free_bytes: int
    live_nodes: int
    dead_nodes: int
    total_files: int
    total_blocks: int
    under_replicated_blocks: int
    over_replicated_blocks: int
    missing_blocks: int


class HAStatusResponse(BaseModel):
    role: Literal["leader", "standby"]
    leader_id: str
    generation: int
    lease_expires_in_s: int | None = None
