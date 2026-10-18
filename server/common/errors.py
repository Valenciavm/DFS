"""The single error vocabulary of DFSha.

``API.md`` Section 3 defines one JSON error body for REST and Section 3.1 maps
the same codes onto gRPC statuses for the block channel, so that one failure
reads identically in a client, in a DataNode log and in the NameNode audit log.

Both mappings live here so they cannot diverge::

    raise DfshaError(ErrorCode.FILE_NOT_FOUND, "/a.txt does not exist")

The REST layer turns that into ``404`` with the body from Section 3; the gRPC
interceptor turns it into ``NOT_FOUND`` plus the ``x-dfsha-error-code``
trailing metadata.
"""
from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """Application error codes. The wire value is the name itself."""

    # 400
    BAD_REQUEST = "BAD_REQUEST"
    NOT_A_DIRECTORY = "NOT_A_DIRECTORY"
    IS_A_DIRECTORY = "IS_A_DIRECTORY"
    # 401 / 403
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    # 404
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    DIR_NOT_FOUND = "DIR_NOT_FOUND"
    BLOCK_NOT_FOUND = "BLOCK_NOT_FOUND"
    GROUP_NOT_FOUND = "GROUP_NOT_FOUND"
    USER_NOT_FOUND = "USER_NOT_FOUND"
    # 409
    ALREADY_EXISTS = "ALREADY_EXISTS"
    UPLOAD_IN_PROGRESS = "UPLOAD_IN_PROGRESS"
    DIR_NOT_EMPTY = "DIR_NOT_EMPTY"
    INSUFFICIENT_REPLICAS = "INSUFFICIENT_REPLICAS"
    FILE_NOT_COMMITTED = "FILE_NOT_COMMITTED"
    NODE_NOT_REGISTERED = "NODE_NOT_REGISTERED"
    # 413 / 416 / 422 / 423 / 429
    FILE_TOO_LARGE = "FILE_TOO_LARGE"
    RANGE_NOT_SATISFIABLE = "RANGE_NOT_SATISFIABLE"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    LOCKED = "LOCKED"
    RATE_LIMITED = "RATE_LIMITED"
    # 503 / 507
    NOT_ENOUGH_DATANODES = "NOT_ENOUGH_DATANODES"
    NOT_LEADER = "NOT_LEADER"
    NO_SPACE = "NO_SPACE"


#: Code to HTTP status (API.md Section 3).
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.BAD_REQUEST: 400,
    ErrorCode.NOT_A_DIRECTORY: 400,
    ErrorCode.IS_A_DIRECTORY: 400,
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.FORBIDDEN: 403,
    ErrorCode.FILE_NOT_FOUND: 404,
    ErrorCode.DIR_NOT_FOUND: 404,
    ErrorCode.BLOCK_NOT_FOUND: 404,
    ErrorCode.GROUP_NOT_FOUND: 404,
    ErrorCode.USER_NOT_FOUND: 404,
    ErrorCode.ALREADY_EXISTS: 409,
    ErrorCode.UPLOAD_IN_PROGRESS: 409,
    ErrorCode.DIR_NOT_EMPTY: 409,
    ErrorCode.INSUFFICIENT_REPLICAS: 409,
    ErrorCode.FILE_NOT_COMMITTED: 409,
    ErrorCode.NODE_NOT_REGISTERED: 409,
    ErrorCode.FILE_TOO_LARGE: 413,
    ErrorCode.RANGE_NOT_SATISFIABLE: 416,
    ErrorCode.CHECKSUM_MISMATCH: 422,
    ErrorCode.LOCKED: 423,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.NOT_ENOUGH_DATANODES: 503,
    ErrorCode.NOT_LEADER: 503,
    ErrorCode.NO_SPACE: 507,
}

#: Code to gRPC status **name** (API.md Section 3.1).
#: Names, not `grpc.StatusCode`, so this module stays importable without grpc
#: (the NameNode never links gRPC — ARCHITECTURE.md Section 2).
GRPC_STATUS: dict[ErrorCode, str] = {
    ErrorCode.UNAUTHORIZED: "UNAUTHENTICATED",
    ErrorCode.FORBIDDEN: "PERMISSION_DENIED",
    ErrorCode.BLOCK_NOT_FOUND: "NOT_FOUND",
    ErrorCode.BAD_REQUEST: "INVALID_ARGUMENT",
    ErrorCode.CHECKSUM_MISMATCH: "INVALID_ARGUMENT",
    ErrorCode.RANGE_NOT_SATISFIABLE: "OUT_OF_RANGE",
    ErrorCode.NO_SPACE: "RESOURCE_EXHAUSTED",
    ErrorCode.NOT_ENOUGH_DATANODES: "UNAVAILABLE",
    ErrorCode.NOT_LEADER: "UNAVAILABLE",
}

#: Trailing metadata key carrying the application code on the block channel.
GRPC_ERROR_METADATA_KEY = "x-dfsha-error-code"


class DfshaError(Exception):
    """An error that already knows how it looks on both wires."""

    def __init__(self, code: ErrorCode, message: str = "", **extra: object) -> None:
        self.code = code
        self.message = message or code.value
        #: extra top-level fields, e.g. `leader_url` for NOT_LEADER
        self.extra = extra
        super().__init__(f"{code.value}: {self.message}")

    @property
    def http_status(self) -> int:
        return HTTP_STATUS[self.code]

    @property
    def grpc_status(self) -> str:
        """gRPC status name, defaulting to INTERNAL for codes with no mapping."""
        return GRPC_STATUS.get(self.code, "INTERNAL")

    def to_body(self) -> dict[str, object]:
        """The REST body of API.md Section 3."""
        body: dict[str, object] = {
            "error": {"code": self.code.value, "message": self.message}
        }
        body.update(self.extra)
        return body


__all__ = [
    "GRPC_ERROR_METADATA_KEY",
    "GRPC_STATUS",
    "HTTP_STATUS",
    "DfshaError",
    "ErrorCode",
]
