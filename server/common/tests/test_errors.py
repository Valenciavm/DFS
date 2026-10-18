"""The error vocabulary, checked against API.md Sections 3 and 3.1."""
from __future__ import annotations

import pytest

from server.common.errors import (
    GRPC_ERROR_METADATA_KEY,
    GRPC_STATUS,
    HTTP_STATUS,
    DfshaError,
    ErrorCode,
)


def test_every_code_has_an_http_status():
    """A code with no mapping would reach a client as a 500."""
    missing = [c.value for c in ErrorCode if c not in HTTP_STATUS]
    assert missing == []


@pytest.mark.parametrize(
    ("code", "status"),
    [
        (ErrorCode.BAD_REQUEST, 400),
        (ErrorCode.UNAUTHORIZED, 401),
        (ErrorCode.FORBIDDEN, 403),
        (ErrorCode.FILE_NOT_FOUND, 404),
        (ErrorCode.ALREADY_EXISTS, 409),
        (ErrorCode.UPLOAD_IN_PROGRESS, 409),
        (ErrorCode.INSUFFICIENT_REPLICAS, 409),
        (ErrorCode.NODE_NOT_REGISTERED, 409),
        (ErrorCode.FILE_TOO_LARGE, 413),
        (ErrorCode.RANGE_NOT_SATISFIABLE, 416),
        (ErrorCode.CHECKSUM_MISMATCH, 422),
        (ErrorCode.LOCKED, 423),
        (ErrorCode.RATE_LIMITED, 429),
        (ErrorCode.NOT_ENOUGH_DATANODES, 503),
        (ErrorCode.NOT_LEADER, 503),
        (ErrorCode.NO_SPACE, 507),
    ],
)
def test_http_statuses_match_the_contract(code, status):
    assert DfshaError(code).http_status == status


@pytest.mark.parametrize(
    ("code", "grpc"),
    [
        (ErrorCode.UNAUTHORIZED, "UNAUTHENTICATED"),
        (ErrorCode.FORBIDDEN, "PERMISSION_DENIED"),
        (ErrorCode.BLOCK_NOT_FOUND, "NOT_FOUND"),
        (ErrorCode.BAD_REQUEST, "INVALID_ARGUMENT"),
        (ErrorCode.CHECKSUM_MISMATCH, "INVALID_ARGUMENT"),
        (ErrorCode.RANGE_NOT_SATISFIABLE, "OUT_OF_RANGE"),
        (ErrorCode.NO_SPACE, "RESOURCE_EXHAUSTED"),
        (ErrorCode.NOT_ENOUGH_DATANODES, "UNAVAILABLE"),
    ],
)
def test_grpc_statuses_match_the_contract(code, grpc):
    assert DfshaError(code).grpc_status == grpc


def test_codes_without_a_grpc_mapping_fall_back_to_internal():
    """Namespace errors never cross the block channel, so they have no mapping."""
    assert ErrorCode.DIR_NOT_EMPTY not in GRPC_STATUS
    assert DfshaError(ErrorCode.DIR_NOT_EMPTY).grpc_status == "INTERNAL"


def test_the_grpc_metadata_key_is_the_one_in_the_contract():
    assert GRPC_ERROR_METADATA_KEY == "x-dfsha-error-code"


def test_the_rest_body_has_the_shape_from_the_contract():
    err = DfshaError(ErrorCode.FILE_NOT_FOUND, "/docs/report.pdf does not exist")
    assert err.to_body() == {
        "error": {
            "code": "FILE_NOT_FOUND",
            "message": "/docs/report.pdf does not exist",
        }
    }


def test_extra_fields_sit_beside_the_error_object():
    """NOT_LEADER carries leader_url at the top level (API.md Section 3)."""
    err = DfshaError(ErrorCode.NOT_LEADER, "not the leader", leader_url="https://nn2:8000")
    body = err.to_body()
    assert body["leader_url"] == "https://nn2:8000"
    assert body["error"]["code"] == "NOT_LEADER"


def test_the_message_defaults_to_the_code():
    assert DfshaError(ErrorCode.FORBIDDEN).message == "FORBIDDEN"


def test_the_wire_value_of_a_code_is_its_name():
    """Clients match on these strings, so they must not drift."""
    for code in ErrorCode:
        assert code.value == code.name
