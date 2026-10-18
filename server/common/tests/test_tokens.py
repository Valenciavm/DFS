"""Block token minting and verification, including the attacks it must resist.

These are the only thing standing between a stranger and the block channel, so
the negative cases matter more than the happy path.
"""
from __future__ import annotations

import json

import pytest

from server.common.errors import DfshaError, ErrorCode
from server.common.tokens import (
    SYSTEM_PRINCIPAL,
    BlockOp,
    _b64url_encode,
    mint,
    verify,
)

SECRET = "cluster-shared-secret"
BLOCK = "blk_7b0c9e0e_000000"
NOW = 1_760_000_000.0


def a_token(**overrides) -> str:
    params = {
        "block_id": BLOCK,
        "op": BlockOp.WRITE,
        "user": "ana",
        "secret": SECRET,
        "ttl_s": 900,
        "now": NOW,
    }
    params.update(overrides)
    return mint(**params)


class TestRoundTrip:
    def test_a_minted_token_verifies_and_keeps_its_claims(self):
        claims = verify(a_token(), SECRET, block_id=BLOCK, op=BlockOp.WRITE, now=NOW)
        assert claims.block_id == BLOCK
        assert claims.op is BlockOp.WRITE
        assert claims.user == "ana"
        assert claims.expires_at == int(NOW) + 900

    def test_the_wire_format_is_two_base64url_parts(self):
        """API.md Section 2.1: base64url(payload) "." base64url(signature)."""
        token = a_token()
        encoded, dot, signature = token.partition(".")
        assert dot == "."
        assert encoded and signature
        assert "=" not in token, "padding would be unsafe in headers"
        assert "+" not in token and "/" not in token, "must be the url-safe alphabet"

    def test_the_payload_carries_the_claim_names_from_the_contract(self):
        encoded = a_token().split(".")[0]
        import base64

        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        assert set(json.loads(raw)) == {"bid", "op", "usr", "exp"}

    def test_a_system_token_is_minted_for_replication_commands(self):
        claims = verify(a_token(user=SYSTEM_PRINCIPAL), SECRET, now=NOW)
        assert claims.user == SYSTEM_PRINCIPAL


class TestRejection:
    def test_a_token_for_another_block_is_forbidden_not_unauthorized(self):
        """We know who they are; they just may not touch this block."""
        token = a_token(block_id="blk_other_000000")
        with pytest.raises(DfshaError) as e:
            verify(token, SECRET, block_id=BLOCK, op=BlockOp.WRITE, now=NOW)
        assert e.value.code is ErrorCode.FORBIDDEN
        assert e.value.http_status == 403
        assert e.value.grpc_status == "PERMISSION_DENIED"

    def test_a_read_token_cannot_write(self):
        token = a_token(op=BlockOp.READ)
        with pytest.raises(DfshaError) as e:
            verify(token, SECRET, block_id=BLOCK, op=BlockOp.WRITE, now=NOW)
        assert e.value.code is ErrorCode.FORBIDDEN

    def test_a_write_token_cannot_read(self):
        token = a_token(op=BlockOp.WRITE)
        with pytest.raises(DfshaError) as e:
            verify(token, SECRET, block_id=BLOCK, op=BlockOp.READ, now=NOW)
        assert e.value.code is ErrorCode.FORBIDDEN

    def test_an_expired_token_is_unauthorized(self):
        token = a_token(ttl_s=900)
        with pytest.raises(DfshaError) as e:
            verify(token, SECRET, block_id=BLOCK, op=BlockOp.WRITE, now=NOW + 901)
        assert e.value.code is ErrorCode.UNAUTHORIZED
        assert "expired" in e.value.message

    def test_expiry_is_exclusive_at_the_boundary(self):
        token = a_token(ttl_s=900)
        verify(token, SECRET, now=NOW + 899)              # still valid
        with pytest.raises(DfshaError):
            verify(token, SECRET, now=NOW + 900)          # exp <= now

    def test_a_token_signed_with_another_secret_is_unauthorized(self):
        token = a_token(secret="a-different-secret")
        with pytest.raises(DfshaError) as e:
            verify(token, SECRET, now=NOW)
        assert e.value.code is ErrorCode.UNAUTHORIZED
        assert e.value.grpc_status == "UNAUTHENTICATED"

    def test_tampering_with_the_payload_invalidates_the_signature(self):
        """The whole point: claims cannot be edited without the secret."""
        _, _, signature = a_token().partition(".")
        forged_payload = _b64url_encode(
            json.dumps(
                {"bid": BLOCK, "exp": 9_999_999_999, "op": "w", "usr": "attacker"},
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        with pytest.raises(DfshaError) as e:
            verify(f"{forged_payload}.{signature}", SECRET, now=NOW)
        assert e.value.code is ErrorCode.UNAUTHORIZED

    @pytest.mark.parametrize(
        "token",
        ["", ".", "no-dot-at-all", "onlypayload.", ".onlysignature"],
        ids=["empty", "bare-dot", "no-separator", "no-signature", "no-payload"],
    )
    def test_malformed_tokens_are_rejected(self, token):
        with pytest.raises(DfshaError) as e:
            verify(token, SECRET, now=NOW)
        assert e.value.code is ErrorCode.UNAUTHORIZED


class TestSecretHandling:
    def test_minting_without_a_secret_is_refused(self):
        """Fails loudly rather than issuing a token anyone could forge."""
        with pytest.raises(ValueError, match="empty secret"):
            mint(BLOCK, BlockOp.READ, "ana", "", 900)

    def test_verifying_without_a_secret_is_refused(self):
        with pytest.raises(ValueError, match="empty secret"):
            verify(a_token(), "", now=NOW)


def test_verification_without_binding_only_checks_authenticity():
    """Useful for logging or introspection, not for authorization."""
    claims = verify(a_token(block_id="blk_whatever_000001"), SECRET, now=NOW)
    assert claims.block_id == "blk_whatever_000001"
