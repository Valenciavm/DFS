"""Settings defaults and the invariants that make the cluster coherent."""
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from server.common.config import ConfigError, GiB, MiB, Settings, load_settings


def test_defaults_match_the_contract():
    """API.md Section 1 and ARCHITECTURE.md Section 16, decisions 16 to 18."""
    s = Settings()
    assert s.block_size == 64 * MiB          # decision 16, not the code's 4 MB
    assert s.block_chunk_size == 1 * MiB
    assert s.replication_factor == 3
    assert s.min_replicas_commit == 2
    assert s.max_file_size_bytes == 10 * GiB
    assert s.heartbeat_interval_s == 5
    assert s.dead_node_timeout_s == 30
    assert s.block_report_interval_s == 60
    assert s.token_ttl_s == 900
    assert s.upload_ttl_s == 3600            # decision 18, not the code's 300
    assert s.lock_default_ttl_s == 60
    assert s.mfa_challenge_ttl_s == 300
    assert s.lease_s == 15
    assert s.failover_grace_s == 30
    assert s.ls_default_limit == 1000
    assert s.jwt_ttl_s == 3600
    assert s.jwt_secret == ""
    assert s.admin_user == ""
    assert s.admin_password == ""


def test_secrets_have_no_usable_default():
    """A deployment that forgets them must fail, not run insecurely."""
    s = Settings()
    assert s.block_token_secret == ""
    assert s.node_key == ""


def test_environment_overrides_are_applied(monkeypatch):
    monkeypatch.setenv("BLOCK_SIZE", str(1 * MiB))   # the demo profile
    monkeypatch.setenv("REPLICATION_FACTOR", "5")
    monkeypatch.setenv("BLOCK_TOKEN_SECRET", "s3cret")
    monkeypatch.setenv("JWT_TTL_S", "1800")
    monkeypatch.setenv("JWT_SECRET", "jwt-secret")
    monkeypatch.setenv("ADMIN_USER", "root")
    monkeypatch.setenv("ADMIN_PASSWORD", "admin-password")
    s = load_settings()
    assert s.block_size == 1 * MiB
    assert s.replication_factor == 5
    assert s.block_token_secret == "s3cret"
    assert s.jwt_ttl_s == 1800
    assert s.jwt_secret == "jwt-secret"
    assert s.admin_user == "root"
    assert s.admin_password == "admin-password"


def test_jwt_lifetime_must_be_positive():
    with pytest.raises(ConfigError, match="JWT_TTL_S"):
        Settings(jwt_ttl_s=0)


def test_a_malformed_value_is_rejected_rather_than_ignored(monkeypatch):
    monkeypatch.setenv("BLOCK_SIZE", "sixty-four")
    with pytest.raises(ConfigError, match="not an integer"):
        load_settings()


def test_quorum_above_replication_factor_is_rejected():
    """No commit could ever succeed, so this must fail at startup."""
    with pytest.raises(ConfigError, match="no commit could ever succeed"):
        Settings(replication_factor=3, min_replicas_commit=4)


def test_chunk_larger_than_the_datanode_limit_is_rejected():
    """A DataNode refuses chunks above 2 MiB (API.md Section 6.1)."""
    with pytest.raises(ConfigError, match="2 MiB"):
        Settings(block_chunk_size=4 * MiB)


def test_dead_timeout_must_exceed_the_heartbeat_interval():
    """Otherwise live nodes are declared dead between beats."""
    with pytest.raises(ConfigError, match="declared dead between beats"):
        Settings(heartbeat_interval_s=30, dead_node_timeout_s=30)


def test_failover_grace_shorter_than_a_heartbeat_is_rejected():
    """A new leader would kill every DataNode before any could report."""
    with pytest.raises(ConfigError, match="before any of them can report"):
        Settings(heartbeat_interval_s=5, failover_grace_s=1)


def test_settings_are_immutable():
    """Nothing may retune the cluster halfway through a request."""
    s = Settings()
    with pytest.raises(FrozenInstanceError):
        s.block_size = 1  # type: ignore[misc]


class TestBlockCount:
    """ARCHITECTURE.md Section 6.1: ceil, with a floor of one block."""

    def test_empty_file_still_has_one_block(self):
        assert Settings().blocks_for(0) == 1

    def test_exact_multiple(self):
        s = Settings(block_size=4 * MiB)
        assert s.blocks_for(8 * MiB) == 2

    def test_remainder_is_not_lost(self):
        # the example from the document: 10 MB with 4 MB blocks is 3, not 2
        s = Settings(block_size=4 * MiB)
        assert s.blocks_for(10 * MiB) == 3

    def test_one_byte_over_a_boundary_adds_a_block(self):
        s = Settings(block_size=4 * MiB)
        assert s.blocks_for(4 * MiB + 1) == 2

    def test_negative_size_is_rejected(self):
        with pytest.raises(ValueError):
            Settings().blocks_for(-1)


def test_lease_renew_interval_is_a_third_of_the_lease():
    assert Settings(lease_s=15).lease_renew_interval_s == 5
