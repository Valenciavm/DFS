"""DataNode registration, heartbeats and dead-node detection (test plan T19)."""
from __future__ import annotations

import random

import pytest

from server.common.config import ConfigError, Settings
from server.common.errors import ErrorCode
from server.nodes.namenode.cluster_state import Command
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.tests.helpers import GIB, add_nodes, code_of


class TestConstruction:
    def test_a_namenode_without_a_token_secret_refuses_to_start(self, clock):
        """Otherwise it would sign tokens with an empty key that anyone could forge."""
        with pytest.raises(ConfigError, match="BLOCK_TOKEN_SECRET"):
            Namenode(settings=Settings(block_token_secret=""), clock=clock)

    def test_the_users_directory_exists_from_the_start(self, nn):
        assert nn.store.resolve_dir("/users").path() == "/users"


class TestRegistration:
    def test_a_new_node_is_registered_alive(self, nn):
        node = nn.register_datanode("dn1", "dn1:9000", "dn1.example.com:9000", 100 * GIB, 5 * GIB)
        assert node.is_alive
        assert node.internal_addr == "dn1:9000"
        assert node.public_addr == "dn1.example.com:9000"
        assert node.free_bytes() == 95 * GIB

    def test_both_addresses_are_kept_apart(self, nn):
        """Clients get public_addr, other DataNodes get internal_addr (API.md Section 1)."""
        node = nn.register_datanode("dn1", "10.0.0.5:9000", "dn1.example.com:9000", GIB)
        assert node.internal_addr != node.public_addr

    def test_registering_again_updates_the_entry_instead_of_duplicating_it(self, nn):
        nn.register_datanode("dn1", "old:9000", "old.example.com:9000", GIB)
        nn.register_datanode("dn1", "new:9000", "new.example.com:9000", 2 * GIB)
        assert len(nn.cluster.datanodes) == 1
        assert nn.cluster.datanodes["dn1"].internal_addr == "new:9000"
        assert nn.cluster.datanodes["dn1"].capacity_bytes == 2 * GIB

    def test_reregistering_keeps_the_replica_locations_already_known(self, nn):
        """A node that restarts must not make the NameNode forget its blocks."""
        add_nodes(nn, 1)
        nn.cluster.replicas["blk_x"] = {"dn1"}
        nn.register_datanode("dn1", "dn1:9000", "dn1.example.com:9000", GIB)
        assert nn.cluster.replicas["blk_x"] == {"dn1"}

    def test_reregistering_revives_a_dead_node(self, nn, clock):
        add_nodes(nn, 1)
        clock.advance(31)
        nn.check_liveness()
        assert not nn.cluster.datanodes["dn1"].is_alive
        nn.register_datanode("dn1", "dn1:9000", "dn1.example.com:9000", GIB)
        assert nn.cluster.datanodes["dn1"].is_alive

    @pytest.mark.parametrize(
        "args",
        [
            ("", "a:1", "b:1", GIB),
            ("dn1", "", "b:1", GIB),
            ("dn1", "a:1", "", GIB),
            ("dn1", "a:1", "b:1", -1),
        ],
        ids=["no-id", "no-internal", "no-public", "negative-capacity"],
    )
    def test_invalid_registrations_are_a_bad_request(self, nn, args):
        assert code_of(lambda: nn.register_datanode(*args)) is ErrorCode.BAD_REQUEST


class TestHeartbeat:
    def test_a_heartbeat_from_an_unknown_node_must_register_again(self, nn):
        """Decision 19: the normal path after a failover, answered with 409."""
        code = code_of(lambda: nn.record_heartbeat("ghost", 10 * GIB, GIB))
        assert code is ErrorCode.NODE_NOT_REGISTERED

    def test_a_heartbeat_updates_the_real_free_space(self, nn):
        """Placement must use real numbers, not the figures from registration."""
        add_nodes(nn, 1, capacity=100 * GIB)
        nn.record_heartbeat("dn1", free_bytes=30 * GIB, used_bytes=70 * GIB)
        node = nn.cluster.datanodes["dn1"]
        assert node.used_bytes == 70 * GIB
        assert node.free_bytes() == 30 * GIB

    def test_the_node_is_trusted_when_it_reports_a_different_capacity(self, nn):
        add_nodes(nn, 1, capacity=100 * GIB)
        nn.record_heartbeat("dn1", free_bytes=10 * GIB, used_bytes=10 * GIB)
        assert nn.cluster.datanodes["dn1"].capacity_bytes == 20 * GIB

    def test_negative_figures_are_rejected(self, nn):
        add_nodes(nn, 1)
        assert code_of(lambda: nn.record_heartbeat("dn1", -1, 0)) is ErrorCode.BAD_REQUEST

    def test_pending_commands_are_delivered_once(self, nn):
        add_nodes(nn, 1)
        nn.cluster.pending_commands["dn1"] = [Command("delete", "blk_a")]
        assert [c.block_id for c in nn.record_heartbeat("dn1", GIB, 0)] == ["blk_a"]
        assert nn.record_heartbeat("dn1", GIB, 0) == []

    def test_commands_for_other_nodes_are_not_delivered(self, nn):
        add_nodes(nn, 2)
        nn.cluster.pending_commands["dn2"] = [Command("delete", "blk_a")]
        assert nn.record_heartbeat("dn1", GIB, 0) == []
        assert len(nn.cluster.pending_commands["dn2"]) == 1

    def test_identical_commands_are_not_queued_twice(self, nn):
        add_nodes(nn, 1)
        nn._enqueue("dn1", Command("delete", "blk_a"))
        nn._enqueue("dn1", Command("delete", "blk_a"))
        nn._enqueue("dn1", Command("delete", "blk_b"))
        assert [c.block_id for c in nn.record_heartbeat("dn1", GIB, 0)] == ["blk_a", "blk_b"]


class TestLiveness:
    def test_a_node_is_alive_until_the_timeout_passes(self, nn, clock):
        add_nodes(nn, 1)
        clock.advance(30)  # DEAD_NODE_TIMEOUT_S is 30; exactly 30 is not "more than"
        assert nn.check_liveness() == []

    def test_one_second_past_the_timeout_it_is_dead(self, nn, clock):
        add_nodes(nn, 1)
        clock.advance(31)
        assert nn.check_liveness() == ["dn1"]
        assert not nn.cluster.datanodes["dn1"].is_alive

    def test_a_node_that_registers_and_never_beats_is_eventually_marked_dead(self, nn, clock):
        """ARCHITECTURE.md Appendix A.2 #2: the original code compared a monotonic reading
        against wall-clock defaults, so such a node was never declared dead."""
        nn.register_datanode("silent", "s:9000", "s.example.com:9000", GIB)
        clock.advance(100)
        assert nn.check_liveness() == ["silent"]

    def test_heartbeats_keep_a_node_alive_indefinitely(self, nn, clock):
        add_nodes(nn, 1)
        for _ in range(20):
            clock.advance(10)
            nn.record_heartbeat("dn1", GIB, 0)
            assert nn.check_liveness() == []

    def test_only_the_silent_node_dies(self, nn, clock):
        add_nodes(nn, 3)
        clock.advance(20)
        nn.record_heartbeat("dn1", GIB, 0)
        nn.record_heartbeat("dn3", GIB, 0)
        clock.advance(15)  # dn2 has now been quiet for 35 s
        assert nn.check_liveness() == ["dn2"]
        assert nn.cluster.datanodes["dn1"].is_alive
        assert nn.cluster.datanodes["dn3"].is_alive

    def test_a_node_is_reported_dead_only_once(self, nn, clock):
        add_nodes(nn, 1)
        clock.advance(31)
        assert nn.check_liveness() == ["dn1"]
        assert nn.check_liveness() == []

    def test_a_late_heartbeat_revives_a_dead_node(self, nn, clock):
        add_nodes(nn, 1)
        clock.advance(31)
        nn.check_liveness()
        nn.record_heartbeat("dn1", GIB, 0)
        assert nn.cluster.datanodes["dn1"].is_alive

    def test_a_wall_clock_jump_cannot_kill_a_healthy_node(self, nn, clock):
        """Why liveness uses the monotonic clock (Section 4.4)."""
        add_nodes(nn, 1)
        clock.jump_wall(+3600)   # NTP steps the clock forward an hour
        assert nn.check_liveness() == []
        clock.jump_wall(-7200)   # and back again
        assert nn.check_liveness() == []

    def test_a_wall_clock_jump_cannot_save_a_dead_node_either(self, nn, clock):
        add_nodes(nn, 1)
        clock.advance(31)
        clock.jump_wall(-3600)
        assert nn.check_liveness() == ["dn1"]

    def test_nobody_is_declared_dead_during_the_failover_grace_period(self, nn, clock):
        """A new leader must wait for DataNodes to report before judging them."""
        add_nodes(nn, 2)
        nn.begin_failover_grace()           # FAILOVER_GRACE_S is 30
        clock.advance(29)
        assert nn.check_liveness() == []

    def test_after_the_grace_period_silent_nodes_are_judged_normally(self, nn, clock):
        add_nodes(nn, 2)
        nn.begin_failover_grace()
        clock.advance(31)
        assert sorted(nn.check_liveness()) == ["dn1", "dn2"]

    def test_the_timeout_comes_from_the_settings(self, clock):
        nn = Namenode(
            settings=Settings(
                block_token_secret="s", heartbeat_interval_s=1, dead_node_timeout_s=3,
                failover_grace_s=2,
            ),
            clock=clock,
            rng=random.Random(1),
        )
        add_nodes(nn, 1)
        clock.advance(4)
        assert nn.check_liveness() == ["dn1"]
