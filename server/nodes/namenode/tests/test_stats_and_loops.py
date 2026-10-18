"""Cluster statistics, the background loops and the process entry point."""
from __future__ import annotations

import asyncio

import pytest

from server.common.config import Settings
from server.nodes.namenode import __main__ as entrypoint
from server.nodes.namenode import core as core_module
from server.nodes.namenode.tests.helpers import GIB, MIB, add_nodes, checksum_of, upload

HOME = "/users/ana"


@pytest.fixture
def cluster(nn):
    add_nodes(nn, 5)
    nn.store.mkdir(HOME, "ana", nn.clock.time())
    return nn


class TestStats:
    def test_an_empty_cluster(self, nn):
        stats = nn.stats()
        assert (stats.files, stats.blocks, stats.under_replicated_blocks) == (0, 0, 0)
        assert (stats.live_nodes, stats.dead_nodes, stats.bytes_stored) == (0, 0, 0)

    def test_totals_count_committed_files_only(self, cluster):
        upload(cluster, f"{HOME}/a", 10 * MIB)     # 3 blocks
        upload(cluster, f"{HOME}/b", 4 * MIB)      # 1 block
        cluster.prepare_file_upload(f"{HOME}/c", 40 * MIB, "ana")  # pending: not counted

        stats = cluster.stats()

        assert stats.files == 2
        assert stats.blocks == 4
        assert stats.bytes_stored == 14 * MIB
        assert stats.live_nodes == 5

    def test_under_replicated_rises_when_a_node_dies_and_returns_to_zero(self, cluster, clock):
        """The number the failure demos watch (T4)."""
        plan = upload(cluster, f"{HOME}/f", 12 * MIB)
        assert cluster.stats().under_replicated_blocks == 0

        victim = plan.blocks[0].pipeline[0].node_id
        clock.advance(31)
        for node_id in cluster.cluster.datanodes:
            if node_id != victim:
                cluster.record_heartbeat(node_id, 100 * GIB, 0)
        cluster.check_liveness()
        stats = cluster.stats()
        assert stats.dead_nodes == 1
        assert stats.under_replicated_blocks >= 1

        cluster.scan_replication()
        for commands in list(cluster.cluster.pending_commands.values()):
            for command in commands:
                if command.cmd == "replicate":
                    block = cluster.store.get_block(command.block_id)
                    for target in command.targets:
                        cluster.record_block_received(
                            target, command.block_id, block.size_bytes, checksum_of(command.block_id)
                        )
        assert cluster.stats().under_replicated_blocks == 0

    def test_the_term_is_reported(self, nn):
        assert nn.stats().term == 1


class TestBackgroundLoops:
    async def test_a_task_runs_repeatedly(self, nn):
        calls = []
        runner = asyncio.create_task(nn._every(0.01, lambda: calls.append(1)))
        await asyncio.sleep(0.1)
        runner.cancel()
        assert len(calls) >= 3

    async def test_a_failing_task_is_logged_and_the_loop_survives(self, nn, caplog):
        """The loops are what notice dead nodes and repair blocks: one bad pass must not end them."""
        calls = []

        def flaky() -> None:
            calls.append(1)
            raise RuntimeError("boom")

        runner = asyncio.create_task(nn._every(0.01, flaky))
        await asyncio.sleep(0.1)
        runner.cancel()

        assert len(calls) >= 3, "the loop must keep going after an exception"
        assert "background task flaky failed" in caplog.text

    async def test_cancelling_a_loop_stops_it_cleanly(self, nn):
        runner = asyncio.create_task(nn.monitor_datanodes())
        await asyncio.sleep(0.01)
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner

    async def test_the_liveness_loop_marks_a_silent_node_dead(self, cluster, clock, monkeypatch):
        monkeypatch.setattr(core_module, "LIVENESS_PERIOD_S", 0.01)
        clock.advance(31)
        runner = asyncio.create_task(cluster.monitor_datanodes())
        await asyncio.sleep(0.05)
        runner.cancel()
        assert cluster.stats().dead_nodes == 5


class TestEntryPoint:
    def test_it_refuses_to_start_without_a_token_secret(self, monkeypatch):
        monkeypatch.setattr(core_module, "default_settings", Settings(block_token_secret=""))
        with pytest.raises(SystemExit) as exit_info:
            entrypoint.main()
        assert "BLOCK_TOKEN_SECRET" in str(exit_info.value)
