"""Block reports: rebuilding replica locations, finding orphans and damaged copies."""
from __future__ import annotations

import random

import pytest

from server.common.errors import ErrorCode
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.results import ReportedBlock
from server.nodes.namenode.tests.helpers import (
    MIB,
    add_nodes,
    checksum_of,
    code_of,
    commit_body,
    upload,
)

HOME = "/users/ana"


@pytest.fixture
def cluster(nn):
    add_nodes(nn, 5)
    nn.store.mkdir(HOME, "ana", nn.clock.time())
    return nn


def report_for(nn: Namenode, node_id: str) -> list[ReportedBlock]:
    """What a healthy DataNode would report: every block this NameNode thinks it holds."""
    return [
        ReportedBlock(block_id, nn.store.get_block(block_id).size_bytes, checksum_of(block_id))
        for block_id, holders in nn.cluster.replicas.items()
        if node_id in holders
    ]


class TestReports:
    def test_a_report_from_an_unknown_node_must_register_first(self, cluster):
        code = code_of(lambda: cluster.process_block_report("ghost", []))
        assert code is ErrorCode.NODE_NOT_REGISTERED

    def test_a_report_confirms_the_blocks_the_node_holds(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0].block
        report = [ReportedBlock(block.block_id, block.size_bytes, checksum_of(block.block_id))]
        cluster.process_block_report("dn1", report)
        assert cluster.cluster.replicas[block.block_id] == {"dn1"}

    def test_the_report_returns_the_pending_commands(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        cluster.rm(f"{HOME}/f")
        holder = next(iter(cluster.cluster.pending_commands))
        commands = cluster.process_block_report(holder, [])
        assert [c.cmd for c in commands] == ["delete"]

    def test_a_block_nobody_owns_is_ordered_deleted(self, cluster):
        commands = cluster.process_block_report("dn1", [ReportedBlock("blk_orphan_000000", MIB, "a" * 64)])
        assert [(c.cmd, c.block_id) for c in commands] == [("delete", "blk_orphan_000000")]

    def test_a_block_of_an_aborted_upload_is_an_orphan(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0].block
        cluster.abort_file_upload(plan.file.file_id)
        cluster.process_block_report("dn1", [ReportedBlock(block.block_id, block.size_bytes, "a" * 64)])
        assert block.block_id not in cluster.cluster.replicas

    def test_a_copy_with_the_wrong_checksum_is_not_a_replica_and_is_deleted(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block = plan.blocks[0].block
        victim = plan.blocks[0].pipeline[0].node_id
        bad = ReportedBlock(block.block_id, block.size_bytes, "f" * 64)

        commands = cluster.process_block_report(victim, [bad])

        assert victim not in cluster.cluster.replicas[block.block_id]
        assert ("delete", block.block_id) in [(c.cmd, c.block_id) for c in commands]

    def test_a_copy_of_the_wrong_size_is_not_a_replica(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block = plan.blocks[0].block
        victim = plan.blocks[0].pipeline[0].node_id
        truncated = ReportedBlock(block.block_id, block.size_bytes - 1, checksum_of(block.block_id))
        cluster.process_block_report(victim, [truncated])
        assert victim not in cluster.cluster.replicas[block.block_id]

    def test_blocks_the_node_no_longer_reports_stop_counting_as_replicas(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        victim = plan.blocks[0].pipeline[0].node_id
        cluster.process_block_report(victim, [])  # it lost everything
        assert victim not in cluster.cluster.replicas[block_id]
        assert len(cluster.cluster.replicas[block_id]) == 2

    def test_other_nodes_replicas_are_untouched_by_someone_elses_report(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        before = set(cluster.cluster.replicas[block_id])
        outsider = next(n for n in cluster.cluster.datanodes if n not in before)
        cluster.process_block_report(outsider, [])
        assert cluster.cluster.replicas[block_id] == before

    def test_a_report_is_idempotent(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        node = plan.blocks[0].pipeline[0].node_id
        for _ in range(3):
            cluster.process_block_report(node, report_for(cluster, node))
        assert len(cluster.cluster.replicas[plan.blocks[0].block.block_id]) == 3


class TestRebuildingAfterFailover:
    def test_replica_locations_are_rebuilt_from_reports_alone(self, cluster, settings, clock):
        """Replicas are soft state, lost with the leader and rebuilt from DataNode reports
        (ARCHITECTURE.md Section 5.1). The hard state survives in the shared store."""
        plan = upload(cluster, f"{HOME}/f", 12 * MIB)
        truth = {nid: report_for(cluster, nid) for nid in cluster.cluster.datanodes}

        successor = Namenode(
            settings=settings,
            store=cluster.store,
            clock=clock,
            rng=random.Random(1),
            auth_store=cluster.auth_store,
        )
        assert successor.cluster.replicas == {}, "a new leader starts knowing nothing"
        for node_id in cluster.cluster.datanodes:
            successor.register_datanode(node_id, f"{node_id}:9000", f"{node_id}.example.com:9000", 100 << 30)
        for node_id, report in truth.items():
            successor.process_block_report(node_id, report)

        location = successor.get_file_blocks(f"{HOME}/f", "ana")
        assert len(location.blocks) == len(plan.blocks)
        assert all(len(b.replicas) == 3 for b in location.blocks)

    def test_a_commit_waits_for_the_reports_after_a_failover(self, cluster, settings, clock):
        """API.md Section 9: right after a failover commit may answer INSUFFICIENT_REPLICAS
        until the block reports arrive, and then succeeds on retry."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        for node in plan.blocks[0].pipeline:
            cluster.record_block_received(node.node_id, plan.blocks[0].block.block_id, 4 * MIB,
                                          checksum_of(plan.blocks[0].block.block_id))
        successor = Namenode(
            settings=settings,
            store=cluster.store,
            clock=clock,
            rng=random.Random(1),
            auth_store=cluster.auth_store,
        )
        for node_id in cluster.cluster.datanodes:
            successor.register_datanode(node_id, f"{node_id}:9000", f"{node_id}.example.com:9000", 100 << 30)

        body = commit_body(plan)
        assert code_of(lambda: successor.commit_file_upload(plan.file.file_id, body)) is ErrorCode.INSUFFICIENT_REPLICAS

        block = plan.blocks[0].block
        for node in plan.blocks[0].pipeline:
            successor.process_block_report(
                node.node_id, [ReportedBlock(block.block_id, block.size_bytes, checksum_of(block.block_id))]
            )
        assert successor.commit_file_upload(plan.file.file_id, body).file_id == plan.file.file_id


class TestCorruptBlocks:
    def test_a_corrupt_replica_stops_counting(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        victim = plan.blocks[0].pipeline[0].node_id
        cluster.record_block_corrupt(victim, block_id)
        assert victim not in cluster.cluster.replicas[block_id]
        assert victim not in cluster.cluster.replica_checksums[block_id]

    def test_the_last_replica_going_corrupt_leaves_no_empty_entry(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 4 * MIB)
        block_id = plan.blocks[0].block.block_id
        for node in plan.blocks[0].pipeline:
            cluster.record_block_corrupt(node.node_id, block_id)
        assert block_id not in cluster.cluster.replicas

    def test_a_corrupt_report_from_an_unknown_node_must_register_first(self, cluster):
        assert code_of(lambda: cluster.record_block_corrupt("ghost", "blk_x")) is ErrorCode.NODE_NOT_REGISTERED

    def test_reporting_a_block_that_was_not_counted_is_harmless(self, cluster):
        cluster.record_block_corrupt("dn1", "blk_never_seen")
