"""Garbage collection of abandoned uploads (test plan T18)."""
from __future__ import annotations

import pytest

from server.common.errors import ErrorCode
from server.nodes.namenode.models import FileStatus
from server.nodes.namenode.results import ReportedBlock
from server.nodes.namenode.tests.helpers import (
    MIB,
    add_nodes,
    code_of,
    commit_body,
    confirm_all,
    upload,
)

HOME = "/users/ana"


@pytest.fixture
def cluster(nn):
    add_nodes(nn, 5)
    nn.store.mkdir(HOME, "ana", nn.clock.time())
    return nn


class TestExpiry:
    def test_an_upload_that_is_never_committed_is_aborted_after_the_ttl(self, cluster, clock):
        """T18: start an upload and never commit it."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s + 1)

        result = cluster.collect_garbage()

        assert result.expired == 1
        assert cluster.store.get_file_by_id(plan.file.file_id).status is FileStatus.ABORTED

    def test_an_upload_still_within_its_ttl_is_left_alone(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s - 1)
        assert cluster.collect_garbage().expired == 0
        assert cluster.store.get_file_by_id(plan.file.file_id).status is FileStatus.PENDING

    def test_expiry_is_inclusive_at_the_deadline(self, cluster, clock):
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s)
        assert cluster.collect_garbage().expired == 1

    def test_expiry_frees_the_path_and_the_reserved_space(self, cluster, clock):
        cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s + 1)
        cluster.collect_garbage()

        assert cluster.cluster.reserved_bytes == {}
        assert cluster.cluster.reservations == {}
        assert cluster.cluster.pipelines == {}
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")  # the path is free again

    def test_expiry_orders_every_node_that_may_hold_a_piece_to_delete_it(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan, replicas=1)
        block_id = plan.blocks[0].block.block_id
        clock.advance(cluster.settings.upload_ttl_s + 1)

        cluster.collect_garbage()

        for node in plan.blocks[0].pipeline:  # including nodes that never confirmed
            commands = cluster.cluster.pending_commands[node.node_id]
            assert ("delete", block_id) in [(c.cmd, c.block_id) for c in commands]
        assert block_id not in cluster.cluster.replicas

    def test_a_committed_file_is_never_collected(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", 8 * MIB)
        clock.advance(cluster.settings.upload_ttl_s * 10)
        cluster.collect_garbage()
        assert cluster.store.get_file_by_id(plan.file.file_id).status is FileStatus.COMMITTED
        assert cluster.get_file_blocks(f"{HOME}/f", "ana").blocks

    def test_only_the_stale_upload_is_collected(self, cluster, clock):
        old = cluster.prepare_file_upload(f"{HOME}/old", MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s - 10)
        fresh = cluster.prepare_file_upload(f"{HOME}/fresh", MIB, "ana")
        clock.advance(20)

        assert cluster.collect_garbage().expired == 1
        assert cluster.store.get_file_by_id(old.file.file_id).status is FileStatus.ABORTED
        assert cluster.store.get_file_by_id(fresh.file.file_id).status is FileStatus.PENDING

    def test_collecting_twice_does_not_expire_the_same_upload_twice(self, cluster, clock):
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s + 1)
        assert cluster.collect_garbage().expired == 1
        assert cluster.collect_garbage().expired == 0


class TestRetention:
    def test_an_aborted_record_is_kept_for_one_more_ttl(self, cluster, clock):
        """So a late commit is told 'aborted' rather than 'unknown'."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        confirm_all(cluster, plan)
        clock.advance(cluster.settings.upload_ttl_s + 1)
        cluster.collect_garbage()

        assert cluster.store.get_file_by_id(plan.file.file_id) is not None
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.FILE_NOT_FOUND

    def test_the_record_is_purged_once_its_retention_has_passed(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s + 1)
        cluster.collect_garbage()
        clock.advance(cluster.settings.upload_ttl_s + 1)

        result = cluster.collect_garbage()

        assert result.purged == 1
        assert cluster.store.get_file_by_id(plan.file.file_id) is None
        assert cluster.store.get_block(plan.blocks[0].block.block_id) is None

    def test_a_purged_block_that_resurfaces_in_a_report_is_an_orphan(self, cluster, clock):
        """Once the record is gone only the block report can still find the leftover bytes."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        block_id = plan.blocks[0].block.block_id
        clock.advance(cluster.settings.upload_ttl_s + 1)
        cluster.collect_garbage()
        clock.advance(cluster.settings.upload_ttl_s + 1)
        cluster.collect_garbage()
        cluster.cluster.pending_commands.clear()

        commands = cluster.process_block_report("dn1", [ReportedBlock(block_id, MIB, "a" * 64)])
        assert [(c.cmd, c.block_id) for c in commands] == [("delete", block_id)]

    def test_nothing_to_collect_is_a_quiet_pass(self, cluster):
        result = cluster.collect_garbage()
        assert (result.expired, result.purged) == (0, 0)
