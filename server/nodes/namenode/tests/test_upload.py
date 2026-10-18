"""put, commit and abort: the write path of the NameNode."""
from __future__ import annotations

import uuid

import pytest

from server.common.errors import ErrorCode
from server.common.tokens import BlockOp, verify
from server.nodes.namenode.models import FileStatus
from server.nodes.namenode.placement import usable_space
from server.nodes.namenode.results import CommittedBlock
from server.nodes.namenode.tests.conftest import SECRET
from server.nodes.namenode.tests.helpers import (
    GIB,
    MIB,
    add_nodes,
    checksum_of,
    code_of,
    commit_body,
    confirm_all,
    upload,
)

HOME = "/users/ana"


@pytest.fixture
def cluster(nn):
    """A five node cluster with Ana's home, the minimum that can partition (decision 11)."""
    add_nodes(nn, 5)
    nn.store.mkdir(HOME, "ana", nn.clock.time())
    return nn


class TestPrepare:
    def test_a_file_is_split_with_ceil_and_the_remainder_in_the_last_block(self, cluster):
        """4 MiB blocks: 10 MiB is 3 blocks, not 2 (Section 6.1)."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 10 * MIB, "ana")
        assert [p.block.size_bytes for p in plan.blocks] == [4 * MIB, 4 * MIB, 2 * MIB]
        assert [p.block.index for p in plan.blocks] == [0, 1, 2]

    def test_an_exact_multiple_has_no_extra_block(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        assert len(plan.blocks) == 2

    def test_an_empty_file_still_has_one_block_of_zero_bytes(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 0, "ana")
        assert [p.block.size_bytes for p in plan.blocks] == [0]

    def test_the_file_id_is_a_uuid_and_block_ids_are_distinct(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 20 * MIB, "ana")
        uuid.UUID(plan.file.file_id)  # raises if it is not one
        ids = [p.block.block_id for p in plan.blocks]
        assert len(set(ids)) == len(ids)
        assert all(i.startswith("blk_") for i in ids)

    def test_each_block_gets_replication_factor_distinct_nodes(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 40 * MIB, "ana")
        for planned in plan.blocks:
            assert len(planned.pipeline) == 3
            assert len({n.node_id for n in planned.pipeline}) == 3

    def test_blocks_of_one_file_are_spread_over_different_node_sets(self, cluster):
        """Partitioning: with five nodes the placement is not identical for every block."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 100 * MIB, "ana")
        sets = {frozenset(n.node_id for n in p.pipeline) for p in plan.blocks}
        assert len(sets) > 1

    def test_the_plan_is_not_a_set_of_replicas(self, cluster):
        """Allocation is not confirmation (Section 6.4)."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        for planned in plan.blocks:
            assert cluster.cluster.replicas.get(planned.block.block_id) is None
            assert cluster.cluster.pipelines[planned.block.block_id]

    def test_every_block_gets_a_write_token_bound_to_it(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        first, second = plan.blocks
        claims = verify(first.token, SECRET, block_id=first.block.block_id, op=BlockOp.WRITE,
                        now=clock.time())
        assert claims.user == "ana"
        # a token for block 0 is useless for block 1
        assert code_of(
            lambda: verify(first.token, SECRET, block_id=second.block.block_id, op=BlockOp.WRITE,
                           now=clock.time())
        ) is ErrorCode.FORBIDDEN
        # and a write token cannot be used to read
        assert code_of(
            lambda: verify(first.token, SECRET, block_id=first.block.block_id, op=BlockOp.READ,
                           now=clock.time())
        ) is ErrorCode.FORBIDDEN

    def test_the_file_is_pending_with_an_expiry_and_invisible(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        assert plan.file.status is FileStatus.PENDING
        assert plan.file.expires_at == clock.time() + cluster.settings.upload_ttl_s
        assert cluster.store.list_dir(HOME) == []
        assert cluster.store.get_file(f"{HOME}/f") is None

    def test_the_plan_records_the_parameters_used_for_this_file(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        assert plan.file.block_size == 4 * MIB
        assert plan.file.replication == 3
        assert plan.file.owner == "ana"

    def test_space_is_reserved_on_every_pipeline_node(self, cluster):
        cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        assert sum(cluster.cluster.reserved_bytes.values()) == 8 * MIB * 3

    def test_reserved_space_lowers_what_placement_sees_as_free(self, cluster):
        """Two uploads must not both count the same free bytes."""
        cluster.prepare_file_upload(f"{HOME}/a", 8 * MIB, "ana")
        reserved = cluster.cluster.reserved_bytes
        assert reserved, "the upload should have reserved space somewhere"
        for node_id, claimed in reserved.items():
            node = cluster.cluster.datanodes[node_id]
            assert usable_space(node, reserved) == node.free_bytes() - claimed


class TestPrepareValidation:
    def test_a_relative_path_is_a_bad_request(self, cluster):
        assert code_of(lambda: cluster.prepare_file_upload("f", 1, "ana")) is ErrorCode.BAD_REQUEST

    def test_a_traversal_is_a_bad_request(self, cluster):
        code = code_of(lambda: cluster.prepare_file_upload("/users/ana/../bruno/f", 1, "ana"))
        assert code is ErrorCode.BAD_REQUEST

    def test_the_root_is_a_directory(self, cluster):
        assert code_of(lambda: cluster.prepare_file_upload("/", 1, "ana")) is ErrorCode.IS_A_DIRECTORY

    @pytest.mark.parametrize("size", [-1, 1.5, "10", None, True], ids=["negative", "float", "str", "none", "bool"])
    def test_a_bad_size_is_rejected(self, cluster, size):
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", size, "ana")) is ErrorCode.BAD_REQUEST

    def test_a_file_over_the_limit_is_too_large(self, cluster):
        too_big = cluster.settings.max_file_size_bytes + 1
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", too_big, "ana")) is ErrorCode.FILE_TOO_LARGE

    def test_an_upload_needs_an_owner(self, cluster):
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", 1, "")) is ErrorCode.BAD_REQUEST

    def test_the_parent_directory_must_exist(self, cluster):
        assert code_of(lambda: cluster.prepare_file_upload("/users/ghost/f", 1, "ana")) is ErrorCode.DIR_NOT_FOUND

    def test_a_path_through_a_file_is_not_a_directory(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f/x", 1, "ana")) is ErrorCode.NOT_A_DIRECTORY

    def test_a_directory_cannot_be_overwritten_by_a_file(self, cluster):
        cluster.store.mkdir(f"{HOME}/d", "ana", cluster.clock.time())
        code = code_of(lambda: cluster.prepare_file_upload(f"{HOME}/d", 1, "ana", overwrite=True))
        assert code is ErrorCode.IS_A_DIRECTORY

    def test_fewer_live_nodes_than_the_replication_factor_is_refused(self, nn):
        add_nodes(nn, 2)
        nn.store.mkdir(HOME, "ana", nn.clock.time())
        assert code_of(lambda: nn.prepare_file_upload(f"{HOME}/f", 1, "ana")) is ErrorCode.NOT_ENOUGH_DATANODES

    def test_dead_nodes_do_not_count_towards_that_minimum(self, cluster, clock):
        clock.advance(31)
        for node_id in ("dn1", "dn2"):  # only two nodes keep beating
            cluster.record_heartbeat(node_id, 100 * GIB, 0)
        cluster.check_liveness()
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", 1, "ana")) is ErrorCode.NOT_ENOUGH_DATANODES

    def test_a_failure_halfway_reserves_nothing(self, nn):
        """Block 0 fits but block 1 does not: no space may stay claimed, no file may remain."""
        add_nodes(nn, 3, capacity=5 * MIB)  # room for one 4 MiB block per node, not two
        nn.store.mkdir(HOME, "ana", nn.clock.time())
        assert code_of(lambda: nn.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")) is ErrorCode.NOT_ENOUGH_DATANODES
        assert nn.cluster.reserved_bytes == {}
        assert nn.cluster.reservations == {}
        assert nn.store.files() == []


class TestConflicts:
    def test_a_second_upload_to_the_same_path_is_in_progress(self, cluster):
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", MIB, "bruno")) is ErrorCode.UPLOAD_IN_PROGRESS

    def test_an_expired_upload_no_longer_blocks_the_path(self, cluster, clock):
        old = cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s + 1)
        fresh = cluster.prepare_file_upload(f"{HOME}/f", MIB, "bruno")
        assert fresh.file.file_id != old.file.file_id
        assert cluster.store.get_file_by_id(old.file.file_id).status is FileStatus.ABORTED

    def test_reclaiming_an_expired_upload_releases_its_reservations(self, cluster, clock):
        cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        clock.advance(cluster.settings.upload_ttl_s + 1)
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        assert sum(cluster.cluster.reserved_bytes.values()) == MIB * 3

    def test_an_existing_file_is_not_overwritten_by_default(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        assert code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")) is ErrorCode.ALREADY_EXISTS

    def test_overwrite_keeps_serving_the_old_version_until_the_new_one_commits(self, cluster):
        """API.md Section 5.4: readers see the complete old version, never a mix."""
        old = upload(cluster, f"{HOME}/f", MIB)
        cluster.prepare_file_upload(f"{HOME}/f", 2 * MIB, "ana", overwrite=True)
        assert cluster.store.get_file(f"{HOME}/f").file_id == old.file.file_id

    def test_a_pending_overwrite_still_reserves_the_path(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana", overwrite=True)
        code = code_of(lambda: cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana", overwrite=True))
        assert code is ErrorCode.UPLOAD_IN_PROGRESS


class TestCommit:
    def test_commit_fails_when_nothing_was_confirmed(self, cluster):
        """The pipeline is a plan; the quorum counts confirmations (Section 6.4)."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.INSUFFICIENT_REPLICAS

    def test_one_confirmed_replica_is_below_the_quorum(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan, replicas=1)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.INSUFFICIENT_REPLICAS

    def test_two_of_three_is_the_write_quorum(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan, replicas=2)
        file = cluster.commit_file_upload(plan.file.file_id, commit_body(plan))
        assert file.status is FileStatus.COMMITTED

    def test_one_under_replicated_block_blocks_the_whole_commit(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan)
        last = plan.blocks[-1]
        for node in last.pipeline[1:]:  # lose two of the three replicas of the last block
            cluster.cluster.replicas[last.block.block_id].discard(node.node_id)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.INSUFFICIENT_REPLICAS

    def test_replicas_on_dead_nodes_do_not_count(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan, replicas=2)
        victim = plan.blocks[0].pipeline[0].node_id
        clock.advance(31)
        for node_id in cluster.cluster.datanodes:
            if node_id != victim:
                cluster.record_heartbeat(node_id, 100 * GIB, 0)
        assert cluster.check_liveness() == [victim]
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.INSUFFICIENT_REPLICAS

    def test_a_committed_file_becomes_visible(self, cluster):
        plan = upload(cluster, f"{HOME}/f", 10 * MIB)
        entry = cluster.store.list_dir(HOME)[0]
        assert (entry.name, entry.type, entry.size_bytes) == ("f", "file", 10 * MIB)
        assert cluster.store.get_file(f"{HOME}/f").file_id == plan.file.file_id

    def test_commit_stores_the_checksums_and_clears_the_expiry(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan)
        whole = "ab" * 32
        file = cluster.commit_file_upload(plan.file.file_id, commit_body(plan), whole)
        assert file.expires_at is None
        assert file.checksum == whole
        assert [b.checksum for b in file.blocks] == [checksum_of(p.block.block_id) for p in plan.blocks]

    def test_a_datanode_that_stored_other_bytes_is_a_checksum_mismatch(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        for node in block.pipeline:
            cluster.record_block_received(node.node_id, block.block.block_id, 4 * MIB, "f" * 64)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.CHECKSUM_MISMATCH

    def test_one_corrupt_replica_among_good_ones_is_dropped_and_deleted(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        good, bad = block.pipeline[:2], block.pipeline[2]
        for node in good:
            cluster.record_block_received(node.node_id, block.block.block_id, 4 * MIB,
                                          checksum_of(block.block.block_id))
        cluster.record_block_received(bad.node_id, block.block.block_id, 4 * MIB, "f" * 64)

        cluster.commit_file_upload(plan.file.file_id, commit_body(plan))

        assert bad.node_id not in cluster.cluster.replicas[block.block.block_id]
        assert [c.cmd for c in cluster.cluster.pending_commands[bad.node_id]] == ["delete"]

    def test_commit_releases_reservations_including_for_nodes_that_never_confirmed(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan, replicas=2)  # the third hop of every block never confirms
        cluster.commit_file_upload(plan.file.file_id, commit_body(plan))
        assert cluster.cluster.reserved_bytes == {}
        assert cluster.cluster.reservations == {}
        assert all(p.block.block_id not in cluster.cluster.pipelines for p in plan.blocks)

    def test_the_commit_quorum_follows_the_settings(self, cluster):
        cluster.settings = type(cluster.settings)(
            block_size=4 * MIB, block_token_secret=SECRET, min_replicas_commit=3,
        )
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan, replicas=2)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.INSUFFICIENT_REPLICAS


class TestCommitBody:
    def test_every_block_must_be_listed(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan)
        body = commit_body(plan)[:1]
        assert code_of(lambda: cluster.commit_file_upload(plan.file.file_id, body)) is ErrorCode.BAD_REQUEST

    def test_a_block_cannot_be_listed_twice(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan)
        body = commit_body(plan)
        assert code_of(lambda: cluster.commit_file_upload(plan.file.file_id, body + [body[0]])) is ErrorCode.BAD_REQUEST

    def test_a_wrong_block_id_is_rejected(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        body = [CommittedBlock(0, "blk_other_000000", 4 * MIB, "a" * 64)]
        assert code_of(lambda: cluster.commit_file_upload(plan.file.file_id, body)) is ErrorCode.BAD_REQUEST

    def test_a_wrong_block_size_is_rejected(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        body = commit_body(plan)
        body[0] = CommittedBlock(0, body[0].block_id, 4 * MIB - 1, body[0].checksum)
        assert code_of(lambda: cluster.commit_file_upload(plan.file.file_id, body)) is ErrorCode.BAD_REQUEST

    @pytest.mark.parametrize("bad", ["", "xyz", "A" * 64, "a" * 63], ids=["empty", "short", "uppercase", "63-chars"])
    def test_a_checksum_must_be_lowercase_sha256_hex(self, cluster, bad):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        body = [CommittedBlock(0, plan.blocks[0].block.block_id, 4 * MIB, bad)]
        assert code_of(lambda: cluster.commit_file_upload(plan.file.file_id, body)) is ErrorCode.BAD_REQUEST

    def test_a_malformed_file_checksum_is_rejected(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan), "nope"))
        assert code is ErrorCode.BAD_REQUEST


class TestCommitLifecycle:
    def test_committing_twice_with_the_same_body_succeeds(self, cluster):
        """Idempotent, so a client that lost the response can retry safely."""
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        confirm_all(cluster, plan)
        first = cluster.commit_file_upload(plan.file.file_id, commit_body(plan))
        again = cluster.commit_file_upload(plan.file.file_id, commit_body(plan))
        assert again is first

    def test_committing_again_with_different_contents_is_refused(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        cluster.commit_file_upload(plan.file.file_id, commit_body(plan))
        other = [CommittedBlock(0, plan.blocks[0].block.block_id, 4 * MIB, "a" * 64)]
        assert code_of(lambda: cluster.commit_file_upload(plan.file.file_id, other)) is ErrorCode.ALREADY_EXISTS

    def test_an_unknown_upload_cannot_be_committed(self, cluster):
        assert code_of(lambda: cluster.commit_file_upload("ghost", [])) is ErrorCode.FILE_NOT_FOUND

    def test_an_aborted_upload_cannot_be_committed(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        cluster.abort_file_upload(plan.file.file_id)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.FILE_NOT_FOUND

    def test_an_expired_upload_cannot_be_committed_and_is_aborted(self, cluster, clock):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        confirm_all(cluster, plan)
        clock.advance(cluster.settings.upload_ttl_s + 1)
        code = code_of(lambda: cluster.commit_file_upload(plan.file.file_id, commit_body(plan)))
        assert code is ErrorCode.FILE_NOT_FOUND
        assert cluster.store.get_file_by_id(plan.file.file_id).status is FileStatus.ABORTED
        assert cluster.cluster.reserved_bytes == {}


class TestOverwrite:
    def test_commit_swaps_the_version_and_forgets_the_old_blocks(self, cluster):
        old = upload(cluster, f"{HOME}/f", 8 * MIB)
        new = upload(cluster, f"{HOME}/f", 4 * MIB, overwrite=True)

        assert cluster.store.get_file(f"{HOME}/f").file_id == new.file.file_id
        assert cluster.store.get_file_by_id(old.file.file_id) is None
        assert all(cluster.store.get_block(p.block.block_id) is None for p in old.blocks)

    def test_the_nodes_holding_the_old_version_are_told_to_delete_it(self, cluster):
        old = upload(cluster, f"{HOME}/f", 4 * MIB)
        holders = {n.node_id for n in old.blocks[0].pipeline}
        upload(cluster, f"{HOME}/f", 4 * MIB, overwrite=True)

        deletes = {
            node_id: [c.block_id for c in cmds if c.cmd == "delete"]
            for node_id, cmds in cluster.cluster.pending_commands.items()
        }
        for node_id in holders:
            assert old.blocks[0].block.block_id in deletes[node_id]
        assert old.blocks[0].block.block_id not in cluster.cluster.replicas


class TestAbort:
    def test_abort_frees_the_path_and_the_reserved_space(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 8 * MIB, "ana")
        cluster.abort_file_upload(plan.file.file_id)
        assert cluster.store.get_pending(f"{HOME}/f") is None
        assert cluster.cluster.reserved_bytes == {}
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")  # the path is free again

    def test_abort_tells_the_pipeline_nodes_to_delete_what_they_may_hold(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        cluster.abort_file_upload(plan.file.file_id)
        for node in block.pipeline:
            commands = cluster.cluster.pending_commands[node.node_id]
            assert [(c.cmd, c.block_id) for c in commands] == [("delete", block.block.block_id)]

    def test_abort_is_idempotent(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        cluster.abort_file_upload(plan.file.file_id)
        cluster.abort_file_upload(plan.file.file_id)

    def test_aborting_an_unknown_upload_is_not_an_error(self, cluster):
        cluster.abort_file_upload("ghost")

    def test_a_committed_file_cannot_be_aborted(self, cluster):
        plan = upload(cluster, f"{HOME}/f", MIB)
        assert code_of(lambda: cluster.abort_file_upload(plan.file.file_id)) is ErrorCode.BAD_REQUEST
        assert cluster.store.get_file(f"{HOME}/f") is not None


class TestBlockReceived:
    def test_a_confirmation_records_the_replica_and_its_checksum(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        node = block.pipeline[0].node_id
        cluster.record_block_received(node, block.block.block_id, 4 * MIB, "c" * 64)
        assert cluster.cluster.replicas[block.block.block_id] == {node}
        assert cluster.cluster.replica_checksums[block.block.block_id] == {node: "c" * 64}

    def test_a_confirmation_releases_that_nodes_reservation_only(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        before = sum(cluster.cluster.reserved_bytes.values())
        cluster.record_block_received(block.pipeline[0].node_id, block.block.block_id, 4 * MIB, "c" * 64)
        assert sum(cluster.cluster.reserved_bytes.values()) == before - 4 * MIB

    def test_confirming_twice_does_not_release_twice(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        node = block.pipeline[0].node_id
        for _ in range(2):
            cluster.record_block_received(node, block.block.block_id, 4 * MIB, "c" * 64)
        assert sum(cluster.cluster.reserved_bytes.values()) == 4 * MIB * 2

    def test_a_confirmation_from_an_unknown_node_must_register_first(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        code = code_of(lambda: cluster.record_block_received("ghost", plan.blocks[0].block.block_id, 4 * MIB, "c" * 64))
        assert code is ErrorCode.NODE_NOT_REGISTERED

    def test_a_block_nobody_asked_for_is_told_to_delete_itself(self, cluster):
        cluster.record_block_received("dn1", "blk_orphan_000000", MIB, "c" * 64)
        assert [(c.cmd, c.block_id) for c in cluster.cluster.pending_commands["dn1"]] == [
            ("delete", "blk_orphan_000000")
        ]

    def test_a_block_of_an_aborted_upload_is_told_to_delete_itself(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        cluster.abort_file_upload(plan.file.file_id)
        cluster.record_block_received("dn1", block.block.block_id, 4 * MIB, "c" * 64)
        assert block.block.block_id not in cluster.cluster.replicas

    def test_a_block_of_the_wrong_size_is_not_counted(self, cluster):
        plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana")
        block = plan.blocks[0]
        node = block.pipeline[0].node_id
        cluster.record_block_received(node, block.block.block_id, 4 * MIB - 1, "c" * 64)
        assert block.block.block_id not in cluster.cluster.replicas
