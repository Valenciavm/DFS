"""Reading a file's block map and the RF1 file system operations."""
from __future__ import annotations

import pytest

from server.common.errors import ErrorCode
from server.common.tokens import BlockOp, verify
from server.nodes.namenode.models import DirectoryMetadata, FileMetadata
from server.nodes.namenode.tests.conftest import SECRET
from server.nodes.namenode.tests.helpers import (
    GIB,
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


class TestGetFileBlocks:
    def test_blocks_come_back_in_order_with_their_replicas(self, cluster):
        upload(cluster, f"{HOME}/f", 10 * MIB)
        location = cluster.get_file_blocks(f"{HOME}/f", "ana")

        assert location.path == f"{HOME}/f"
        assert [b.block.index for b in location.blocks] == [0, 1, 2]
        for block in location.blocks:
            assert len(block.replicas) == 3
            assert {r.node_id for r in block.replicas} <= set(cluster.cluster.datanodes)

    def test_replicas_carry_the_public_address_clients_need(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        replica = cluster.get_file_blocks(f"{HOME}/f", "ana").blocks[0].replicas[0]
        assert replica.public_addr.endswith(".example.com:9000")

    def test_each_block_has_a_read_token_bound_to_it_and_the_user(self, cluster, clock):
        upload(cluster, f"{HOME}/f", 8 * MIB)
        first, second = cluster.get_file_blocks(f"{HOME}/f", "bruno").blocks

        claims = verify(first.token, SECRET, block_id=first.block.block_id, op=BlockOp.READ,
                        now=clock.time())
        assert claims.user == "bruno"
        assert code_of(
            lambda: verify(first.token, SECRET, block_id=second.block.block_id, op=BlockOp.READ,
                           now=clock.time())
        ) is ErrorCode.FORBIDDEN

    def test_a_read_token_cannot_be_used_to_write(self, cluster, clock):
        upload(cluster, f"{HOME}/f", MIB)
        block = cluster.get_file_blocks(f"{HOME}/f", "ana").blocks[0]
        code = code_of(
            lambda: verify(block.token, SECRET, block_id=block.block.block_id, op=BlockOp.WRITE,
                           now=clock.time())
        )
        assert code is ErrorCode.FORBIDDEN

    def test_dead_replicas_are_left_out(self, cluster, clock):
        plan = upload(cluster, f"{HOME}/f", MIB)
        victim = plan.blocks[0].pipeline[0].node_id
        clock.advance(31)
        for node_id in cluster.cluster.datanodes:
            if node_id != victim:
                cluster.record_heartbeat(node_id, 100 * GIB, 0)
        cluster.check_liveness()

        replicas = cluster.get_file_blocks(f"{HOME}/f", "ana").blocks[0].replicas
        assert victim not in {r.node_id for r in replicas}
        assert len(replicas) == 2

    def test_a_block_with_no_live_replica_is_listed_with_none(self, cluster, clock):
        upload(cluster, f"{HOME}/f", MIB)
        clock.advance(31)
        cluster.check_liveness()  # nobody beat: everyone is dead
        assert cluster.get_file_blocks(f"{HOME}/f", "ana").blocks[0].replicas == []

    def test_replicas_are_shuffled_so_readers_spread_out(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        firsts = {
            cluster.get_file_blocks(f"{HOME}/f", "ana").blocks[0].replicas[0].node_id
            for _ in range(60)
        }
        assert len(firsts) > 1

    def test_a_pending_upload_cannot_be_read(self, cluster):
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        assert code_of(lambda: cluster.get_file_blocks(f"{HOME}/f", "ana")) is ErrorCode.FILE_NOT_FOUND

    def test_a_missing_file_is_not_found(self, cluster):
        assert code_of(lambda: cluster.get_file_blocks(f"{HOME}/nope", "ana")) is ErrorCode.FILE_NOT_FOUND

    def test_a_directory_cannot_be_read_as_a_file(self, cluster):
        assert code_of(lambda: cluster.get_file_blocks(HOME, "ana")) is ErrorCode.IS_A_DIRECTORY

    def test_a_read_needs_a_user(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        assert code_of(lambda: cluster.get_file_blocks(f"{HOME}/f", "")) is ErrorCode.BAD_REQUEST

    def test_an_invalid_path_is_a_bad_request(self, cluster):
        assert code_of(lambda: cluster.get_file_blocks("/users/ana/../x", "ana")) is ErrorCode.BAD_REQUEST

    def test_readers_see_the_old_version_until_an_overwrite_commits(self, cluster):
        """A reader gets the complete old version or the complete new one, never a mix."""
        old = upload(cluster, f"{HOME}/f", 8 * MIB)
        new_plan = cluster.prepare_file_upload(f"{HOME}/f", 4 * MIB, "ana", overwrite=True)
        confirm_all(cluster, new_plan)

        during = cluster.get_file_blocks(f"{HOME}/f", "ana")
        assert during.file.file_id == old.file.file_id
        assert len(during.blocks) == 2

        cluster.commit_file_upload(new_plan.file.file_id, commit_body(new_plan))
        after = cluster.get_file_blocks(f"{HOME}/f", "ana")
        assert after.file.file_id == new_plan.file.file_id
        assert len(after.blocks) == 1


class TestDirectories:
    def test_mkdir_and_ls(self, cluster):
        cluster.mkdir(f"{HOME}/docs", "ana")
        assert [(e.name, e.type) for e in cluster.ls(HOME)] == [("docs", "dir")]

    def test_mkdir_with_parents(self, cluster):
        cluster.mkdir(f"{HOME}/a/b/c", "ana", parents=True)
        assert cluster.stat(f"{HOME}/a/b/c").path() == f"{HOME}/a/b/c"

    def test_mkdir_needs_an_owner(self, cluster):
        assert code_of(lambda: cluster.mkdir(f"{HOME}/d", "")) is ErrorCode.BAD_REQUEST

    def test_mkdir_over_an_existing_name_fails(self, cluster):
        cluster.mkdir(f"{HOME}/d", "ana")
        assert code_of(lambda: cluster.mkdir(f"{HOME}/d", "ana")) is ErrorCode.ALREADY_EXISTS

    def test_ls_hides_uploads_in_progress(self, cluster):
        cluster.prepare_file_upload(f"{HOME}/f", MIB, "ana")
        assert cluster.ls(HOME) == []

    def test_ls_shows_a_file_only_once_committed(self, cluster):
        upload(cluster, f"{HOME}/f", 3 * MIB)
        assert [(e.name, e.size_bytes) for e in cluster.ls(HOME)] == [("f", 3 * MIB)]

    def test_stat_returns_files_and_directories(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        assert isinstance(cluster.stat(f"{HOME}/f"), FileMetadata)
        assert isinstance(cluster.stat(HOME), DirectoryMetadata)

    def test_stat_of_a_missing_path_is_not_found(self, cluster):
        assert code_of(lambda: cluster.stat(f"{HOME}/nope")) is ErrorCode.FILE_NOT_FOUND


class TestRemoval:
    def test_rm_makes_the_file_disappear_at_once(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        cluster.rm(f"{HOME}/f")
        assert cluster.ls(HOME) == []
        assert code_of(lambda: cluster.get_file_blocks(f"{HOME}/f", "ana")) is ErrorCode.FILE_NOT_FOUND

    def test_rm_orders_the_holders_to_delete_the_blocks(self, cluster):
        plan = upload(cluster, f"{HOME}/f", MIB)
        block_id = plan.blocks[0].block.block_id
        holders = {n.node_id for n in plan.blocks[0].pipeline}

        cluster.rm(f"{HOME}/f")

        for node_id in holders:
            commands = cluster.cluster.pending_commands[node_id]
            assert ("delete", block_id) in [(c.cmd, c.block_id) for c in commands]
        assert block_id not in cluster.cluster.replicas
        assert block_id not in cluster.cluster.replica_checksums

    def test_rm_errors(self, cluster):
        cluster.mkdir(f"{HOME}/d", "ana")
        assert code_of(lambda: cluster.rm(f"{HOME}/nope")) is ErrorCode.FILE_NOT_FOUND
        assert code_of(lambda: cluster.rm(f"{HOME}/d")) is ErrorCode.IS_A_DIRECTORY

    def test_rmdir_refuses_a_non_empty_directory_unless_recursive(self, cluster):
        cluster.mkdir(f"{HOME}/d", "ana")
        upload(cluster, f"{HOME}/d/f", MIB)
        assert code_of(lambda: cluster.rmdir(f"{HOME}/d")) is ErrorCode.DIR_NOT_EMPTY

    def test_rmdir_recursive_removes_everything_and_deletes_every_block(self, cluster):
        cluster.mkdir(f"{HOME}/d/e", "ana", parents=True)
        plans = [upload(cluster, f"{HOME}/d/a", MIB), upload(cluster, f"{HOME}/d/e/b", MIB)]

        assert cluster.rmdir(f"{HOME}/d", recursive=True) == 2

        assert cluster.store.stat(f"{HOME}/d") is None
        for plan in plans:
            assert plan.blocks[0].block.block_id not in cluster.cluster.replicas
        queued = {c.block_id for cmds in cluster.cluster.pending_commands.values() for c in cmds}
        assert {p.blocks[0].block.block_id for p in plans} <= queued

    def test_rmdir_of_an_empty_directory(self, cluster):
        cluster.mkdir(f"{HOME}/d", "ana")
        assert cluster.rmdir(f"{HOME}/d") == 0

    def test_rmdir_refuses_while_an_upload_is_in_progress_below(self, cluster):
        cluster.mkdir(f"{HOME}/d", "ana")
        cluster.prepare_file_upload(f"{HOME}/d/f", MIB, "ana")
        assert code_of(lambda: cluster.rmdir(f"{HOME}/d", recursive=True)) is ErrorCode.DIR_NOT_EMPTY

    def test_a_removed_path_can_be_uploaded_again(self, cluster):
        upload(cluster, f"{HOME}/f", MIB)
        cluster.rm(f"{HOME}/f")
        upload(cluster, f"{HOME}/f", 2 * MIB)
        assert cluster.stat(f"{HOME}/f").size_bytes == 2 * MIB
