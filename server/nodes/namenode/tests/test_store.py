"""The in-memory MetadataStore, against the contract in store/base.py."""
from __future__ import annotations

import os

import pytest

from server.common.errors import DfshaError, ErrorCode
from server.nodes.namenode.models import BlockMetadata, FileMetadata, FileStatus
from server.nodes.namenode.store import (
    USERS_DIR,
    InMemoryMetadataStore,
    MetadataStore,
    PostgresMetadataStore,
)

T0 = 1_000.0


def a_file(name: str, file_id: str = "f1", blocks: int = 2, owner: str = "ana") -> FileMetadata:
    return FileMetadata(
        file_id=file_id,
        name=name,
        owner=owner,
        size_bytes=100 * blocks,
        block_size=100,
        replication=3,
        created_at=T0,
        modified_at=T0,
        expires_at=T0 + 3600,
        blocks=[
            BlockMetadata(block_id=f"blk_{file_id}_{i}", index=i, size_bytes=100)
            for i in range(blocks)
        ],
    )


@pytest.fixture(
    params=[
        pytest.param("memory", id="memory"),
        pytest.param(
            "postgres",
            id="postgres",
            marks=pytest.mark.skipif(
                not os.getenv("POSTGRES_TEST_DSN"),
                reason="POSTGRES_TEST_DSN is not configured",
            ),
        ),
    ]
)
def store(request) -> MetadataStore:
    if request.param == "postgres":
        postgres_database = request.getfixturevalue("postgres_database")
        s = PostgresMetadataStore(postgres_database, now=T0)
    else:
        s = InMemoryMetadataStore(now=T0)
    s.mkdir("/users/ana", "ana", T0)
    return s


def code_of(call) -> ErrorCode:
    with pytest.raises(DfshaError) as error:
        call()
    return error.value.code


class TestNamespace:
    def test_the_users_directory_exists_from_the_start(self, store):
        """Decision 20: every home lives under /users."""
        assert store.resolve_dir(USERS_DIR).path() == "/users"

    def test_mkdir_creates_and_lists_a_directory(self, store):
        store.mkdir("/users/ana/docs", "ana", T0 + 1)
        assert [e.name for e in store.list_dir("/users/ana")] == ["docs"]

    def test_mkdir_over_an_existing_directory_fails(self, store):
        store.mkdir("/users/ana/docs", "ana", T0)
        assert code_of(lambda: store.mkdir("/users/ana/docs", "ana", T0)) is ErrorCode.ALREADY_EXISTS

    def test_mkdir_without_parents_needs_the_parent(self, store):
        assert code_of(lambda: store.mkdir("/users/ana/a/b", "ana", T0)) is ErrorCode.DIR_NOT_FOUND

    def test_mkdir_with_parents_creates_the_chain_and_is_idempotent(self, store):
        store.mkdir("/users/ana/a/b/c", "ana", T0, parents=True)
        again = store.mkdir("/users/ana/a/b/c", "ana", T0 + 5, parents=True)
        assert again.path() == "/users/ana/a/b/c"

    def test_mkdir_cannot_recreate_the_root(self, store):
        assert code_of(lambda: store.mkdir("/", "ana", T0)) is ErrorCode.ALREADY_EXISTS

    def test_a_path_through_a_file_is_not_a_directory(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.commit_file("f1", T0)
        assert code_of(lambda: store.resolve_dir("/users/ana/f/x")) is ErrorCode.NOT_A_DIRECTORY
        assert code_of(lambda: store.mkdir("/users/ana/f/x", "ana", T0)) is ErrorCode.NOT_A_DIRECTORY

    def test_a_file_cannot_be_used_as_parent_for_an_upload(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.commit_file("f1", T0)
        assert (
            code_of(
                lambda: store.create_pending_file(
                    "/users/ana/f/child", a_file("child", file_id="child")
                )
            )
            is ErrorCode.NOT_A_DIRECTORY
        )

    def test_listing_a_missing_directory_fails(self, store):
        assert code_of(lambda: store.list_dir("/users/nobody")) is ErrorCode.DIR_NOT_FOUND

    def test_listing_is_sorted_and_marks_types(self, store):
        store.mkdir("/users/ana/zeta", "ana", T0)
        store.create_pending_file("/users/ana/alpha", a_file("alpha"))
        store.commit_file("f1", T0)
        entries = store.list_dir("/users/ana")
        assert [(e.name, e.type) for e in entries] == [("alpha", "file"), ("zeta", "dir")]
        assert entries[0].size_bytes == 200

    def test_stat_finds_files_directories_and_the_root(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.commit_file("f1", T0)
        assert store.stat("/").path() == "/"
        assert store.stat("/users/ana").path() == "/users/ana"
        assert store.stat("/users/ana/f").name == "f"
        assert store.stat("/users/ana/missing") is None
        assert store.stat("/nowhere/at/all") is None


class TestPendingUploads:
    def test_a_pending_file_is_invisible(self, store):
        """ARCHITECTURE.md Section 3: nothing shows until commit."""
        store.create_pending_file("/users/ana/f", a_file("f"))
        assert store.list_dir("/users/ana") == []
        assert store.stat("/users/ana/f") is None
        assert store.get_file("/users/ana/f") is None

    def test_but_it_is_reachable_by_id_and_by_path_as_pending(self, store):
        pending = a_file("f")
        store.create_pending_file("/users/ana/f", pending)
        assert store.get_file_by_id("f1") == pending
        assert store.get_pending("/users/ana/f") == pending

    def test_a_pending_upload_reserves_its_path(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        second = a_file("f", file_id="f2")
        assert code_of(lambda: store.create_pending_file("/users/ana/f", second)) is ErrorCode.UPLOAD_IN_PROGRESS

    def test_a_pending_upload_blocks_mkdir_of_the_same_name(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        assert code_of(lambda: store.mkdir("/users/ana/f", "ana", T0)) is ErrorCode.ALREADY_EXISTS

    def test_a_file_cannot_take_the_name_of_a_directory(self, store):
        store.mkdir("/users/ana/docs", "ana", T0)
        assert code_of(lambda: store.create_pending_file("/users/ana/docs", a_file("docs"))) is ErrorCode.IS_A_DIRECTORY

    def test_the_parent_directory_must_exist(self, store):
        assert code_of(lambda: store.create_pending_file("/users/ana/no/f", a_file("f"))) is ErrorCode.DIR_NOT_FOUND

    def test_the_name_must_match_the_path(self, store):
        with pytest.raises(ValueError, match="does not match"):
            store.create_pending_file("/users/ana/other", a_file("f"))

    def test_duplicate_block_ids_are_refused(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        clash = a_file("g", file_id="f1")  # same block ids as the first file
        with pytest.raises(ValueError, match="duplicate"):
            store.create_pending_file("/users/ana/g", clash)

    def test_blocks_are_indexed_with_their_file(self, store):
        pending = a_file("f")
        store.create_pending_file("/users/ana/f", pending)
        assert store.get_block("blk_f1_1") == pending.blocks[1]
        assert store.file_of_block("blk_f1_1") == pending

    def test_path_of_works_for_pending_and_committed(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        assert store.path_of("f1") == "/users/ana/f"
        store.commit_file("f1", T0)
        assert store.path_of("f1") == "/users/ana/f"

    def test_path_of_follows_a_file_at_the_top_level(self, store):
        store.create_pending_file("/top", a_file("top"))
        assert store.path_of("f1") == "/top"


class TestCommit:
    def test_commit_makes_the_file_visible_and_clears_the_expiry(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        replaced = store.commit_file("f1", T0 + 10)
        file = store.get_file("/users/ana/f")
        assert replaced is None
        assert file is not None and file.status is FileStatus.COMMITTED
        assert file.expires_at is None
        assert file.modified_at == T0 + 10
        assert store.get_pending("/users/ana/f") is None

    def test_commit_over_an_existing_file_returns_the_old_one(self, store):
        store.create_pending_file("/users/ana/f", a_file("f", file_id="old"))
        store.commit_file("old", T0)
        store.create_pending_file("/users/ana/f", a_file("f", file_id="new"))
        replaced = store.commit_file("new", T0 + 1)

        assert replaced is not None and replaced.file_id == "old"
        assert store.get_file("/users/ana/f").file_id == "new"
        # the old version is gone from every index, blocks included
        assert store.get_file_by_id("old") is None
        assert store.get_block("blk_old_0") is None

    def test_the_old_version_stays_visible_until_the_new_one_commits(self, store):
        """Overwrite semantics, API.md Section 5.4: readers never see a mix."""
        store.create_pending_file("/users/ana/f", a_file("f", file_id="old"))
        store.commit_file("old", T0)
        store.create_pending_file("/users/ana/f", a_file("f", file_id="new"))
        assert store.get_file("/users/ana/f").file_id == "old"

    def test_only_a_pending_file_can_be_committed(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.commit_file("f1", T0)
        with pytest.raises(ValueError, match="only PENDING"):
            store.commit_file("f1", T0)

    def test_committing_an_unknown_file_is_a_key_error(self, store):
        with pytest.raises(KeyError):
            store.commit_file("ghost", T0)


class TestAbort:
    def test_abort_releases_the_path_but_keeps_the_record(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.abort_file("f1", T0 + 5)
        store.abort_file("f1", T0 + 10)
        assert store.get_pending("/users/ana/f") is None
        record = store.get_file_by_id("f1")
        assert record is not None and record.status is FileStatus.ABORTED
        # the path is free for a fresh upload
        store.create_pending_file("/users/ana/f", a_file("f", file_id="f2"))

    def test_aborted_blocks_stay_indexed_so_late_reports_can_be_recognised(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.abort_file("f1", T0)
        assert store.file_of_block("blk_f1_0").status is FileStatus.ABORTED

    def test_purge_forgets_only_old_aborted_files(self, store):
        store.create_pending_file("/users/ana/a", a_file("a", file_id="fa"))
        store.create_pending_file("/users/ana/b", a_file("b", file_id="fb"))
        store.abort_file("fa", T0 + 10)
        store.abort_file("fb", T0 + 100)

        purged = store.purge_aborted(older_than=T0 + 50)

        assert [f.file_id for f in purged] == ["fa"]
        assert store.get_file_by_id("fa") is None
        assert store.get_block("blk_fa_0") is None
        assert store.get_file_by_id("fb") is not None

    def test_purge_never_touches_pending_or_committed_files(self, store):
        store.create_pending_file("/users/ana/p", a_file("p", file_id="fp"))
        store.create_pending_file("/users/ana/c", a_file("c", file_id="fc"))
        store.commit_file("fc", T0)
        assert store.purge_aborted(older_than=T0 + 1_000_000) == []


class TestRemoval:
    def test_remove_file_returns_it_and_forgets_its_blocks(self, store):
        store.create_pending_file("/users/ana/f", a_file("f"))
        store.commit_file("f1", T0)
        removed = store.remove_file("/users/ana/f", T0 + 1)
        assert removed.file_id == "f1"
        assert store.get_file("/users/ana/f") is None
        assert store.get_block("blk_f1_0") is None

    def test_remove_file_errors(self, store):
        store.mkdir("/users/ana/d", "ana", T0)
        assert code_of(lambda: store.remove_file("/users/ana/nope", T0)) is ErrorCode.FILE_NOT_FOUND
        assert code_of(lambda: store.remove_file("/users/ana/d", T0)) is ErrorCode.IS_A_DIRECTORY
        assert code_of(lambda: store.remove_file("/", T0)) is ErrorCode.IS_A_DIRECTORY

    def test_remove_dir_refuses_a_non_empty_directory(self, store):
        store.mkdir("/users/ana/d/e", "ana", T0, parents=True)
        assert code_of(lambda: store.remove_dir("/users/ana/d", False, T0)) is ErrorCode.DIR_NOT_EMPTY

    def test_remove_dir_recursive_returns_every_file_below(self, store):
        store.mkdir("/users/ana/d/e", "ana", T0, parents=True)
        for i, where in enumerate(("/users/ana/d/a", "/users/ana/d/e/b")):
            store.create_pending_file(where, a_file(where.rsplit("/", 1)[1], file_id=f"f{i}"))
            store.commit_file(f"f{i}", T0)

        removed = store.remove_dir("/users/ana/d", True, T0 + 1)

        assert sorted(f.file_id for f in removed) == ["f0", "f1"]
        assert store.stat("/users/ana/d") is None
        assert store.get_block("blk_f0_0") is None

    def test_remove_empty_dir_without_recursive(self, store):
        store.mkdir("/users/ana/d", "ana", T0)
        assert store.remove_dir("/users/ana/d", False, T0) == []
        assert store.stat("/users/ana/d") is None

    def test_remove_dir_refuses_while_an_upload_is_in_progress_below(self, store):
        """Otherwise the pending file would lose its parent mid-upload."""
        store.mkdir("/users/ana/d", "ana", T0)
        store.create_pending_file("/users/ana/d/f", a_file("f"))
        assert code_of(lambda: store.remove_dir("/users/ana/d", True, T0)) is ErrorCode.DIR_NOT_EMPTY

    def test_the_root_and_the_users_directory_are_protected(self, store):
        assert code_of(lambda: store.remove_dir("/", True, T0)) is ErrorCode.BAD_REQUEST
        assert code_of(lambda: store.remove_dir("/users", True, T0)) is ErrorCode.BAD_REQUEST


class TestMove:
    def test_move_file_updates_its_path_and_timestamp(self, store):
        store.mkdir("/users/ana/dest", "ana", T0)
        store.create_pending_file("/users/ana/source", a_file("source"))
        store.commit_file("f1", T0)

        moved = store.mv("/users/ana/source", "/users/ana/dest/renamed", T0 + 5)

        assert moved.file_id == "f1"
        assert moved.name == "renamed"
        assert moved.modified_at == T0 + 5
        assert store.get_file("/users/ana/source") is None
        assert store.get_file("/users/ana/dest/renamed").file_id == "f1"
        assert store.path_of("f1") == "/users/ana/dest/renamed"

    def test_move_directory_updates_descendant_and_pending_upload_paths(self, store):
        store.mkdir("/users/ana/source/nested", "ana", T0, parents=True)
        store.mkdir("/users/ana/destination", "ana", T0)
        store.create_pending_file(
            "/users/ana/source/nested/pending",
            a_file("pending", file_id="pending"),
        )
        store.create_pending_file(
            "/users/ana/source/nested/committed",
            a_file("committed", file_id="committed"),
        )
        store.commit_file("committed", T0)

        moved = store.mv(
            "/users/ana/source", "/users/ana/destination/moved", T0 + 5
        )

        assert moved.path() == "/users/ana/destination/moved"
        assert store.path_of("committed") == (
            "/users/ana/destination/moved/nested/committed"
        )
        assert store.get_pending(
            "/users/ana/destination/moved/nested/pending"
        ).file_id == "pending"
        store.commit_file("pending", T0 + 6)
        assert store.get_file(
            "/users/ana/destination/moved/nested/pending"
        ).file_id == "pending"

    def test_move_rejects_descendants_and_existing_destinations(self, store):
        store.mkdir("/users/ana/source/child", "ana", T0, parents=True)
        store.mkdir("/users/ana/occupied", "ana", T0)

        assert (
            code_of(
                lambda: store.mv(
                    "/users/ana/source", "/users/ana/source/child/moved", T0
                )
            )
            is ErrorCode.BAD_REQUEST
        )
        assert (
            code_of(
                lambda: store.mv("/users/ana/source", "/users/ana/occupied", T0)
            )
            is ErrorCode.ALREADY_EXISTS
        )


class TestSnapshots:
    def test_files_filters_by_status(self, store):
        store.create_pending_file("/users/ana/p", a_file("p", file_id="fp"))
        store.create_pending_file("/users/ana/c", a_file("c", file_id="fc"))
        store.create_pending_file("/users/ana/x", a_file("x", file_id="fx"))
        store.commit_file("fc", T0)
        store.abort_file("fx", T0)

        assert [f.file_id for f in store.files(FileStatus.PENDING)] == ["fp"]
        assert [f.file_id for f in store.files(FileStatus.COMMITTED)] == ["fc"]
        assert [f.file_id for f in store.files(FileStatus.ABORTED)] == ["fx"]
        assert len(store.files()) == 3

    def test_files_returns_a_copy_that_survives_mutation(self, store):
        """Background tasks iterate while other calls mutate (Section 4.4)."""
        store.create_pending_file("/users/ana/c", a_file("c", file_id="fc"))
        store.commit_file("fc", T0)
        snapshot = store.files(FileStatus.COMMITTED)
        store.remove_file("/users/ana/c", T0)
        assert [f.file_id for f in snapshot] == ["fc"]
