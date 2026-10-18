"""Path validation, including the traversal attempts it exists to stop."""
from __future__ import annotations

import pytest

from server.common.errors import DfshaError, ErrorCode
from server.common.paths import MAX_NAME_BYTES, join_path, parent_and_name, split_path


class TestValidPaths:
    def test_the_root_has_no_segments(self):
        assert split_path("/") == []

    def test_segments_come_back_in_order(self):
        assert split_path("/users/ana/docs/report.pdf") == ["users", "ana", "docs", "report.pdf"]

    def test_one_trailing_slash_is_tolerated(self):
        """Shells add it; /a/ and /a must name the same thing."""
        assert split_path("/users/ana/") == split_path("/users/ana")

    def test_unicode_names_are_allowed(self):
        assert split_path("/users/ana/informe-año.txt") == ["users", "ana", "informe-año.txt"]

    def test_a_name_at_the_limit_is_accepted(self):
        assert split_path("/" + "a" * MAX_NAME_BYTES) == ["a" * MAX_NAME_BYTES]


class TestRejectedPaths:
    @pytest.mark.parametrize(
        "path",
        [
            "",
            "relative/path",
            "users/ana",
            "//",
            "/users//ana",
            "/users/ana//",
            "/./a",
            "/a/./b",
            "/a/..",
            "/users/ana/../bruno",
            "/../etc",
            "/a\x00b",
        ],
        ids=[
            "empty", "relative", "relative-2", "double-slash", "empty-segment",
            "empty-trailing", "dot-first", "dot-middle", "dotdot-last",
            "traversal-into-another-home", "traversal-above-root", "nul-byte",
        ],
    )
    def test_invalid_paths_are_a_bad_request(self, path):
        with pytest.raises(DfshaError) as error:
            split_path(path)
        assert error.value.code is ErrorCode.BAD_REQUEST
        assert error.value.http_status == 400

    def test_a_name_over_the_limit_is_rejected(self):
        with pytest.raises(DfshaError, match="longer than"):
            split_path("/" + "a" * (MAX_NAME_BYTES + 1))

    def test_the_limit_counts_bytes_not_characters(self):
        """A 4-byte character repeated 64 times is 256 bytes in 64 characters."""
        with pytest.raises(DfshaError, match="longer than"):
            split_path("/" + "😀" * 64)

    def test_non_strings_are_rejected(self):
        with pytest.raises(DfshaError):
            split_path(None)  # type: ignore[arg-type]


class TestHelpers:
    def test_join_is_the_inverse_of_split(self):
        assert join_path(split_path("/users/ana/docs")) == "/users/ana/docs"

    def test_joining_nothing_is_the_root(self):
        assert join_path([]) == "/"

    def test_parent_and_name(self):
        assert parent_and_name("/users/ana/report.pdf") == ("/users/ana", "report.pdf")

    def test_the_parent_of_a_top_level_entry_is_the_root(self):
        assert parent_and_name("/users") == ("/", "users")

    def test_the_root_has_no_parent(self):
        with pytest.raises(DfshaError, match="no parent"):
            parent_and_name("/")
