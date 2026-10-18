"""Validation and canonical form of filesystem paths.

``API.md`` Section 1: paths are absolute, use ``/``, live in one global tree
(each home is ``/users/<username>``), and contain no ``.`` or ``..`` segments.
Every layer that accepts a path goes through `split_path`, so there is exactly
one definition of "valid" and one canonical spelling, and a traversal such as
``/users/ana/../bruno`` is rejected before anything resolves it.
"""
from __future__ import annotations

from collections.abc import Sequence

from server.common.errors import DfshaError, ErrorCode

MAX_NAME_BYTES = 255


def _bad(message: str) -> DfshaError:
    return DfshaError(ErrorCode.BAD_REQUEST, message)


def split_path(path: str) -> list[str]:
    """Validates ``path`` and returns its segments; the root is ``[]``.

    A single trailing slash is tolerated (``/a/`` is ``/a``) because shells add
    it. Anything else ambiguous is refused instead of guessed at.
    """
    if not isinstance(path, str) or not path:
        raise _bad("path must be a non-empty string")
    if not path.startswith("/"):
        raise _bad(f"path must be absolute: {path!r}")
    if "\x00" in path:
        raise _bad("path contains a NUL character")
    if path == "/":
        return []
    segments = path.removesuffix("/")[1:].split("/")
    for segment in segments:
        if segment == "":
            raise _bad(f"path has an empty segment: {path!r}")
        if segment in (".", ".."):
            raise _bad(f"path must not contain {segment!r} segments: {path!r}")
        if len(segment.encode("utf-8")) > MAX_NAME_BYTES:
            raise _bad(f"name longer than {MAX_NAME_BYTES} bytes: {segment[:20]!r}...")
    return segments


def join_path(segments: Sequence[str]) -> str:
    """Canonical absolute path for already-validated segments."""
    return "/" + "/".join(segments)


def parent_and_name(path: str) -> tuple[str, str]:
    """Splits ``path`` into its parent directory and final name.

    The root has no name, so asking for it is an error.
    """
    segments = split_path(path)
    if not segments:
        raise _bad("the root has no parent")
    return join_path(segments[:-1]), segments[-1]


__all__ = ["MAX_NAME_BYTES", "join_path", "parent_and_name", "split_path"]
