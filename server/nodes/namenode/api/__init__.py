"""NameNode REST API package."""
from __future__ import annotations

from server.nodes.namenode.api.app import create_app

__all__ = ["create_app"]
