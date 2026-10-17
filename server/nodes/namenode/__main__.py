"""NameNode process entry point.

Run with: python -m server.nodes.namenode
"""
from __future__ import annotations

import asyncio

from server.nodes.namenode.core import Namenode


def main() -> None:
    asyncio.run(Namenode().run())


if __name__ == "__main__":
    main()
