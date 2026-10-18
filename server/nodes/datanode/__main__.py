"""DataNode process entry point.

Run with: python -m server.nodes.datanode
"""
from __future__ import annotations

import asyncio

from server.nodes.datanode.core import DataNode


def main() -> None:
    node = DataNode(node_id="dn1", grpc_port=9001, storage_dir="./data/dn1")
    asyncio.run(node.run())


if __name__ == "__main__":
    main()
