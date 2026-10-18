"""DataNode process entry point.

Run with: python -m server.nodes.datanode --id dn1 --port 9001 --storage ./data/dn1
"""
from __future__ import annotations

import argparse
import asyncio

from server.nodes.datanode.core import DEFAULT_NAMENODE_URL, DataNode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DFSha DataNode")
    parser.add_argument("--id", default="dn1", help="unique DataNode id (e.g. dn1)")
    parser.add_argument("--host", default="127.0.0.1", help="host the gRPC server binds to")
    parser.add_argument("--port", type=int, default=9001, help="gRPC port for the block service")
    parser.add_argument("--storage", default="./data/dn1", help="directory where blocks are stored")
    parser.add_argument(
        "--namenode-url", default=DEFAULT_NAMENODE_URL, help="NameNode REST base URL"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    node = DataNode(
        node_id=args.id,
        grpc_host=args.host,
        grpc_port=args.port,
        storage_dir=args.storage,
        namenode_url=args.namenode_url,
    )
    try:
        asyncio.run(node.run())  # arranca el bucle de eventos principal
    except KeyboardInterrupt:
        print(f"\n[DataNode {args.id}] Detenido por el usuario.")


if __name__ == "__main__":
    main()
