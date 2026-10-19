"""DataNode process entry point.

Run with: python -m server.nodes.datanode --id dn1 --port 9001 --storage ./data/dn1
"""

from __future__ import annotations

import argparse
import asyncio

from server.common.log_config import configure_logging
from server.nodes.datanode.core import DEFAULT_NAMENODE_URLS, DataNode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DFSha DataNode")
    parser.add_argument("--id", default="dn1", help="unique DataNode id (e.g. dn1)")
    parser.add_argument(
        "--host", default="127.0.0.1", help="host the gRPC server binds to"
    )
    parser.add_argument(
        "--port", type=int, default=9001, help="gRPC port for the block service"
    )
    parser.add_argument(
        "--internal-addr",
        default=None,
        help="internal address advertised to other DataNodes",
    )
    parser.add_argument(
        "--public-addr", default=None, help="public address advertised to clients"
    )
    parser.add_argument(
        "--storage", default="./data/dn1", help="directory where blocks are stored"
    )
    parser.add_argument(
        "--namenode-urls",
        default=DEFAULT_NAMENODE_URLS,
        help="comma-separated NameNode REST base URLs",
    )
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    node = DataNode(
        node_id=args.id,
        grpc_host=args.host,
        grpc_port=args.port,
        internal_addr=args.internal_addr,
        public_addr=args.public_addr,
        storage_dir=args.storage,
        namenode_urls=args.namenode_urls,
    )
    try:
        asyncio.run(node.run())  # arranca el bucle de eventos principal
    except KeyboardInterrupt:
        print(f"\n[DataNode {args.id}] Detenido por el usuario.")


if __name__ == "__main__":
    main()
