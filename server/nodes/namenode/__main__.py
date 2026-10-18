"""NameNode process entry point.

Run with: python -m server.nodes.namenode

Configuration comes from the environment (``server/common/config.py``). The
process refuses to start without ``BLOCK_TOKEN_SECRET`` instead of signing block
tokens with an empty key.
"""
from __future__ import annotations

import asyncio
import logging

from server.common.config import ConfigError
from server.nodes.namenode.core import Namenode


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        namenode = Namenode()
    except ConfigError as error:
        raise SystemExit(f"NameNode cannot start, configuration error: {error}") from None
    try:
        asyncio.run(namenode.run())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("NameNode stopped")


if __name__ == "__main__":
    main()
