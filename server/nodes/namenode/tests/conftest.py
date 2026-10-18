"""Shared fixtures: a NameNode on a fake clock with a seeded random source."""
from __future__ import annotations

import random

import pytest

from server.common.clock import FakeClock
from server.common.config import MiB, Settings
from server.nodes.namenode.core import Namenode

SECRET = "test-block-token-secret"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings() -> Settings:
    # small blocks so a few MiB of "file" produce several blocks
    return Settings(block_size=4 * MiB, block_token_secret=SECRET, node_key="test-node-key")


@pytest.fixture
def nn(settings: Settings, clock: FakeClock) -> Namenode:
    return Namenode(settings=settings, clock=clock, rng=random.Random(7))
