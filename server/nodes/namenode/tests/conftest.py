"""Shared fixtures: a NameNode on a fake clock with a seeded random source."""
from __future__ import annotations

import random

import pytest
from argon2 import PasswordHasher

from server.common.clock import FakeClock
from server.common.config import MiB, Settings
from server.nodes.namenode.auth_context import reset_principal, set_principal
from server.nodes.namenode.auth_models import User
from server.nodes.namenode.core import Namenode

SECRET = "test-block-token-secret"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings() -> Settings:
    # small blocks so a few MiB of "file" produce several blocks
    return Settings(
        block_size=4 * MiB,
        block_token_secret=SECRET,
        node_key="test-node-key",
        jwt_secret="test-jwt-secret-for-signing-tests-32-bytes",
    )


@pytest.fixture
def nn(settings: Settings, clock: FakeClock) -> Namenode:
    namenode = Namenode(settings=settings, clock=clock, rng=random.Random(7))
    hasher = PasswordHasher(time_cost=1, memory_cost=8192)
    namenode.auth_store.create_user(
        User("ana", hasher.hash("test-password"), "user", clock.time())
    )
    token = set_principal("ana")
    yield namenode
    reset_principal(token)
