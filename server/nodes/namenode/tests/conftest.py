"""Shared fixtures: a NameNode on a fake clock with a seeded random source."""
from __future__ import annotations

import os
import random
import uuid

import psycopg
import pytest
from argon2 import PasswordHasher
from psycopg.sql import SQL, Identifier

from server.common.clock import FakeClock
from server.common.config import MiB, Settings
from server.nodes.namenode.auth_context import reset_principal, set_principal
from server.nodes.namenode.auth_models import User
from server.nodes.namenode.core import Namenode
from server.nodes.namenode.store.postgres import PostgresDatabase

POSTGRES_TEST_DSN = os.getenv("POSTGRES_TEST_DSN")

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


@pytest.fixture
def postgres_database():
    """An isolated schema in the explicitly designated test database."""
    if not POSTGRES_TEST_DSN:
        pytest.skip("POSTGRES_TEST_DSN is not configured")
    schema = f"test_{uuid.uuid4().hex}"
    with psycopg.connect(POSTGRES_TEST_DSN, autocommit=True) as connection:
        connection.execute(SQL("CREATE SCHEMA {}").format(Identifier(schema)))
    try:
        yield PostgresDatabase(POSTGRES_TEST_DSN, schema=schema)
    finally:
        with psycopg.connect(POSTGRES_TEST_DSN, autocommit=True) as connection:
            connection.execute(
                SQL("DROP SCHEMA {} CASCADE").format(Identifier(schema))
            )
