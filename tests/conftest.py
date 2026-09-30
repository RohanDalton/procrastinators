"""Fixtures shared across the whole suite."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from tests.limiters import Rig
from tests.postgres_service import POSTGRES_URL, statement
from tests.postgres_service import reachable as postgres_reachable
from tests.redis_service import SERVICE_URLS, reachable, remove_run_keys
from tests.service_gate import (
    pytest_addoption,
    pytest_collection_modifyitems,
    pytest_configure,
    pytest_runtest_makereport,
    pytest_sessionfinish,
    service_available,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.service_gate import ServiceGate
else:
    pass

__all__ = [
    "postgres",
    "project_root",
    "pytest_addoption",
    "pytest_collection_modifyitems",
    "pytest_configure",
    "pytest_runtest_makereport",
    "pytest_sessionfinish",
    "redis_servers_used",
    "redis_url",
    "rig",
    "schema",
    "service_available",
]

pytest_plugins = ("pytester",)


@pytest.fixture(scope="session")
def project_root() -> Path:
    """The repository root, so tests can inspect the tree and run tools from it."""
    root = Path(__file__).resolve().parents[1]
    return root


@pytest.fixture(scope="session")
def redis_servers_used() -> Iterator[set[str]]:
    """The Redis and Valkey servers this session wrote to, emptied of its keys at the end."""
    used: set[str] = set()
    yield used
    for url in used:
        remove_run_keys(url)


@pytest.fixture(
    params=[pytest.param(name, marks=pytest.mark.service(name)) for name in SERVICE_URLS],
)
def redis_url(
    request: pytest.FixtureRequest, service_available: ServiceGate, redis_servers_used: set[str]
) -> str:
    """The address of a reachable Redis, then Valkey, server; each test runs against both."""
    name = request.param
    url = SERVICE_URLS[name]
    service_available(name, lambda: reachable(url), f"nothing answers at {url}")
    redis_servers_used.add(url)
    return url


@pytest.fixture
def postgres(service_available: ServiceGate) -> str:
    """The PostgreSQL server's URL, once it is known to be reachable."""
    service_available("postgres", postgres_reachable, f"cannot connect to {POSTGRES_URL}")
    return POSTGRES_URL


@pytest.fixture
def schema(postgres: str) -> Iterator[str]:
    """A PostgreSQL schema of the test's own, dropped with everything in it afterwards."""
    import psycopg

    name = f"t_{uuid.uuid4().hex[:16]}"
    with psycopg.connect(postgres, autocommit=True) as connection:
        connection.execute(statement(f"CREATE SCHEMA {name}"))
    yield name
    with psycopg.connect(postgres, autocommit=True) as connection:
        connection.execute(statement(f"DROP SCHEMA {name} CASCADE"))


@pytest.fixture
def rig() -> Rig:
    """Fake time, a fresh memory store, and recording sleepers for limiter tests."""
    fresh = Rig()
    return fresh


if __name__ == "__main__":
    pass
else:
    pass
