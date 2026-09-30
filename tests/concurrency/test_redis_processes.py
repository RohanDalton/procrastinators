"""Independent processes sharing one Redis or Valkey server obey its quotas together.

Children are *spawned*, so each starts a fresh interpreter and shares nothing
with the parent but the server; half of them use the asynchronous driver. Rules
are per hour, so every count is exact however the processes interleave, and
every test runs against both Redis and Valkey.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

import pytest

from procrastinators.backends.redis import KeyLayout, RedisBackend, RedisStore
from procrastinators.models import DurationMicros
from tests.concurrency import redis_workers
from tests.concurrency.sqlite_workers import ENDPOINT, KILLED, SINGLE, VENDOR
from tests.redis_service import fresh_prefix

if TYPE_CHECKING:
    from collections.abc import Iterator
    from multiprocessing.context import SpawnContext

    from redis import Redis

    from procrastinators.models import Constraint
else:
    pass

WORKERS = 4
ATTEMPTS = 40
JOIN_S = 60


@pytest.fixture(scope="module")
def spawn() -> SpawnContext:
    context = multiprocessing.get_context("spawn")
    return context


@pytest.fixture
def prefix() -> str:
    fresh = fresh_prefix()
    return fresh


@pytest.fixture
def raw(redis_url: str) -> Iterator[Redis]:
    import redis

    client = redis.Redis.from_url(redis_url.replace("valkey://", "redis://", 1))
    yield client
    client.close()


def _events(raw: Redis, prefix: str, constraint: Constraint) -> int:
    keys = KeyLayout(prefix).rule_keys(constraint.rule, KeyLayout.rule_tag(constraint))
    count = raw.zcard(keys.events)
    return count


def test_spawned_processes_share_one_quota_and_never_debit_partially(
    spawn: SpawnContext, redis_url: str, prefix: str, raw: Redis
) -> None:
    """
    Given: Four spawned processes, sync and async, racing composed (vendor + endpoint)
           and vendor-only requests on one server, the vendor allowing 50 per hour and
           the endpoint 30.
    When:  They make 160 attempts between them.
    Then:  The vendor admits exactly 50, the endpoint no more than 30, and the server
           holds exactly one endpoint event per admitted composed request: a composed
           request denied by one rule charged neither (A6).
    """
    barrier = spawn.Barrier(WORKERS)
    queue = spawn.Queue()
    workers = [
        spawn.Process(
            target=redis_workers.contend, args=(redis_url, prefix, ATTEMPTS, barrier, queue)
        )
        for _ in range(WORKERS)
    ]
    for worker in workers:
        worker.start()
    results = [queue.get(timeout=JOIN_S) for _ in workers]
    for worker in workers:
        worker.join(JOIN_S)
    composed = sum(result[0] for result in results)
    vendor_only = sum(result[1] for result in results)

    assert composed + vendor_only == VENDOR.capacity
    assert composed <= ENDPOINT.capacity
    assert _events(raw, prefix, ENDPOINT) == composed
    assert _events(raw, prefix, VENDOR) == VENDOR.capacity


def test_a_worker_killed_after_commit_consumed_its_admission(
    spawn: SpawnContext, redis_url: str, prefix: str
) -> None:
    """
    Given: A spawned worker killed as soon as its admission's reply arrived.
    When:  Another process then admits on the same rule of capacity one.
    Then:  It is denied: a commit counts whether or not anyone heard of it (O4).
    """
    worker = spawn.Process(target=redis_workers.die_after_commit, args=(redis_url, prefix))
    worker.start()
    worker.join(JOIN_S)

    with ProcessPoolExecutor(1, mp_context=spawn) as pool:
        allowed, _ = pool.submit(redis_workers.admit_single, redis_url, prefix).result(JOIN_S)

    assert worker.exitcode == KILLED
    assert not allowed


def test_a_cooldown_reaches_other_processes(
    spawn: SpawnContext, redis_url: str, prefix: str
) -> None:
    """
    Given: This process pausing a scope for a minute.
    When:  A spawned process admits on a rule of that scope.
    Then:  It is denied for about the remaining minute (K2, K3).
    """
    handle = RedisBackend(RedisStore(redis_url, prefix=prefix))
    handle.defer_for(SINGLE.rule.scope, DurationMicros(60_000_000), reason="429")
    handle.close()

    with ProcessPoolExecutor(1, mp_context=spawn) as pool:
        allowed, retry = pool.submit(redis_workers.admit_single, redis_url, prefix).result(JOIN_S)

    assert not allowed
    assert 50_000_000 < retry <= 60_000_000
