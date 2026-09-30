"""Independent processes sharing one PostgreSQL database obey its quotas together.

Children are *spawned*, so each starts a fresh interpreter and shares nothing
with the parent but the database: the coordination measured here is the
server's. Rules are per hour, so every count is exact however the processes
interleave.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

import psycopg
import pytest

from procrastinators.protocols import ObservationPoint
from tests.concurrency import postgres_workers
from tests.concurrency.postgres_workers import ENDPOINT, KILLED, VENDOR
from tests.postgres_service import statement

if TYPE_CHECKING:
    from multiprocessing.context import SpawnContext
else:
    pass

WORKERS = 4
ATTEMPTS = 40
JOIN_S = 60

UNCOMMITTED_POINTS = (
    ObservationPoint.BEFORE_LOCK,
    ObservationPoint.AFTER_LOAD,
    ObservationPoint.BEFORE_COMMIT,
)


@pytest.fixture(scope="module")
def spawn() -> SpawnContext:
    context = multiprocessing.get_context("spawn")
    return context


@pytest.fixture(params=[point.value for point in UNCOMMITTED_POINTS])
def uncommitted_point(request: pytest.FixtureRequest) -> str:
    point: str = request.param
    return point


def _event_count(url: str, schema: str, name: str) -> int:
    query = statement(
        f'SELECT count(*) FROM {schema}."procrastinators_events" AS event '
        f'JOIN {schema}."procrastinators_rules" AS rule ON rule.id = event.rule_id '
        "WHERE rule.name = %s"
    )
    with psycopg.connect(url) as connection:
        (count,) = connection.execute(query, (name,)).fetchone() or (0,)
    return count


def _admit_single(spawn: SpawnContext, url: str, schema: str) -> bool:
    with ProcessPoolExecutor(1, mp_context=spawn) as pool:
        allowed = pool.submit(postgres_workers.admit_single, url, schema).result(JOIN_S)
    return allowed


def _die(spawn: SpawnContext, url: str, schema: str, point: str) -> int | None:
    process = spawn.Process(target=postgres_workers.die_at, args=(url, schema, point))
    process.start()
    process.join(JOIN_S)
    code = process.exitcode
    return code


@pytest.mark.service("postgres")
def test_spawned_processes_share_one_quota_and_never_debit_partially(
    spawn: SpawnContext, postgres: str, schema: str
) -> None:
    """
    Given: Four spawned processes, each with its own store and connection, alternating
           composed (vendor + endpoint) and vendor-only requests against 50 and 30 per hour.
    When:  They race, far past both capacities.
    Then:  The vendor admits exactly 50, the endpoint no more than 30, and the database
           holds exactly one endpoint event per admitted composed request: a composed
           request denied by one rule charged neither (A6).
    """
    barrier = spawn.Barrier(WORKERS)
    results = spawn.Queue()
    workers = [
        spawn.Process(
            target=postgres_workers.contend, args=(postgres, schema, ATTEMPTS, barrier, results)
        )
        for _ in range(WORKERS)
    ]
    for worker in workers:
        worker.start()
    counts = [results.get(timeout=JOIN_S) for _ in workers]
    for worker in workers:
        worker.join(JOIN_S)
    composed = sum(count[0] for count in counts)
    vendor_only = sum(count[1] for count in counts)

    assert composed + vendor_only == VENDOR.capacity
    assert composed <= ENDPOINT.capacity
    assert _event_count(postgres, schema, "endpoint") == composed
    assert _event_count(postgres, schema, "vendor") == VENDOR.capacity


@pytest.mark.service("postgres")
def test_a_worker_killed_before_commit_consumes_nothing(
    spawn: SpawnContext, postgres: str, schema: str, uncommitted_point: str
) -> None:
    """
    Given: A capacity-one rule, and a worker killed at a point before its commit.
    When:  Another process admits afterwards.
    Then:  The worker died as planned and the other process is admitted: the server
           rolled the dead worker's transaction back.
    """
    code = _die(spawn, postgres, schema, uncommitted_point)
    allowed = _admit_single(spawn, postgres, schema)

    expected = (KILLED, True)
    actual = (code, allowed)
    assert actual == expected


@pytest.mark.service("postgres")
def test_a_worker_killed_after_commit_has_consumed_its_quota(
    spawn: SpawnContext, postgres: str, schema: str
) -> None:
    """
    Given: A capacity-one rule, and a worker killed after its commit, before reporting.
    When:  Another process admits afterwards.
    Then:  It is denied: the committed admission stands although nobody heard of it.
    """
    code = _die(spawn, postgres, schema, ObservationPoint.AFTER_COMMIT.value)
    allowed = _admit_single(spawn, postgres, schema)

    expected = (KILLED, False)
    actual = (code, allowed)
    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
