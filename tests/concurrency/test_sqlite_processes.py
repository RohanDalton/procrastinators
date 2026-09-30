"""Independent processes sharing one SQLite file obey its quotas together.

Children are *spawned*, so each starts a fresh interpreter and shares nothing
with the parent but the database file: the coordination measured here is the
file's, not an accident of inherited memory. Rules are per hour, so every
count is exact however the processes interleave.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import multiprocessing
import os
import sqlite3
import warnings
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

import pytest

from procrastinators.backends.sqlite import AsyncSQLiteBackend, SQLiteBackend, SQLiteStore
from procrastinators.models import AdmissionRequest
from procrastinators.protocols import ObservationPoint
from tests.concurrency import sqlite_workers
from tests.concurrency.sqlite_workers import ENDPOINT, KILLED, SINGLE, VENDOR

if TYPE_CHECKING:
    from multiprocessing.context import SpawnContext
    from pathlib import Path
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

requires_fork = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")


@pytest.fixture(scope="module")
def spawn() -> SpawnContext:
    context = multiprocessing.get_context("spawn")
    return context


@pytest.fixture
def shared_file(tmp_path: Path) -> Path:
    path = tmp_path / "shared.sqlite3"
    return path


def _event_count(path: Path, name: str) -> int:
    with sqlite3.connect(path) as connection:
        (count,) = connection.execute(
            "SELECT count(*) FROM procrastinators_events AS event "
            "JOIN procrastinators_rules AS rule ON rule.id = event.rule_id WHERE rule.name = ?",
            (name,),
        ).fetchone()
    return count


def _admit_single(path: Path) -> bool:
    handle = SQLiteBackend(SQLiteStore(path))
    allowed = handle.admit(AdmissionRequest((SINGLE,))).allowed
    handle.close()
    return allowed


def test_spawned_processes_share_one_quota_and_never_debit_partially(
    spawn: SpawnContext, shared_file: Path
) -> None:
    """
    Given: Four spawned processes racing composed (vendor + endpoint) and vendor-only
           requests on one file, the vendor allowing 50 per hour and the endpoint 30.
    When:  They make 160 attempts between them.
    Then:  The vendor admits exactly 50, the endpoint no more than 30, and the file
           holds exactly one endpoint event per admitted composed request: a
           composed request denied by one rule charged neither (A6).
    """
    barrier = spawn.Barrier(WORKERS)
    queue = spawn.Queue()
    workers = [
        spawn.Process(
            target=sqlite_workers.contend, args=(str(shared_file), ATTEMPTS, barrier, queue)
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
    assert _event_count(shared_file, "endpoint") == composed
    assert _event_count(shared_file, "vendor") == VENDOR.capacity


@pytest.mark.parametrize("point", UNCOMMITTED_POINTS, ids=str)
def test_a_worker_killed_before_commit_consumed_nothing(
    spawn: SpawnContext, shared_file: Path, point: ObservationPoint
) -> None:
    """
    Given: A spawned worker killed inside an admission before its commit.
    When:  Another process then admits on the same rule of capacity one.
    Then:  It is admitted: the dead worker's transaction was rolled back, its lock
           released, and nothing consumed.
    """
    worker = spawn.Process(target=sqlite_workers.die_at, args=(str(shared_file), point.value))
    worker.start()
    worker.join(JOIN_S)

    allowed = _admit_single(shared_file)

    assert worker.exitcode == KILLED
    assert allowed


def test_a_worker_killed_after_commit_consumed_its_admission(
    spawn: SpawnContext, shared_file: Path
) -> None:
    """
    Given: A spawned worker killed after its commit, before it could report success.
    When:  Another process then admits on the same rule of capacity one.
    Then:  It is denied: a commit is durable whether or not anyone heard of it (O4).
    """
    worker = spawn.Process(
        target=sqlite_workers.die_at,
        args=(str(shared_file), ObservationPoint.AFTER_COMMIT.value),
    )
    worker.start()
    worker.join(JOIN_S)

    allowed = _admit_single(shared_file)

    assert worker.exitcode == KILLED
    assert not allowed


def test_a_restarted_process_sees_the_quota_and_policy_it_left(
    spawn: SpawnContext, shared_file: Path
) -> None:
    """
    Given: A rule of capacity one used up by this process, whose handle is closed.
    When:  A freshly spawned process opens the file.
    Then:  The rule is still spent there and a changed policy is still refused (L12).
    """
    first = _admit_single(shared_file)
    expected = (False, True)

    with ProcessPoolExecutor(1, mp_context=spawn) as pool:
        actual = pool.submit(sqlite_workers.after_restart, str(shared_file)).result(JOIN_S)

    assert first
    assert actual == expected


def _forked(child: object) -> int:
    """Run ``child`` in a forked process and return its exit status."""
    # Forking a process with threads is deprecated because locks may be copied
    # held; the handles under test are exactly what must cope with that.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        status = 1
        try:
            status = 0 if child() else 1  # ty: ignore[call-non-callable]
        finally:
            os._exit(status)
    else:
        pass
    _, wait_status = os.waitpid(pid, 0)
    code = os.waitstatus_to_exitcode(wait_status)
    return code


@requires_fork
def test_a_forked_child_reopens_and_coordinates_through_the_file(shared_file: Path) -> None:
    """
    Given: A handle that has admitted once, holding an open connection, on a rule of two.
    When:  The process forks and the child admits through the inherited handle.
    Then:  The child opens its own connection and is admitted, and the parent,
           still on its original connection, then finds the rule spent (L8).
    """
    rule = sqlite_workers.hourly("ankh", "forked", 2)
    handle = SQLiteBackend(SQLiteStore(shared_file))
    request = AdmissionRequest((rule,))
    first = handle.admit(request).allowed

    def child() -> bool:
        admitted = handle.admit(request).allowed
        return admitted

    code = _forked(child)
    third = handle.admit(request).allowed
    handle.close()

    assert first
    assert code == 0
    assert not third


@requires_fork
def test_a_forked_child_gets_a_fresh_executor_for_async_calls(shared_file: Path) -> None:
    """
    Given: An asynchronous handle whose executor thread is running, on a rule of two.
    When:  The process forks and the child admits through the inherited handle.
    Then:  The child starts its own executor and connection and is admitted; the
           parent then finds the rule spent.
    """
    rule = sqlite_workers.hourly("ankh", "forked-async", 2)
    handle = AsyncSQLiteBackend(SQLiteStore(shared_file))
    request = AdmissionRequest((rule,))
    runner = asyncio.Runner()
    first = runner.run(handle.admit(request)).allowed

    def child() -> bool:
        admitted = asyncio.run(handle.admit(request)).allowed
        return admitted

    code = _forked(child)
    third = runner.run(handle.admit(request)).allowed
    runner.run(handle.aclose())
    runner.close()

    assert first
    assert code == 0
    assert not third


if __name__ == "__main__":
    pass
else:
    pass
