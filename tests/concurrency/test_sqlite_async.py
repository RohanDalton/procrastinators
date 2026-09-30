"""The asynchronous SQLite handle keeps the event loop free and settles cancelled work.

Another connection holding the file's write lock stands in for a busy peer
process: an admission then waits inside SQLite, on the executor's thread, for
as long as the test chooses. Progress is observed through events and counters,
never through sleeping for a guessed duration.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import sqlite3
from typing import TYPE_CHECKING

import pytest

from procrastinators.backends.executor import DedicatedExecutor
from procrastinators.backends.sqlite import AsyncSQLiteBackend, SQLiteBackend, SQLiteStore
from procrastinators.errors import BackendBusy
from procrastinators.models import (
    AdmissionRequest,
    DurationMicros,
    OperationBudget,
    Ownership,
    SlidingLogPolicy,
)
from tests.backends.conftest import SECOND, constraint

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from procrastinators.models import Constraint
    from procrastinators.testing import FakeTimeline
else:
    pass

PATIENT = OperationBudget(lock_timeout_us=DurationMicros(30 * SECOND))
"""Long enough that a held lock is released by the test, never by the budget."""

HEARTBEATS = 20


@pytest.fixture
def pair() -> Constraint:
    """Two per second on ``ankh``."""
    built = constraint("ankh", "pair", SlidingLogPolicy(2, SECOND))
    return built


@pytest.fixture
def initialized(sqlite_store: SQLiteStore) -> SQLiteStore:
    """A store whose file exists, so a raw connection can hold its lock."""
    warm = SQLiteBackend(sqlite_store)
    warm.admit(AdmissionRequest((constraint("quirm", "warm-up", SlidingLogPolicy(1, SECOND)),)))
    warm.close()
    return sqlite_store


@pytest.fixture
def lock_holder(initialized: SQLiteStore, database: Path) -> Iterator[sqlite3.Connection]:
    """A peer connection holding the write lock until the test rolls it back."""
    connection = sqlite3.connect(database, isolation_level=None, check_same_thread=False)
    connection.execute("BEGIN IMMEDIATE")
    yield connection
    if connection.in_transaction:
        connection.execute("ROLLBACK")
    else:
        pass
    connection.close()


def _remaining(store: SQLiteStore, rule: Constraint) -> int:
    """How many unit admissions still fit, counted by admitting until denied."""
    handle = SQLiteBackend(store)
    count = 0
    while handle.admit(AdmissionRequest((rule,))).allowed:
        count += 1
    handle.close()
    return count


async def _until(condition: object) -> None:
    while not condition():  # ty: ignore[call-non-callable]
        await asyncio.sleep(0)


def test_the_loop_keeps_running_while_an_admission_waits_on_the_file(
    initialized: SQLiteStore, lock_holder: sqlite3.Connection, pair: Constraint
) -> None:
    """
    Given: A peer holding the file's write lock.
    When:  An admission is awaited alongside a heartbeat task.
    Then:  The heartbeat keeps beating while the admission waits, and the admission
           succeeds once the peer lets go (W4).
    """
    handle = AsyncSQLiteBackend(initialized)
    beats = [0]

    async def heartbeat() -> None:
        while True:
            beats[0] += 1
            await asyncio.sleep(0.001)

    async def scenario() -> tuple[bool, bool]:
        beating = asyncio.ensure_future(heartbeat())
        admitting = asyncio.ensure_future(handle.admit(AdmissionRequest((pair,), budget=PATIENT)))
        await _until(lambda: beats[0] >= HEARTBEATS)
        waited = not admitting.done()
        lock_holder.execute("ROLLBACK")
        decision = await admitting
        beating.cancel()
        await handle.aclose()
        outcome = (waited, decision.allowed)
        return outcome

    expected = (True, True)
    actual = asyncio.run(scenario())

    assert actual == expected


def test_a_call_cancelled_while_queued_never_runs(
    initialized: SQLiteStore, lock_holder: sqlite3.Connection, pair: Constraint
) -> None:
    """
    Given: One admission waiting on the file's lock and a second queued behind it.
    When:  The second's caller is cancelled, and the lock is released.
    Then:  The second never ran: only the first consumed quota (O5).
    """
    handle = AsyncSQLiteBackend(initialized)
    request = AdmissionRequest((pair,), budget=PATIENT)

    async def scenario() -> None:
        first = asyncio.ensure_future(handle.admit(request))
        await _until(lambda: handle.executor.outstanding == 1)
        second = asyncio.ensure_future(handle.admit(request))
        await _until(lambda: handle.executor.outstanding == 2)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        lock_holder.execute("ROLLBACK")
        await first
        await handle.aclose()

    asyncio.run(scenario())

    assert _remaining(initialized, pair) == 1


def test_a_call_cancelled_mid_transaction_may_commit_and_close_waits_for_it(
    initialized: SQLiteStore, lock_holder: sqlite3.Connection, pair: Constraint
) -> None:
    """
    Given: An admission already running on the executor, waiting on the file's lock.
    When:  Its caller is cancelled, the lock is released, and the handle is closed.
    Then:  The caller saw cancellation, yet the transaction committed — possibly
           consumed capacity, never permission — and closing waited for it (O5, L4).
    """
    handle = AsyncSQLiteBackend(initialized)

    async def scenario() -> None:
        admitting = asyncio.ensure_future(handle.admit(AdmissionRequest((pair,), budget=PATIENT)))
        await _until(lambda: handle.executor.running)
        admitting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await admitting
        lock_holder.execute("ROLLBACK")
        await handle.aclose()

    asyncio.run(scenario())

    assert handle.executor.outstanding == 0
    assert _remaining(initialized, pair) == 1


def test_a_full_executor_is_contention(
    initialized: SQLiteStore, lock_holder: sqlite3.Connection, pair: Constraint
) -> None:
    """
    Given: A handle whose executor holds at most one call, busy on a held lock.
    When:  A second admission is attempted.
    Then:  It raises BackendBusy at once rather than queueing without bound (O2).
    """
    handle = AsyncSQLiteBackend(initialized, max_pending=1)
    request = AdmissionRequest((pair,), budget=PATIENT)

    async def scenario() -> None:
        first = asyncio.ensure_future(handle.admit(request))
        await _until(lambda: handle.executor.outstanding == 1)
        with pytest.raises(BackendBusy):
            await handle.admit(request)
        lock_holder.execute("ROLLBACK")
        await first
        await handle.aclose()

    asyncio.run(scenario())


def test_a_borrowed_executor_outlives_the_handle(
    initialized: SQLiteStore, pair: Constraint, timeline: FakeTimeline
) -> None:
    """
    Given: Two handles sharing one injected executor.
    When:  One handle is closed.
    Then:  It reports the executor as borrowed and leaves it running, so the other
           handle still admits (L2).
    """
    executor = DedicatedExecutor(name="shared")
    closing = AsyncSQLiteBackend(initialized, executor=executor)
    staying = AsyncSQLiteBackend(initialized, executor=executor)
    del timeline

    async def scenario() -> bool:
        await closing.admit(AdmissionRequest((pair,)))
        await closing.aclose()
        decision = await staying.admit(AdmissionRequest((pair,)))
        await staying.aclose()
        return decision.allowed

    allowed = asyncio.run(scenario())
    executor.close()

    assert closing.ownership.executor is Ownership.BORROWED
    assert allowed
    assert executor.closed


if __name__ == "__main__":
    pass
else:
    pass
