"""Fixtures for the memory and SQLite backend tests.

Every store reads a fresh fake timeline, so tests control time exactly and
never sleep. Stores, handles, and timelines are mutable and function-scoped;
SQLite files live in the test's temporary directory, and handles are closed
when the test ends.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
from typing import TYPE_CHECKING

import pytest

from procrastinators.backends.memory import AsyncMemoryBackend, MemoryBackend, MemoryStore
from procrastinators.backends.sqlite import AsyncSQLiteBackend, SQLiteBackend, SQLiteStore
from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    AdmissionRequest,
    Constraint,
    DurationMicros,
    QuotaIdentity,
    RuleId,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.testing import FakeTimeline

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path
else:
    pass

SECOND = DurationMicros(1_000_000)


class DelayedCloseExecutor:
    """A worker boundary that holds cleanup until a test releases it."""

    def __init__(self) -> None:
        self.closed = False
        self.outstanding = 1
        self.started = asyncio.Event()
        self.proceed = asyncio.Event()

    async def run(self, operation: Callable[[], None], *, bounded: bool) -> None:
        self.started.set()
        await self.proceed.wait()
        operation()

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def delayed_close_executor() -> DelayedCloseExecutor:
    fresh = DelayedCloseExecutor()
    return fresh


def constraint(scope: str, name: str, policy: object) -> Constraint:
    """A constraint in the ``discworld`` namespace, fingerprinted."""
    built = Constraint(
        RuleId(QuotaIdentity("discworld", scope), name),
        policy,  # ty: ignore[invalid-argument-type]
        policy_fingerprint(policy),  # ty: ignore[invalid-argument-type]
    )
    return built


@pytest.fixture
def timeline() -> FakeTimeline:
    fresh = FakeTimeline()
    return fresh


@pytest.fixture
def store(timeline: FakeTimeline) -> MemoryStore:
    fresh = MemoryStore(name="unseen-university", clock=timeline.epoch_clock)
    return fresh


@pytest.fixture
def backend(store: MemoryStore) -> MemoryBackend:
    handle = MemoryBackend(store, namespace="discworld")
    return handle


@pytest.fixture
def async_backend(store: MemoryStore) -> AsyncMemoryBackend:
    handle = AsyncMemoryBackend(store, namespace="discworld")
    return handle


@pytest.fixture
def database(tmp_path: Path) -> Path:
    """Where the test's SQLite file lives; nothing is created until a handle connects."""
    path = tmp_path / "quota.sqlite3"
    return path


@pytest.fixture
def sqlite_store(database: Path, timeline: FakeTimeline) -> SQLiteStore:
    fresh = SQLiteStore(database, clock=timeline.epoch_clock)
    return fresh


@pytest.fixture
def sqlite_backend(sqlite_store: SQLiteStore) -> Iterator[SQLiteBackend]:
    handle = SQLiteBackend(sqlite_store, namespace="discworld")
    yield handle
    handle.close()


@pytest.fixture
def async_sqlite_backend(
    sqlite_store: SQLiteStore, event_loop_runner: asyncio.Runner
) -> Iterator[AsyncSQLiteBackend]:
    handle = AsyncSQLiteBackend(sqlite_store, namespace="discworld")
    yield handle
    event_loop_runner.run(handle.aclose())


@pytest.fixture
def event_loop_runner() -> Iterator[asyncio.Runner]:
    """One event loop for a whole test, so async handles outlive a single ``run``."""
    with asyncio.Runner() as runner:
        yield runner


@pytest.fixture
def two_per_second() -> Constraint:
    """A sliding log of two per second on the ``ankh`` scope."""
    built = constraint("ankh", "burst", SlidingLogPolicy(2, SECOND))
    return built


@pytest.fixture
def ten_per_minute() -> Constraint:
    """A sliding log of ten per minute on the ``ankh`` scope."""
    built = constraint("ankh", "sustained", SlidingLogPolicy(10, DurationMicros(60 * SECOND)))
    return built


@pytest.fixture
def empty_bucket() -> Constraint:
    """A token bucket of two, refilling one per second, that starts empty."""
    built = constraint("quirm", "bucket", TokenBucketPolicy(2, 1, SECOND, initial_tokens=0))
    return built


@pytest.fixture
def one_request(two_per_second: Constraint) -> AdmissionRequest:
    request = AdmissionRequest((two_per_second,))
    return request


if __name__ == "__main__":
    pass
else:
    pass
