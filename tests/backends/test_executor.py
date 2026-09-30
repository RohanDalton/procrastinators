"""The dedicated executor: one operation at a time, bounded, cancellable before it starts.

Operations block on events the test controls, never on sleeps, so ordering and
outstanding work are observed exactly.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
from typing import TYPE_CHECKING

import pytest

from procrastinators.backends.executor import DedicatedExecutor
from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
else:
    pass

WAIT_S = 10


@pytest.fixture
def executor() -> Iterator[DedicatedExecutor]:
    fresh = DedicatedExecutor(name="test-executor", max_pending=2)
    yield fresh
    fresh.close()


@pytest.fixture
def gate() -> threading.Event:
    """Held shut by a test until it lets a blocked operation finish."""
    event = threading.Event()
    return event


def _blocked(started: threading.Event, gate: threading.Event, result: str) -> str:
    started.set()
    gate.wait(WAIT_S)
    return result


def test_constructing_an_executor_starts_no_thread() -> None:
    """
    Given: The threads running before.
    When:  An executor is constructed and closed without work.
    Then:  No thread was started.
    """
    before = set(threading.enumerate())

    DedicatedExecutor(name="idle").close()

    assert set(threading.enumerate()) <= before


@pytest.mark.parametrize("max_pending", [0, -1, True, 1.5])
def test_the_pending_bound_must_be_a_positive_integer(max_pending: object) -> None:
    """
    Given: A bound that is not a positive integer.
    When:  An executor is constructed with it.
    Then:  ConfigurationError is raised.
    """
    with pytest.raises(ConfigurationError):
        DedicatedExecutor(max_pending=max_pending)  # ty: ignore[invalid-argument-type]


def test_operations_run_one_at_a_time_in_order_on_one_worker(
    executor: DedicatedExecutor,
) -> None:
    """
    Given: Two operations, each recording its thread and the operations running with it.
    When:  Both are submitted.
    Then:  They ran in order, on the same thread that is not the caller's, never together.
    """
    running = list()
    trace = list()

    def operation(label: str) -> str:
        running.append(label)
        trace.append((label, threading.current_thread().name, tuple(running)))
        running.remove(label)
        return label

    first = executor.submit(lambda: operation("first"))
    second = executor.submit(lambda: operation("second"))
    expected = [
        ("first", "test-executor", ("first",)),
        ("second", "test-executor", ("second",)),
    ]

    results = (first.result(WAIT_S), second.result(WAIT_S))

    assert results == ("first", "second")
    assert trace == expected


def test_a_full_executor_refuses_rather_than_queueing_without_limit(
    executor: DedicatedExecutor, gate: threading.Event
) -> None:
    """
    Given: An executor bounded at two, with one operation running and one queued.
    When:  A third is submitted, then the first two finish.
    Then:  The third is refused as contention, and room returns once work settles.
    """
    started = threading.Event()
    running = executor.submit(lambda: _blocked(started, gate, "running"))
    started.wait(WAIT_S)
    queued = executor.submit(lambda: "queued")

    with pytest.raises(BackendBusy):
        executor.submit(lambda: "refused")
    gate.set()
    results = (running.result(WAIT_S), queued.result(WAIT_S))
    later = executor.submit(lambda: "later").result(WAIT_S)

    assert results == ("running", "queued")
    assert later == "later"


def test_an_operation_that_cannot_start_in_time_is_skipped(
    executor: DedicatedExecutor, gate: threading.Event
) -> None:
    """
    Given: A running operation, and a queued one that must start within one microsecond.
    When:  The running operation finishes much later.
    Then:  The queued operation never ran and reports contention (O2).
    """
    started = threading.Event()
    ran = threading.Event()
    executor.submit(lambda: _blocked(started, gate, "running"))
    started.wait(WAIT_S)
    impatient = executor.submit(ran.set, start_within_us=1)

    gate.set()

    with pytest.raises(BackendBusy):
        impatient.result(WAIT_S)
    assert not ran.is_set()


def test_cancelling_a_queued_operation_means_it_never_runs(
    executor: DedicatedExecutor, gate: threading.Event
) -> None:
    """
    Given: An operation queued behind a running one, awaited by a task.
    When:  The task is cancelled before the operation starts.
    Then:  The task sees cancellation and the operation never runs (O5).
    """
    started = threading.Event()
    ran = threading.Event()

    async def scenario() -> None:
        executor.submit(lambda: _blocked(started, gate, "running"))
        await asyncio.to_thread(started.wait, WAIT_S)
        waiter = asyncio.ensure_future(executor.run(ran.set))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        gate.set()
        await executor.aclose()

    asyncio.run(scenario())

    assert not ran.is_set()


def test_cancelling_a_running_operation_lets_it_finish(
    executor: DedicatedExecutor, gate: threading.Event
) -> None:
    """
    Given: A task awaiting an operation that has started.
    When:  The task is cancelled, then the executor is closed.
    Then:  The task sees cancellation, yet the operation completed, and closing
           waited for it (O5, L4).
    """
    started = threading.Event()
    finished = threading.Event()

    def operation() -> None:
        started.set()
        gate.wait(WAIT_S)
        finished.set()

    async def scenario() -> None:
        waiter = asyncio.ensure_future(executor.run(operation))
        await asyncio.to_thread(started.wait, WAIT_S)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        gate.set()
        await executor.aclose()

    asyncio.run(scenario())

    assert finished.is_set()


def test_exceptions_from_operations_reach_the_caller_unchanged(
    executor: DedicatedExecutor,
) -> None:
    """
    Given: Operations raising an ordinary error and a cancellation.
    When:  Each is awaited.
    Then:  Each exception reaches the caller as it was raised, not translated.
    """

    def fails() -> None:
        raise LookupError("the Luggage ate it")

    def cancels() -> None:
        raise asyncio.CancelledError

    async def scenario() -> None:
        with pytest.raises(LookupError, match="Luggage"):
            await executor.run(fails)
        with pytest.raises(asyncio.CancelledError):
            await executor.run(cancels)

    asyncio.run(scenario())


def test_closing_waits_for_outstanding_work_then_refuses_more(
    executor: DedicatedExecutor, gate: threading.Event
) -> None:
    """
    Given: A running operation.
    When:  The executor is closed from another thread, twice, and work is then submitted.
    Then:  Closing returned only after the operation finished, was idempotent, and
           later submissions raise ClosedResource (L1, L4, L5).
    """
    started = threading.Event()
    running = executor.submit(lambda: _blocked(started, gate, "running"))
    started.wait(WAIT_S)
    closer = threading.Thread(target=executor.close)
    closer.start()
    closer.join(0.05)
    blocked_while_running = closer.is_alive()

    gate.set()
    closer.join(WAIT_S)
    executor.close()

    assert blocked_while_running
    assert running.done()
    assert executor.outstanding == 0
    with pytest.raises(ClosedResource):
        executor.submit(lambda: None)


def test_a_finished_operation_frees_its_place_before_its_caller_wakes(
    executor: DedicatedExecutor,
) -> None:
    """
    Given: An executor bounded at two, filled with two operations.
    When:  The caller of each wakes on its result and at once submits another.
    Then:  Every submission is accepted: an operation stops counting against the
           bound before its caller learns it has finished.
    """

    async def scenario() -> list[int]:
        results = list()
        for value in range(50):
            first = asyncio.ensure_future(executor.run(lambda value=value: value))
            second = asyncio.ensure_future(executor.run(lambda value=value: -value))
            results.append(await first)
            results.append(await executor.run(lambda value=value: value))
            await second
        return results

    expected = [value for value in range(50) for _ in range(2)]
    actual = asyncio.run(scenario())

    assert actual == expected


def test_unbounded_cleanup_runs_even_when_the_executor_is_full(
    executor: DedicatedExecutor, gate: threading.Event
) -> None:
    """
    Given: An executor holding its limit of two operations.
    When:  Cleanup is submitted outside the bound, and the operations finish.
    Then:  The cleanup was accepted and ran after them, in order.
    """
    started = threading.Event()
    order = list()
    executor.submit(lambda: order.append(_blocked(started, gate, "running")))
    started.wait(WAIT_S)
    executor.submit(lambda: order.append("queued"))

    cleanup = executor.submit(lambda: order.append("cleanup"), bounded=False)
    gate.set()
    cleanup.result(WAIT_S)

    assert order == ["running", "queued", "cleanup"]


def test_a_forked_copy_refuses_work(executor: DedicatedExecutor) -> None:
    """
    Given: An executor that believes it was created by another process.
    When:  Work is submitted.
    Then:  BackendUnavailable is raised, since its worker does not exist here (L8).
    """
    executor._pid = -1

    with pytest.raises(BackendUnavailable, match="fork"):
        executor.submit(lambda: None)


if __name__ == "__main__":
    pass
else:
    pass
