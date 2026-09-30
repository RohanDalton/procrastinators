"""Waiting for quota, sync and async, on fake time.

The rig's sleepers advance a fake timeline instead of sleeping and refuse to
sleep while the store's lock is held, so every test here also checks that
waiting happens outside the critical section (W1).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
from typing import TYPE_CHECKING

import pytest

from procrastinators import RateLimiter
from procrastinators.errors import AcquireTimeout
from procrastinators.models import AdmittedEvent, DeniedEvent, DurationMicros, Limit, WaitEvent
from procrastinators.testing import T0
from tests.limiters import QUIRM, Rig

if TYPE_CHECKING:
    from procrastinators.models import Admission
else:
    pass

SECOND = DurationMicros(1_000_000)


def test_acquire_waits_exactly_the_reported_delay_then_rechecks(rig: Rig) -> None:
    """
    Given: A limiter of two per second, used twice at once.
    When:  A third acquisition is made with no deadline.
    Then:  It sleeps exactly one second — the policy's delay, with no jitter (W3) —
           and is then admitted at the fresh authority time (T5).
    """
    limiter = rig.limiter()
    limiter.acquire()
    limiter.acquire()

    admission = limiter.acquire()

    assert rig.sleeper.sleeps == (SECOND,)
    assert admission.admitted_at == T0 + SECOND


def test_mort_rechecks_after_waking_rather_than_trusting_the_delay(rig: Rig) -> None:
    """
    Given: An exhausted limiter, and another worker that takes the freed capacity
           while the first is asleep.
    When:  The first acquires.
    Then:  It wakes, is denied again, sleeps again, and only then is admitted: a
           retry delay is advisory, never permission (R4, W2).
    """
    limiter = rig.limiter(limits=[Limit(1, per="1s")])
    rival = rig.limiter(limits=[Limit(1, per="1s")])
    stolen = list()

    def steal(duration: DurationMicros) -> None:
        # The rival acts at the instant the sleeper will wake: move there, take
        # the unit, and step back so the sleeper's own advance lands on it too.
        if not stolen:
            rig.timeline.advance(duration)
            stolen.append(rival.try_acquire().allowed)
            rig.timeline.step_epoch(-duration)
        else:
            pass

    limiter.acquire()
    rig.during = steal

    admission = limiter.acquire()

    assert stolen == [True]
    assert rig.sleeper.sleeps == (SECOND, SECOND)
    assert admission.admitted_at == T0 + 2 * SECOND


def test_timeout_zero_makes_one_attempt_and_consumes_nothing(rig: Rig) -> None:
    """
    Given: An exhausted limiter.
    When:  It acquires with ``timeout=0``.
    Then:  ``AcquireTimeout`` is raised after a single attempt and no sleep, carrying
           the blocking rule and advisory delay; nothing was consumed (B2, B5).
    """
    limiter = rig.limiter()
    limiter.acquire()
    limiter.acquire()

    with pytest.raises(AcquireTimeout) as raised:
        limiter.acquire(timeout=0)

    assert rig.sleeper.sleeps == tuple()
    assert raised.value.blocking == limiter.rules
    assert raised.value.retry_after_us == SECOND
    assert raised.value.cost == 1
    rig.timeline.advance(SECOND)
    assert limiter.try_acquire().allowed


def test_a_deadline_shorter_than_the_delay_times_out_without_sleeping(rig: Rig) -> None:
    """
    Given: An exhausted limiter whose next opening is a second away.
    When:  It acquires with a half-second timeout.
    Then:  It times out at once rather than sleeping to the deadline first, because
           no one else's activity could make the opening earlier (K5).
    """
    limiter = rig.limiter()
    limiter.acquire()
    limiter.acquire()

    with pytest.raises(AcquireTimeout, match="only"):
        limiter.acquire(timeout=0.5)

    assert rig.sleeper.sleeps == tuple()


def test_a_deadline_long_enough_waits_and_admits(rig: Rig) -> None:
    """
    Given: An exhausted limiter whose default timeout is two seconds.
    When:  It acquires without overriding the timeout.
    Then:  It waits one second and is admitted within the deadline.
    """
    limiter = rig.limiter(timeout=2)
    limiter.acquire()
    limiter.acquire()

    admission = limiter.acquire()

    assert admission.admitted_at == T0 + SECOND


def test_a_sleep_that_overshoots_the_deadline_does_not_consume_quota(rig: Rig) -> None:
    """
    Given: A full one-per-second quota and a one-second wait budget.
    When:  The sleeper wakes one microsecond after the deadline.
    Then:  Acquisition times out before another admission is attempted.
    """
    limiter = rig.limiter(limits=[Limit(1, per="1s")])
    limiter.acquire()

    def overshoot(_duration: DurationMicros) -> None:
        rig.timeline.advance(DurationMicros(1))

    rig.during = overshoot
    with pytest.raises(AcquireTimeout):
        limiter.acquire(timeout="1s")

    assert rig.sleeper.sleeps == (SECOND,)
    assert limiter.try_acquire().allowed


def test_no_timeout_waits_as_long_as_the_policy_requires(rig: Rig) -> None:
    """
    Given: A limiter of one per day, already used.
    When:  It acquires with ``timeout=None``.
    Then:  It sleeps the whole day and is admitted: ``None`` waits indefinitely (B2).
    """
    limiter = rig.limiter(limits=[Limit(1, per="1d")], timeout=5)
    limiter.acquire()

    admission = limiter.acquire(timeout=None)

    assert rig.sleeper.sleeps == (86_400 * SECOND,)
    assert admission.admitted_at == T0 + 86_400 * SECOND


def test_slow_bodies_do_not_delay_replenishment(rig: Rig) -> None:
    """
    Given: A limiter of ten per second whose first ten callers each hold it for ten
           seconds — RazerM/ratelimiter's charge-on-exit scenario.
    When:  An eleventh caller arrives while every body is still running.
    Then:  It waits one second, not ten: admissions age from when they were
           recorded, not from when the work finished (W5, A2).
    """
    limiter = rig.limiter(limits=[Limit(10, per="1s")])
    bodies = [limiter.__enter__() for _ in range(10)]

    eleventh = limiter.acquire()
    for _ in bodies:
        limiter.__exit__(None, None, None)

    assert rig.sleeper.sleeps == (SECOND,)
    assert eleventh.admitted_at == T0 + SECOND


def test_diagnostics_trace_a_wait_in_order(rig: Rig) -> None:
    """
    Given: A limiter of one per second, already used.
    When:  A second acquisition waits and is admitted.
    Then:  The events are the first admission, then a denial, a wait, and an admission
           whose ``waited_us`` is the time slept.
    """
    limiter = rig.limiter(limits=[Limit(1, per="1s")])
    limiter.acquire()

    limiter.acquire()
    admitted = rig.events[-1]

    assert rig.event_types() == ["AdmittedEvent", "DeniedEvent", "WaitEvent", "AdmittedEvent"]
    assert isinstance(rig.events[1], DeniedEvent)
    assert isinstance(rig.events[2], WaitEvent)
    assert rig.events[2].slept_us == SECOND
    assert isinstance(admitted, AdmittedEvent)
    assert admitted.waited_us == SECOND


def test_threads_acquiring_concurrently_each_own_their_admission() -> None:
    """
    Given: One limiter of fifty per second on real time, and eight threads.
    When:  Each thread acquires five times through the shared limiter.
    Then:  Forty distinct admissions exist: concurrent use never overwrites another
           call's result (L7).
    """
    limiter = RateLimiter(key=QUIRM, limits=[Limit(50, per="1h")], backend="memory://threads")
    admissions: list[Admission] = list()
    guard = threading.Lock()
    barrier = threading.Barrier(8)

    def work() -> None:
        barrier.wait(10)
        for _ in range(5):
            admission = limiter.acquire(timeout=0)
            with guard:
                admissions.append(admission)

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)

    assert len({admission.admission_id for admission in admissions}) == 40


def test_acquire_async_waits_on_the_async_sleeper(rig: Rig) -> None:
    """
    Given: A limiter of two per second, used twice.
    When:  It acquires asynchronously.
    Then:  The async sleeper recorded one second and the admission follows it.
    """
    limiter = rig.limiter()

    async def scenario() -> Admission:
        await limiter.acquire_async()
        await limiter.acquire_async()
        admission = await limiter.acquire_async()
        return admission

    admission = asyncio.run(scenario())

    assert rig.async_sleeper.sleeps == (SECOND,)
    assert rig.sleeper.sleeps == tuple()
    assert admission.admitted_at == T0 + SECOND


def test_a_heartbeat_and_an_unrelated_key_keep_running_during_a_quota_wait(rig: Rig) -> None:
    """
    Given: An exhausted limiter whose wait lasts until an unrelated limiter has been
           admitted, and a heartbeat counting while that wait is in progress.
    When:  All three run on one event loop.
    Then:  The unrelated key is admitted and the heartbeat beats while the exhausted
           key waits, and the waiter is admitted afterwards: one exhausted key blocks
           neither the loop nor other keys (W4).
    """
    exhausted = rig.limiter(limits=[Limit(1, per="1s")])
    unrelated = rig.limiter(key=QUIRM, limits=[Limit(1, per="1s")])
    order: list[str] = list()

    async def scenario() -> int:
        unrelated_admitted = asyncio.Event()
        waiting = asyncio.Event()
        beats = 0

        async def sleep(duration: DurationMicros) -> None:
            waiting.set()
            await unrelated_admitted.wait()
            rig.timeline.advance(duration)

        rig.async_sleeper.sleep = sleep  # ty: ignore[invalid-assignment]

        async def wait_for_quota() -> None:
            await exhausted.acquire_async()
            await exhausted.acquire_async()
            order.append("waiter admitted")

        async def other_key() -> None:
            await waiting.wait()
            for _ in range(3):  # other I/O this task does before it needs quota
                await asyncio.sleep(0)
            await unrelated.acquire_async()
            order.append("unrelated admitted")
            unrelated_admitted.set()

        async def heartbeat() -> None:
            nonlocal beats
            await waiting.wait()
            while not unrelated_admitted.is_set():
                beats += 1
                await asyncio.sleep(0)

        await asyncio.gather(wait_for_quota(), other_key(), heartbeat())
        return beats

    beats = asyncio.run(scenario())

    assert order == ["unrelated admitted", "waiter admitted"]
    assert beats >= 1


def test_cancelling_a_waiting_acquisition_propagates_and_consumes_nothing(rig: Rig) -> None:
    """
    Given: An exhausted limiter whose acquisition is waiting for quota.
    When:  The waiting task is cancelled.
    Then:  ``CancelledError`` propagates unchanged (O5) and nothing was consumed: once
           the window passes, both units are available.
    """
    limiter = rig.limiter()
    release = asyncio.Event()

    async def blocking_sleep(duration: DurationMicros) -> None:
        del duration
        await release.wait()

    rig.async_sleeper.sleep = blocking_sleep  # ty: ignore[invalid-assignment]

    async def scenario() -> None:
        await limiter.acquire_async()
        await limiter.acquire_async()
        waiting = asyncio.create_task(limiter.acquire_async())
        for _ in range(5):
            await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

    asyncio.run(scenario())
    rig.timeline.advance(SECOND)

    assert [limiter.try_acquire().allowed for _ in range(3)] == [True, True, False]


if __name__ == "__main__":
    pass
else:
    pass
