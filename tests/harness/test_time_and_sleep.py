"""Fake time moves only when told to, and recording sleepers leave evidence.

The History Monks keep time in stores and let it out as needed; the fake
timeline does the same.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio

import pytest

from procrastinators.models import DurationMicros
from procrastinators.protocols import (
    AdmissionClock,
    AsyncAdmissionClock,
    AsyncSleeper,
    DeadlineClock,
    Sleeper,
)
from procrastinators.testing import (
    DEFAULT_EPOCH_US,
    AsyncRecordingSleeper,
    ContractViolation,
    FakeTimeline,
    RecordingSleeper,
)


def test_the_monks_hand_out_clocks_for_each_domain(timeline: FakeTimeline) -> None:
    """
    Given: A fake timeline.
    When:  Its clocks are checked against the clock protocols.
    Then:  Each satisfies the protocol of its own time domain.
    """
    assert isinstance(timeline.epoch_clock, AdmissionClock)
    assert isinstance(timeline.async_epoch_clock, AsyncAdmissionClock)
    assert isinstance(timeline.deadline_clock, DeadlineClock)


def test_advancing_moves_both_clocks_together(timeline: FakeTimeline) -> None:
    """
    Given: A fake timeline at its default instant.
    When:  It advances by 1.5 s.
    Then:  Epoch and monotonic time both move by exactly that much.
    """
    expected = (DEFAULT_EPOCH_US + 1_500_000, 1_500_000)

    timeline.advance(1_500_000)
    actual = (timeline.epoch_clock.now(), timeline.deadline_clock.now())

    assert actual == expected


def test_a_wall_clock_step_leaves_monotonic_time_alone(timeline: FakeTimeline) -> None:
    """
    Given: A fake timeline.
    When:  The wall clock steps back by one second.
    Then:  Epoch time goes back and monotonic time does not, as on a real machine.
    """
    expected = (DEFAULT_EPOCH_US - 1_000_000, 0)

    timeline.step_epoch(-1_000_000)
    actual = (timeline.epoch_clock.now(), timeline.deadline_clock.now())

    assert actual == expected


def test_time_does_not_pass_backwards(timeline: FakeTimeline) -> None:
    """
    Given: A fake timeline.
    When:  It is asked to advance by a negative amount, or to an earlier instant.
    Then:  ValueError is raised for each.
    """
    with pytest.raises(ValueError, match="backwards"):
        timeline.advance(-1)
    with pytest.raises(ValueError, match="step_epoch"):
        timeline.advance_to(DEFAULT_EPOCH_US - 1)


def test_reads_are_counted_so_sampling_can_be_asserted(timeline: FakeTimeline) -> None:
    """
    Given: A fake timeline.
    When:  Its epoch clock is read twice and its monotonic clock once; then peeked.
    Then:  The counts say so, and peeking does not count.
    """
    expected = (2, 1)

    timeline.epoch_clock.now()
    timeline.epoch_clock.now()
    timeline.deadline_clock.now()
    timeline.peek_epoch()
    actual = (timeline.epoch_reads, timeline.monotonic_reads)

    assert actual == expected


def test_a_recording_sleeper_takes_no_time_but_advances_the_timeline(
    timeline: FakeTimeline,
) -> None:
    """
    Given: A recording sleeper on a fake timeline.
    When:  It sleeps 250 ms and then 750 ms.
    Then:  Both are recorded, the timeline moved one second, and it is a Sleeper.
    """
    sleeper = RecordingSleeper(timeline)
    expected = ((250_000, 750_000), 1_000_000, DEFAULT_EPOCH_US + 1_000_000)

    sleeper.sleep(DurationMicros(250_000))
    sleeper.sleep(DurationMicros(750_000))
    actual = (sleeper.sleeps, sleeper.total_us, timeline.peek_epoch())

    assert actual == expected
    assert isinstance(sleeper, Sleeper)


def test_the_during_hook_runs_while_asleep_before_time_moves(timeline: FakeTimeline) -> None:
    """
    Given: A sleeper whose hook records the time it observes.
    When:  It sleeps.
    Then:  The hook saw the time before the sleep ended: another worker's chance to act.
    """
    seen = list()

    def another_worker_acts(duration: DurationMicros) -> None:
        del duration
        seen.append(timeline.peek_epoch())

    sleeper = RecordingSleeper(timeline, during=another_worker_acts)
    expected = [DEFAULT_EPOCH_US]

    sleeper.sleep(DurationMicros(10))
    actual = seen

    assert actual == expected


def test_sleeping_under_a_storage_lock_is_a_violation() -> None:
    """
    Given: A sleeper told a storage lock is held.
    When:  It is asked to sleep.
    Then:  ContractViolation cites W1, and nothing is recorded.
    """

    def lock_is_held() -> bool:
        return True

    sleeper = RecordingSleeper(lock_held=lock_is_held)

    with pytest.raises(ContractViolation, match="W1"):
        sleeper.sleep(DurationMicros(1))

    assert sleeper.sleeps == tuple()


@pytest.mark.parametrize("duration", [-1, 1.5, True], ids=["negative", "float", "bool"])
def test_a_nonsense_sleep_is_a_violation(duration: object) -> None:
    """
    Given: A sleeper.
    When:  It is asked to sleep a negative, fractional, or boolean duration.
    Then:  ContractViolation is raised: a waiter computed something wrong.
    """
    sleeper = RecordingSleeper()

    with pytest.raises(ContractViolation):
        sleeper.sleep(duration)  # ty: ignore[invalid-argument-type]


def test_an_async_sleeper_yields_and_advances(timeline: FakeTimeline) -> None:
    """
    Given: An async recording sleeper and a second task that notes when it runs.
    When:  The sleeper sleeps.
    Then:  The other task ran during the sleep, the sleep is recorded, and the
           timeline advanced: waiting does not block the loop (W4).
    """
    sleeper = AsyncRecordingSleeper(timeline)
    events = list()

    async def heartbeat() -> None:
        events.append("heartbeat")

    async def main() -> None:
        beat = asyncio.create_task(heartbeat())
        await sleeper.sleep(DurationMicros(5))
        events.append("woke")
        await beat

    asyncio.run(main())
    expected = (["heartbeat", "woke"], (5,), DEFAULT_EPOCH_US + 5)

    actual = (events, sleeper.sleeps, timeline.peek_epoch())

    assert actual == expected
    assert isinstance(sleeper, AsyncSleeper)


def test_a_cancelled_async_sleep_does_not_advance_time(timeline: FakeTimeline) -> None:
    """
    Given: An async sleeper whose sleeping task is cancelled at its yield.
    When:  The task is awaited.
    Then:  CancelledError propagates unchanged and the timeline did not move.
    """
    sleeper = AsyncRecordingSleeper(timeline)

    async def main() -> None:
        task = asyncio.create_task(sleeper.sleep(DurationMicros(5)))
        await asyncio.sleep(0)
        task.cancel()
        await task

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(main())

    assert timeline.peek_epoch() == DEFAULT_EPOCH_US


if __name__ == "__main__":
    pass
else:
    pass
