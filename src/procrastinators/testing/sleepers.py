"""Recording sleepers: waits that take no real time and leave evidence.

A waiter under test sleeps through one of these instead of the real clock.
Each sleep is recorded, then advances a :class:`~procrastinators.testing.clocks.FakeTimeline`
by exactly the duration asked for, so a test can assert both how long a waiter
chose to wait and what it did when it woke.

Two hooks make the contract rules about waiting testable:

``during``
    Runs while the caller is "asleep", before time advances — another worker
    consuming the capacity the sleeper was waiting for, say. That is how a test
    shows a waiter rechecks after sleeping instead of trusting a retry delay
    (contracts R4, W2).
``lock_held``
    Reports whether a storage lock or transaction is held. Sleeping while it
    returns ``True`` raises :exc:`~procrastinators.testing.violations.ContractViolation`
    (contract W1).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
from typing import TYPE_CHECKING

from procrastinators.models import DurationMicros
from procrastinators.testing.violations import ContractViolation

if TYPE_CHECKING:
    from collections.abc import Callable

    from procrastinators.testing.clocks import FakeTimeline
else:
    pass

__all__ = ["AsyncRecordingSleeper", "RecordingSleeper"]


class _SleepLog:
    """What the sync and async sleepers share: validation, hooks, and the record."""

    def __init__(
        self,
        timeline: FakeTimeline | None = None,
        *,
        during: Callable[[DurationMicros], None] | None = None,
        lock_held: Callable[[], bool] | None = None,
    ) -> None:
        self._timeline = timeline
        self._during = during
        self._lock_held = lock_held
        self._sleeps: list[DurationMicros] = list()

    @property
    def sleeps(self) -> tuple[DurationMicros, ...]:
        """Every requested duration, in order."""
        sleeps = tuple(self._sleeps)
        return sleeps

    @property
    def total_us(self) -> int:
        """The sum of every requested duration."""
        total = sum(self._sleeps)
        return total

    def _begin(self, duration: DurationMicros) -> None:
        if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
            raise ContractViolation(f"a sleeper was asked to wait {duration!r} microseconds")
        elif self._lock_held is not None and self._lock_held():
            raise ContractViolation("slept while holding a storage lock or transaction (W1)")
        else:
            pass
        self._sleeps.append(duration)
        if self._during is not None:
            self._during(duration)
        else:
            pass

    def _end(self, duration: DurationMicros) -> None:
        if self._timeline is not None:
            self._timeline.advance(duration)
        else:
            pass


class RecordingSleeper(_SleepLog):
    """A :class:`~procrastinators.protocols.Sleeper` that records and advances fake time.

    :param timeline: Advanced by each duration after it is recorded; ``None``
        records only.
    :param during: Called with each duration while the caller is asleep.
    :param lock_held: Returns whether a storage lock is held; sleeping then
        violates W1.
    """

    def sleep(self, duration: DurationMicros) -> None:
        """Record ``duration``, run the ``during`` hook, and advance the timeline.

        :param duration: How long the caller asked to wait, in microseconds.
        :raises ~procrastinators.testing.violations.ContractViolation: The duration is not a
            non-negative integer, or a storage lock is held.
        """
        self._begin(duration)
        self._end(duration)


class AsyncRecordingSleeper(_SleepLog):
    """An :class:`~procrastinators.protocols.AsyncSleeper` that records and advances fake time.

    Yields to the event loop once per sleep, so other tasks run and the sleep
    is a genuine cancellation point. A sleep cancelled at that point stays
    recorded but does not advance the timeline.

    :param timeline: Advanced by each duration after it is recorded; ``None``
        records only.
    :param during: Called with each duration while the caller is asleep.
    :param lock_held: Returns whether a storage lock is held; sleeping then
        violates W1.
    """

    async def sleep(self, duration: DurationMicros) -> None:
        """Record ``duration``, yield once, then advance the timeline.

        :param duration: How long the caller asked to wait, in microseconds.
        :raises ~procrastinators.testing.violations.ContractViolation: The duration is not a
            non-negative integer, or a storage lock is held.
        :raises asyncio.CancelledError: The sleep was cancelled; propagated unchanged.
        """
        self._begin(duration)
        await asyncio.sleep(0)
        self._end(duration)


if __name__ == "__main__":
    pass
else:
    pass
