"""Fake clocks: one controllable instant, read through each time domain.

A :class:`FakeTimeline` holds the current authority epoch time and local
monotonic time together, the way a real machine does, and hands out clocks that
read it. Advancing the timeline moves both; stepping the wall clock moves only
epoch time, which is how a clock regression or forward jump is simulated
without the monotonic clock ever running backwards.

Nothing here sleeps. Tests advance time explicitly, so their outcome never
depends on scheduling luck.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import threading
from typing import Final

from procrastinators.models import MAX_TIMESTAMP_US, EpochMicros, MonotonicMicros

__all__ = [
    "DEFAULT_EPOCH_US",
    "FakeAdmissionClock",
    "FakeAsyncAdmissionClock",
    "FakeDeadlineClock",
    "FakeTimeline",
]

DEFAULT_EPOCH_US: Final = EpochMicros(1_700_000_000_000_000)
"""2023-11-14T22:13:20Z: a whole multiple of 100 seconds, so common periods align to it."""


class FakeTimeline:
    """A controllable instant, thread-safe, starting at ``epoch_us`` and ``monotonic_us``.

    :param epoch_us: Initial authority epoch time.
    :param monotonic_us: Initial local monotonic time.
    :raises ValueError: ``epoch_us`` is outside the supported timestamp range or
        ``monotonic_us`` is negative.
    """

    def __init__(
        self,
        epoch_us: int = DEFAULT_EPOCH_US,
        monotonic_us: int = 0,
    ) -> None:
        self._lock = threading.Lock()
        self._epoch = self._checked_epoch(epoch_us)
        if monotonic_us < 0:
            raise ValueError(f"monotonic time must not be negative, got {monotonic_us}")
        else:
            pass
        self._monotonic = monotonic_us
        self._epoch_reads = 0
        self._monotonic_reads = 0
        self.epoch_clock: Final = FakeAdmissionClock(self)
        """An :class:`~procrastinators.protocols.AdmissionClock` reading this timeline."""
        self.async_epoch_clock: Final = FakeAsyncAdmissionClock(self)
        """An :class:`~procrastinators.protocols.AsyncAdmissionClock` reading this timeline."""
        self.deadline_clock: Final = FakeDeadlineClock(self)
        """A :class:`~procrastinators.protocols.DeadlineClock` reading this timeline."""

    @staticmethod
    def _checked_epoch(epoch_us: int) -> int:
        if not 0 <= epoch_us <= MAX_TIMESTAMP_US:
            raise ValueError(f"epoch time {epoch_us} is outside 0 to {MAX_TIMESTAMP_US}")
        else:
            pass
        return epoch_us

    @property
    def epoch_reads(self) -> int:
        """How many times authority epoch time has been read."""
        with self._lock:
            reads = self._epoch_reads
        return reads

    @property
    def monotonic_reads(self) -> int:
        """How many times local monotonic time has been read."""
        with self._lock:
            reads = self._monotonic_reads
        return reads

    def epoch_now(self) -> EpochMicros:
        """Read authority epoch time, counting the read."""
        with self._lock:
            self._epoch_reads += 1
            now = EpochMicros(self._epoch)
        return now

    def monotonic_now(self) -> MonotonicMicros:
        """Read local monotonic time, counting the read."""
        with self._lock:
            self._monotonic_reads += 1
            now = MonotonicMicros(self._monotonic)
        return now

    def peek_epoch(self) -> EpochMicros:
        """Epoch time without counting a read, for assertions."""
        with self._lock:
            now = EpochMicros(self._epoch)
        return now

    def advance(self, duration_us: int) -> None:
        """Let ``duration_us`` pass: both clocks move forward together.

        :param duration_us: Microseconds to advance, not negative.
        :raises ValueError: ``duration_us`` is negative or would leave the timestamp range.
        """
        if duration_us < 0:
            raise ValueError(f"time cannot pass backwards: {duration_us}")
        else:
            pass
        with self._lock:
            self._epoch = self._checked_epoch(self._epoch + duration_us)
            self._monotonic += duration_us

    def advance_to(self, epoch_us: int) -> None:
        """Let time pass until epoch time reads ``epoch_us``.

        :param epoch_us: The epoch time to reach; not earlier than now.
        :raises ValueError: ``epoch_us`` is in the past. Use :meth:`step_epoch`
            to simulate a wall clock stepping backwards.
        """
        with self._lock:
            delta = epoch_us - self._epoch
        if delta < 0:
            raise ValueError(
                f"cannot advance to {epoch_us}, which is before {epoch_us - delta}; "
                "use step_epoch for a backwards wall-clock step"
            )
        else:
            self.advance(delta)

    def step_epoch(self, delta_us: int) -> None:
        """Step the wall clock by ``delta_us`` in either direction; monotonic time stays put.

        :param delta_us: Microseconds to add to epoch time; may be negative.
        :raises ValueError: The result would leave the timestamp range.
        """
        with self._lock:
            self._epoch = self._checked_epoch(self._epoch + delta_us)


class FakeAdmissionClock:
    """Authority epoch time from a :class:`FakeTimeline`."""

    __slots__ = ("_timeline",)

    def __init__(self, timeline: FakeTimeline) -> None:
        self._timeline = timeline

    def now(self) -> EpochMicros:
        """Return the timeline's epoch time."""
        now = self._timeline.epoch_now()
        return now


class FakeAsyncAdmissionClock:
    """Authority epoch time from a :class:`FakeTimeline`, read as if it were I/O."""

    __slots__ = ("_timeline",)

    def __init__(self, timeline: FakeTimeline) -> None:
        self._timeline = timeline

    async def now(self) -> EpochMicros:
        """Return the timeline's epoch time."""
        now = self._timeline.epoch_now()
        return now


class FakeDeadlineClock:
    """Local monotonic time from a :class:`FakeTimeline`."""

    __slots__ = ("_timeline",)

    def __init__(self, timeline: FakeTimeline) -> None:
        self._timeline = timeline

    def now(self) -> MonotonicMicros:
        """Return the timeline's monotonic time."""
        now = self._timeline.monotonic_now()
        return now


if __name__ == "__main__":
    pass
else:
    pass
