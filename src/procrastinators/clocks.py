"""Clocks and deadline budgeting.

Two clocks, never interchanged (contract T1):

* :class:`MonotonicClock` reads local monotonic time, the only domain for
  deadlines and elapsed measurement.
* :class:`LocalEpochClock` and :class:`SystemClock` read authority epoch time
  for an in-process authority, the domain stored state and fixed-window
  alignment use. A remote store samples its own time instead.

Public seconds become internal microseconds in exactly one place per argument
(contract T3): :func:`seconds_to_micros` for durations, and
:func:`budget_for_timeout` for the facade's ``timeout`` and ``storage_timeout``.

Constructing a clock reads the time; importing this module does not.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import datetime as dt
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from procrastinators.errors import InvalidPolicy
from procrastinators.models import (
    MAX_DURATION_US,
    MAX_TIMESTAMP_US,
    USECS_PER_SECOND,
    DurationMicros,
    EpochMicros,
    MonotonicMicros,
    OperationBudget,
    duration_to_micros,
)
from procrastinators.protocols import DeadlineClock

if TYPE_CHECKING:
    from collections.abc import Callable
else:
    pass

__all__ = [
    "Deadline",
    "LocalEpochClock",
    "MonotonicClock",
    "SystemClock",
    "budget_for_timeout",
    "seconds_to_micros",
]

_NANOS_PER_MICRO = 1_000


class MonotonicClock:
    """Local monotonic time in microseconds.

    Satisfies :class:`~procrastinators.protocols.DeadlineClock`. Meaningful only
    within this process. Never persisted or sent anywhere.
    """

    __slots__ = ()

    def now(self) -> MonotonicMicros:
        """Return the current local monotonic time, in microseconds."""
        micros = MonotonicMicros(time.monotonic_ns() // _NANOS_PER_MICRO)
        return micros


def _checked_epoch(micros: int) -> EpochMicros:
    if not 0 <= micros <= MAX_TIMESTAMP_US:
        raise InvalidPolicy(
            f"epoch time {micros} is outside the supported range 0 to {MAX_TIMESTAMP_US}"
        )
    else:
        pass
    return EpochMicros(micros)


class SystemClock:
    """The local wall clock as authority epoch time.

    Satisfies :class:`~procrastinators.protocols.AdmissionClock`. It follows
    every adjustment of the system clock, backwards included, so a backend
    using it must clamp against its last observed time (contract T7). Prefer
    :class:`LocalEpochClock` for an in-process authority.
    """

    __slots__ = ()

    def now(self) -> EpochMicros:
        """Return the wall clock's current epoch time, in microseconds.

        :raises ~procrastinators.errors.InvalidPolicy: The system clock is before the epoch or
            beyond the supported range.
        """
        micros = _checked_epoch(time.time_ns() // _NANOS_PER_MICRO)
        return micros


class LocalEpochClock:
    """Epoch-aligned time that never runs backwards, for an in-process authority.

    Reads the wall clock once, at construction, then advances with the
    monotonic clock. Fixed windows therefore align to the Unix epoch rather than
    to process startup (contract T8), and a wall-clock step during the process's
    life cannot move admission time backwards (T7). The price is that a
    long-running process does not follow later wall-clock corrections, which is
    the conservative direction for a limiter whose state lives only in memory.

    Satisfies :class:`~procrastinators.protocols.AdmissionClock`.

    :param wall_ns: Reads wall-clock nanoseconds since the epoch. Injected for tests.
    :param monotonic_ns: Reads monotonic nanoseconds. Injected for tests.
    :raises ~procrastinators.errors.InvalidPolicy: The wall clock is outside the supported range.
    """

    __slots__ = ("_epoch_anchor_us", "_monotonic_anchor_ns", "_monotonic_ns")

    def __init__(
        self,
        *,
        wall_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._monotonic_ns = monotonic_ns
        self._monotonic_anchor_ns = monotonic_ns()
        self._epoch_anchor_us = _checked_epoch(wall_ns() // _NANOS_PER_MICRO)

    def now(self) -> EpochMicros:
        """Return the current epoch time, in microseconds, never less than before.

        :raises ~procrastinators.errors.InvalidPolicy: The process has outlived the supported
            timestamp range.
        """
        elapsed_us = (self._monotonic_ns() - self._monotonic_anchor_ns) // _NANOS_PER_MICRO
        micros = _checked_epoch(self._epoch_anchor_us + elapsed_us)
        return micros


def seconds_to_micros(
    value: float | int | str | dt.timedelta, *, what: str, allow_zero: bool = True
) -> DurationMicros:
    """Convert a public duration in seconds into internal microseconds.

    Accepts every form :func:`~procrastinators.models.duration_to_micros`
    accepts, rounds a sub-microsecond remainder up, and bounds the result.

    :param value: Seconds as a number, a duration string, or a timedelta.
    :param what: The argument's name, for the error message.
    :param allow_zero: Whether zero is acceptable.
    :returns: The duration in microseconds.
    :raises ~procrastinators.errors.InvalidPolicy: ``value`` is not a duration, is negative, is zero
        when ``allow_zero`` is false, or exceeds :data:`~procrastinators.models.MAX_DURATION_US`.
    """
    try:
        micros = duration_to_micros(value)
    except InvalidPolicy as error:
        raise InvalidPolicy(f"{what}: {error}") from error
    if micros > MAX_DURATION_US:
        raise InvalidPolicy(f"{what} must be at most {MAX_DURATION_US // USECS_PER_SECOND} seconds")
    elif micros == 0 and not allow_zero:
        raise InvalidPolicy(f"{what} must be positive")
    else:
        pass
    return micros


@dataclass(frozen=True, slots=True)
class Deadline:
    """A local give-up time and the clock that measures it.

    ``at`` is local monotonic time, so a deadline cannot be sent to a remote
    store by mistake (contract T2). ``None`` means no deadline: quota may be
    waited for indefinitely, though every individual storage call stays bounded
    (B3), which is what :meth:`cap` is for.
    """

    at: MonotonicMicros | None
    """When the caller gives up, or ``None`` for never."""

    clock: DeadlineClock
    """The local monotonic clock ``at`` is measured on."""

    @classmethod
    def after(cls, timeout_us: DurationMicros | None, clock: DeadlineClock) -> Deadline:
        """A deadline ``timeout_us`` from now, or none when ``timeout_us`` is ``None``.

        :param timeout_us: How long from now, in microseconds, or ``None``.
        :param clock: The local monotonic clock to measure on.
        """
        at = None if timeout_us is None else MonotonicMicros(clock.now() + timeout_us)
        deadline = cls(at, clock)
        return deadline

    @classmethod
    def from_budget(cls, budget: OperationBudget, clock: DeadlineClock) -> Deadline:
        """The deadline an :class:`~procrastinators.models.OperationBudget` carries.

        :param budget: The operation budget.
        :param clock: The clock the budget's deadline was set on.
        """
        deadline = cls(budget.deadline_us, clock)
        return deadline

    def remaining(self) -> DurationMicros | None:
        """Microseconds left, never negative, or ``None`` for no deadline."""
        left = None if self.at is None else DurationMicros(max(0, self.at - self.clock.now()))
        return left

    def expired(self) -> bool:
        """Whether the deadline has passed. Never true without a deadline."""
        left = self.remaining()
        expired = left is not None and left == 0
        return expired

    def cap(self, duration: DurationMicros) -> DurationMicros:
        """``duration`` shortened to fit the time remaining.

        Bounds a storage call or a sleep by whichever ends first.

        :param duration: The longest the operation may take on its own terms.
        :returns: The smaller of ``duration`` and the time remaining.
        """
        left = self.remaining()
        capped = duration if left is None else DurationMicros(min(duration, left))
        return capped


def budget_for_timeout(
    timeout: float | int | str | dt.timedelta | None,
    *,
    clock: DeadlineClock,
    storage_timeout: float | int | str | dt.timedelta = 5.0,
    lock_timeout: float | int | str | dt.timedelta | None = None,
    max_contention_retries: int = 3,
) -> OperationBudget:
    """Translate the facade's public timeouts into an operation budget (contract B2).

    ============ ===========================================================
    ``timeout``  Budget
    ============ ===========================================================
    ``None``     No deadline; wait for quota indefinitely.
    ``0``        A deadline of now and ``wait_for_quota=False``: one attempt.
    ``> 0``      A deadline covering contention, storage work, and waiting.
    ============ ===========================================================

    :param timeout: The quota-wait budget in seconds, or ``None``.
    :param clock: The local monotonic clock the deadline is set on.
    :param storage_timeout: The cap on each storage call, in seconds. Never ``None``.
    :param lock_timeout: The cap on contention waits, in seconds; the storage
        timeout when ``None``.
    :param max_contention_retries: Retries allowed for definitely uncommitted
        transient failures.
    :returns: The budget.
    :raises ~procrastinators.errors.InvalidPolicy: Any value is malformed or out of range.
    """
    storage_us = seconds_to_micros(storage_timeout, what="storage_timeout", allow_zero=False)
    if lock_timeout is None:
        lock_us = storage_us
    else:
        lock_us = seconds_to_micros(lock_timeout, what="lock_timeout", allow_zero=False)
    if timeout is None:
        deadline_us = None
        wait_for_quota = True
    elif (timeout_us := seconds_to_micros(timeout, what="timeout")) == 0:
        deadline_us = clock.now()
        wait_for_quota = False
    else:
        deadline_us = MonotonicMicros(clock.now() + timeout_us)
        wait_for_quota = True
    budget = OperationBudget(
        deadline_us=deadline_us,
        storage_timeout_us=storage_us,
        lock_timeout_us=lock_us,
        max_contention_retries=max_contention_retries,
        wait_for_quota=wait_for_quota,
    )
    return budget


if __name__ == "__main__":
    pass
else:
    pass
