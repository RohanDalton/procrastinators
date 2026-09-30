"""Waiting for quota: one atomic attempt at a time, sleeping outside every lock.

The waiting logic is written once, as a plan: a generator that yields steps —
:class:`AdmitStep` to make one atomic backend attempt, :class:`SleepStep` to
wait — and receives each step's result. :func:`run` performs a plan against a
synchronous backend and :class:`~procrastinators.protocols.Sleeper`;
:func:`run_async` awaits it against an asynchronous backend and
:class:`~procrastinators.protocols.AsyncSleeper`. Sync and async callers
therefore wait by exactly the same rules:

* **Sleep outside the backend** (W1). A step is either a backend call or a
  sleep, never both, so no lock or transaction can be held while waiting.
* **Recheck after every sleep** (W2, T5). Every attempt is a fresh backend
  admission, which samples fresh authority time; a retry delay is advisory
  (R4) and is never trusted as permission.
* **Wait the maximum delay** (C7), exactly as reported: jitter is not added,
  so nothing can shorten a required wait (W3).
* **Respect the deadline** (B2). ``timeout=0`` makes exactly one attempt. A
  positive deadline caps each attempt's storage and lock budgets as well as the
  total wait, and a denial whose delay outlasts the deadline times out at once
  rather than sleeping to the deadline first (K5).
* **Retry only what never committed** (B4). Contention and unavailability are
  retried with a capped backoff within the budget; an indeterminate commit is
  surfaced immediately and never retried or refunded (O4).

Diagnostics are delivered between steps, so outside every critical section,
and a callback that raises is logged and ignored (D1, D2).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import dataclasses
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Generic, TypeAlias, TypeVar

from procrastinators.clocks import Deadline
from procrastinators.errors import (
    AcquireTimeout,
    BackendBusy,
    BackendError,
    BackendUnavailable,
    IndeterminateAdmission,
    PolicyConflict,
    ProcrastinatorsError,
)
from procrastinators.models import (
    USECS_PER_SECOND,
    AdmittedEvent,
    BackendFailureEvent,
    DeniedEvent,
    DurationMicros,
    PolicyConflictEvent,
    WaitEvent,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from procrastinators.models import (
        Admission,
        AdmissionRequest,
        BackendIdentity,
        Decision,
        DiagnosticEvent,
    )
    from procrastinators.protocols import (
        AsyncBackend,
        AsyncSleeper,
        DeadlineClock,
        DiagnosticsCallback,
        Sleeper,
        SyncBackend,
    )
else:
    pass

__all__ = [
    "CONTENTION_BACKOFF_MAX_US",
    "CONTENTION_BACKOFF_US",
    "MIN_SLEEP_US",
    "AdmitStep",
    "AsyncioSleeper",
    "Plan",
    "SleepStep",
    "ThreadSleeper",
    "Waiter",
    "deliver",
    "run",
    "run_async",
]

logger = logging.getLogger(__name__)

T = TypeVar("T")
"""What a plan produces when it finishes."""

MIN_SLEEP_US: Final = DurationMicros(1)
"""The shortest wait after a denial, so a zero delay cannot spin without time passing."""

CONTENTION_BACKOFF_US: Final = DurationMicros(1_000)
"""The first pause before retrying contention or unavailability; it doubles per retry."""

CONTENTION_BACKOFF_MAX_US: Final = DurationMicros(250_000)
"""The longest pause between contention retries."""


@dataclass(frozen=True, slots=True)
class AdmitStep:
    """Make one atomic admission attempt; the plan receives the decision."""

    request: AdmissionRequest
    """The request to admit, with its per-attempt budget."""


@dataclass(frozen=True, slots=True)
class SleepStep:
    """Wait, holding nothing; the plan receives ``None``."""

    duration: DurationMicros
    """How long to wait, in microseconds."""


Step: TypeAlias = AdmitStep | SleepStep
"""One thing a plan asks its runner to do."""

Plan: TypeAlias = "Generator[Step, Decision | None, T]"
"""A waiting plan: yields steps, receives their results, returns what it produced."""


class ThreadSleeper:
    """The real :class:`~procrastinators.protocols.Sleeper`: blocks the calling thread."""

    __slots__ = ()

    def sleep(self, duration: DurationMicros) -> None:
        """Block for ``duration`` microseconds.

        :param duration: How long to wait.
        """
        time.sleep(duration / USECS_PER_SECOND)


class AsyncioSleeper:
    """The real :class:`~procrastinators.protocols.AsyncSleeper`: yields to the event loop."""

    __slots__ = ()

    async def sleep(self, duration: DurationMicros) -> None:
        """Wait ``duration`` microseconds without blocking the loop; cancellable.

        :param duration: How long to wait.
        :raises asyncio.CancelledError: The wait was cancelled; propagated unchanged.
        """
        await asyncio.sleep(duration / USECS_PER_SECOND)


def deliver(on_event: DiagnosticsCallback | None, event: DiagnosticEvent) -> None:
    """Hand ``event`` to ``on_event``, isolating whatever it raises.

    :param on_event: The callback, or ``None``.
    :param event: The event.
    """
    if on_event is None:
        pass
    else:
        try:
            on_event(event)
        except Exception:
            # Contract D2 requires isolating every callback failure: letting it
            # propagate would report a committed admission as a failure.
            logger.warning("diagnostics callback failed on %r", event, exc_info=True)


class Waiter:
    """Builds waiting plans for one limiter.

    :param clock: Local monotonic time, for deadlines and wait measurement.
    :param on_event: Receives diagnostics, or ``None``.
    """

    __slots__ = ("_clock", "_on_event")

    def __init__(self, *, clock: DeadlineClock, on_event: DiagnosticsCallback | None) -> None:
        self._clock = clock
        self._on_event = on_event

    def _emit(self, event: DiagnosticEvent) -> None:
        deliver(self._on_event, event)

    def acquire(self, request: AdmissionRequest, backend: BackendIdentity) -> Plan[Admission]:
        """Wait until ``request`` is admitted, then return its admission.

        :param request: The request, whose budget carries the deadline.
        :param backend: The backend's identity, for diagnostics.
        :returns: A plan producing the admission.
        :raises ~procrastinators.errors.AcquireTimeout: The deadline passed, or
            ``timeout=0`` and the one attempt was denied. Nothing was consumed.
        """
        deadline = Deadline.from_budget(request.budget, self._clock)
        started = self._clock.now()
        attempt = 1
        blocking = request.rules
        retry_after = DurationMicros(0)
        while True:
            if request.budget.wait_for_quota and deadline.expired():
                raise AcquireTimeout(
                    "quota was not granted before the deadline; nothing was consumed",
                    cost=request.cost,
                    waited_us=DurationMicros(self._clock.now() - started),
                    blocking=blocking,
                    retry_after_us=retry_after,
                )
            else:
                pass
            decision = yield from self._admit(request, deadline, backend)
            if decision.admission is not None:
                self._emit(
                    AdmittedEvent(
                        at=(now := self._clock.now()),
                        rules=decision.admission.charged,
                        cost=request.cost,
                        waited_us=DurationMicros(now - started),
                        backend=decision.admission.backend,
                    )
                )
                return decision.admission
            else:
                pass
            blocking = decision.blocking
            retry_after = decision.retry_after_us
            delay = DurationMicros(max(MIN_SLEEP_US, decision.retry_after_us))
            remaining = deadline.remaining()
            if not request.budget.wait_for_quota or (remaining is not None and delay > remaining):
                waited = DurationMicros(self._clock.now() - started)
                reason = (
                    "timeout=0 permits one attempt"
                    if not request.budget.wait_for_quota
                    else f"the next opening is {delay} µs away but only {remaining} µs remain"
                )
                raise AcquireTimeout(
                    f"quota was not granted: {reason}; nothing was consumed",
                    cost=request.cost,
                    waited_us=waited,
                    blocking=decision.blocking,
                    retry_after_us=decision.retry_after_us,
                )
            else:
                pass
            self._emit(
                WaitEvent(
                    at=self._clock.now(), rules=decision.blocking, slept_us=delay, attempt=attempt
                )
            )
            yield SleepStep(delay)
            attempt += 1

    def attempt(self, request: AdmissionRequest, backend: BackendIdentity) -> Plan[Decision]:
        """Make one admission attempt and return its decision, whatever it is.

        :param request: The request.
        :param backend: The backend's identity, for diagnostics.
        :returns: A plan producing the decision.
        """
        deadline = Deadline.from_budget(request.budget, self._clock)
        decision = yield from self._admit(request, deadline, backend)
        return decision

    def _admit(
        self, request: AdmissionRequest, deadline: Deadline, backend: BackendIdentity
    ) -> Plan[Decision]:
        """One admission, retrying definitely uncommitted failures within budget (B4)."""
        retries = 0
        while True:
            try:
                decision = yield AdmitStep(self._bounded(request, deadline))
            except PolicyConflict as error:
                if (
                    error.rule is not None
                    and error.expected is not None
                    and error.found is not None
                ):
                    self._emit(
                        PolicyConflictEvent(
                            at=self._clock.now(),
                            rule=error.rule,
                            expected=error.expected,
                            found=error.found,
                        )
                    )
                else:
                    pass
                raise
            except BackendError as error:
                self._emit(
                    BackendFailureEvent(
                        at=self._clock.now(),
                        backend=backend,
                        operation="admit",
                        error_type=type(error).__name__,
                        indeterminate=isinstance(error, IndeterminateAdmission),
                    )
                )
                backoff = DurationMicros(
                    min(CONTENTION_BACKOFF_US * 2**retries, CONTENTION_BACKOFF_MAX_US)
                )
                remaining = deadline.remaining()
                if (
                    not isinstance(error, (BackendBusy, BackendUnavailable))
                    or retries >= request.budget.max_contention_retries
                    or (remaining is not None and backoff > remaining)
                ):
                    raise
                else:
                    retries += 1
            else:
                break
            yield SleepStep(backoff)
        assert decision is not None
        if not decision.allowed:
            self._emit(
                DeniedEvent(
                    at=self._clock.now(),
                    blocking=decision.blocking,
                    cost=request.cost,
                    retry_after_us=decision.retry_after_us,
                )
            )
        else:
            pass
        return decision

    @staticmethod
    def _bounded(request: AdmissionRequest, deadline: Deadline) -> AdmissionRequest:
        """``request`` with its storage and lock budgets capped by the time remaining.

        A positive deadline covers contention and storage work as well as
        waiting (B2). With ``timeout=0`` those keep their own budgets: one
        attempt still gets a bounded, not an instant, storage call (B3).
        """
        budget = request.budget
        if budget.wait_for_quota and (remaining := deadline.remaining()) is not None:
            cap = max(1, remaining)
            bounded = dataclasses.replace(
                request,
                budget=dataclasses.replace(
                    budget,
                    storage_timeout_us=DurationMicros(min(budget.storage_timeout_us, cap)),
                    lock_timeout_us=DurationMicros(min(budget.lock_timeout_us, cap)),
                ),
            )
        else:
            bounded = request
        return bounded


@dataclass(frozen=True, slots=True)
class _Finished(Generic[T]):
    value: T


def _resume(
    plan: Plan[T], result: Decision | None, error: ProcrastinatorsError | None
) -> Step | _Finished[T]:
    try:
        step = plan.send(result) if error is None else plan.throw(error)
    except StopIteration as stop:
        finished: _Finished[T] = _Finished(stop.value)
        return finished
    return step


def run(plan: Plan[T], backend: SyncBackend, sleeper: Sleeper) -> T:
    """Perform ``plan`` against a synchronous backend.

    Library errors from the backend are handed to the plan, which decides
    whether to retry; anything else propagates unchanged.

    :param plan: The plan.
    :param backend: Performs its admission steps.
    :param sleeper: Performs its sleeps.
    :returns: What the plan produced.
    """
    step: Step | _Finished[T] = next(plan)
    while not isinstance(step, _Finished):
        result: Decision | None = None
        failure: ProcrastinatorsError | None = None
        try:
            if isinstance(step, AdmitStep):
                result = backend.admit(step.request)
            else:
                sleeper.sleep(step.duration)
        except ProcrastinatorsError as error:
            failure = error
        step = _resume(plan, result, failure)
    return step.value


async def run_async(plan: Plan[T], backend: AsyncBackend, sleeper: AsyncSleeper) -> T:
    """Perform ``plan`` against an asynchronous backend.

    As :func:`run`, awaiting each step. Cancellation propagates unchanged from
    whichever step it lands in (O5).

    :param plan: The plan.
    :param backend: Performs its admission steps.
    :param sleeper: Performs its sleeps.
    :returns: What the plan produced.
    """
    step: Step | _Finished[T] = next(plan)
    while not isinstance(step, _Finished):
        result: Decision | None = None
        failure: ProcrastinatorsError | None = None
        try:
            if isinstance(step, AdmitStep):
                result = await backend.admit(step.request)
            else:
                await sleeper.sleep(step.duration)
        except ProcrastinatorsError as error:
            failure = error
        step = _resume(plan, result, failure)
    return step.value


if __name__ == "__main__":
    pass
else:
    pass
