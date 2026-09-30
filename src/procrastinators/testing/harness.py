"""Running traces against a subject, synchronously or asynchronously, with one driver.

A check is written once, as a generator that yields :data:`Call` values and
receives each call's :class:`Outcome`. :func:`pump` performs the calls against
a synchronous :class:`TraceSubject`; :func:`apump` awaits them against an
:class:`AsyncTraceSubject`. The sync and async suites therefore check exactly
the same sequence, rather than two copies that drift apart.

A subject is whatever turns requests into decisions. A backend is one, through
:class:`BackendSubject`; a limiter facade will be another, whose ``finish``
exits its context. Separating "entered" from "finished" is what lets a trace
catch an implementation that charges on exit.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, TypeAlias, TypeVar, runtime_checkable

from procrastinators.models import AdmissionRequest, Decision
from procrastinators.testing.traces import Attempt, Finish
from procrastinators.testing.violations import ContractViolation

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from procrastinators.models import Admission, RuleId, Snapshot
    from procrastinators.protocols import AsyncBackend, SyncBackend
    from procrastinators.testing.clocks import FakeTimeline
    from procrastinators.testing.traces import Trace
else:
    pass

__all__ = [
    "DEFAULT_NAMESPACE",
    "AdmitCall",
    "AsyncBackendSubject",
    "AsyncTraceSubject",
    "BackendSubject",
    "Call",
    "CloseCall",
    "Driver",
    "FinishCall",
    "InspectCall",
    "Outcome",
    "TraceMismatch",
    "TraceReport",
    "apump",
    "pump",
    "run_trace",
    "run_trace_async",
    "trace_driver",
]

DEFAULT_NAMESPACE = "conformance"
"""The quota namespace traces run in."""

T = TypeVar("T")
"""What a driver returns once its calls are done."""


@runtime_checkable
class TraceSubject(Protocol):
    """Something that admits requests synchronously and can be told a body finished."""

    def admit(self, request: AdmissionRequest) -> Decision:
        """Attempt one atomic admission."""
        ...

    def finish(self, admission: Admission) -> None:
        """The body admitted by ``admission`` has ended."""
        ...

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation."""
        ...

    def close(self) -> None:
        """Release resources."""
        ...


@runtime_checkable
class AsyncTraceSubject(Protocol):
    """The asynchronous counterpart of :class:`TraceSubject`."""

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Attempt one atomic admission."""
        ...

    async def finish(self, admission: Admission) -> None:
        """The body admitted by ``admission`` has ended."""
        ...

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation."""
        ...

    async def close(self) -> None:
        """Release resources."""
        ...


class BackendSubject:
    """A :class:`~procrastinators.protocols.SyncBackend` as a trace subject.

    ``finish`` does nothing, because a backend has no notion of a body ending:
    quota was consumed at admission and nothing is refunded (contract A3).

    :param backend: The backend to drive.
    """

    def __init__(self, backend: SyncBackend) -> None:
        self.backend = backend

    def admit(self, request: AdmissionRequest) -> Decision:
        """Forward to the backend."""
        decision = self.backend.admit(request)
        return decision

    def finish(self, admission: Admission) -> None:
        """Nothing happens when a body ends."""
        del admission

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Forward to the backend."""
        snapshot = self.backend.inspect(rules)
        return snapshot

    def close(self) -> None:
        """Forward to the backend."""
        self.backend.close()


class AsyncBackendSubject:
    """An :class:`~procrastinators.protocols.AsyncBackend` as a trace subject.

    :param backend: The backend to drive.
    """

    def __init__(self, backend: AsyncBackend) -> None:
        self.backend = backend

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Forward to the backend."""
        decision = await self.backend.admit(request)
        return decision

    async def finish(self, admission: Admission) -> None:
        """Nothing happens when a body ends."""
        del admission

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Forward to the backend."""
        snapshot = await self.backend.inspect(rules)
        return snapshot

    async def close(self) -> None:
        """Forward to the backend's ``aclose``."""
        await self.backend.aclose()


@dataclass(frozen=True, slots=True)
class AdmitCall:
    """Ask the subject to admit ``request``."""

    request: AdmissionRequest


@dataclass(frozen=True, slots=True)
class FinishCall:
    """Tell the subject the body admitted by ``admission`` ended."""

    admission: Admission


@dataclass(frozen=True, slots=True)
class InspectCall:
    """Ask the subject for an advisory snapshot of ``rules``."""

    rules: tuple[RuleId, ...]


@dataclass(frozen=True, slots=True)
class CloseCall:
    """Ask the subject to close."""


Call: TypeAlias = AdmitCall | FinishCall | InspectCall | CloseCall
"""One operation a driver asks the pump to perform."""


@dataclass(frozen=True, slots=True)
class Outcome:
    """What a call returned, or what it raised."""

    value: object = None
    """The return value, when nothing was raised."""

    error: BaseException | None = None
    """The exception raised, or ``None``."""

    def decision(self) -> Decision:
        """The returned decision.

        :raises ~procrastinators.testing.violations.ContractViolation: The call raised, or
            returned something other than a decision.
        """
        if self.error is not None:
            raise ContractViolation(f"expected a decision, but {_describe(self.error)} was raised")
        elif not isinstance(self.value, Decision):
            raise ContractViolation(f"expected a decision, got {self.value!r}")
        else:
            pass
        return self.value


Driver: TypeAlias = "Generator[Call, Outcome, T]"
"""A check: yields calls, receives outcomes, returns a result."""

# Exceptions a pump captures and hands back to the driver. Cancellation is
# included so drivers can assert it propagated unchanged; interpreter exits
# (KeyboardInterrupt, SystemExit) are not.
_CAPTURED = (Exception, asyncio.CancelledError)


def _describe(error: BaseException) -> str:
    text = f"{type(error).__name__}({error})"
    return text


def pump(driver: Driver[T], subject: TraceSubject) -> T:
    """Run ``driver`` against a synchronous subject and return its result.

    :param driver: The check to run.
    :param subject: What to perform its calls on.
    :returns: Whatever the driver returns.
    """
    call = next(driver)
    while True:
        try:
            if isinstance(call, AdmitCall):
                outcome = Outcome(value=subject.admit(call.request))
            elif isinstance(call, FinishCall):
                outcome = Outcome(value=subject.finish(call.admission))
            elif isinstance(call, InspectCall):
                outcome = Outcome(value=subject.inspect(call.rules))
            else:
                outcome = Outcome(value=subject.close())
        except _CAPTURED as error:
            outcome = Outcome(error=error)
        try:
            call = driver.send(outcome)
        except StopIteration as stop:
            result: T = stop.value
            break
    return result


async def apump(driver: Driver[T], subject: AsyncTraceSubject) -> T:
    """Run ``driver`` against an asynchronous subject and return its result.

    A cancellation raised *by the subject* is captured and handed to the
    driver; cancellation of the task running this coroutine still propagates.

    :param driver: The check to run.
    :param subject: What to await its calls on.
    :returns: Whatever the driver returns.
    """
    call = next(driver)
    while True:
        try:
            if isinstance(call, AdmitCall):
                outcome = Outcome(value=await subject.admit(call.request))
            elif isinstance(call, FinishCall):
                outcome = Outcome(value=await subject.finish(call.admission))
            elif isinstance(call, InspectCall):
                outcome = Outcome(value=await subject.inspect(call.rules))
            else:
                outcome = Outcome(value=await subject.close())
        except _CAPTURED as error:
            outcome = Outcome(error=error)
        try:
            call = driver.send(outcome)
        except StopIteration as stop:
            result: T = stop.value
            break
    return result


@dataclass(frozen=True, slots=True)
class TraceMismatch:
    """One step where the subject disagreed with the trace."""

    step: int
    """Index of the step in the trace."""

    at: int
    """Authority epoch time of the step."""

    expected: str
    """What the trace required."""

    actual: str
    """What happened."""

    note: str = ""
    """The step's note."""

    def __str__(self) -> str:
        suffix = f" ({self.note})" if self.note else ""
        text = f"step {self.step}{suffix}: expected {self.expected}, got {self.actual}"
        return text


@dataclass(frozen=True, slots=True)
class TraceReport:
    """The result of running one trace."""

    trace: Trace
    """The trace that was run."""

    mismatches: tuple[TraceMismatch, ...]
    """Every step that disagreed; empty when the subject conformed."""

    @property
    def passed(self) -> bool:
        """Whether every step matched."""
        passed = not self.mismatches
        return passed

    def raise_for_mismatches(self) -> None:
        """Raise unless every step matched.

        :raises ~procrastinators.testing.violations.ContractViolation: Listing every mismatch.
        """
        if self.mismatches:
            details = "\n  ".join(str(mismatch) for mismatch in self.mismatches)
            raise ContractViolation(f"trace {self.trace.name} failed:\n  {details}")
        else:
            pass


def _describe_decision(decision: Decision, labels: dict[RuleId, str]) -> str:
    if decision.allowed:
        text = "allowed"
    else:
        blocking = sorted(labels.get(rule, str(rule)) for rule in decision.blocking)
        text = f"denied by {blocking} retrying after {decision.retry_after_us}"
    return text


def _verdict_matches(
    step: Attempt, decision: Decision, labels: dict[RuleId, str], *, check_retry: bool
) -> bool:
    if decision.allowed != step.allowed:
        matches = False
    elif decision.allowed:
        matches = True
    else:
        retry_matches = not check_retry or step.retry_after_us in (None, decision.retry_after_us)
        blocking = sorted(labels.get(rule, str(rule)) for rule in decision.blocking)
        blocking_matches = step.blocking is None or sorted(step.blocking) == blocking
        matches = retry_matches and blocking_matches
    return matches


def trace_driver(
    trace: Trace,
    timeline: FakeTimeline,
    *,
    namespace: str = DEFAULT_NAMESPACE,
    check_retry: bool = True,
    check_timestamps: bool = True,
) -> Driver[TraceReport]:
    """The check that runs ``trace``, moving ``timeline`` to each step's instant.

    For each attempt it compares the verdict; for a denial, the exact retry
    delay and the blocking rules where the trace names them; for an admission,
    the rules charged, the cost, and — when ``check_timestamps`` — that the
    admission was stamped with the authority time of the attempt. A finish
    tells the subject the body ended and expects nothing to happen.

    :param trace: The trace.
    :param timeline: The clock the subject reads; must not be past the first step.
    :param namespace: The quota namespace to run in.
    :param check_retry: Whether to compare retry delays.
    :param check_timestamps: Whether to compare admission timestamps.
    :returns: A driver producing the report.
    """
    constraints = {rule.label: rule.constraint(namespace) for rule in trace.rules}
    all_labels = tuple(constraints)
    labels = trace.labels_of(namespace)
    admissions: dict[int, Admission] = dict()
    mismatches: list[TraceMismatch] = list()
    for index, step in enumerate(trace.steps):
        timeline.advance_to(step.at)
        if isinstance(step, Attempt):
            request = AdmissionRequest(
                tuple(constraints[label] for label in step.rules or all_labels), cost=step.cost
            )
            outcome = yield AdmitCall(request)
            expected = "allowed" if step.allowed else "denied"
            if step.retry_after_us is not None and check_retry:
                expected += f" retrying after {step.retry_after_us}"
            else:
                pass
            if step.blocking is not None:
                expected += f" by {sorted(step.blocking)}"
            else:
                pass
            if outcome.error is not None:
                actual = f"{_describe(outcome.error)} raised"
            elif not isinstance(decision := outcome.value, Decision):
                actual = f"{decision!r} returned"
            elif not _verdict_matches(step, decision, labels, check_retry=check_retry):
                actual = _describe_decision(decision, labels)
            elif decision.admission is not None and (
                set(decision.admission.charged) != set(request.rules)
                or decision.admission.cost != step.cost
                or (check_timestamps and decision.admission.admitted_at != step.at)
            ):
                admission = decision.admission
                charged = sorted(str(rule) for rule in admission.charged)
                requested = sorted(str(rule) for rule in request.rules)
                actual = (
                    f"an admission charging {charged} cost {admission.cost} "
                    f"at {admission.admitted_at}"
                )
                expected += f" charging {requested} cost {step.cost} at {step.at}"
            else:
                actual = ""
                if decision.admission is not None:
                    admissions[index] = decision.admission
                else:
                    pass
            if actual:
                mismatches.append(TraceMismatch(index, step.at, expected, actual, step.note))
            else:
                pass
        elif isinstance(step, Finish):
            if (admission := admissions.get(step.attempt)) is None:
                pass  # the attempt already failed to match; there is no body to finish
            elif (outcome := (yield FinishCall(admission))).error is not None:
                mismatches.append(
                    TraceMismatch(
                        index,
                        step.at,
                        "a body to finish quietly",
                        f"{_describe(outcome.error)} raised",
                        step.note,
                    )
                )
            else:
                pass
        else:
            raise TypeError(f"unknown trace step: {step!r}")
    report = TraceReport(trace, tuple(mismatches))
    return report


def run_trace(
    trace: Trace,
    subject: TraceSubject,
    timeline: FakeTimeline,
    *,
    check_retry: bool = True,
    check_timestamps: bool = True,
) -> TraceReport:
    """Run ``trace`` against a synchronous subject.

    :param trace: The trace.
    :param subject: A :class:`BackendSubject` or any :class:`TraceSubject`.
    :param timeline: The clock the subject reads.
    :param check_retry: Whether to compare retry delays.
    :param check_timestamps: Whether to compare admission timestamps.
    :returns: The report; call :meth:`TraceReport.raise_for_mismatches` to assert.
    """
    report = pump(
        trace_driver(trace, timeline, check_retry=check_retry, check_timestamps=check_timestamps),
        subject,
    )
    return report


async def run_trace_async(
    trace: Trace,
    subject: AsyncTraceSubject,
    timeline: FakeTimeline,
    *,
    check_retry: bool = True,
    check_timestamps: bool = True,
) -> TraceReport:
    """Run ``trace`` against an asynchronous subject.

    :param trace: The trace.
    :param subject: An :class:`AsyncBackendSubject` or any :class:`AsyncTraceSubject`.
    :param timeline: The clock the subject reads.
    :param check_retry: Whether to compare retry delays.
    :param check_timestamps: Whether to compare admission timestamps.
    :returns: The report; call :meth:`TraceReport.raise_for_mismatches` to assert.
    """
    report = await apump(
        trace_driver(trace, timeline, check_retry=check_retry, check_timestamps=check_timestamps),
        subject,
    )
    return report


if __name__ == "__main__":
    pass
else:
    pass
