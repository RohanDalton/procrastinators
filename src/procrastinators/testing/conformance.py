"""The conformance suite for backends and algorithms.

A backend author describes their backend with a :class:`BackendCase` — how to
build one on a fake timeline with an observer, and which :class:`Guarantee`
values it claims — and calls :func:`check_backend` (or
:func:`check_async_backend`). The suite builds a fresh backend for every check,
runs it, and reports each check as passed, failed, or skipped with the reason.

Checks are selected by the backend's declared capabilities and guarantees, and
a skipped check is reported, never silently dropped: a CI job can require that
nothing it cares about was skipped (:meth:`ConformanceReport.raise_for_failures`
with ``allow_skips=False``).

An algorithm author calls :func:`check_algorithm`, which hosts the algorithm in
an :class:`~procrastinators.testing.host.EvaluatorHost` and runs its traces and
scenarios, checking along the way that evaluation is deterministic.

What the suite deliberately catches, because each has shipped in a real
library: charging on exit instead of entry, debiting an earlier rule before a
later one denies, and reporting permission after a commit whose outcome is
unknown.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import contextlib
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from procrastinators.errors import (
    BackendUnavailable,
    ClosedResource,
    IndeterminateAdmission,
    PolicyConflict,
    ProcrastinatorsError,
)
from procrastinators.models import AdmissionRequest
from procrastinators.protocols import ObservationPoint
from procrastinators.testing.catalog import CONFLICTING_PROBES, PROBES, SCENARIOS, TRACES
from procrastinators.testing.clocks import FakeTimeline
from procrastinators.testing.harness import (
    DEFAULT_NAMESPACE,
    AdmitCall,
    AsyncBackendSubject,
    BackendSubject,
    CloseCall,
    InspectCall,
    Outcome,
    apump,
    pump,
    trace_driver,
)
from procrastinators.testing.host import EvaluatorHost
from procrastinators.testing.observation import FaultInjector
from procrastinators.testing.traces import TraceRule
from procrastinators.testing.violations import ContractViolation

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

    from procrastinators.models import (
        Capabilities,
        Decision,
        EpochMicros,
        PolicySpec,
        StateChange,
        Transition,
    )
    from procrastinators.protocols import (
        AdmissionObserver,
        Algorithm,
        AsyncBackend,
        RuleState,
        StateCodec,
        StateRequirements,
        StateView,
        SyncBackend,
    )
    from procrastinators.testing.harness import AsyncTraceSubject, Driver, TraceSubject
    from procrastinators.testing.traces import Trace
else:
    pass

__all__ = [
    "AsyncBackendCase",
    "BackendCase",
    "CheckResult",
    "CheckStatus",
    "ConformanceReport",
    "Guarantee",
    "check_algorithm",
    "check_async_backend",
    "check_backend",
]


class Guarantee(StrEnum):
    """What a backend claims, and so which checks apply to it."""

    REFERENCE_DECISIONS = "reference_decisions"
    """Verdicts match every applicable shared trace and scenario (contract N4)."""

    EXACT_RETRY_DELAYS = "exact_retry_delays"
    """Retry delays match the traces exactly, not merely the verdicts."""

    AUTHORITY_TIMESTAMPS = "authority_timestamps"
    """Admissions are stamped with the injected clock's time, sampled after the lock (T4)."""

    OBSERVATION_POINTS = "observation_points"
    """The backend calls its observer at every observation point, so failures can be injected."""

    POLICY_CONFLICTS = "policy_conflicts"
    """A rule's stored fingerprint is checked at admission (I4) and a conflict resets nothing."""

    LIFECYCLE = "lifecycle"
    """Closing is idempotent and calls afterwards raise ClosedResource (L1, L5)."""


ALL_GUARANTEES = frozenset(Guarantee)
"""Every guarantee; the default claim of a case."""


@dataclass(frozen=True, slots=True)
class BackendCase:
    """How to build a synchronous backend for the suite, and what it claims.

    ``factory`` receives a fresh timeline and fault injector per check; the
    backend must read authority time from ``timeline.epoch_clock`` and call the
    injector at each observation point if it claims those guarantees.

    ``subject`` adapts the backend for driving; the default treats body exit as
    a no-op. ``probe`` and ``conflicting_probe`` supply capacity-one policies
    for a backend that implements no built-in algorithm.
    """

    name: str
    """Shown in the report."""

    factory: Callable[[FakeTimeline, AdmissionObserver], SyncBackend]
    """Builds a fresh backend."""

    guarantees: frozenset[Guarantee] = ALL_GUARANTEES
    """What the backend claims."""

    subject: Callable[[SyncBackend], TraceSubject] = BackendSubject
    """Adapts the backend into what the checks drive."""

    probe: PolicySpec | None = None
    """A policy of capacity one, for failure checks; a built-in probe when ``None``."""

    conflicting_probe: PolicySpec | None = None
    """Same algorithm as ``probe``, other parameters, for the conflict check."""


@dataclass(frozen=True, slots=True)
class AsyncBackendCase:
    """How to build an asynchronous backend for the suite, and what it claims.

    As :class:`BackendCase`, for :class:`~procrastinators.protocols.AsyncBackend`.
    """

    name: str
    """Shown in the report."""

    factory: Callable[[FakeTimeline, AdmissionObserver], AsyncBackend]
    """Builds a fresh backend."""

    guarantees: frozenset[Guarantee] = ALL_GUARANTEES
    """What the backend claims."""

    subject: Callable[[AsyncBackend], AsyncTraceSubject] = AsyncBackendSubject
    """Adapts the backend into what the checks drive."""

    probe: PolicySpec | None = None
    """A policy of capacity one, for failure checks; a built-in probe when ``None``."""

    conflicting_probe: PolicySpec | None = None
    """Same algorithm as ``probe``, other parameters, for the conflict check."""


class CheckStatus(StrEnum):
    """The result of one check."""

    PASSED = "passed"
    """The backend behaved as the contract requires."""

    FAILED = "failed"
    """The backend broke a contract rule."""

    SKIPPED = "skipped"
    """The check does not apply to what the backend declares or claims."""


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One check's result."""

    name: str
    """The check, such as ``trace:sliding_log.basics``."""

    status: CheckStatus
    """Passed, failed, or skipped."""

    detail: str = ""
    """Why it failed or was skipped."""

    def __str__(self) -> str:
        suffix = f": {self.detail}" if self.detail else ""
        text = f"{self.status.value} {self.name}{suffix}"
        return text


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    """Every check's result for one subject."""

    subject: str
    """What was checked."""

    results: tuple[CheckResult, ...]
    """One result per check, in the order they ran."""

    def _with(self, status: CheckStatus) -> tuple[CheckResult, ...]:
        selected = tuple(result for result in self.results if result.status is status)
        return selected

    @property
    def failures(self) -> tuple[CheckResult, ...]:
        """The checks that failed."""
        return self._with(CheckStatus.FAILED)

    @property
    def skipped(self) -> tuple[CheckResult, ...]:
        """The checks that did not apply."""
        return self._with(CheckStatus.SKIPPED)

    @property
    def passed(self) -> bool:
        """Whether no check failed."""
        passed = not self.failures
        return passed

    def status_of(self, name: str) -> CheckStatus:
        """The status of the check called ``name``.

        :param name: A check name.
        :raises KeyError: No check has that name.
        """
        for result in self.results:
            if result.name == name:
                status = result.status
                break
            else:
                pass
        else:
            raise KeyError(f"no check named {name!r} in the report for {self.subject}")
        return status

    def raise_for_failures(self, *, allow_skips: bool = True) -> None:
        """Raise if any check failed, or if any was skipped and skips are not allowed.

        :param allow_skips: Whether skipped checks are acceptable.
        :raises ~procrastinators.testing.violations.ContractViolation: Listing the offending checks.
        """
        offending = self.failures if allow_skips else self.failures + self.skipped
        if offending:
            details = "\n  ".join(str(result) for result in offending)
            raise ContractViolation(f"{self.subject} does not conform:\n  {details}")
        else:
            pass


@dataclass(frozen=True, slots=True)
class _Context:
    """What one check's driver works with."""

    timeline: FakeTimeline
    injector: FaultInjector
    capabilities: Capabilities
    guarantees: frozenset[Guarantee]
    probe: PolicySpec | None
    conflicting_probe: PolicySpec | None


def _always_applies(context: _Context) -> str | None:
    del context
    return None


@dataclass(frozen=True, slots=True)
class _Check:
    """A named check: when it applies, and the driver that performs it."""

    name: str
    requires: frozenset[Guarantee]
    driver: Callable[[_Context], Driver[None]]
    applies: Callable[[_Context], str | None] = _always_applies


def _probe_constraint(policy: PolicySpec) -> AdmissionRequest:
    request = AdmissionRequest(
        (TraceRule("probe", policy, scope="probe").constraint(DEFAULT_NAMESPACE),)
    )
    return request


def _probe_policy(context: _Context) -> PolicySpec | None:
    if context.probe is not None:
        policy: PolicySpec | None = context.probe
    else:
        supported = sorted(
            algorithm for algorithm in PROBES if algorithm in context.capabilities.algorithms
        )
        policy = PROBES[supported[0]] if supported else None
    return policy


def _needs_probe(context: _Context) -> str | None:
    reason = None if _probe_policy(context) is not None else "no probe policy for its algorithms"
    return reason


def _needs_conflicting_probe(context: _Context) -> str | None:
    if (policy := _probe_policy(context)) is None:
        reason: str | None = "no probe policy for its algorithms"
    elif context.probe is not None and context.conflicting_probe is None:
        reason = "a custom probe was given without a conflicting probe"
    elif context.probe is None and policy.algorithm not in CONFLICTING_PROBES:
        reason = "no conflicting probe for its algorithms"
    else:
        reason = None
    return reason


def _conflicting_policy(context: _Context) -> PolicySpec:
    policy = _probe_policy(context)
    assert policy is not None
    conflicting = context.conflicting_probe or CONFLICTING_PROBES[policy.algorithm]
    return conflicting


def _probe(context: _Context) -> AdmissionRequest:
    policy = _probe_policy(context)
    assert policy is not None
    request = _probe_constraint(policy)
    return request


def _expect_decision(outcome: Outcome, *, allowed: bool, why: str) -> Decision:
    decision = outcome.decision()
    if decision.allowed != allowed:
        verdict = "allowed" if allowed else "denied"
        raise ContractViolation(f"expected the attempt to be {verdict} ({why}), got {decision}")
    else:
        pass
    return decision


def _expect_error(
    outcome: Outcome, expected: tuple[type[BaseException], ...], *, why: str
) -> BaseException:
    names = " or ".join(error.__name__ for error in expected)
    if outcome.error is None:
        raise ContractViolation(f"expected {names} ({why}), but {outcome.value!r} was returned")
    elif not isinstance(outcome.error, expected):
        raised = f"{type(outcome.error).__name__}({outcome.error})"
        raise ContractViolation(f"expected {names} ({why}), but {raised} was raised")
    else:
        pass
    return outcome.error


def _observation_order(context: _Context) -> Driver[None]:
    request = _probe(context)
    _expect_decision((yield AdmitCall(request)), allowed=True, why="the probe's first attempt")
    admitted = context.injector.points
    _expect_decision((yield AdmitCall(request)), allowed=False, why="the probe holds one")
    denied = context.injector.points[len(admitted) :]
    expected_admitted = (
        ObservationPoint.BEFORE_LOCK,
        ObservationPoint.AFTER_LOAD,
        ObservationPoint.BEFORE_COMMIT,
        ObservationPoint.AFTER_COMMIT,
    )
    expected_denied = (ObservationPoint.BEFORE_LOCK, ObservationPoint.AFTER_LOAD)
    if admitted != expected_admitted or denied != expected_denied:
        raise ContractViolation(
            f"observation points out of order: admitted {[point.value for point in admitted]}, "
            f"denied {[point.value for point in denied]}"
        )
    else:
        pass


def _failure_before_commit(point: ObservationPoint) -> Callable[[_Context], Driver[None]]:
    def driver(context: _Context) -> Driver[None]:
        request = _probe(context)
        injected = ConnectionError(f"injected at {point.value}")
        context.injector.arm(point, injected)
        error = _expect_error(
            (yield AdmitCall(request)),
            (BackendUnavailable,),
            why=f"storage failed at {point.value}",
        )
        if error.__cause__ is not injected:
            raise ContractViolation(
                "BackendUnavailable must keep the driver error as its cause (O6)"
            )
        else:
            pass
        _expect_decision(
            (yield AdmitCall(request)), allowed=True, why="the failure committed nothing"
        )
        _expect_decision((yield AdmitCall(request)), allowed=False, why="the retry was committed")

    return driver


def _failure_after_commit(context: _Context) -> Driver[None]:
    request = _probe(context)
    context.injector.arm(ObservationPoint.AFTER_COMMIT, ConnectionError("response lost"))
    _expect_error(
        (yield AdmitCall(request)),
        (IndeterminateAdmission,),
        why="the commit may have happened, so permission must not be reported (O4)",
    )
    _expect_decision(
        (yield AdmitCall(request)), allowed=False, why="an unknown commit is not refunded"
    )


def _cancellation_before_commit(context: _Context) -> Driver[None]:
    request = _probe(context)
    context.injector.arm(ObservationPoint.BEFORE_COMMIT, asyncio.CancelledError())
    _expect_error(
        (yield AdmitCall(request)),
        (asyncio.CancelledError,),
        why="cancellation propagates unchanged (O5)",
    )
    _expect_decision(
        (yield AdmitCall(request)), allowed=True, why="cancellation before commit consumed nothing"
    )


def _cancellation_after_commit(context: _Context) -> Driver[None]:
    request = _probe(context)
    context.injector.arm(ObservationPoint.AFTER_COMMIT, asyncio.CancelledError())
    _expect_error(
        (yield AdmitCall(request)),
        (asyncio.CancelledError, IndeterminateAdmission),
        why="cancellation after commit is never permission (O5)",
    )
    _expect_decision(
        (yield AdmitCall(request)), allowed=False, why="nothing is refunded after commit"
    )


_LOCK_WAIT_US = 250_000


def _time_sampled_after_lock(context: _Context) -> Driver[None]:
    request = _probe(context)
    start = context.timeline.peek_epoch()

    def queue_for_the_lock(request: AdmissionRequest) -> None:
        del request
        context.timeline.advance(_LOCK_WAIT_US)

    context.injector.arm(ObservationPoint.BEFORE_LOCK, queue_for_the_lock)
    decision = _expect_decision(
        (yield AdmitCall(request)), allowed=True, why="the probe's first attempt"
    )
    assert decision.admission is not None
    if decision.admission.admitted_at != start + _LOCK_WAIT_US:
        raise ContractViolation(
            f"admission stamped {decision.admission.admitted_at}, but the lock was acquired at "
            f"{start + _LOCK_WAIT_US}: time must be sampled after locks are acquired (T4)"
        )
    else:
        pass


def _policy_conflict(context: _Context) -> Driver[None]:
    request = _probe(context)
    conflicting = _probe_constraint(_conflicting_policy(context))
    _expect_decision((yield AdmitCall(request)), allowed=True, why="the probe's first attempt")
    _expect_error(
        (yield AdmitCall(conflicting)),
        (PolicyConflict,),
        why="the same rule under another policy is a conflict (I4)",
    )
    _expect_decision(
        (yield AdmitCall(request)), allowed=False, why="a conflict must not reset the stored state"
    )


def _lifecycle(context: _Context) -> Driver[None]:
    request = _probe(context)
    for attempt in ("first", "second"):
        if (outcome := (yield CloseCall())).error is not None:
            raise ContractViolation(
                f"the {attempt} close raised {outcome.error!r}; closing is idempotent (L1)"
            )
        else:
            pass
    _expect_error((yield AdmitCall(request)), (ClosedResource,), why="admission after close (L5)")
    _expect_error(
        (yield InspectCall(request.rules)), (ClosedResource,), why="inspection after close (L5)"
    )


def _trace_check(trace: Trace) -> _Check:
    def applies(context: _Context) -> str | None:
        capabilities = context.capabilities
        if missing := trace.algorithms - capabilities.algorithms:
            reason: str | None = f"does not implement {sorted(missing)}"
        elif trace.max_rules > 1 and not capabilities.supports_composition:
            reason = "does not support composition"
        elif (
            capabilities.max_composed_rules is not None
            and trace.max_rules > capabilities.max_composed_rules
        ):
            reason = f"composes at most {capabilities.max_composed_rules} rules"
        else:
            reason = None
        return reason

    def driver(context: _Context) -> Driver[None]:
        report = yield from trace_driver(
            trace,
            context.timeline,
            check_retry=Guarantee.EXACT_RETRY_DELAYS in context.guarantees,
            check_timestamps=Guarantee.AUTHORITY_TIMESTAMPS in context.guarantees,
        )
        report.raise_for_mismatches()

    check = _Check(
        f"trace:{trace.name}", frozenset({Guarantee.REFERENCE_DECISIONS}), driver, applies
    )
    return check


def _scenario_checks() -> Iterator[_Check]:
    for scenario in SCENARIOS:
        for expectation in scenario.expectations:
            trace = scenario.trace_for(expectation)
            check = _trace_check(trace)
            yield _Check(f"scenario:{trace.name}", check.requires, check.driver, check.applies)


_OBSERVED = frozenset({Guarantee.OBSERVATION_POINTS})


def _backend_checks() -> tuple[_Check, ...]:
    checks = (
        *(_trace_check(trace) for trace in TRACES),
        *_scenario_checks(),
        _Check("observation_order", _OBSERVED, _observation_order, _needs_probe),
        *(
            _Check(
                f"failure_before_commit:{point.value}",
                _OBSERVED,
                _failure_before_commit(point),
                _needs_probe,
            )
            for point in (
                ObservationPoint.BEFORE_LOCK,
                ObservationPoint.AFTER_LOAD,
                ObservationPoint.BEFORE_COMMIT,
            )
        ),
        _Check("failure_after_commit", _OBSERVED, _failure_after_commit, _needs_probe),
        _Check("cancellation_before_commit", _OBSERVED, _cancellation_before_commit, _needs_probe),
        _Check("cancellation_after_commit", _OBSERVED, _cancellation_after_commit, _needs_probe),
        _Check(
            "time_sampled_after_lock",
            _OBSERVED | {Guarantee.AUTHORITY_TIMESTAMPS},
            _time_sampled_after_lock,
            _needs_probe,
        ),
        _Check(
            "policy_conflict",
            frozenset({Guarantee.POLICY_CONFLICTS}),
            _policy_conflict,
            _needs_conflicting_probe,
        ),
        _Check("lifecycle", frozenset({Guarantee.LIFECYCLE}), _lifecycle, _needs_probe),
    )
    return checks


def _skip_reason(check: _Check, context: _Context) -> str | None:
    if missing := check.requires - context.guarantees:
        reason: str | None = f"does not claim {sorted(guarantee.value for guarantee in missing)}"
    else:
        reason = check.applies(context)
    return reason


def _failure_detail(error: ContractViolation) -> str:
    detail = str(error)
    return detail


def check_backend(case: BackendCase) -> ConformanceReport:
    """Run the conformance suite against a synchronous backend.

    :param case: How to build the backend and what it claims.
    :returns: One result per check.
    """
    results = list()
    for check in _backend_checks():
        timeline = FakeTimeline()
        injector = FaultInjector()
        backend = case.factory(timeline, injector)
        context = _Context(
            timeline,
            injector,
            backend.capabilities,
            case.guarantees,
            case.probe,
            case.conflicting_probe,
        )
        subject = case.subject(backend)
        if (reason := _skip_reason(check, context)) is not None:
            results.append(CheckResult(check.name, CheckStatus.SKIPPED, reason))
        else:
            try:
                pump(check.driver(context), subject)
            except ContractViolation as error:
                results.append(CheckResult(check.name, CheckStatus.FAILED, _failure_detail(error)))
            else:
                results.append(CheckResult(check.name, CheckStatus.PASSED))
        _close_quietly(backend)
    report = ConformanceReport(case.name, tuple(results))
    return report


async def check_async_backend(case: AsyncBackendCase) -> ConformanceReport:
    """Run the conformance suite against an asynchronous backend.

    The same checks as :func:`check_backend`, awaited.

    :param case: How to build the backend and what it claims.
    :returns: One result per check.
    """
    results = list()
    for check in _backend_checks():
        timeline = FakeTimeline()
        injector = FaultInjector()
        backend = case.factory(timeline, injector)
        context = _Context(
            timeline,
            injector,
            backend.capabilities,
            case.guarantees,
            case.probe,
            case.conflicting_probe,
        )
        subject = case.subject(backend)
        if (reason := _skip_reason(check, context)) is not None:
            results.append(CheckResult(check.name, CheckStatus.SKIPPED, reason))
        else:
            try:
                await apump(check.driver(context), subject)
            except ContractViolation as error:
                results.append(CheckResult(check.name, CheckStatus.FAILED, _failure_detail(error)))
            else:
                results.append(CheckResult(check.name, CheckStatus.PASSED))
        await _aclose_quietly(backend)
    report = ConformanceReport(case.name, tuple(results))
    return report


def _close_quietly(backend: SyncBackend) -> None:
    # Teardown after a check; the lifecycle check is where closing is judged.
    with contextlib.suppress(ProcrastinatorsError):
        backend.close()


async def _aclose_quietly(backend: AsyncBackend) -> None:
    with contextlib.suppress(ProcrastinatorsError):
        await backend.aclose()


class _Deterministic:
    """Wraps an algorithm and checks every evaluation gives the same answer twice."""

    def __init__(self, algorithm: Algorithm[Any]) -> None:
        self._algorithm = algorithm

    @property
    def id(self) -> str:
        return self._algorithm.id

    @property
    def state_version(self) -> int:
        return self._algorithm.state_version

    @property
    def codec(self) -> StateCodec[RuleState]:
        return self._algorithm.codec

    def validate(self, policy: object) -> None:
        self._algorithm.validate(policy)

    def requirements(self, policy: object) -> StateRequirements:
        requirements = self._algorithm.requirements(policy)
        return requirements

    def initial_changes(self, policy: object, now: EpochMicros) -> tuple[StateChange, ...]:
        changes = self._algorithm.initial_changes(policy, now)
        return changes

    def evaluate(self, policy: object, state: StateView, now: EpochMicros, cost: int) -> Transition:
        first = self._algorithm.evaluate(policy, state, now, cost)
        second = self._algorithm.evaluate(policy, state, now, cost)
        if first != second:
            raise ContractViolation(
                f"{self.id} evaluated the same inputs twice and disagreed: {first} then {second}"
            )
        else:
            pass
        return first


def check_algorithm(
    algorithm: Algorithm[Any], *, traces: Sequence[Trace] | None = None
) -> ConformanceReport:
    """Run the traces and scenarios for ``algorithm`` on an evaluator host.

    Every evaluation is performed twice and must agree, so an evaluator that
    reads a clock or keeps state on itself fails here.

    :param algorithm: The algorithm to check.
    :param traces: The traces to run; by default every shared trace and
        scenario whose rules use only ``algorithm``.
    :returns: One result per trace.
    """
    if traces is None:
        selected = [trace for trace in TRACES if trace.algorithms == {algorithm.id}]
        selected += [
            scenario.trace_for(expectation)
            for scenario in SCENARIOS
            for expectation in scenario.expectations
            if expectation.policy.algorithm == algorithm.id
        ]
    else:
        selected = list(traces)
    results = list()
    for trace in selected:
        timeline = FakeTimeline()
        host = EvaluatorHost([_Deterministic(algorithm)], clock=timeline.epoch_clock)
        try:
            pump(trace_driver(trace, timeline), BackendSubject(host)).raise_for_mismatches()
        except ContractViolation as error:
            results.append(
                CheckResult(f"trace:{trace.name}", CheckStatus.FAILED, _failure_detail(error))
            )
        else:
            results.append(CheckResult(f"trace:{trace.name}", CheckStatus.PASSED))
    if not selected:
        results.append(
            CheckResult("traces", CheckStatus.SKIPPED, f"no shared traces for {algorithm.id!r}")
        )
    else:
        pass
    report = ConformanceReport(f"algorithm {algorithm.id}", tuple(results))
    return report


if __name__ == "__main__":
    pass
else:
    pass
