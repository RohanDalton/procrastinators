"""The Phase 3 contract checkpoint: the harness passes a correct backend and catches broken ones.

The checkpoint requires the conformance suite to catch doubles that charge on
exit, debit one rule before another denies, or return permission after an
unknown commit. The suite is only evidence if it can fail, so each broken
double below must fail the checks aimed at its flaw — and the unbroken host
must pass every check without skipping any.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
from typing import TYPE_CHECKING

import pytest

from procrastinators.models import DurationMicros
from procrastinators.testing import (
    AsyncBackendCase,
    BackendCase,
    CheckStatus,
    ContractViolation,
    EvaluatorHost,
    FakeTimeline,
    Guarantee,
    check_async_backend,
    check_backend,
)
from tests.doubles import (
    AsyncEvaluatorHost,
    ChargesOnExit,
    ClacksAlgorithm,
    ClacksPolicy,
    ClaimsSuccessAfterUnknownCommit,
    DebitsAsItGoes,
    ExitAwareSubject,
    ForgetsIdleState,
    IgnoresPolicyConflicts,
    RefundsUnknownCommit,
    ReopensAfterClose,
    SamplesTimeBeforeLock,
    TranslatesCancellation,
)
from tests.oracles import all_oracles

if TYPE_CHECKING:
    from procrastinators.protocols import AdmissionObserver, SyncBackend
    from procrastinators.testing import ConformanceReport, TraceSubject
else:
    pass


def _build(host_type: type[EvaluatorHost]) -> BackendCase:
    def factory(timeline: FakeTimeline, observer: AdmissionObserver) -> SyncBackend:
        backend = host_type(all_oracles(), clock=timeline.epoch_clock, observer=observer)
        return backend

    case = BackendCase(host_type.__name__, factory)
    return case


def _exit_aware(backend: SyncBackend) -> TraceSubject:
    assert isinstance(backend, ChargesOnExit)
    subject = ExitAwareSubject(backend)
    return subject


def _charges_on_exit() -> BackendCase:
    def factory(timeline: FakeTimeline, observer: AdmissionObserver) -> SyncBackend:
        backend = ChargesOnExit(all_oracles(), clock=timeline.epoch_clock, observer=observer)
        return backend

    case = BackendCase(
        "ChargesOnExit",
        factory,
        guarantees=frozenset({Guarantee.REFERENCE_DECISIONS, Guarantee.EXACT_RETRY_DELAYS}),
        subject=_exit_aware,
    )
    return case


BROKEN_DOUBLES: dict[str, tuple[BackendCase, frozenset[str]]] = {
    "charges_on_exit": (
        _charges_on_exit(),
        frozenset(
            {
                "trace:sliding_log.long_running",
                "trace:fixed_window.long_running",
                "scenario:ratelimiter.charge_on_exit.sliding_log",
            }
        ),
    ),
    "debits_one_rule_before_another_denies": (
        _build(DebitsAsItGoes),
        frozenset(
            {
                "trace:composition.later_rule_denies",
                "trace:composition.exact_limit_with_pacing",
            }
        ),
    ),
    "claims_success_after_unknown_commit": (
        _build(ClaimsSuccessAfterUnknownCommit),
        frozenset({"failure_after_commit"}),
    ),
    "refunds_unknown_commit": (
        _build(RefundsUnknownCommit),
        frozenset({"failure_after_commit"}),
    ),
    "samples_time_before_lock": (
        _build(SamplesTimeBeforeLock),
        frozenset({"time_sampled_after_lock"}),
    ),
    "translates_cancellation": (
        _build(TranslatesCancellation),
        frozenset({"cancellation_before_commit", "cancellation_after_commit"}),
    ),
    "ignores_policy_conflicts": (
        _build(IgnoresPolicyConflicts),
        frozenset({"policy_conflict"}),
    ),
    "reopens_after_close": (
        _build(ReopensAfterClose),
        frozenset({"lifecycle"}),
    ),
    "forgets_idle_state": (
        _build(ForgetsIdleState),
        frozenset({"trace:token_bucket.drained_bucket_is_not_forgotten"}),
    ),
}


@pytest.fixture(scope="module")
def ankh_morpork_report() -> ConformanceReport:
    """The suite's verdict on the correct host. Built once: the report is immutable."""
    report = check_backend(_build(EvaluatorHost))
    return report


@pytest.fixture(params=list(BROKEN_DOUBLES), scope="module")
def broken_double(request: pytest.FixtureRequest) -> tuple[ConformanceReport, frozenset[str]]:
    case, expected_failures = BROKEN_DOUBLES[request.param]
    report = check_backend(case)
    return report, expected_failures


def test_the_watch_passes_a_correct_backend(ankh_morpork_report: ConformanceReport) -> None:
    """
    Given: The evaluator host running the independent oracles, claiming every guarantee.
    When:  The full conformance suite runs against it.
    Then:  Every check passes and none is skipped.
    """
    ankh_morpork_report.raise_for_failures(allow_skips=False)


def test_the_auditors_catch_each_broken_double(
    broken_double: tuple[ConformanceReport, frozenset[str]],
) -> None:
    """
    Given: A backend with one deliberate flaw.
    When:  The conformance suite runs against it.
    Then:  Every check aimed at that flaw fails.
    """
    report, expected = broken_double
    failed = {result.name for result in report.failures}

    actual = expected - failed

    assert actual == frozenset(), f"not caught: {sorted(actual)}; report: {report.results}"


def test_async_backends_face_the_same_checks() -> None:
    """
    Given: The evaluator host behind the asynchronous protocol.
    When:  The asynchronous suite runs against it.
    Then:  Every check passes and none is skipped: the async suite is the same suite.
    """

    def factory(timeline: FakeTimeline, observer: AdmissionObserver) -> AsyncEvaluatorHost:
        host = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock, observer=observer)
        backend = AsyncEvaluatorHost(host)
        return backend

    report = asyncio.run(check_async_backend(AsyncBackendCase("async host", factory)))

    report.raise_for_failures(allow_skips=False)


def test_the_sync_and_async_suites_run_the_same_checks(
    ankh_morpork_report: ConformanceReport,
) -> None:
    """
    Given: The sync suite's report and an async suite run.
    When:  Their check names are compared.
    Then:  They are identical.
    """

    def factory(timeline: FakeTimeline, observer: AdmissionObserver) -> AsyncEvaluatorHost:
        host = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock, observer=observer)
        return AsyncEvaluatorHost(host)

    async_report = asyncio.run(check_async_backend(AsyncBackendCase("async host", factory)))
    expected = [result.name for result in ankh_morpork_report.results]

    actual = [result.name for result in async_report.results]

    assert actual == expected


def test_unclaimed_guarantees_are_reported_as_skipped_not_passed() -> None:
    """
    Given: A correct backend that claims only reference decisions.
    When:  The suite runs, and skips are then disallowed.
    Then:  Failure-injection checks are skipped with a reason, and a job that forbids
           skips fails rather than passing on the checks that did run.
    """
    case = BackendCase(
        "modest host",
        _build(EvaluatorHost).factory,
        guarantees=frozenset({Guarantee.REFERENCE_DECISIONS}),
    )
    report = check_backend(case)
    expected = CheckStatus.SKIPPED

    actual = report.status_of("failure_after_commit")

    assert actual == expected
    assert report.passed
    with pytest.raises(ContractViolation, match="skipped failure_after_commit"):
        report.raise_for_failures(allow_skips=False)


def test_a_third_party_algorithm_is_checked_with_its_own_probe() -> None:
    """
    Given: A host running only a third-party algorithm, with probe policies of its own.
    When:  The suite runs.
    Then:  The shared traces are skipped as inapplicable, and every failure-injection,
           conflict, and lifecycle check runs and passes against the custom algorithm.
    """

    def factory(timeline: FakeTimeline, observer: AdmissionObserver) -> SyncBackend:
        backend = EvaluatorHost([ClacksAlgorithm()], clock=timeline.epoch_clock, observer=observer)
        return backend

    one_second = DurationMicros(1_000_000)
    case = BackendCase(
        "clacks tower",
        factory,
        probe=ClacksPolicy(1, one_second),
        conflicting_probe=ClacksPolicy(2, one_second),
    )
    report = check_backend(case)
    expected = {
        "observation_order",
        "failure_after_commit",
        "cancellation_before_commit",
        "cancellation_after_commit",
        "time_sampled_after_lock",
        "policy_conflict",
        "lifecycle",
    }

    actual = {result.name for result in report.results if result.status is CheckStatus.PASSED}

    assert expected <= actual
    assert report.passed
    assert all(result.name.startswith(("trace:", "scenario:")) for result in report.skipped)


if __name__ == "__main__":
    pass
else:
    pass
