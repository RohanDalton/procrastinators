"""How waiting treats storage failures, unknown commits, conflicts, and diagnostics.

Scripted backends replay chosen outcomes, so each test states exactly what the
authority did and checks what the limiter made of it: definitely uncommitted
failures are retried within budget (B4), unknown commits are surfaced and never
retried or refunded (O4), and a failing diagnostics callback changes nothing
(D2).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import logging

import pytest

from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    IndeterminateAdmission,
    PolicyConflict,
)
from procrastinators.models import (
    BackendFailureEvent,
    DiagnosticEvent,
    DurationMicros,
    PolicyConflictEvent,
    PolicyFingerprint,
)
from procrastinators.testing import AsyncScriptedBackend, ScriptedBackend, allow, deny
from tests.limiters import Rig


def _scripted(rig: Rig, *steps: object) -> ScriptedBackend:
    backend = ScriptedBackend(steps, clock=rig.timeline.epoch_clock)  # ty: ignore[invalid-argument-type]
    return backend


def test_contention_is_retried_with_backoff_then_admitted(rig: Rig) -> None:
    """
    Given: A backend that is busy twice, then admits.
    When:  The limiter acquires.
    Then:  It backs off one then two milliseconds and is admitted; each failure was
           reported as a diagnostic, never as a denial (O2, B4).
    """
    backend = _scripted(rig, BackendBusy("locked"), BackendBusy("locked"), allow())
    limiter = rig.limiter(backend=backend)

    admission = limiter.acquire()

    assert admission.cost == 1
    assert rig.sleeper.sleeps == (1_000, 2_000)
    assert rig.event_types() == ["BackendFailureEvent", "BackendFailureEvent", "AdmittedEvent"]
    backend.assert_exhausted()


def test_unavailability_beyond_the_retry_budget_is_raised(rig: Rig) -> None:
    """
    Given: A backend that never answers.
    When:  The limiter acquires with the default three contention retries.
    Then:  After three backoffs the last ``BackendUnavailable`` is raised: never
           ``AcquireTimeout``, which would mean the quota said no (B5).
    """
    failures = [BackendUnavailable(f"refused {attempt}") for attempt in range(4)]
    limiter = rig.limiter(backend=_scripted(rig, *failures))

    with pytest.raises(BackendUnavailable, match="refused 3"):
        limiter.acquire()

    assert rig.sleeper.sleeps == (1_000, 2_000, 4_000)


def test_a_single_attempt_does_not_retry_contention(rig: Rig) -> None:
    """
    Given: A busy backend.
    When:  ``try_acquire`` is called.
    Then:  ``BackendBusy`` is raised at once: one attempt means one attempt (B2).
    """
    limiter = rig.limiter(backend=_scripted(rig, BackendBusy("locked"), allow()))

    with pytest.raises(BackendBusy):
        limiter.try_acquire()

    assert rig.sleeper.sleeps == tuple()


def test_a_deadline_shorter_than_the_backoff_stops_retrying(rig: Rig) -> None:
    """
    Given: A busy backend and a half-millisecond timeout.
    When:  The limiter acquires.
    Then:  The one-millisecond backoff does not fit, so ``BackendBusy`` is raised
           without sleeping.
    """
    limiter = rig.limiter(backend=_scripted(rig, BackendBusy("locked"), allow()))

    with pytest.raises(BackendBusy):
        limiter.acquire(timeout=0.0005)

    assert rig.sleeper.sleeps == tuple()


def test_a_positive_deadline_caps_each_attempts_storage_and_lock_budgets(rig: Rig) -> None:
    """
    Given: A scripted backend that denies once, then admits, and a limiter with a
           two-second timeout and five-second storage calls.
    When:  It acquires, sleeping half a second after the denial.
    Then:  The first attempt's budgets were capped at two seconds and the second's at
           the one and a half remaining: the deadline covers storage work too (B2).
    """
    backend = _scripted(rig, deny(500_000), allow())
    limiter = rig.limiter(backend=backend, timeout=2)

    limiter.acquire()
    budgets = [
        (request.budget.storage_timeout_us, request.budget.lock_timeout_us)
        for request in backend.requests
    ]

    assert budgets == [(2_000_000, 2_000_000), (1_500_000, 1_500_000)]


def test_rincewind_never_runs_the_body_after_an_unknown_commit(rig: Rig) -> None:
    """
    Given: A backend whose response was lost after the commit may have happened.
    When:  A decorated function is called.
    Then:  ``IndeterminateAdmission`` is raised without retrying, the body never ran,
           and the failure was reported as indeterminate (O4).
    """
    backend = _scripted(rig, IndeterminateAdmission("response lost"), allow())
    limiter = rig.limiter(backend=backend)
    ran = list()

    @limiter
    def body() -> None:
        ran.append(True)

    with pytest.raises(IndeterminateAdmission):
        body()

    assert ran == list()
    assert backend.remaining_steps == 1
    (event,) = rig.events
    assert isinstance(event, BackendFailureEvent)
    assert event.indeterminate


def test_a_policy_conflict_is_reported_and_propagates(rig: Rig) -> None:
    """
    Given: A backend reporting that the stored policy differs from this worker's.
    When:  The limiter acquires.
    Then:  ``PolicyConflict`` propagates unretried and a ``PolicyConflictEvent`` names
           both fingerprints.
    """
    limiter = rig.limiter(
        backend=_scripted(
            rig,
            PolicyConflict(
                "stored under another policy",
                rule=rig.limiter().rules[0],
                expected=PolicyFingerprint("p1-mine"),
                found=PolicyFingerprint("p1-theirs"),
            ),
        )
    )

    with pytest.raises(PolicyConflict):
        limiter.acquire()

    (event,) = rig.events
    assert isinstance(event, PolicyConflictEvent)
    assert (event.expected, event.found) == ("p1-mine", "p1-theirs")


def test_a_raising_diagnostics_callback_cannot_undo_an_admission(
    rig: Rig, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Given: A diagnostics callback that raises on every event.
    When:  The limiter acquires, is denied, and waits.
    Then:  The admission is still returned, and each failure was logged rather than
           turning a committed admission into a reported failure (D2).
    """

    def broken(event: DiagnosticEvent) -> None:
        raise RuntimeError(f"metrics exporter is down: {type(event).__name__}")

    limiter = rig.limiter(on_event=broken, backend=_scripted(rig, deny(10), allow()))

    with caplog.at_level(logging.WARNING, logger="procrastinators.waiting"):
        admission = limiter.acquire()

    assert admission.cost == 1
    assert len(caplog.records) == 3


def test_a_foreign_exception_from_a_third_party_backend_propagates_unchanged(rig: Rig) -> None:
    """
    Given: A third-party backend that leaks a raw driver error.
    When:  The limiter acquires.
    Then:  The error propagates as it was, unretried: only library errors can be
           known to be uncommitted.
    """
    limiter = rig.limiter(backend=_scripted(rig, ConnectionResetError("peer"), allow()))

    with pytest.raises(ConnectionResetError):
        limiter.acquire()

    assert rig.sleeper.sleeps == tuple()


def test_cancellation_from_an_async_backend_propagates_unchanged(rig: Rig) -> None:
    """
    Given: An async backend whose admission is cancelled mid-flight.
    When:  The limiter acquires asynchronously.
    Then:  ``CancelledError`` propagates as it is, never translated into a quota or
           storage error (O5), and nothing is retried.
    """
    backend = AsyncScriptedBackend(
        [asyncio.CancelledError(), allow()], clock=rig.timeline.epoch_clock
    )
    limiter = rig.limiter(backend=backend)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(limiter.acquire_async())

    assert backend.remaining_steps == 1


def test_async_waiting_retries_contention_like_sync_waiting(rig: Rig) -> None:
    """
    Given: An async backend that is busy once, denies once, then admits.
    When:  The limiter acquires asynchronously.
    Then:  The async sleeper recorded the same backoff and policy delay a sync
           caller would have: both run one plan.
    """
    backend = AsyncScriptedBackend(
        [BackendBusy("locked"), deny(250_000), allow()], clock=rig.timeline.epoch_clock
    )
    limiter = rig.limiter(backend=backend)

    admission = asyncio.run(limiter.acquire_async())

    assert admission.cost == 1
    assert rig.async_sleeper.sleeps == (DurationMicros(1_000), DurationMicros(250_000))


if __name__ == "__main__":
    pass
else:
    pass
