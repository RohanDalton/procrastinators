"""Contract tests for the outcome taxonomy (section 6)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio

import pytest

from procrastinators import errors
from procrastinators.errors import (
    AcquireTimeout,
    BackendBusy,
    BackendError,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    IndeterminateAdmission,
    InvalidCost,
    InvalidIdentity,
    InvalidPolicy,
    PolicyConflict,
    ProcrastinatorsError,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.models import DurationMicros, PolicyFingerprint, RuleId

EVERY_ERROR = (
    AcquireTimeout,
    BackendBusy,
    BackendError,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    IndeterminateAdmission,
    InvalidCost,
    InvalidIdentity,
    InvalidPolicy,
    PolicyConflict,
    StateCorruption,
    UnsupportedCapability,
)


@pytest.fixture(params=EVERY_ERROR, ids=[error.__name__ for error in EVERY_ERROR])
def error_class(request: pytest.FixtureRequest) -> type[Exception]:
    error: type[Exception] = request.param
    return error


@pytest.fixture(
    params=(*EVERY_ERROR, ProcrastinatorsError),
    ids=[error.__name__ for error in (*EVERY_ERROR, ProcrastinatorsError)],
)
def any_library_error(request: pytest.FixtureRequest) -> type[Exception]:
    error: type[Exception] = request.param
    return error


def test_every_outcome_is_catchable_as_one_family(error_class: type[Exception]) -> None:
    """
    Given: Any specific outcome raised by the library.
    When:  Its ancestry is checked.
    Then:  It is a ProcrastinatorsError (O1).
    """
    assert issubclass(error_class, ProcrastinatorsError)


def test_death_does_not_come_for_the_cancelled(any_library_error: type[Exception]) -> None:
    """
    Given: Any library error, including the family base.
    When:  It is compared with asyncio.CancelledError.
    Then:  Neither is a subclass of the other, and the error is an ordinary
           Exception, since cancellation must never be catchable as quota exhaustion (O5).
    """
    assert not issubclass(any_library_error, asyncio.CancelledError)
    assert not issubclass(asyncio.CancelledError, any_library_error)
    assert issubclass(any_library_error, Exception)


def test_the_familiar_builtin_is_also_the_right_one() -> None:
    """
    Given: The input-validation and timeout outcomes.
    When:  Their ancestry is checked.
    Then:  Callers validating input can catch ValueError and waiters TimeoutError.
    """
    assert issubclass(InvalidCost, ValueError)
    assert issubclass(InvalidPolicy, ValueError)
    assert issubclass(AcquireTimeout, TimeoutError)


def test_contention_and_unavailability_are_not_the_same_answer() -> None:
    """
    Given: The busy and unavailable backend outcomes.
    When:  Their ancestry is checked.
    Then:  Both are backend errors but neither is the other, so a busy database
           does not read as a strict vendor (O2).
    """
    assert not issubclass(BackendBusy, BackendUnavailable)
    assert not issubclass(BackendUnavailable, BackendBusy)
    assert issubclass(BackendBusy, BackendError)
    assert issubclass(BackendUnavailable, BackendError)


def test_an_unknown_commit_is_its_own_outcome() -> None:
    """
    Given: The indeterminate admission outcome.
    When:  Its ancestry is checked.
    Then:  It is a backend error, not a timeout: neither a denial nor a plain failure (O4).
    """
    assert issubclass(IndeterminateAdmission, BackendError)
    assert not issubclass(IndeterminateAdmission, AcquireTimeout)


def test_wrapping_a_driver_error_keeps_the_cause() -> None:
    """
    Given: A driver error.
    When:  It is wrapped via the ``cause`` keyword.
    Then:  It becomes ``__cause__``, so a raise site cannot forget ``from`` (O6).
    """
    driver = OSError("the Luggage ate the socket")

    wrapped = BackendUnavailable("cannot reach the store", cause=driver)

    assert wrapped.__cause__ is driver


def test_an_unknown_commit_says_what_might_have_been_charged(burst_rule: RuleId) -> None:
    """
    Given: A lost reply while admitting a cost against a rule.
    When:  An indeterminate admission is raised for it.
    Then:  It carries the cost, the rules as a tuple, and the driver cause.
    """
    driver = TimeoutError("no reply")

    error = IndeterminateAdmission("lost the reply", cause=driver, cost=3, rules=[burst_rule])

    assert error.cost == 3
    assert error.rules == (burst_rule,)
    assert error.__cause__ is driver


def test_a_timeout_reports_what_it_waited_for(burst_rule: RuleId) -> None:
    """
    Given: A wait that gave up while a rule was blocking.
    When:  An acquire timeout is raised for it.
    Then:  It names the blocking rules and how long it waited, since nothing was
           granted and the caller can say what blocked it (B5).
    """
    error = AcquireTimeout(
        "gave up",
        cost=1,
        waited_us=DurationMicros(30_000_000),
        blocking=[burst_rule],
        retry_after_us=DurationMicros(500_000),
    )

    assert error.blocking == (burst_rule,)
    assert error.waited_us == 30_000_000


def test_a_policy_conflict_reports_both_fingerprints(burst_rule: RuleId) -> None:
    """
    Given: Two policies with different fingerprints for one rule.
    When:  A policy conflict is raised for them.
    Then:  It reports both, since an operator needs to know which two disagreed (I4).
    """
    expected = ("old", "new")

    error = PolicyConflict(
        "disagreement",
        rule=burst_rule,
        expected=PolicyFingerprint("old"),
        found=PolicyFingerprint("new"),
    )
    actual = (error.expected, error.found)

    assert actual == expected


def test_no_error_carries_shared_mutable_state() -> None:
    """
    Given: The errors module.
    When:  Its public module-level containers are collected.
    Then:  There are none, so there is no 'last failure' for callers to race on (O7).
    """
    expected: dict[str, object] = dict()

    actual = {
        name: value
        for name, value in vars(errors).items()
        if not name.startswith("_") and isinstance(value, (list, dict, set))
    }

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
