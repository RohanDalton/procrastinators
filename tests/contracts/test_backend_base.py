"""The backend base classes enforce lifecycle and capability rules (sections 11, 15)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

import pytest

from procrastinators import (
    AdmissionRequest,
    Algorithms,
    BackendIdentity,
    Capabilities,
    ClosedResource,
    Constraint,
    CoordinationScope,
    Decision,
    Durability,
    Ownership,
    RuleId,
    SlidingLogPolicy,
    Snapshot,
    UnsupportedCapability,
)
from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.models import DurationMicros, PolicyFingerprint

if TYPE_CHECKING:
    from collections.abc import Sequence
else:
    pass

FAST = SlidingLogPolicy(10, DurationMicros(1_000_000))


class Igor(BaseSyncBackend):
    """A backend that does as it is told and counts how often it is closed."""

    def __init__(self, **capability_overrides: object) -> None:
        self.closes = 0
        self._overrides = capability_overrides

    @property
    def capabilities(self) -> Capabilities:
        defaults: dict[str, object] = {
            "algorithms": frozenset({Algorithms.SLIDING_LOG}),
            "coordination": CoordinationScope.IN_PROCESS,
            "durability": Durability.EPHEMERAL,
        }
        capabilities = Capabilities(**{**defaults, **self._overrides})  # ty: ignore[invalid-argument-type]
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        identity = BackendIdentity("igor", "uberwald", "discworld")
        return identity

    def admit(self, request: AdmissionRequest) -> Decision:
        raise AssertionError("not reached in these tests")

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        raise AssertionError("not reached in these tests")

    def close(self) -> None:
        if self._mark_closed():
            self.closes += 1
        else:
            pass


class Forgetful(BaseSyncBackend):
    """A backend that implements none of the abstract methods."""


@pytest.fixture
def igor() -> Igor:
    backend = Igor()
    return backend


@pytest.fixture(scope="session")
def burst_constraint(burst_rule: RuleId) -> Constraint:
    constraint = Constraint(burst_rule, FAST, PolicyFingerprint("v1"))
    return constraint


@pytest.fixture(scope="session")
def daily_constraint(daily_rule: RuleId) -> Constraint:
    constraint = Constraint(daily_rule, FAST, PolicyFingerprint("v1"))
    return constraint


@pytest.fixture(scope="session")
def composed_request(
    burst_constraint: Constraint, daily_constraint: Constraint
) -> AdmissionRequest:
    request = AdmissionRequest(constraints=(burst_constraint, daily_constraint))
    return request


def test_closing_twice_is_not_an_error_but_only_releases_once(igor: Igor) -> None:
    """
    Given: An open backend.
    When:  It is closed three times.
    Then:  It is closed, and its resources are released exactly once (L1).
    """
    expected = 1

    igor.close()
    igor.close()
    igor.close()
    actual = igor.closes

    assert actual == expected
    assert igor.closed


def test_using_a_closed_backend_fails_loudly_rather_than_reopening(igor: Igor) -> None:
    """
    Given: A backend that has been closed.
    When:  It is checked for being open before use.
    Then:  ClosedResource is raised rather than the backend reopening (L5).
    """
    igor.close()

    with pytest.raises(ClosedResource, match="closed"):
        igor._ensure_open()


def test_an_open_backend_passes_the_open_check(igor: Igor) -> None:
    """
    Given: A backend that has not been closed.
    When:  It is checked for being open before use.
    Then:  No error is raised and it still reports itself open.
    """
    igor._ensure_open()

    assert not igor.closed


def test_a_backend_owns_what_it_made_unless_it_says_otherwise(igor: Igor) -> None:
    """
    Given: A backend constructed without any ownership declaration.
    When:  Its client ownership is read.
    Then:  It owns the client (L2).
    """
    assert igor.ownership.client is Ownership.OWNED


def test_a_backend_used_as_a_context_manager_closes_on_exit(igor: Igor) -> None:
    """
    Given: An open backend.
    When:  It is used as a context manager.
    Then:  It is open inside the block and closed on exit, unlike a limiter context,
           which must not close storage (L6).
    """
    with igor:
        assert not igor.closed

    assert igor.closed


def test_an_unsupported_algorithm_is_refused_before_any_storage_work(
    burst_constraint: Constraint,
) -> None:
    """
    Given: A backend that supports only fixed windows.
    When:  A sliding-log request is validated.
    Then:  UnsupportedCapability is raised before any storage work (Y2).
    """
    backend = Igor(algorithms=frozenset({Algorithms.FIXED_WINDOW}))

    with pytest.raises(UnsupportedCapability, match="sliding_log"):
        backend.validate_request(AdmissionRequest(constraints=(burst_constraint,)))


def test_a_backend_without_composition_will_not_charge_rules_one_at_a_time(
    igor: Igor, composed_request: AdmissionRequest
) -> None:
    """
    Given: A backend without composition support.
    When:  A request naming two rules is validated.
    Then:  It is refused, because the alternative to atomic composition is refusal,
           not approximation (C1).
    """
    with pytest.raises(UnsupportedCapability, match="atomically"):
        igor.validate_request(composed_request)


def test_a_composition_limit_is_enforced(composed_request: AdmissionRequest) -> None:
    """
    Given: A backend that composes at most one rule.
    When:  A request naming two rules is validated.
    Then:  UnsupportedCapability is raised naming the limit.
    """
    backend = Igor(supports_composition=True, max_composed_rules=1)

    with pytest.raises(UnsupportedCapability, match="at most 1"):
        backend.validate_request(composed_request)


def test_a_cluster_backend_demands_a_declared_coordination_domain(
    composed_request: AdmissionRequest,
    burst_constraint: Constraint,
    daily_constraint: Constraint,
) -> None:
    """
    Given: A composing backend that requires a shared coordination domain.
    When:  Requests with and without a declared domain are validated.
    Then:  The undeclared one is refused and the declared one passes (C4, I6).
    """
    backend = Igor(supports_composition=True, requires_shared_coordination_domain=True)

    with pytest.raises(UnsupportedCapability, match="coordination domain"):
        backend.validate_request(composed_request)

    slotted = tuple(
        Constraint(
            original.rule, original.policy, original.fingerprint, coordination_domain="slot-7"
        )
        for original in (burst_constraint, daily_constraint)
    )
    backend.validate_request(AdmissionRequest(constraints=slotted))


def test_a_valid_request_passes_validation_quietly(composed_request: AdmissionRequest) -> None:
    """
    Given: A backend that supports composition.
    When:  A request naming two rules is validated.
    Then:  Validation completes without raising.
    """
    Igor(supports_composition=True).validate_request(composed_request)


def test_the_base_classes_hand_out_no_admission_of_their_own() -> None:
    """
    Given: The sync and async backend base classes.
    When:  Their abstract methods are listed.
    Then:  admit, inspect, and the close method remain abstract, because atomicity
           cannot be inherited: a concrete ``admit`` here would be a non-atomic
           read-modify-write that every subclass would silently advertise as atomic.
    """
    for base in (BaseSyncBackend, BaseAsyncBackend):
        assert "admit" in base.__abstractmethods__
        assert "inspect" in base.__abstractmethods__
    assert "close" in BaseSyncBackend.__abstractmethods__
    assert "aclose" in BaseAsyncBackend.__abstractmethods__


def test_an_incomplete_backend_cannot_be_instantiated() -> None:
    """
    Given: A backend subclass that implements none of the abstract methods.
    When:  It is instantiated.
    Then:  TypeError is raised naming the abstract methods.
    """
    with pytest.raises(TypeError, match="abstract"):
        Forgetful()  # ty: ignore[call-non-callable]


if __name__ == "__main__":
    pass
else:
    pass
