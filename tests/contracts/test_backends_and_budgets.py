"""Contract tests for backend identity, capabilities, and budgets (sections 8, 15)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import copy

import pytest

from procrastinators import (
    Algorithms,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Durability,
    InvalidPolicy,
    OperationBudget,
    Ownership,
)
from procrastinators.models import DurationMicros, MonotonicMicros, ResourceOwnership

UNUSABLE_BUDGETS: dict[str, dict[str, object]] = {
    "zero storage timeout": {"storage_timeout_us": DurationMicros(0)},
    "negative lock timeout": {"lock_timeout_us": DurationMicros(-1)},
    "negative retries": {"max_contention_retries": -1},
    "excessive retries": {"max_contention_retries": 1000},
}


@pytest.fixture(params=list(UNUSABLE_BUDGETS.values()), ids=list(UNUSABLE_BUDGETS))
def unusable_budget(request: pytest.FixtureRequest) -> dict[str, object]:
    budget: dict[str, object] = copy.deepcopy(request.param)
    return budget


def test_two_handles_on_one_store_recognize_each_other(memory_identity: BackendIdentity) -> None:
    """
    Given: Backend identities differing by namespace, and by authority.
    When:  They are compared for addressing the same authority.
    Then:  A different namespace shares the authority but is a distinct identity, and a
           different authority does not, since composition across separate
           authorities is impossible and so must be detected (C6).
    """
    other_namespace = BackendIdentity("memory", "unseen-university", "roundworld")
    elsewhere = BackendIdentity("memory", "brazeneck", "discworld")

    assert memory_identity.addresses_same_authority(other_namespace)
    assert not memory_identity.addresses_same_authority(elsewhere)
    assert other_namespace != memory_identity


def test_a_backend_authority_may_not_carry_a_password() -> None:
    """
    Given: A backend authority string containing credentials.
    When:  A backend identity is built from it.
    Then:  InvalidPolicy is raised, because the authority is printed in diagnostics
           and explain() (G9, D3).
    """
    with pytest.raises(InvalidPolicy, match="credentials"):
        BackendIdentity("redis", "rincewind:luggage@localhost:6379/0", "discworld")


def test_injected_clients_default_to_being_closed_by_whoever_made_them() -> None:
    """
    Given: Resource ownership built with defaults, and with a borrowed client.
    When:  The ownership of each resource is read.
    Then:  Unspecified resources are owned and the borrowed client is borrowed, so
           this library closes what it created and leaves borrowed clients alone (L2).
    """
    borrowed = ResourceOwnership(client=Ownership.BORROWED)

    assert ResourceOwnership().client is Ownership.OWNED
    assert borrowed.client is Ownership.BORROWED
    assert borrowed.executor is Ownership.OWNED


def test_capabilities_state_what_a_backend_actually_does() -> None:
    """
    Given: Capabilities declaring async support and nothing about composition.
    When:  The supported modes are read.
    Then:  Sync defaults on, async is on as declared, and composition defaults off.
    """
    capabilities = Capabilities(
        algorithms=frozenset({Algorithms.SLIDING_LOG}),
        coordination=CoordinationScope.IN_PROCESS,
        durability=Durability.EPHEMERAL,
        supports_async=True,
    )

    assert capabilities.supports_sync
    assert capabilities.supports_async
    assert not capabilities.supports_composition


def test_a_backend_that_does_nothing_cannot_be_described() -> None:
    """
    Given: Capabilities with no algorithms, or with neither sync nor async support.
    When:  They are constructed.
    Then:  InvalidPolicy is raised for each.
    """
    with pytest.raises(InvalidPolicy, match="at least one algorithm"):
        Capabilities(
            algorithms=frozenset(),
            coordination=CoordinationScope.IN_PROCESS,
            durability=Durability.EPHEMERAL,
        )
    with pytest.raises(InvalidPolicy, match="sync or async"):
        Capabilities(
            algorithms=frozenset({Algorithms.SLIDING_LOG}),
            coordination=CoordinationScope.IN_PROCESS,
            durability=Durability.EPHEMERAL,
            supports_sync=False,
            supports_async=False,
        )


def test_a_native_executor_cannot_claim_an_algorithm_the_backend_lacks() -> None:
    """
    Given: Capabilities naming a native executor for an unsupported algorithm.
    When:  They are constructed.
    Then:  InvalidPolicy is raised (Y5).
    """
    with pytest.raises(InvalidPolicy, match="native executors"):
        Capabilities(
            algorithms=frozenset({Algorithms.SLIDING_LOG}),
            coordination=CoordinationScope.SHARED_SERVICE,
            durability=Durability.SERVICE_DURABLE,
            native_executors=frozenset({Algorithms.TOKEN_BUCKET}),
        )


def test_the_default_budget_bounds_storage_even_with_no_deadline() -> None:
    """
    Given: A default operation budget.
    When:  Its limits are read.
    Then:  It has no deadline and waits for quota, yet storage and lock timeouts stay
           bounded: an unbounded quota wait does not license an unbounded round trip (B3).
    """
    budget = OperationBudget()

    assert budget.deadline_us is None
    assert budget.wait_for_quota
    assert budget.storage_timeout_us > 0
    assert budget.lock_timeout_us > 0


def test_a_single_attempt_is_expressed_by_declining_to_wait() -> None:
    """
    Given: A budget that declines to wait for quota.
    When:  Its limits are read.
    Then:  It does not wait, and its storage timeout is still positive, because
           timeout=0 means one attempt, not a zero-length storage timeout (B2).
    """
    budget = OperationBudget(wait_for_quota=False)

    assert not budget.wait_for_quota
    assert budget.storage_timeout_us > 0


def test_a_deadline_is_local_monotonic_time() -> None:
    """
    Given: A budget built with a monotonic deadline.
    When:  Its deadline is read.
    Then:  It is the monotonic value given (T2, B1).
    """
    expected = 123

    actual = OperationBudget(deadline_us=MonotonicMicros(123)).deadline_us

    assert actual == expected


def test_an_unusable_budget_is_refused(unusable_budget: dict[str, object]) -> None:
    """
    Given: Budget settings with a non-positive timeout or an out-of-range retry count.
    When:  An operation budget is built from them.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        OperationBudget(**unusable_budget)  # ty: ignore[invalid-argument-type]


if __name__ == "__main__":
    pass
else:
    pass
