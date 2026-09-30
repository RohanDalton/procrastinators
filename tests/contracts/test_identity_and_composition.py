"""Contract tests for identity and composition (sections 5, 9)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import pytest

from procrastinators import (
    Constraint,
    InvalidCost,
    InvalidPolicy,
    PolicyConflict,
    QuotaIdentity,
    RuleId,
    SlidingLogPolicy,
)
from procrastinators.models import (
    AdmissionRequest,
    DurationMicros,
    PolicyFingerprint,
    canonical_constraints,
)

FAST = SlidingLogPolicy(10, DurationMicros(1_000_000))
SLOW = SlidingLogPolicy(500, DurationMicros(60_000_000))
PRINT = PolicyFingerprint("fingerprint-a")

UNUSABLE_IDENTITIES = {
    "empty namespace": ("", "k"),
    "empty key": ("n", ""),
    "nul in key": ("n", "a\x00b"),
    "newline in namespace": ("n\n", "k"),
}

MALFORMED_COSTS: dict[str, object] = {
    "zero": 0,
    "negative": -1,
    "fractional": 1.5,
    "boolean": True,
    "string": "2",
}


def make_constraint(scope: QuotaIdentity, name: str, policy: SlidingLogPolicy = FAST) -> Constraint:
    constraint = Constraint(RuleId(scope, name), policy, PRINT)
    return constraint


@pytest.fixture(scope="session")
def burst(ankh: QuotaIdentity) -> Constraint:
    constraint = make_constraint(ankh, "burst")
    return constraint


@pytest.fixture(scope="session")
def daily(ankh: QuotaIdentity) -> Constraint:
    constraint = make_constraint(ankh, "daily", SLOW)
    return constraint


@pytest.fixture(scope="session")
def quirm_burst(quirm: QuotaIdentity) -> Constraint:
    constraint = make_constraint(quirm, "burst")
    return constraint


@pytest.fixture(params=list(UNUSABLE_IDENTITIES.values()), ids=list(UNUSABLE_IDENTITIES))
def unusable_identity(request: pytest.FixtureRequest) -> tuple[str, str]:
    namespace_and_key: tuple[str, str] = request.param
    return namespace_and_key


@pytest.fixture(params=list(MALFORMED_COSTS.values()), ids=list(MALFORMED_COSTS))
def malformed_cost(request: pytest.FixtureRequest) -> object:
    cost: object = request.param
    return cost


def test_a_quota_identity_is_a_namespace_and_a_key(ankh: QuotaIdentity) -> None:
    """
    Given: A quota identity with a namespace and a key.
    When:  It is rendered as a string.
    Then:  It reads as namespace/key.
    """
    expected = "discworld/ankh-morpork"

    actual = str(ankh)

    assert actual == expected


def test_an_unusable_identity_is_refused(unusable_identity: tuple[str, str]) -> None:
    """
    Given: A namespace or key that is empty or contains control characters.
    When:  A quota identity is built from it.
    Then:  InvalidPolicy is raised.
    """
    namespace, key = unusable_identity

    with pytest.raises(InvalidPolicy):
        QuotaIdentity(namespace, key)


def test_positional_rule_names_follow_list_order(ankh: QuotaIdentity) -> None:
    """
    Given: Rules from the simple list API and a named rule.
    When:  Their names and positional status are read.
    Then:  Positional rules are numbered by list position, so reordering them is a
           conflict, and named rules are not positional (I3).
    """
    expected = "#0"

    actual = RuleId.positional(ankh, 0).name

    assert actual == expected
    assert RuleId.positional(ankh, 1).is_positional
    assert not RuleId(ankh, "burst").is_positional


def test_the_positional_prefix_is_reserved(ankh: QuotaIdentity) -> None:
    """
    Given: A rule name starting with the positional prefix.
    When:  A named rule is built from it.
    Then:  InvalidPolicy is raised because the prefix is reserved.
    """
    with pytest.raises(InvalidPolicy, match="reserved"):
        RuleId(ankh, "#burst")


def test_naming_one_quota_twice_charges_it_once(burst: Constraint) -> None:
    """
    Given: The same constraint named three times.
    When:  The constraints are canonicalized.
    Then:  Identical constraints are deduplicated to one (C2).
    """
    expected = (burst,)

    actual = canonical_constraints([burst, burst, burst])

    assert actual == expected


def test_the_auditors_reject_conflicting_policies_for_one_rule(ankh: QuotaIdentity) -> None:
    """
    Given: Two constraints on one rule with different policies.
    When:  The constraints are canonicalized.
    Then:  PolicyConflict is raised naming that rule (C3).
    """
    expected = RuleId(ankh, "burst")

    with pytest.raises(PolicyConflict) as caught:
        canonical_constraints(
            [make_constraint(ankh, "burst", FAST), make_constraint(ankh, "burst", SLOW)]
        )
    actual = caught.value.rule

    assert actual == expected


def test_constraints_are_ordered_independently_of_how_they_were_passed(
    burst: Constraint, daily: Constraint, quirm_burst: Constraint
) -> None:
    """
    Given: The same constraints passed in two different orders.
    When:  Each ordering is canonicalized.
    Then:  Both yield the same stable order, which is what lets backends take locks
           without deadlocking (C5).
    """
    expected = (burst, daily, quirm_burst)

    reversed_order = canonical_constraints([quirm_burst, daily, burst])
    forward_order = canonical_constraints([burst, daily, quirm_burst])

    assert reversed_order == forward_order == expected


def test_composition_across_coordination_domains_is_refused(
    burst_rule: RuleId, quirm: QuotaIdentity
) -> None:
    """
    Given: Two constraints in different coordination domains.
    When:  They are canonicalized together.
    Then:  PolicyConflict is raised, since no single atomic admission can span two
           hash slots (C4).
    """
    here = Constraint(burst_rule, FAST, PRINT, coordination_domain="slot-7")
    there = Constraint(RuleId(quirm, "burst"), FAST, PRINT, coordination_domain="slot-9")

    with pytest.raises(PolicyConflict, match="coordination domain"):
        canonical_constraints([here, there])


def test_a_request_needs_something_to_check() -> None:
    """
    Given: No constraints at all.
    When:  They are canonicalized.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        canonical_constraints(list())


def test_a_request_canonicalizes_whatever_it_is_given(burst: Constraint, daily: Constraint) -> None:
    """
    Given: Constraints out of order and with a duplicate.
    When:  An admission request is built from them.
    Then:  Its constraints and rules are deduplicated and in canonical order.
    """
    expected_constraints = (burst, daily)
    expected_rules = (burst.rule, daily.rule)

    request = AdmissionRequest(constraints=(daily, burst, burst))

    assert request.constraints == expected_constraints
    assert request.rules == expected_rules


def test_a_malformed_cost_is_refused(malformed_cost: object, burst: Constraint) -> None:
    """
    Given: A cost that is not a positive integer.
    When:  An admission request is built with it.
    Then:  InvalidCost is raised.
    """
    with pytest.raises(InvalidCost):
        AdmissionRequest(constraints=(burst,), cost=malformed_cost)  # ty: ignore[invalid-argument-type]


def test_an_impossible_cost_is_an_error_rather_than_an_endless_wait(burst: Constraint) -> None:
    """
    Given: A cost larger than the rule's capacity.
    When:  An admission request is built with it.
    Then:  InvalidCost is raised, because denying it would imply an unbounded retry
           delay (P3).
    """
    with pytest.raises(InvalidCost, match="can never be admitted"):
        AdmissionRequest(constraints=(burst,), cost=11)


if __name__ == "__main__":
    pass
else:
    pass
