"""Contract tests for decisions, admissions, and transitions (sections 1, 7)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import Protocol

import pytest

from procrastinators import (
    Admission,
    BackendIdentity,
    Decision,
    InvalidPolicy,
    RemainingEstimate,
    RuleId,
    Snapshot,
)
from procrastinators.models import (
    AppendEvent,
    ClearState,
    DropEventsBefore,
    DurationMicros,
    EpochMicros,
    SetScalar,
    Transition,
)


class AdmissionFactory(Protocol):
    def __call__(self, cost: int = 1) -> Admission: ...


@pytest.fixture
def make_admission(burst_rule: RuleId, memory_identity: BackendIdentity) -> AdmissionFactory:
    def build(cost: int = 1) -> Admission:
        admission = Admission(
            charged=(burst_rule,),
            cost=cost,
            admitted_at=EpochMicros(1_000),
            backend=memory_identity,
        )
        return admission

    return build


@pytest.fixture
def admission(make_admission: AdmissionFactory) -> Admission:
    built = make_admission()
    return built


def test_an_allowed_decision_carries_its_proof_and_blocks_nobody(admission: Admission) -> None:
    """
    Given: An admission.
    When:  An allowing decision is built from it.
    Then:  It is truthy, carries the admission, blocks nothing, and asks for no wait (R1).
    """
    decision = Decision.allow(admission)

    assert decision.allowed
    assert decision
    assert decision.admission is not None
    assert decision.blocking == tuple()
    assert decision.retry_after_us == 0


def test_a_denied_decision_names_who_blocked_it_and_for_how_long(
    burst_rule: RuleId, daily_rule: RuleId
) -> None:
    """
    Given: Two blocking rules and a retry delay.
    When:  A denying decision is built from them.
    Then:  It is falsy, carries no admission, and names the rules and the delay (R2).
    """
    decision = Decision.deny([burst_rule, daily_rule], DurationMicros(250_000))

    assert not decision
    assert decision.admission is None
    assert decision.blocking == (burst_rule, daily_rule)
    assert decision.retry_after_us == 250_000


def test_a_decision_cannot_claim_to_allow_and_block_at_once(
    admission: Admission, burst_rule: RuleId
) -> None:
    """
    Given: An admission and a blocking rule.
    When:  A decision is built that both allows and blocks.
    Then:  InvalidPolicy is raised at construction, not left to reviewers (R2).
    """
    with pytest.raises(InvalidPolicy, match="blocking"):
        Decision(allowed=True, admission=admission, blocking=(burst_rule,))


def test_an_allowed_decision_without_proof_cannot_be_built() -> None:
    """
    Given: No admission.
    When:  An allowing decision is built.
    Then:  InvalidPolicy is raised asking for an Admission.
    """
    with pytest.raises(InvalidPolicy, match="Admission"):
        Decision(allowed=True)


def test_an_allowed_decision_does_not_ask_the_caller_to_wait(admission: Admission) -> None:
    """
    Given: An admission and a retry delay.
    When:  An allowing decision is built with both.
    Then:  InvalidPolicy is raised about the retry.
    """
    with pytest.raises(InvalidPolicy, match="retry"):
        Decision(allowed=True, admission=admission, retry_after_us=DurationMicros(5))


def test_a_denied_decision_must_not_smuggle_an_admission(
    admission: Admission, burst_rule: RuleId
) -> None:
    """
    Given: An admission and a blocking rule.
    When:  A denying decision is built carrying the admission.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy, match="must not carry"):
        Decision(allowed=False, admission=admission, blocking=(burst_rule,))


def test_a_denial_must_say_what_blocked_it() -> None:
    """
    Given: No blocking rules.
    When:  A denying decision is built.
    Then:  InvalidPolicy is raised asking it to name the rules.
    """
    with pytest.raises(InvalidPolicy, match="name the rules"):
        Decision(allowed=False)


def test_a_negative_retry_delay_is_impossible(burst_rule: RuleId) -> None:
    """
    Given: A negative retry delay.
    When:  A denying decision is built with it.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        Decision.deny([burst_rule], DurationMicros(-1))


def test_remaining_counts_are_advisory_extras_not_reservations(
    admission: Admission, burst_rule: RuleId
) -> None:
    """
    Given: An admission and a remaining-quota estimate.
    When:  An allowing decision is built with both.
    Then:  The estimate is carried as an advisory extra (R5).
    """
    expected = (RemainingEstimate(burst_rule, 9),)

    decision = Decision.allow(admission, remaining=[RemainingEstimate(burst_rule, 9)])
    actual = decision.remaining

    assert actual == expected


def test_an_admission_offers_no_way_to_hand_quota_back(admission: Admission) -> None:
    """
    Given: An admission.
    When:  It is searched for release-like methods.
    Then:  There are none, because there is no refund and nothing should suggest one (A3).
    """
    for forbidden in ("release", "refund", "cancel", "rollback", "__exit__"):
        assert not hasattr(admission, forbidden), forbidden


def test_two_admissions_are_distinguishable(make_admission: AdmissionFactory) -> None:
    """
    Given: Two admissions built identically.
    When:  Their ids are compared.
    Then:  They differ: each acquisition is its own proof, with no shared mutable result (R6).
    """
    assert make_admission().admission_id != make_admission().admission_id


def test_an_admission_must_record_what_it_charged(memory_identity: BackendIdentity) -> None:
    """
    Given: No charged rules.
    When:  An admission is built.
    Then:  InvalidPolicy is raised asking for a charged rule.
    """
    with pytest.raises(InvalidPolicy, match="charged rule"):
        Admission(charged=(), cost=1, admitted_at=EpochMicros(1), backend=memory_identity)


def test_a_snapshot_says_out_loud_that_it_is_advisory(memory_identity: BackendIdentity) -> None:
    """
    Given: A snapshot.
    When:  Its advisory flag and methods are inspected.
    Then:  It is marked advisory and offers no way to acquire, commit, or reserve (R7).
    """
    snapshot = Snapshot(rules=(), sampled_at=EpochMicros(5), backend=memory_identity)

    assert snapshot.advisory is True
    for forbidden in ("acquire", "commit", "reserve"):
        assert not hasattr(snapshot, forbidden), forbidden


def test_a_denied_transition_may_tidy_up_but_may_not_charge(burst_rule: RuleId) -> None:
    """
    Given: Denying transitions with pruning changes and with charging changes.
    When:  They are constructed.
    Then:  Pruning is accepted and charging is refused: denial can prune obsolete
           state and nothing else (R8, A5).
    """
    Transition(rule=burst_rule, admitted=False, changes=(DropEventsBefore(EpochMicros(10)),))
    Transition(rule=burst_rule, admitted=False, changes=(ClearState(),))
    Transition(rule=burst_rule, admitted=False, changes=(SetScalar("count", 3, prunes_only=True),))

    with pytest.raises(InvalidPolicy, match="must not consume quota"):
        Transition(rule=burst_rule, admitted=False, changes=(AppendEvent(EpochMicros(10), 1),))
    with pytest.raises(InvalidPolicy, match="must not consume quota"):
        Transition(rule=burst_rule, admitted=False, changes=(SetScalar("count", 3),))


def test_an_admitting_transition_does_not_also_ask_for_a_retry(burst_rule: RuleId) -> None:
    """
    Given: A retry delay.
    When:  An admitting transition is built with it.
    Then:  InvalidPolicy is raised about the retry.
    """
    with pytest.raises(InvalidPolicy, match="retry"):
        Transition(rule=burst_rule, admitted=True, retry_after_us=DurationMicros(1))


def test_a_transition_cannot_commit_anything_itself(burst_rule: RuleId) -> None:
    """
    Given: An admitting transition with a change.
    When:  It is searched for commit-like methods.
    Then:  There are none: an evaluator proposes and only the backend commits (R8).
    """
    transition = Transition(
        rule=burst_rule, admitted=True, changes=(AppendEvent(EpochMicros(10), 1),)
    )

    for forbidden in ("commit", "apply", "save", "write"):
        assert not hasattr(transition, forbidden), forbidden


def test_one_weighted_operation_is_one_stored_event() -> None:
    """
    Given: An operation of cost 50.
    When:  It is recorded as an appended event.
    Then:  One event carries the whole cost rather than fifty entries (P6).
    """
    expected = 50

    actual = AppendEvent(EpochMicros(10), 50).cost

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
