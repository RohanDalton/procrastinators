"""A third party can implement an algorithm and a backend from the protocols alone.

The acceptance criterion for Phase 2: the doubles below satisfy the published
protocols *without importing a single concrete built-in* — no reference
algorithm, no shipped backend, no codec. If that ever stops being true, the
extension points have quietly become decoration around a closed implementation.

The Hogfather delivers on a strict schedule and does not negotiate, which makes
him a reasonable stand-in for a rate limiter.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import pytest

from procrastinators import (
    Admission,
    AdmissionRequest,
    BackendIdentity,
    Capabilities,
    ClosedResource,
    Constraint,
    CoordinationScope,
    Decision,
    Durability,
    EventWindow,
    LogEvent,
    QuotaIdentity,
    RuleId,
    RuleSnapshot,
    RuleState,
    Snapshot,
    StateRepresentation,
    StateRequirements,
    StateView,
    SyncBackend,
    Transition,
)
from procrastinators.models import (
    AppendEvent,
    DurationMicros,
    EpochMicros,
    PolicyFingerprint,
    StateChange,
)
from procrastinators.protocols import Algorithm, StateCodec

if TYPE_CHECKING:
    from collections.abc import Sequence
else:
    pass


@dataclass(frozen=True, slots=True)
class TeatimePolicy:
    """At most ``gifts`` deliveries per night. Not one of the built-in five."""

    gifts: int
    night_us: DurationMicros

    algorithm: ClassVar[str] = "hogswatch.teatime"
    state_version: ClassVar[int] = 3

    @property
    def capacity(self) -> int:
        return self.gifts


class TeatimeCodec:
    version = 3
    max_encoded_size = 4096

    def encode(self, state: RuleState) -> bytes:
        payload = repr((state.scalars, state.events, state.exists)).encode()
        return payload

    def decode(self, payload: bytes) -> RuleState:
        del payload
        state = RuleState()
        return state


class Teatime:
    """A minimal sliding-log-ish evaluator written against the protocol only."""

    id = "hogswatch.teatime"
    state_version = 3

    @property
    def codec(self) -> StateCodec[RuleState]:
        codec = TeatimeCodec()
        return codec

    def validate(self, policy: TeatimePolicy) -> None:
        if policy.gifts < 1:
            raise ValueError("the Hogfather delivers at least one gift")
        else:
            pass

    def requirements(self, policy: TeatimePolicy) -> StateRequirements:
        requirements = StateRequirements(
            representation=StateRepresentation.EVENT_LOG,
            events=EventWindow(policy.night_us, max_events=policy.gifts),
        )
        return requirements

    def initial_changes(self, policy: TeatimePolicy, now: EpochMicros) -> tuple[StateChange, ...]:
        del policy, now
        changes: tuple[StateChange, ...] = tuple()
        return changes

    def evaluate(
        self,
        policy: TeatimePolicy,
        state: StateView,
        now: EpochMicros,
        cost: int,
    ) -> Transition:
        live = [event for event in state.events() if event.at > now - policy.night_us]
        spent = sum(event.cost for event in live)
        if spent + cost > policy.gifts:
            transition = Transition(
                rule=state.rule,
                admitted=False,
                retry_after_us=DurationMicros(live[0].at + policy.night_us - now),
            )
        else:
            transition = Transition(
                rule=state.rule,
                admitted=True,
                changes=(AppendEvent(now, cost),),
                remaining=policy.gifts - spent - cost,
            )
        return transition


@dataclass(frozen=True, slots=True)
class ListView:
    rule: RuleId
    stored: tuple[LogEvent, ...] = tuple()
    exists: bool = True
    truncated: bool = False

    def scalar(self, name: str, default: int = 0) -> int:
        del name
        return default

    def events(self) -> Sequence[LogEvent]:
        return self.stored


class Sleigh:
    """A backend double. Not atomic, and it says so: this is a protocol check."""

    def __init__(self) -> None:
        self._events: dict[RuleId, list[LogEvent]] = dict()
        self._clock = EpochMicros(1_000_000)
        self._closed = False
        self.algorithm = Teatime()

    @property
    def capabilities(self) -> Capabilities:
        capabilities = Capabilities(
            algorithms=frozenset({self.algorithm.id}),
            coordination=CoordinationScope.IN_PROCESS,
            durability=Durability.EPHEMERAL,
            state_representations=frozenset({StateRepresentation.EVENT_LOG}),
        )
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        identity = BackendIdentity("sleigh", "hogswatch-night", "discworld")
        return identity

    def admit(self, request: AdmissionRequest) -> Decision:
        if self._closed:
            raise ClosedResource("the sleigh has gone")
        else:
            pass
        constraint = request.constraints[0]
        policy = constraint.policy
        assert isinstance(policy, TeatimePolicy)
        view = ListView(constraint.rule, tuple(self._events.get(constraint.rule, ())))
        transition = self.algorithm.evaluate(policy, view, self._clock, request.cost)
        if not transition.admitted:
            decision = Decision.deny([constraint.rule], transition.retry_after_us)
        else:
            for change in transition.changes:
                assert isinstance(change, AppendEvent)
                self._events.setdefault(constraint.rule, []).append(
                    LogEvent(change.at, change.cost)
                )
            decision = Decision.allow(
                Admission(
                    charged=request.rules,
                    cost=request.cost,
                    admitted_at=self._clock,
                    backend=self.identity,
                )
            )
        return decision

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        snapshot = Snapshot(
            rules=tuple(RuleSnapshot(rule, self.algorithm.id) for rule in rules),
            sampled_at=self._clock,
            backend=self.identity,
        )
        return snapshot

    def close(self) -> None:
        self._closed = True


POLICY = TeatimePolicy(gifts=2, night_us=DurationMicros(1_000_000))

REPRESENTATIONS_BY_EVENT_SUPPORT = {
    True: frozenset({StateRepresentation.EVENT_LOG}),
    False: frozenset({StateRepresentation.SCALARS}),
}


@pytest.fixture(scope="session")
def deliveries_rule() -> RuleId:
    rule = RuleId(QuotaIdentity("discworld", "hogswatch"), "deliveries")
    return rule


@pytest.fixture(scope="session")
def teatime_constraint(deliveries_rule: RuleId) -> Constraint:
    constraint = Constraint(deliveries_rule, POLICY, PolicyFingerprint("v3"))
    return constraint


@pytest.fixture
def sleigh() -> Sleigh:
    backend = Sleigh()
    return backend


@pytest.fixture(
    params=list(REPRESENTATIONS_BY_EVENT_SUPPORT),
    ids=["events-supported", "events-unsupported"],
)
def backend_supports_events(request: pytest.FixtureRequest) -> bool:
    supports_events: bool = request.param
    return supports_events


def test_a_stranger_may_write_an_algorithm() -> None:
    """
    Given: An algorithm written only against the published protocol.
    When:  It is checked against the Algorithm protocol.
    Then:  It satisfies it.
    """
    assert isinstance(Teatime(), Algorithm)


def test_a_stranger_may_write_a_backend(sleigh: Sleigh) -> None:
    """
    Given: A backend written only against the published protocol.
    When:  It is checked against the SyncBackend protocol.
    Then:  It satisfies it.
    """
    assert isinstance(sleigh, SyncBackend)


def test_a_stranger_may_write_a_state_view_and_a_codec(deliveries_rule: RuleId) -> None:
    """
    Given: A state view and a codec written only against the published protocols.
    When:  They are checked against StateView and StateCodec.
    Then:  Each satisfies its protocol.
    """
    assert isinstance(ListView(deliveries_rule), StateView)
    assert isinstance(TeatimeCodec(), StateCodec)


def test_a_custom_policy_needs_no_blessing_from_the_built_in_union(
    teatime_constraint: Constraint,
) -> None:
    """
    Given: A constraint built around a third-party policy.
    When:  Its algorithm, state version, and capacity are read.
    Then:  They come from the custom policy on its own terms.
    """
    assert teatime_constraint.algorithm == "hogswatch.teatime"
    assert teatime_constraint.state_version == 3
    assert teatime_constraint.capacity == 2


def test_the_custom_pieces_actually_work_together(
    sleigh: Sleigh, teatime_constraint: Constraint
) -> None:
    """
    Given: The custom backend and a two-gift policy.
    When:  Three single-gift admissions are requested.
    Then:  The first two are allowed and the third is denied, naming the rule and a
           positive retry delay.
    """
    request = AdmissionRequest(constraints=(teatime_constraint,))

    assert sleigh.admit(request).allowed
    assert sleigh.admit(request).allowed

    denied = sleigh.admit(request)
    assert not denied.allowed
    assert denied.blocking == (teatime_constraint.rule,)
    assert denied.retry_after_us > 0


def test_this_module_imports_no_concrete_implementation() -> None:
    """
    Given: This module's own source.
    When:  Its imports are read from the syntax tree rather than its text, so the
           check cannot be satisfied or broken by what a docstring mentions.
    Then:  It imports protocols and models only, never a concrete built-in.
    """
    expected: set[str] = set()

    tree = ast.parse(Path(__file__).read_text())
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (module := node.module):
            imported.add(module)
            imported.update(f"{module}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        else:
            pass

    actual = {
        name
        for name in imported
        if name.startswith(("procrastinators.algorithms", "procrastinators.backends"))
        or name.rpartition(".")[2]
        in {
            "SlidingLogPolicy",
            "FixedWindowPolicy",
            "TokenBucketPolicy",
            "LeakyBucketPolicy",
            "SlidingCounterPolicy",
        }
    }

    assert actual == expected


def test_an_evaluator_is_pure(deliveries_rule: RuleId) -> None:
    """
    Given: A custom evaluator and an empty state view.
    When:  A transition is evaluated.
    Then:  The view is unchanged and the transition offers no way to commit, apply,
           sleep, or lock: evaluation proposes and nothing more (A1, R8).
    """
    view = ListView(deliveries_rule)
    expected = view.events()

    transition = Teatime().evaluate(POLICY, view, EpochMicros(5_000_000), 1)
    actual = view.events()

    assert actual == expected
    assert isinstance(transition, Transition)
    for forbidden in ("commit", "apply", "sleep", "lock"):
        assert not hasattr(transition, forbidden), forbidden


def test_a_backend_that_cannot_hold_a_log_says_so_up_front(backend_supports_events: bool) -> None:
    """
    Given: Backend capabilities with or without event-log state.
    When:  The custom algorithm's required representation is looked up in them.
    Then:  It is found only when the backend supports events, which is why
           StateRepresentation is negotiated rather than assumed (Y4).
    """
    expected = backend_supports_events

    capabilities = Capabilities(
        algorithms=frozenset({"hogswatch.teatime"}),
        coordination=CoordinationScope.SHARED_SERVICE,
        durability=Durability.BEST_EFFORT,
        state_representations=REPRESENTATIONS_BY_EVENT_SUPPORT[backend_supports_events],
    )
    needed = Teatime().requirements(POLICY).representation
    actual = needed in capabilities.state_representations

    assert actual is expected


if __name__ == "__main__":
    pass
else:
    pass
