"""Deliberately naive models of the five algorithms, for checking the traces.

These are not the reference algorithms in ``procrastinators.algorithms`` and
must never be imported from ``src/``; they exist so the reference algorithms have
something independent to agree with. They answer "would this cost be admitted at time
``t``, if nothing else happened?" as literally as possible, and find a retry
delay by *searching* forward in time for the first microsecond that answer is
yes. The hand-computed retry delays in the traces use closed forms; agreement
between the two is the evidence that the traces say what the contract says.

The Auditors of Reality insist on every rule being followed exactly, however
tediously. These oracles are written in that spirit.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from procrastinators.errors import InvalidPolicy
from procrastinators.models import (
    MAX_DURATION_US,
    Algorithms,
    AppendEvent,
    DropEventsBefore,
    DurationMicros,
    EpochMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    SetScalar,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
    Transition,
)
from procrastinators.protocols import EventWindow, StateRepresentation, StateRequirements
from procrastinators.state import RuleStateCodec

if TYPE_CHECKING:
    from collections.abc import Callable

    from procrastinators.models import StateChange
    from procrastinators.protocols import Algorithm, StateView
else:
    pass

PolicyT = TypeVar("PolicyT")


def earliest_delay(admits_after: Callable[[int], bool]) -> DurationMicros:
    """The smallest delay ``d >= 1`` for which ``admits_after(d)`` holds.

    Exponential then binary search, relying only on the answer being monotone
    in time when nothing else happens — which holds for all five policies.
    """
    high = 1
    while not admits_after(high):
        if high >= MAX_DURATION_US:
            raise AssertionError("no admission within the supported duration range")
        else:
            high = min(high * 2, MAX_DURATION_US)
    low = high // 2 + 1 if high > 1 else 1
    while low < high:
        middle = (low + high) // 2
        if admits_after(middle):
            high = middle
        else:
            low = middle + 1
    return DurationMicros(low)


class Oracle(ABC, Generic[PolicyT]):
    """Shared shape: admit if :meth:`admits` now; otherwise search for the delay."""

    id: ClassVar[str]
    policy_type: ClassVar[type]
    state_version: ClassVar[int] = 1
    scalars: ClassVar[frozenset[str]] = frozenset()

    @property
    def codec(self) -> RuleStateCodec:
        codec = RuleStateCodec(self.state_version)
        return codec

    def validate(self, policy: PolicyT) -> None:
        if not isinstance(policy, self.policy_type):
            raise InvalidPolicy(f"{self.id} needs a {self.policy_type.__name__}")
        else:
            pass

    def requirements(self, policy: PolicyT) -> StateRequirements:
        del policy
        requirements = StateRequirements(scalars=self.scalars)
        return requirements

    def initial_changes(self, policy: PolicyT, now: EpochMicros) -> tuple[StateChange, ...]:
        del policy, now
        return tuple()

    @abstractmethod
    def admits(self, policy: PolicyT, state: StateView, at: int, cost: int) -> bool:
        """Whether ``cost`` would be admitted at ``at`` given only ``state``."""

    @abstractmethod
    def charge(
        self, policy: PolicyT, state: StateView, now: EpochMicros, cost: int
    ) -> tuple[StateChange, ...]:
        """The changes recording an admission of ``cost`` at ``now``."""

    def evaluate(
        self, policy: PolicyT, state: StateView, now: EpochMicros, cost: int
    ) -> Transition:
        if self.admits(policy, state, now, cost):
            transition = Transition(state.rule, True, self.charge(policy, state, now, cost))
        else:

            def admits_after(delay: int) -> bool:
                admitted = self.admits(policy, state, now + delay, cost)
                return admitted

            transition = Transition(state.rule, False, retry_after_us=earliest_delay(admits_after))
        return transition


class FixedWindowOracle(Oracle[FixedWindowPolicy]):
    id = Algorithms.FIXED_WINDOW
    policy_type = FixedWindowPolicy
    scalars = frozenset({"window", "count"})

    @staticmethod
    def window_of(policy: FixedWindowPolicy, at: int) -> int:
        offset = policy.epoch_offset_us
        start = (at - offset) // policy.period_us * policy.period_us + offset
        return start

    def used(self, policy: FixedWindowPolicy, state: StateView, at: int) -> int:
        in_window = state.exists and state.scalar("window") == self.window_of(policy, at)
        used = state.scalar("count") if in_window else 0
        return used

    def admits(self, policy: FixedWindowPolicy, state: StateView, at: int, cost: int) -> bool:
        admitted = self.used(policy, state, at) + cost <= policy.amount
        return admitted

    def charge(
        self, policy: FixedWindowPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> tuple[StateChange, ...]:
        changes = (
            SetScalar("window", self.window_of(policy, now)),
            SetScalar("count", self.used(policy, state, now) + cost),
        )
        return changes


class SlidingLogOracle(Oracle[SlidingLogPolicy]):
    id = Algorithms.SLIDING_LOG
    policy_type = SlidingLogPolicy

    def requirements(self, policy: SlidingLogPolicy) -> StateRequirements:
        requirements = StateRequirements(
            representation=StateRepresentation.EVENT_LOG,
            events=EventWindow(policy.period_us, max_events=policy.amount),
        )
        return requirements

    def admits(self, policy: SlidingLogPolicy, state: StateView, at: int, cost: int) -> bool:
        live = sum(event.cost for event in state.events() if event.at > at - policy.period_us)
        admitted = not state.truncated and live + cost <= policy.amount
        return admitted

    def charge(
        self, policy: SlidingLogPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> tuple[StateChange, ...]:
        del state
        changes = (
            AppendEvent(now, cost),
            DropEventsBefore(EpochMicros(now - policy.period_us)),
        )
        return changes


class TokenBucketOracle(Oracle[TokenBucketPolicy]):
    id = Algorithms.TOKEN_BUCKET
    policy_type = TokenBucketPolicy
    scalars = frozenset({"tokens", "anchor"})

    def initial_changes(
        self, policy: TokenBucketPolicy, now: EpochMicros
    ) -> tuple[StateChange, ...]:
        changes = (SetScalar("tokens", policy.starting_tokens), SetScalar("anchor", now))
        return changes

    @staticmethod
    def refilled(policy: TokenBucketPolicy, state: StateView, at: int) -> tuple[int, int]:
        tokens, anchor = state.scalar("tokens"), state.scalar("anchor")
        refills = (at - anchor) // policy.refill_period_us
        tokens += refills * policy.refill_amount
        if tokens >= policy.capacity:
            refilled = (policy.capacity, at)
        else:
            refilled = (tokens, anchor + refills * policy.refill_period_us)
        return refilled

    def admits(self, policy: TokenBucketPolicy, state: StateView, at: int, cost: int) -> bool:
        tokens, _ = self.refilled(policy, state, at)
        admitted = cost <= tokens
        return admitted

    def charge(
        self, policy: TokenBucketPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> tuple[StateChange, ...]:
        tokens, anchor = self.refilled(policy, state, now)
        changes = (SetScalar("tokens", tokens - cost), SetScalar("anchor", anchor))
        return changes


class LeakyBucketOracle(Oracle[LeakyBucketPolicy]):
    id = Algorithms.LEAKY_BUCKET
    policy_type = LeakyBucketPolicy
    scalars = frozenset({"tat"})

    @staticmethod
    def next_eligible(state: StateView, at: int) -> int:
        tat = state.scalar("tat") if state.exists else at
        return tat

    def admits(self, policy: LeakyBucketPolicy, state: StateView, at: int, cost: int) -> bool:
        del cost
        tolerance = policy.burst_tolerance * policy.period_us // policy.amount
        admitted = self.next_eligible(state, at) - at <= tolerance
        return admitted

    def charge(
        self, policy: LeakyBucketPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> tuple[StateChange, ...]:
        interval = -(-cost * policy.period_us // policy.amount)
        changes = (SetScalar("tat", max(self.next_eligible(state, now), now) + interval),)
        return changes


class SlidingCounterOracle(Oracle[SlidingCounterPolicy]):
    id = Algorithms.SLIDING_COUNTER
    policy_type = SlidingCounterPolicy
    scalars = frozenset({"window", "current", "previous"})

    @staticmethod
    def counts(policy: SlidingCounterPolicy, state: StateView, at: int) -> tuple[int, int, int]:
        start = at // policy.period_us * policy.period_us
        stored = state.scalar("window")
        if state.exists and stored == start:
            previous, current = state.scalar("previous"), state.scalar("current")
        elif state.exists and stored == start - policy.period_us:
            previous, current = state.scalar("current"), 0
        else:
            previous, current = 0, 0
        counts = (start, previous, current)
        return counts

    def admits(self, policy: SlidingCounterPolicy, state: StateView, at: int, cost: int) -> bool:
        start, previous, current = self.counts(policy, state, at)
        weighted = previous * (policy.period_us - (at - start)) // policy.period_us
        admitted = weighted + current + cost <= policy.amount
        return admitted

    def charge(
        self, policy: SlidingCounterPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> tuple[StateChange, ...]:
        start, previous, current = self.counts(policy, state, now)
        changes = (
            SetScalar("window", start),
            SetScalar("previous", previous),
            SetScalar("current", current + cost),
        )
        return changes


def all_oracles() -> tuple[Algorithm[Any], ...]:
    """A fresh instance of every oracle."""
    oracles: tuple[Algorithm[Any], ...] = (
        FixedWindowOracle(),
        SlidingLogOracle(),
        TokenBucketOracle(),
        LeakyBucketOracle(),
        SlidingCounterOracle(),
    )
    return oracles


if __name__ == "__main__":
    pass
else:
    pass
