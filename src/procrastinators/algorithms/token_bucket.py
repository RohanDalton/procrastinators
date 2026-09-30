"""Token bucket: refill ``refill_amount`` every ``refill_period``, capped at ``capacity``.

Contract E3. State is constant: a token balance and the anchor its refills are
counted from. A rule's first attempt records ``initial_tokens`` at that
attempt's time, admitted or not (P5); otherwise an initially empty bucket would
restart its refill clock on every denied retry and never fill.

A full bucket accrues no further credit, so its anchor moves to ``now``. Refill
is whole ``refill_amount`` steps only: a partial period contributes nothing,
which never rounds replenishment up (T6).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Final

from procrastinators.algorithms.reference import ReferenceAlgorithm, ceil_div
from procrastinators.models import DurationMicros, EpochMicros, SetScalar, TokenBucketPolicy
from procrastinators.protocols import StateRequirements

if TYPE_CHECKING:
    from procrastinators.models import StateChange, Transition
    from procrastinators.protocols import StateView
else:
    pass

__all__ = ["TokenBucket"]

_TOKENS: Final = "tokens"
_ANCHOR: Final = "anchor"
_REQUIREMENTS: Final = StateRequirements(scalars=frozenset({_TOKENS, _ANCHOR}))


class TokenBucket(ReferenceAlgorithm[TokenBucketPolicy]):
    """The reference token-bucket evaluator.

    State may be forgotten once the bucket is full again, but only when the
    policy also *starts* full (L9). A bucket configured to start with fewer
    tokens is never forgotten: re-creating it would hand back its configured
    initial balance rather than the full bucket it had refilled to, and P5
    allows forgetting only state indistinguishable from the initial state.
    """

    policy_type = TokenBucketPolicy

    def requirements(self, policy: TokenBucketPolicy) -> StateRequirements:
        """The balance and its refill anchor; the same for every policy.

        :param policy: A validated policy. Unused.
        """
        del policy
        return _REQUIREMENTS

    def initial_changes(
        self, policy: TokenBucketPolicy, now: EpochMicros
    ) -> tuple[StateChange, ...]:
        """Start with ``initial_tokens`` and count refills from the first attempt.

        :param policy: A validated policy.
        :param now: Authority epoch time of the rule's first attempt.
        """
        changes = (SetScalar(_TOKENS, policy.starting_tokens), SetScalar(_ANCHOR, now))
        return changes

    def evaluate(
        self, policy: TokenBucketPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> Transition:
        """Refill, then admit when ``cost`` tokens are present.

        A denial waits for the whole refill periods that supply the missing
        tokens (E3) and changes nothing.

        :param policy: A validated policy.
        :param state: The rule's balance and anchor, including initial state.
        :param now: Authority epoch time.
        :param cost: The requested cost, within capacity.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` exceeds ``capacity``.
        """
        self.check_cost(policy, cost)
        tokens, anchor = self.refilled(policy, state, now)
        if cost <= tokens:
            transition = self.admit(
                state.rule,
                (SetScalar(_TOKENS, tokens - cost), SetScalar(_ANCHOR, anchor)),
                safe_forget_after_us=self._full_at(policy, tokens - cost, anchor),
                remaining=tokens - cost,
            )
        else:
            periods = ceil_div(cost - tokens, policy.refill_amount)
            transition = self.deny(
                state.rule,
                DurationMicros(anchor + periods * policy.refill_period_us - now),
                safe_forget_after_us=self._full_at(policy, tokens, anchor),
                remaining=tokens,
            )
        return transition

    @staticmethod
    def refilled(
        policy: TokenBucketPolicy, state: StateView, now: EpochMicros
    ) -> tuple[int, EpochMicros]:
        """The balance and anchor after the refills due by ``now``.

        Elapsed time is clamped at zero, so an authority clock that stepped
        backwards refills nothing rather than draining the bucket (T7).
        Refills are counted only up to the number that fill the bucket, which
        keeps every intermediate value within the exact-integer range however
        long the bucket sat idle.

        :param policy: A validated policy.
        :param state: The rule's balance and anchor.
        :param now: Authority epoch time.
        :returns: ``(tokens, anchor)``.
        """
        tokens, anchor = state.scalar(_TOKENS), state.scalar(_ANCHOR)
        refills = max(0, now - anchor) // policy.refill_period_us
        if refills >= ceil_div(policy.capacity - tokens, policy.refill_amount):
            refilled = (policy.capacity, EpochMicros(max(anchor, now)))
        else:
            refilled = (
                tokens + refills * policy.refill_amount,
                EpochMicros(anchor + refills * policy.refill_period_us),
            )
        return refilled

    @staticmethod
    def _full_at(policy: TokenBucketPolicy, tokens: int, anchor: int) -> EpochMicros | None:
        if policy.starting_tokens < policy.capacity:
            full_at = None
        else:
            periods = ceil_div(policy.capacity - tokens, policy.refill_amount)
            full_at = EpochMicros(anchor + periods * policy.refill_period_us)
        return full_at


if __name__ == "__main__":
    pass
else:
    pass
