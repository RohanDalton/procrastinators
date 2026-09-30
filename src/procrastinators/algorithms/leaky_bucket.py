"""Leaky bucket: pace admissions at ``amount`` per ``period``, with no idle credit by default.

Contract E4, a GCRA. State is one scalar, the theoretical arrival time ``tat``:
the instant the schedule next has room. An operation may start when it is no
more than ``tolerance`` ahead of that schedule, and an admitted cost ``c``
pushes the schedule ``interval(c) = ceil(c * period / amount)`` past
``max(tat, now)``.

Weighting and tolerance are independent (P4). ``burst_tolerance`` says how
many cost units may run ahead of the pace; it does not cap the size of one
operation, which may be up to ``amount`` and pays by pushing ``tat`` further
out. Each interval rounds up while the tolerance rounds down, so the pace is
never faster than configured (T6).

This is what separates it from a token bucket (P7): an idle token bucket
banks capacity, while an idle leaky bucket with zero tolerance still spaces
the next operations one interval apart.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Final

from procrastinators.algorithms.reference import ReferenceAlgorithm, ceil_div
from procrastinators.errors import InvalidPolicy
from procrastinators.models import (
    MAX_TIMESTAMP_US,
    DurationMicros,
    EpochMicros,
    LeakyBucketPolicy,
    SetScalar,
)
from procrastinators.protocols import StateRequirements

if TYPE_CHECKING:
    from procrastinators.models import Transition
    from procrastinators.protocols import StateView
else:
    pass

__all__ = ["LeakyBucket"]

_TAT: Final = "tat"
_REQUIREMENTS: Final = StateRequirements(scalars=frozenset({_TAT}))


class LeakyBucket(ReferenceAlgorithm[LeakyBucketPolicy]):
    """The reference leaky-bucket evaluator.

    State may be forgotten once ``tat`` has passed (L9): a schedule already in
    the past behaves exactly like a never-used rule, whose ``tat`` is ``now``.
    """

    policy_type = LeakyBucketPolicy

    def requirements(self, policy: LeakyBucketPolicy) -> StateRequirements:
        """The theoretical arrival time; the same for every policy.

        :param policy: A validated policy. Unused.
        """
        del policy
        return _REQUIREMENTS

    @staticmethod
    def interval(policy: LeakyBucketPolicy, cost: int) -> DurationMicros:
        """How far an admission of ``cost`` pushes the schedule, rounded up.

        :param policy: A validated policy.
        :param cost: The admitted cost.
        """
        interval = DurationMicros(ceil_div(cost * policy.period_us, policy.amount))
        return interval

    @staticmethod
    def tolerance(policy: LeakyBucketPolicy) -> DurationMicros:
        """How far ahead of the schedule an operation may start, rounded down.

        :param policy: A validated policy.
        """
        tolerance = DurationMicros(policy.burst_tolerance * policy.period_us // policy.amount)
        return tolerance

    def evaluate(
        self, policy: LeakyBucketPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> Transition:
        """Admit when the schedule is no more than the tolerance ahead of ``now``.

        A denial waits until it is (E4) and changes nothing.

        :param policy: A validated policy.
        :param state: The rule's theoretical arrival time.
        :param now: Authority epoch time.
        :param cost: The requested cost, within capacity.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` exceeds ``amount``.
        :raises ~procrastinators.errors.InvalidPolicy: Admitting would schedule the rule past
            :data:`~procrastinators.models.MAX_TIMESTAMP_US`; out-of-range values are
            rejected, never saturated (N2).
        """
        self.check_cost(policy, cost)
        tat = state.scalar(_TAT) if state.exists else now
        tolerance = self.tolerance(policy)
        if tat - now <= tolerance:
            scheduled = EpochMicros(max(tat, now) + self.interval(policy, cost))
            if scheduled > MAX_TIMESTAMP_US:
                raise InvalidPolicy(
                    f"admitting cost {cost} would schedule {state.rule} at {scheduled} µs, "
                    f"beyond the supported timestamp range ending at {MAX_TIMESTAMP_US} (N2)"
                )
            else:
                pass
            transition = self.admit(
                state.rule, (SetScalar(_TAT, scheduled),), safe_forget_after_us=scheduled
            )
        else:
            transition = self.deny(
                state.rule,
                DurationMicros(tat - tolerance - now),
                safe_forget_after_us=EpochMicros(tat),
            )
        return transition


if __name__ == "__main__":
    pass
else:
    pass
