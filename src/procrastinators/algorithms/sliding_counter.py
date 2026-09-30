"""Sliding counter: an approximate rolling limit from two epoch-aligned counters.

Contract E5. Admission requires
``previous * (period - elapsed) // period + current + cost <= amount``, where
``elapsed`` is how far ``now`` is into its epoch-aligned window. The counts roll
forward lazily: one window on, the current count becomes the previous one; two
windows on, both are zero.

The weighting assumes the previous window's admissions were spread evenly, so
this policy does *not* promise a strict rolling bound (P4) and must be chosen
explicitly. The policy model bounds ``amount * period`` so the product above is
exact in every executor (N3).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Final

from procrastinators.algorithms.reference import ReferenceAlgorithm
from procrastinators.models import DurationMicros, EpochMicros, SetScalar, SlidingCounterPolicy
from procrastinators.protocols import StateRequirements

if TYPE_CHECKING:
    from procrastinators.models import Transition
    from procrastinators.protocols import StateView
else:
    pass

__all__ = ["SlidingCounter"]

_WINDOW: Final = "window"
_CURRENT: Final = "current"
_PREVIOUS: Final = "previous"
_REQUIREMENTS: Final = StateRequirements(scalars=frozenset({_WINDOW, _CURRENT, _PREVIOUS}))


class SlidingCounter(ReferenceAlgorithm[SlidingCounterPolicy]):
    """The reference sliding-counter evaluator.

    State may be forgotten two periods after the start of the window it counts
    (L9), when both counts have rolled to zero.
    """

    policy_type = SlidingCounterPolicy

    def requirements(self, policy: SlidingCounterPolicy) -> StateRequirements:
        """The window start and its current and previous counts.

        :param policy: A validated policy. Unused.
        """
        del policy
        return _REQUIREMENTS

    @staticmethod
    def counts(
        policy: SlidingCounterPolicy, state: StateView, now: EpochMicros
    ) -> tuple[EpochMicros, int, int]:
        """The window containing ``now`` and its rolled-forward counts.

        :param policy: A validated policy.
        :param state: The rule's stored window and counts.
        :param now: Authority epoch time.
        :returns: ``(start, previous, current)``.
        """
        period = policy.period_us
        start = EpochMicros(now // period * period)
        stored = state.scalar(_WINDOW)
        if state.exists and stored == start:
            previous, current = state.scalar(_PREVIOUS), state.scalar(_CURRENT)
        elif state.exists and stored == start - period:
            previous, current = state.scalar(_CURRENT), 0
        else:
            previous, current = 0, 0
        counts = (start, previous, current)
        return counts

    def evaluate(
        self, policy: SlidingCounterPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> Transition:
        """Admit when the weighted estimate plus ``cost`` fits in ``amount``.

        A denial waits for the first microsecond at which the inequality would
        hold with no other admissions (E5) and changes nothing.

        :param policy: A validated policy.
        :param state: The rule's window and counts.
        :param now: Authority epoch time.
        :param cost: The requested cost, within capacity.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` exceeds ``amount``.
        """
        self.check_cost(policy, cost)
        period = policy.period_us
        start, previous, current = self.counts(policy, state, now)
        elapsed = now - start
        weighted = previous * (period - elapsed) // period
        if weighted + current + cost <= policy.amount:
            transition = self.admit(
                state.rule,
                (
                    SetScalar(_WINDOW, start),
                    SetScalar(_PREVIOUS, previous),
                    SetScalar(_CURRENT, current + cost),
                ),
                safe_forget_after_us=EpochMicros(start + 2 * period),
                remaining=policy.amount - weighted - current - cost,
            )
        else:
            # A denial means a count is non-zero, so the stored window is the
            # current or the previous one and still bounds the state's life.
            transition = self.deny(
                state.rule,
                self._retry_after(policy, now, start, previous, current, cost),
                safe_forget_after_us=EpochMicros(state.scalar(_WINDOW) + 2 * period),
                remaining=max(0, policy.amount - weighted - current),
            )
        return transition

    @classmethod
    def _retry_after(
        cls,
        policy: SlidingCounterPolicy,
        now: EpochMicros,
        start: EpochMicros,
        previous: int,
        current: int,
        cost: int,
    ) -> DurationMicros:
        period = policy.period_us
        if (within := cls._earliest(policy, previous, current, cost, now - start)) is not None:
            delay = DurationMicros(start + within - now)
        elif (following := cls._earliest(policy, current, 0, cost, 0)) is not None:
            delay = DurationMicros(start + period + following - now)
        else:
            delay = DurationMicros(start + 2 * period - now)
        return delay

    @staticmethod
    def _earliest(
        policy: SlidingCounterPolicy, previous: int, current: int, cost: int, elapsed: int
    ) -> int | None:
        """The least ``r`` in ``[elapsed, period)`` admitting ``cost``, or ``None``.

        ``previous * (period - r) // period <= slack`` holds exactly when
        ``previous * (period - r) <= (slack + 1) * period - 1``, which solves
        for ``r`` without searching.
        """
        period = policy.period_us
        slack = policy.amount - current - cost
        if slack < 0:
            earliest = None
        elif previous == 0:
            earliest = elapsed
        elif (bound := max(elapsed, period - ((slack + 1) * period - 1) // previous)) < period:
            earliest = bound
        else:
            earliest = None
        return earliest


if __name__ == "__main__":
    pass
else:
    pass
