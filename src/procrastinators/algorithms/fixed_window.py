"""Fixed window: at most ``amount`` cost units per aligned interval.

Contract E1. Windows are ``[start, start + period)`` with
``start = (now - offset) // period * period + offset``, aligned to the Unix
epoch plus the policy's offset rather than to process startup (T8). State is
constant: the start of the window the count belongs to, and the count.

Up to ``2 * amount`` can be admitted across a boundary. That is what a fixed
window means, and why it is never judged by a rolling-window test (P4).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Final

from procrastinators.algorithms.reference import ReferenceAlgorithm
from procrastinators.models import DurationMicros, EpochMicros, FixedWindowPolicy, SetScalar
from procrastinators.protocols import StateRequirements

if TYPE_CHECKING:
    from procrastinators.models import Transition
    from procrastinators.protocols import StateView
else:
    pass

__all__ = ["FixedWindow", "window_start"]

_WINDOW: Final = "window"
_COUNT: Final = "count"
_REQUIREMENTS: Final = StateRequirements(scalars=frozenset({_WINDOW, _COUNT}))


def window_start(policy: FixedWindowPolicy, now: EpochMicros) -> EpochMicros:
    """The start of the aligned window containing ``now``.

    :param policy: The fixed-window policy.
    :param now: Authority epoch time.
    """
    offset = policy.epoch_offset_us
    start = EpochMicros((now - offset) // policy.period_us * policy.period_us + offset)
    return start


class FixedWindow(ReferenceAlgorithm[FixedWindowPolicy]):
    """The reference fixed-window evaluator.

    State may be forgotten at the end of the window it counts (L9): a count for
    a past window already reads as zero, which is exactly the unused state.
    """

    policy_type = FixedWindowPolicy

    def requirements(self, policy: FixedWindowPolicy) -> StateRequirements:
        """The window start and its count; the same for every policy.

        :param policy: A validated policy. Unused.
        """
        del policy
        return _REQUIREMENTS

    def evaluate(
        self, policy: FixedWindowPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> Transition:
        """Admit when the current window's count plus ``cost`` fits in ``amount``.

        A denial waits until the window ends (E1) and changes nothing.

        :param policy: A validated policy.
        :param state: The rule's window and count.
        :param now: Authority epoch time.
        :param cost: The requested cost, within capacity.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` exceeds ``amount``.
        """
        self.check_cost(policy, cost)
        start = window_start(policy, now)
        end = EpochMicros(start + policy.period_us)
        current = state.exists and state.scalar(_WINDOW) == start
        used = state.scalar(_COUNT) if current else 0
        if used + cost <= policy.amount:
            transition = self.admit(
                state.rule,
                (SetScalar(_WINDOW, start), SetScalar(_COUNT, used + cost)),
                safe_forget_after_us=end,
                remaining=policy.amount - used - cost,
            )
        else:
            transition = self.deny(
                state.rule,
                DurationMicros(end - now),
                safe_forget_after_us=end,
                remaining=policy.amount - used,
            )
        return transition


if __name__ == "__main__":
    pass
else:
    pass
