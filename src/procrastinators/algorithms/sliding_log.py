"""Sliding log: at most ``amount`` cost units in every rolling ``(now - period, now]``.

Contract E2, and the default policy. Each admission is one weighted entry
``(now, cost)``, never ``cost`` entries (P6). An entry exactly ``period`` old has
left the window (P1) and is pruned, on admission and denial alike: pruning
consumes nothing (A5).

Retained entries are bounded by ``amount``, because every live entry weighs at
least one unit. A view holding more than that was not written by this policy;
it is marked truncated and the rule denies rather than deciding on a partial
history (S6).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

from procrastinators.algorithms.reference import ReferenceAlgorithm
from procrastinators.errors import InvalidCost
from procrastinators.models import (
    AppendEvent,
    DropEventsBefore,
    DurationMicros,
    EpochMicros,
    SlidingLogPolicy,
)
from procrastinators.protocols import EventWindow, StateRepresentation, StateRequirements

if TYPE_CHECKING:
    from collections.abc import Sequence

    from procrastinators.models import Transition
    from procrastinators.protocols import LogEvent, StateView
else:
    pass

__all__ = ["SlidingLog"]


class SlidingLog(ReferenceAlgorithm[SlidingLogPolicy]):
    """The reference sliding-log evaluator.

    State may be forgotten once its newest entry is ``period`` old (L9): every
    entry has then left the window, and an empty log is the unused state.
    """

    policy_type = SlidingLogPolicy

    def requirements(self, policy: SlidingLogPolicy) -> StateRequirements:
        """One period of history, at most ``amount`` entries of it.

        :param policy: A validated policy.
        """
        requirements = StateRequirements(
            representation=StateRepresentation.EVENT_LOG,
            events=EventWindow(policy.period_us, max_events=policy.amount),
        )
        return requirements

    def evaluate(
        self, policy: SlidingLogPolicy, state: StateView, now: EpochMicros, cost: int
    ) -> Transition:
        """Admit when the live cost plus ``cost`` fits in ``amount``.

        A denial waits until enough of the oldest entries have left the window
        to make room for ``cost`` (E2).

        :param policy: A validated policy.
        :param state: The rule's live entries, oldest first.
        :param now: Authority epoch time.
        :param cost: The requested cost, within capacity.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` exceeds ``amount``.
        """
        self.check_cost(policy, cost)
        events = state.events()
        prune = (DropEventsBefore(EpochMicros(now - policy.period_us)),)
        newest = events[-1].at if events else now
        forget = EpochMicros(newest + policy.period_us)
        live = sum(event.cost for event in events)
        if state.truncated:
            # Every entry now live has left the window one period from now,
            # whatever the entries the view could not hold.
            transition = self.deny(
                state.rule,
                DurationMicros(policy.period_us),
                changes=prune,
                safe_forget_after_us=forget,
                remaining=0,
            )
        elif live + cost <= policy.amount:
            transition = self.admit(
                state.rule,
                (*prune, AppendEvent(now, cost)),
                safe_forget_after_us=EpochMicros(now + policy.period_us),
                remaining=policy.amount - live - cost,
            )
        else:
            transition = self.deny(
                state.rule,
                self._retry_after(policy, events, now, live + cost - policy.amount),
                changes=prune,
                safe_forget_after_us=forget,
                remaining=max(0, policy.amount - live),
            )
        return transition

    @staticmethod
    def _retry_after(
        policy: SlidingLogPolicy, events: Sequence[LogEvent], now: EpochMicros, excess: int
    ) -> DurationMicros:
        """How long until the oldest entries free at least ``excess`` units."""
        freed = 0
        for event in events:
            freed += event.cost
            if freed >= excess:
                delay = DurationMicros(event.at + policy.period_us - now)
                break
            else:
                pass
        else:
            # Freeing every live entry frees `live` units, and excess <= live
            # whenever cost <= amount, which check_cost has established.
            raise InvalidCost(f"cost can never be admitted by {policy}")
        return delay


if __name__ == "__main__":
    pass
else:
    pass
