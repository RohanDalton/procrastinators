"""Base class for algorithm implementations.

Inheriting is optional: :class:`~procrastinators.protocols.Algorithm` is
structural, and a third-party evaluator that implements it is a first-class
algorithm without importing anything from here.

What this class adds is the small amount of behavior every evaluator repeats —
cost checking against capacity, and building the two shapes of
:class:`~procrastinators.models.Transition`. What it deliberately does not add
is a default :meth:`BaseAlgorithm.evaluate`. A base method that returned "admitted" for an
algorithm that forgot to override it would be the most expensive kind of
convenience.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from procrastinators.errors import InvalidCost
from procrastinators.models import DurationMicros, Transition

if TYPE_CHECKING:
    from procrastinators.models import EpochMicros, RuleId, StateChange
    from procrastinators.protocols import RuleState, StateCodec, StateRequirements, StateView
else:
    pass

__all__ = ["BaseAlgorithm"]

PolicyT = TypeVar("PolicyT")
"""The policy type a :class:`BaseAlgorithm` subclass evaluates."""


class BaseAlgorithm(ABC, Generic[PolicyT]):
    """Optional base for a deterministic policy evaluator.

    Subclasses must be stateless and safe to share between threads and tasks:
    everything an evaluation needs arrives as an argument. An implementation
    that stored per-rule state on ``self`` would be a second, unsynchronized
    copy of the state the backend is holding a transaction open for.

    Subclasses implement the abstract members; :meth:`check_cost`,
    :meth:`admit` and :meth:`deny` are helpers for building their results.
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """Stable identifier, persisted and compared across processes.

        Built-ins use :class:`~procrastinators.models.Algorithms` values. A
        third-party algorithm picks its own and must not change it afterwards.
        """

    @property
    @abstractmethod
    def state_version(self) -> int:
        """Version of the stored representation, part of the policy fingerprint.

        Bump it whenever the codec or the meaning of the state changes (contract
        I5).
        """

    @property
    @abstractmethod
    def codec(self) -> StateCodec[RuleState]:
        """Codec used by backends that persist state as bytes."""

    @abstractmethod
    def validate(self, policy: PolicyT) -> None:
        """Check that ``policy`` is usable by this algorithm.

        :param policy: The policy to check.
        :raises ~procrastinators.errors.InvalidPolicy: The policy is
            contradictory, out of range, or unsupported.
        """

    @abstractmethod
    def requirements(self, policy: PolicyT) -> StateRequirements:
        """Declare the state a backend loads before calling :meth:`evaluate`.

        An evaluator sees only what it declared here (contract S4).

        :param policy: Validated policy for the rule; the same algorithm may need
            different history for different parameters.
        """

    @abstractmethod
    def capacity(self, policy: PolicyT) -> int:
        """Largest single cost ``policy`` could ever admit.

        Abstract because getting it wrong is silent: too low denies valid work,
        and too high turns an impossible cost into an unbounded wait.

        :param policy: Validated policy for the rule.
        :returns: The capacity, which :meth:`check_cost` compares costs against.
        """

    @abstractmethod
    def evaluate(
        self,
        policy: PolicyT,
        state: StateView,
        now: EpochMicros,
        cost: int,
    ) -> Transition:
        """Decide, purely, what should happen. See the protocol for the contract.

        Must not read a clock, sleep, lock, perform I/O, or mutate ``state``.
        Ordinary denial is a returned value, never an exception. See
        :meth:`procrastinators.protocols.Algorithm.evaluate` for the full
        contract.

        :param policy: Validated policy for this rule.
        :param state: Observations already collected inside the backend's
            transaction. Reading it performs no I/O.
        :param now: Authority epoch time, sampled inside that transaction after
            locks were acquired (contract T4).
        :param cost: Positive integer, already known to be within capacity.
        :returns: A transition proposing changes, usually built with
            :meth:`admit` or :meth:`deny`. It commits nothing.
        """

    def initial_changes(self, policy: PolicyT, now: EpochMicros) -> tuple[StateChange, ...]:
        """State to write when a rule is first used.

        Empty by default, which is correct only when unused state already means
        the configured initial state. An algorithm whose initial state differs
        from "nothing stored" — a token bucket that starts partly full, or whose
        refill clock starts at first use — must override this, or being
        forgotten would silently refill it (P5). See
        :meth:`procrastinators.protocols.Algorithm.initial_changes`.

        :param policy: Validated policy for the rule. Unused by the default.
        :param now: Authority epoch time of the first attempt. Unused by the
            default.
        :returns: The changes to apply on first use; empty by default.
        """
        del policy, now
        return tuple()

    def check_cost(self, policy: PolicyT, cost: int) -> None:
        """Reject a cost this policy could never admit.

        Contract P3: an impossible cost is an error, because denying it would
        imply an unbounded retry delay.

        :param policy: Validated policy for the rule.
        :param cost: The requested cost.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` exceeds
            :meth:`capacity` for ``policy``.
        """
        if cost > (capacity := self.capacity(policy)):
            raise InvalidCost(
                f"cost {cost} can never be admitted by {self.id}: its capacity is {capacity}"
            )
        else:
            pass

    def admit(
        self,
        rule: RuleId,
        changes: tuple[StateChange, ...],
        *,
        safe_forget_after_us: EpochMicros | None = None,
        remaining: int | None = None,
    ) -> Transition:
        """Build an admitting transition proposing ``changes``.

        :param rule: The rule being admitted.
        :param changes: The state changes the backend commits if every rule in
            the request admits.
        :param safe_forget_after_us: Earliest authority epoch time at which the
            rule's state stops affecting any decision, or ``None``.
        :param remaining: Advisory remaining capacity after this admission, or
            ``None`` (contract R5).
        :returns: A :class:`~procrastinators.models.Transition` with
            ``admitted=True`` and no retry delay.
        """
        transition = Transition(
            rule=rule,
            admitted=True,
            changes=changes,
            safe_forget_after_us=safe_forget_after_us,
            remaining=remaining,
        )
        return transition

    def deny(
        self,
        rule: RuleId,
        retry_after_us: DurationMicros,
        *,
        changes: tuple[StateChange, ...] = tuple(),
        safe_forget_after_us: EpochMicros | None = None,
        remaining: int | None = None,
    ) -> Transition:
        """Build a denying transition.

        ``changes`` may contain only pruning;
        :class:`~procrastinators.models.Transition` enforces that by raising
        :exc:`~procrastinators.errors.InvalidPolicy`, so a denial cannot
        consume quota (contract A5).

        :param rule: The rule that denies.
        :param retry_after_us: Policy-derived delay before capacity may be
            available. Negative values are clamped to zero.
        :param changes: Pruning-only state changes, each marked
            ``prunes_only``. Empty by default.
        :param safe_forget_after_us: Earliest authority epoch time at which the
            rule's state stops affecting any decision, or ``None``.
        :param remaining: Advisory remaining capacity, or ``None`` (contract
            R5).
        :returns: A :class:`~procrastinators.models.Transition` with
            ``admitted=False``.
        """
        transition = Transition(
            rule=rule,
            admitted=False,
            changes=changes,
            retry_after_us=DurationMicros(max(0, retry_after_us)),
            safe_forget_after_us=safe_forget_after_us,
            remaining=remaining,
        )
        return transition


if __name__ == "__main__":
    pass
else:
    pass
