"""What the five built-in evaluators share.

Each built-in algorithm evaluates exactly one of the policy dataclasses in
:mod:`procrastinators.models`, persists state through a
:class:`~procrastinators.state.RuleStateCodec` of that policy's state version,
and takes its stable id from the policy class. :class:`ReferenceAlgorithm`
holds that bookkeeping so each evaluator module is only its arithmetic.

Third-party algorithms need none of this; they implement
:class:`~procrastinators.protocols.Algorithm` directly or subclass
:class:`~procrastinators.algorithms.base.BaseAlgorithm`.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from abc import ABC
from typing import TYPE_CHECKING, ClassVar, TypeVar

from procrastinators.algorithms.base import BaseAlgorithm
from procrastinators.errors import InvalidPolicy
from procrastinators.models import PolicySpec
from procrastinators.state import RuleStateCodec

if TYPE_CHECKING:
    from procrastinators.models import Policy
    from procrastinators.protocols import RuleState, StateCodec
else:
    pass

__all__ = ["ReferenceAlgorithm", "ceil_div"]

ReferencePolicyT = TypeVar("ReferencePolicyT", bound=PolicySpec)
"""The built-in policy dataclass a :class:`ReferenceAlgorithm` evaluates."""


def ceil_div(numerator: int, denominator: int) -> int:
    """``ceil(numerator / denominator)`` in exact integer arithmetic.

    Every delay and interval in contract §17 rounds this way, so a required wait
    is never rounded down (contract T6).

    :param numerator: Any integer.
    :param denominator: A positive integer.
    """
    quotient = -(-numerator // denominator)
    return quotient


class ReferenceAlgorithm(BaseAlgorithm[ReferencePolicyT], ABC):
    """A built-in evaluator bound to one policy class.

    Subclasses set :attr:`policy_type` and implement ``requirements`` and
    ``evaluate``. Instances hold nothing but their codec, so one instance is
    safely shared by every thread, task, and backend.
    """

    policy_type: ClassVar[type[Policy]]
    """The policy dataclass this algorithm evaluates; its id and state version are this one's."""

    def __init__(self) -> None:
        self._codec = RuleStateCodec(self.state_version)

    @property
    def id(self) -> str:
        """The policy class's stable algorithm id."""
        algorithm_id = str(self.policy_type.algorithm)
        return algorithm_id

    @property
    def state_version(self) -> int:
        """The policy class's state version."""
        version = self.policy_type.state_version
        return version

    @property
    def codec(self) -> StateCodec[RuleState]:
        """The canonical binary codec for this algorithm's state version."""
        return self._codec

    def validate(self, policy: ReferencePolicyT) -> None:
        """Check that ``policy`` is an instance of :attr:`policy_type`.

        The policy dataclasses validate their own fields on construction, so
        the type is all that is left to check.

        :param policy: The policy to check.
        :raises ~procrastinators.errors.InvalidPolicy: ``policy`` is another type.
        """
        if not isinstance(policy, self.policy_type):
            raise InvalidPolicy(
                f"{self.id} evaluates {self.policy_type.__name__}, got {type(policy).__name__}"
            )
        else:
            pass

    def capacity(self, policy: ReferencePolicyT) -> int:
        """The policy's own capacity: the largest cost it could ever admit.

        :param policy: A validated policy.
        """
        capacity = policy.capacity
        return capacity

    def __repr__(self) -> str:
        text = f"{type(self).__name__}()"
        return text


if __name__ == "__main__":
    pass
else:
    pass
