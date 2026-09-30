"""Reference evaluators and their codecs.

The five built-in algorithms, each a pure implementation of its rule in
contract §17. They never sleep, lock, read a clock, or perform I/O: a backend
collects their declared state inside its own critical section and applies the
proposals they return.

:func:`reference_algorithms` builds one of each, and :func:`builtin_specs`
describes them for a :class:`~procrastinators.registry.Registry`.
"""

__author__ = "Rohan B. Dalton"

from procrastinators.algorithms.base import BaseAlgorithm
from procrastinators.algorithms.fixed_window import FixedWindow
from procrastinators.algorithms.leaky_bucket import LeakyBucket
from procrastinators.algorithms.reference import ReferenceAlgorithm
from procrastinators.algorithms.sliding_counter import SlidingCounter
from procrastinators.algorithms.sliding_log import SlidingLog
from procrastinators.algorithms.token_bucket import TokenBucket
from procrastinators.protocols import AlgorithmSpec, StateRepresentation

__all__ = [
    "BaseAlgorithm",
    "FixedWindow",
    "LeakyBucket",
    "ReferenceAlgorithm",
    "SlidingCounter",
    "SlidingLog",
    "TokenBucket",
    "builtin_specs",
    "reference_algorithms",
]

_BUILTINS: tuple[type[ReferenceAlgorithm], ...] = (
    FixedWindow,
    SlidingLog,
    TokenBucket,
    LeakyBucket,
    SlidingCounter,
)


def reference_algorithms() -> tuple[ReferenceAlgorithm, ...]:
    """A fresh instance of each of the five reference algorithms.

    Instances are stateless, so sharing one between backends is safe; a fresh
    tuple is returned only so callers cannot alter a shared one.
    """
    algorithms = tuple(algorithm() for algorithm in _BUILTINS)
    return algorithms


def builtin_specs() -> tuple[AlgorithmSpec, ...]:
    """Registrations for the five reference algorithms.

    The sliding log needs an event log; the other four hold scalars only, which
    is what lets a constant-state store refuse the sliding log at construction
    (contract Y6).
    """
    specs = tuple(
        AlgorithmSpec(
            id=str(algorithm.policy_type.algorithm),
            state_version=algorithm.policy_type.state_version,
            representation=(
                StateRepresentation.EVENT_LOG
                if algorithm is SlidingLog
                else StateRepresentation.SCALARS
            ),
            factory=algorithm,
        )
        for algorithm in _BUILTINS
    )
    return specs


if __name__ == "__main__":
    pass
else:
    pass
