"""Policy normalization: from a public ``Limit`` to a validated integer policy.

``Limit(10, per="1s")`` says what a vendor's documentation says. Which policy it
becomes depends on the algorithm the caller chose, and the translation must be
exact: two spellings of the same rate must normalize to the same policy, and
therefore the same fingerprint, or equivalent workers would be told they
conflict.

Arithmetic is on integers and :class:`fractions.Fraction` only. Where a value
cannot be represented exactly it is rounded in the stricter direction
(contract T6), and anything out of range is rejected rather than saturated
(N2).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import datetime as dt
import math
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Final

from procrastinators.errors import InvalidPolicy
from procrastinators.models import (
    MIN_PERIOD_US,
    Algorithms,
    DurationMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    Limit,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
    duration_to_micros,
)

if TYPE_CHECKING:
    from procrastinators.models import Policy
else:
    pass

__all__ = ["OPTIONS_BY_ALGORITHM", "normalize_limit", "normalize_limits", "resolve_algorithm"]

OPTIONS_BY_ALGORITHM: Final[Mapping[Algorithms, frozenset[str]]] = {
    Algorithms.FIXED_WINDOW: frozenset({"epoch_offset"}),
    Algorithms.SLIDING_LOG: frozenset(),
    Algorithms.SLIDING_COUNTER: frozenset(),
    Algorithms.TOKEN_BUCKET: frozenset({"capacity", "initial_tokens"}),
    Algorithms.LEAKY_BUCKET: frozenset({"burst_tolerance"}),
}
"""Algorithm-specific options :func:`normalize_limit` accepts, by algorithm.

``epoch_offset`` is a duration in any form
:func:`~procrastinators.models.duration_to_micros` accepts; the others are
integers validated by the policy itself.
"""


def resolve_algorithm(algorithm: Algorithms | str) -> Algorithms:
    """Return the built-in algorithm ``algorithm`` names.

    :param algorithm: An :class:`~procrastinators.models.Algorithms` member or its value.
    :raises ~procrastinators.errors.InvalidPolicy: ``algorithm`` names no built-in algorithm. A
        custom algorithm builds its own policy; there is no ``Limit`` translation to guess at.
    """
    if isinstance(algorithm, Algorithms):
        resolved = algorithm
    elif isinstance(algorithm, str) and algorithm in set(Algorithms):
        resolved = Algorithms(algorithm)
    else:
        raise InvalidPolicy(
            f"unknown algorithm {algorithm!r}; built-ins are "
            f"{sorted(member.value for member in Algorithms)}, and a custom algorithm "
            "supplies its own policy object"
        )
    return resolved


def normalize_limit(
    limit: Limit,
    algorithm: Algorithms | str = Algorithms.SLIDING_LOG,
    options: Mapping[str, object] | None = None,
) -> Policy:
    """Translate ``limit`` into the policy ``algorithm`` enforces.

    ============== ====================================================================
    Algorithm      Policy
    ============== ====================================================================
    fixed window   ``amount`` per aligned ``period``, offset by ``epoch_offset`` (default 0).
    sliding log    ``amount`` in every rolling ``period``.
    sliding count  ``amount`` per ``period``, weighted.
    token bucket   ``capacity`` (default ``amount``) with the rate reduced to lowest terms:
                   ``amount / g`` tokens every ``period / g`` for ``g = gcd(amount,
                   period)``, scaled up only as far as the one-millisecond minimum period
                   requires. The rate is exact, and refills are as fine as it allows.
    leaky bucket   ``amount`` per ``period`` with ``burst_tolerance`` (default 0).
    ============== ====================================================================

    :param limit: The public rate.
    :param algorithm: The algorithm to enforce it with.
    :param options: Algorithm-specific options, from :data:`OPTIONS_BY_ALGORITHM`.
    :returns: The validated policy.
    :raises ~procrastinators.errors.InvalidPolicy: ``limit`` is not a
        :class:`~procrastinators.models.Limit`, the
        algorithm is unknown, an option does not apply to it, or the resulting policy is invalid.
    """
    if not isinstance(limit, Limit):
        raise InvalidPolicy(f"expected a Limit, got {type(limit).__name__}: {limit!r}")
    else:
        pass
    resolved = resolve_algorithm(algorithm)
    chosen = dict(options or dict())
    if unknown := set(chosen) - OPTIONS_BY_ALGORITHM[resolved]:
        allowed = sorted(OPTIONS_BY_ALGORITHM[resolved]) or "none"
        raise InvalidPolicy(
            f"options {sorted(unknown)} do not apply to {resolved.value}; it accepts {allowed}"
        )
    else:
        pass

    if resolved is Algorithms.FIXED_WINDOW:
        offset = _duration_option(chosen, "epoch_offset")
        policy: Policy = FixedWindowPolicy(limit.amount, limit.period_us, offset)
    elif resolved is Algorithms.SLIDING_LOG:
        policy = SlidingLogPolicy(limit.amount, limit.period_us)
    elif resolved is Algorithms.SLIDING_COUNTER:
        policy = SlidingCounterPolicy(limit.amount, limit.period_us)
    elif resolved is Algorithms.TOKEN_BUCKET:
        refill_amount, refill_period_us = _reduced_rate(limit.amount, limit.period_us)
        initial_tokens = (
            _int_option(chosen, "initial_tokens") if "initial_tokens" in chosen else None
        )
        policy = TokenBucketPolicy(
            capacity=_int_option(chosen, "capacity", default=limit.amount),
            refill_amount=refill_amount,
            refill_period_us=refill_period_us,
            initial_tokens=initial_tokens,
        )
    else:
        policy = LeakyBucketPolicy(
            limit.amount,
            limit.period_us,
            burst_tolerance=_int_option(chosen, "burst_tolerance", default=0),
        )
    return policy


def _int_option(options: Mapping[str, object], name: str, *, default: int = 0) -> int:
    # Range and bool checks belong to the policy; this only establishes the type.
    value = options.get(name, default)
    if not isinstance(value, int):
        raise InvalidPolicy(f"option {name} must be an integer, got {value!r}")
    else:
        pass
    return value


def _duration_option(options: Mapping[str, object], name: str) -> DurationMicros:
    value = options.get(name, 0)
    if not isinstance(value, (str, int, float, dt.timedelta)):
        raise InvalidPolicy(f"option {name} must be a duration, got {value!r}")
    else:
        pass
    duration = duration_to_micros(value)
    return duration


def _reduced_rate(amount: int, period_us: int) -> tuple[int, DurationMicros]:
    divisor = math.gcd(amount, period_us)
    refill_amount, refill_period_us = amount // divisor, period_us // divisor
    # period_us >= MIN_PERIOD_US, so scale <= divisor and refill_amount stays <= amount.
    scale = -(-MIN_PERIOD_US // refill_period_us)
    reduced = (refill_amount * scale, DurationMicros(refill_period_us * scale))
    return reduced


def normalize_limits(
    limits: Sequence[Limit],
    algorithm: Algorithms | str = Algorithms.SLIDING_LOG,
    options: Mapping[str, object] | None = None,
) -> tuple[Policy, ...]:
    """Translate every limit of one scope, in order.

    Order matters: positional rule names follow it (contract I3).

    :param limits: The scope's limits, at least one.
    :param algorithm: The one algorithm applied to every limit.
    :param options: Algorithm-specific options applied to every limit.
    :returns: The policies in the order given.
    :raises ~procrastinators.errors.InvalidPolicy: ``limits`` is empty or a string, or any limit is
        invalid as for :func:`normalize_limit`.
    """
    if isinstance(limits, (str, bytes)) or not limits:
        raise InvalidPolicy("a scope needs at least one Limit")
    else:
        pass
    policies = tuple(normalize_limit(limit, algorithm, options) for limit in limits)
    return policies


if __name__ == "__main__":
    pass
else:
    pass
