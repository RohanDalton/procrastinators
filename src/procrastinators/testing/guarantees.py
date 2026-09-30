"""Checkers for what each policy guarantees about a history of admissions.

A concurrency test does not know which of its racing callers should have won,
but it does know what the winners, taken together, must satisfy. Each function
here takes the admissions that actually happened — ``(authority time, cost)``
pairs, in any order — and returns every violation of one policy's guarantee
(contract P4). An empty result means the history is consistent with the policy.

Each policy is judged by its own guarantee: a fixed window's boundary burst is
not a rolling-window violation, and a sliding counter promises no rolling bound
at all, only its per-window one.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
else:
    pass

__all__ = [
    "Violation",
    "fixed_window_violations",
    "pacing_violations",
    "rolling_window_violations",
    "token_bucket_violations",
]


@dataclass(frozen=True, slots=True)
class Violation:
    """One interval in which a history exceeded what its policy allows."""

    start: int
    """Authority epoch time at which the interval starts."""

    end: int
    """Authority epoch time at which the interval ends."""

    admitted: int
    """Cost units admitted in the interval."""

    allowed: int
    """Cost units the policy allows in it."""

    def __str__(self) -> str:
        text = f"{self.admitted} admitted in [{self.start}, {self.end}], at most {self.allowed}"
        return text


def _ordered(history: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    ordered = sorted(history)
    return ordered


def rolling_window_violations(
    history: Iterable[tuple[int, int]], amount: int, period_us: int
) -> tuple[Violation, ...]:
    """Violations of "at most ``amount`` in every rolling ``(t - period, t]``".

    The sliding-log guarantee.

    :param history: ``(time, cost)`` of every admission.
    :param amount: The most cost units any rolling window may hold.
    :param period_us: The window length.
    :returns: For each admission ending an over-full window, that window.
    """
    ordered = _ordered(history)
    violations = list()
    start = 0
    total = 0
    for at, cost in ordered:
        total += cost
        while ordered[start][0] <= at - period_us:
            total -= ordered[start][1]
            start += 1
        if total > amount:
            violations.append(Violation(at - period_us + 1, at, total, amount))
        else:
            pass
    return tuple(violations)


def fixed_window_violations(
    history: Iterable[tuple[int, int]], amount: int, period_us: int, offset_us: int = 0
) -> tuple[Violation, ...]:
    """Violations of "at most ``amount`` per aligned window ``[start, start + period)``".

    The fixed-window guarantee. A sliding counter never admits more than
    ``amount`` in one of its windows either, so this is also the strongest
    history check that policy supports.

    :param history: ``(time, cost)`` of every admission.
    :param amount: The most cost units one window may hold.
    :param period_us: The window length.
    :param offset_us: The windows' offset from the epoch.
    :returns: Each over-full window.
    """
    totals: dict[int, int] = dict()
    for at, cost in history:
        start = (at - offset_us) // period_us * period_us + offset_us
        totals[start] = totals.get(start, 0) + cost
    violations = tuple(
        Violation(start, start + period_us - 1, total, amount)
        for start, total in sorted(totals.items())
        if total > amount
    )
    return violations


def token_bucket_violations(
    history: Iterable[tuple[int, int]],
    capacity: int,
    refill_amount: int,
    refill_period_us: int,
) -> tuple[Violation, ...]:
    """Violations of the burst-plus-refill envelope.

    Over any interval of length ``L`` a token bucket admits at most
    ``capacity + refill_amount * ceil(L / refill_period)``: it holds at most
    ``capacity`` at the start, and refills at most that many times in between.

    :param history: ``(time, cost)`` of every admission.
    :param capacity: The bucket's capacity.
    :param refill_amount: Tokens added per refill.
    :param refill_period_us: Time between refills.
    :returns: Each over-full interval between two admissions.
    """
    ordered = _ordered(history)
    violations = list()
    for first, (start, _) in enumerate(ordered):
        total = 0
        for end, cost in ordered[first:]:
            total += cost
            refills = -(-(end - start) // refill_period_us)
            if total > (allowed := capacity + refill_amount * refills):
                violations.append(Violation(start, end, total, allowed))
            else:
                pass
    return tuple(violations)


def pacing_violations(
    history: Iterable[tuple[int, int]],
    amount: int,
    period_us: int,
    burst_tolerance: int = 0,
) -> tuple[Violation, ...]:
    """Violations of leaky-bucket pacing.

    For admissions ``i < j`` in time order, admission ``j`` may be at most the
    tolerance early relative to the schedule the earlier ones set:
    ``t_j >= t_i + sum(ceil(c_k * period / amount) for i <= k < j) - tolerance``,
    where ``tolerance = floor(burst_tolerance * period / amount)``.

    :param history: ``(time, cost)`` of every admission.
    :param amount: Cost units per period.
    :param period_us: The pacing period.
    :param burst_tolerance: Cost units that may arrive early.
    :returns: Each pair of admissions closer together than pacing allows,
        reported as the interval between them.
    """
    ordered = _ordered(history)
    tolerance = burst_tolerance * period_us // amount
    violations = list()
    for first, (start, _) in enumerate(ordered):
        spacing = 0
        admitted = 0
        for end, cost in ordered[first:]:
            if end < start + spacing - tolerance:
                violations.append(Violation(start, end, admitted + cost, admitted))
            else:
                pass
            admitted += cost
            spacing += -(-cost * period_us // amount)
    return tuple(violations)


if __name__ == "__main__":
    pass
else:
    pass
