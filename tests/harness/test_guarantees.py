"""History checkers catch violations, and random histories never produce one.

The property tests generate random attempt schedules, admit them on the oracle
host, and check that whatever was admitted satisfies that policy's guarantee.
They are the template for the concurrency tests of later phases, where the
history comes from racing threads and processes instead.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from procrastinators.models import (
    MIN_PERIOD_US,
    AdmissionRequest,
    DurationMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.testing import (
    DEFAULT_EPOCH_US,
    EvaluatorHost,
    FakeTimeline,
    TraceRule,
    fixed_window_violations,
    pacing_violations,
    rolling_window_violations,
    token_bucket_violations,
)
from procrastinators.testing.harness import DEFAULT_NAMESPACE
from tests.oracles import all_oracles

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from procrastinators.models import PolicySpec
    from procrastinators.testing import Violation

    CheckerCase = tuple[
        Callable[..., tuple[Violation, ...]], list[tuple[int, int]], tuple[int, ...]
    ]
else:
    pass

amounts = st.integers(min_value=3, max_value=6)
periods = st.integers(min_value=MIN_PERIOD_US, max_value=2_000_000).map(DurationMicros)
schedules = st.lists(
    st.tuples(st.integers(min_value=0, max_value=600_000), st.integers(min_value=1, max_value=3)),
    min_size=1,
    max_size=40,
)


def _admitted(
    rules: Sequence[TraceRule], schedule: Sequence[tuple[int, int, tuple[int, ...]]]
) -> dict[str, list[tuple[int, int]]]:
    """Admit ``(gap, cost, rule indices)`` attempts in order; return each rule's charges."""
    timeline = FakeTimeline()
    host = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock)
    constraints = [rule.constraint(DEFAULT_NAMESPACE) for rule in rules]
    charged: dict[str, list[tuple[int, int]]] = {rule.label: list() for rule in rules}
    for gap, cost, indices in schedule:
        timeline.advance(gap)
        decision = host.admit(
            AdmissionRequest(tuple(constraints[index] for index in indices), cost)
        )
        if decision.allowed:
            for index in set(indices):
                charged[rules[index].label].append((timeline.peek_epoch(), cost))
        else:
            pass
    return charged


def _single(policy: PolicySpec, schedule: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    history = _admitted([TraceRule("rule", policy)], [(gap, cost, (0,)) for gap, cost in schedule])
    return history["rule"]


@settings(deadline=None)
@given(amount=amounts, period=periods, schedule=schedules)
def test_a_sliding_log_never_exceeds_any_rolling_window(
    amount: int, period: DurationMicros, schedule: list[tuple[int, int]]
) -> None:
    """
    Given: A random sliding-log policy and random weighted attempts.
    When:  They are admitted on the oracle host.
    Then:  No rolling window holds more than the amount (P4).
    """
    history = _single(SlidingLogPolicy(amount, period), schedule)

    actual = rolling_window_violations(history, amount, period)

    assert actual == tuple()


@settings(deadline=None)
@given(amount=amounts, period=periods, schedule=schedules)
def test_a_fixed_window_never_overfills_an_aligned_window(
    amount: int, period: DurationMicros, schedule: list[tuple[int, int]]
) -> None:
    """
    Given: A random fixed-window policy and random weighted attempts.
    When:  They are admitted on the oracle host.
    Then:  No aligned window holds more than the amount (P4).
    """
    history = _single(FixedWindowPolicy(amount, period), schedule)

    actual = fixed_window_violations(history, amount, period)

    assert actual == tuple()


@settings(deadline=None)
@given(amount=amounts, period=periods, schedule=schedules)
def test_a_sliding_counter_never_overfills_its_own_window(
    amount: int, period: DurationMicros, schedule: list[tuple[int, int]]
) -> None:
    """
    Given: A random sliding-counter policy and random weighted attempts.
    When:  They are admitted on the oracle host.
    Then:  No aligned window holds more than the amount — the only bound it promises.
    """
    history = _single(SlidingCounterPolicy(amount, period), schedule)

    actual = fixed_window_violations(history, amount, period)

    assert actual == tuple()


@settings(deadline=None)
@given(
    capacity=amounts,
    refill=st.integers(min_value=1, max_value=4),
    period=periods,
    schedule=schedules,
    starts_empty=st.booleans(),
)
def test_a_token_bucket_stays_inside_its_envelope(
    capacity: int,
    refill: int,
    period: DurationMicros,
    schedule: list[tuple[int, int]],
    starts_empty: bool,
) -> None:
    """
    Given: A random token bucket, full or empty at first, and random weighted attempts.
    When:  They are admitted on the oracle host.
    Then:  Every interval stays within capacity plus refills (P4).
    """
    policy = TokenBucketPolicy(capacity, refill, period, initial_tokens=0 if starts_empty else None)
    history = _single(policy, schedule)

    actual = token_bucket_violations(history, capacity, refill, period)

    assert actual == tuple()


@settings(deadline=None)
@given(
    amount=amounts,
    period=periods,
    tolerance=st.integers(min_value=0, max_value=3),
    schedule=schedules,
)
def test_a_leaky_bucket_keeps_its_pace(
    amount: int, period: DurationMicros, tolerance: int, schedule: list[tuple[int, int]]
) -> None:
    """
    Given: A random leaky bucket and random weighted attempts.
    When:  They are admitted on the oracle host.
    Then:  No admission arrives earlier than the schedule and tolerance allow (P4).
    """
    history = _single(LeakyBucketPolicy(amount, period, burst_tolerance=tolerance), schedule)

    actual = pacing_violations(history, amount, period, tolerance)

    assert actual == tuple()


@settings(deadline=None)
@given(
    schedule=st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=300_000),
            st.integers(min_value=1, max_value=2),
            st.sampled_from([(0,), (1,), (0, 1)]),
        ),
        min_size=1,
        max_size=40,
    )
)
def test_composition_never_breaks_either_rule(
    schedule: list[tuple[int, int, tuple[int, ...]]],
) -> None:
    """
    Given: A sliding log and a fixed window, requested alone and together at random.
    When:  The attempts are admitted on the oracle host.
    Then:  Each rule's charges satisfy its own guarantee: a composed attempt that one
           rule denied debited neither (A6).
    """
    one_second = DurationMicros(1_000_000)
    rules = (
        TraceRule("log", SlidingLogPolicy(3, one_second), scope="ankh"),
        TraceRule("window", FixedWindowPolicy(2, one_second), scope="ankh.orders"),
    )
    expected = (tuple(), tuple())

    charged = _admitted(rules, schedule)
    actual = (
        rolling_window_violations(charged["log"], 3, one_second),
        fixed_window_violations(charged["window"], 2, one_second),
    )

    assert actual == expected


VIOLATING_HISTORIES = {
    "rolling": (rolling_window_violations, [(0, 2), (999, 2)], (3, 1_000)),
    "fixed": (fixed_window_violations, [(1_000, 2), (1_999, 2)], (3, 1_000)),
    "token": (token_bucket_violations, [(0, 3), (0, 1)], (3, 1, 1_000)),
    "pacing": (pacing_violations, [(0, 1), (999, 1)], (1, 1_000)),
}

RESPECTING_HISTORIES = {
    "rolling": (rolling_window_violations, [(0, 2), (1_000, 2)], (3, 1_000)),
    "fixed": (fixed_window_violations, [(999, 2), (1_000, 2)], (3, 1_000)),
    "token": (token_bucket_violations, [(0, 3), (1_000, 1)], (3, 1, 1_000)),
    "pacing": (pacing_violations, [(0, 1), (1_000, 1)], (1, 1_000)),
}


@pytest.fixture(params=list(VIOLATING_HISTORIES))
def violating(request: pytest.FixtureRequest) -> CheckerCase:
    case = VIOLATING_HISTORIES[request.param]
    return case


@pytest.fixture(params=list(RESPECTING_HISTORIES))
def respecting(request: pytest.FixtureRequest) -> CheckerCase:
    case = RESPECTING_HISTORIES[request.param]
    return case


def test_each_checker_catches_a_violation(
    violating: CheckerCase,
) -> None:
    """
    Given: A two-admission history one microsecond inside what its policy allows.
    When:  The policy's checker reads it.
    Then:  At least one violation is reported.
    """
    checker, history, parameters = violating

    actual = checker(history, *parameters)

    assert actual


def test_each_checker_accepts_a_boundary_history(
    respecting: CheckerCase,
) -> None:
    """
    Given: The same histories moved to exactly the boundary each policy allows.
    When:  The policy's checker reads them.
    Then:  Nothing is reported; in particular a fixed window's boundary burst is not
           judged by a rolling rule.
    """
    checker, history, parameters = respecting

    actual = checker(history, *parameters)

    assert actual == tuple()


def test_violations_describe_themselves() -> None:
    """
    Given: A rolling-window violation.
    When:  It is formatted.
    Then:  It states what was admitted, where, and the limit.
    """
    (violation,) = rolling_window_violations([(DEFAULT_EPOCH_US, 2), (DEFAULT_EPOCH_US, 2)], 3, 10)
    expected = f"4 admitted in [{DEFAULT_EPOCH_US - 9}, {DEFAULT_EPOCH_US}], at most 3"

    actual = str(violation)

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
