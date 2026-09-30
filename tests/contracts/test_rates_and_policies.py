"""Contract tests for durations, rates, and normalized policies (sections 3-4)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import copy
import datetime as dt

import pytest

from procrastinators import (
    FixedWindowPolicy,
    InvalidPolicy,
    LeakyBucketPolicy,
    Limit,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.models import (
    MAX_AMOUNT,
    MAX_EXACT_INT,
    MAX_PERIOD_US,
    MIN_PERIOD_US,
    Algorithms,
    DurationMicros,
    Policy,
    duration_to_micros,
)

ONE_SECOND = DurationMicros(1_000_000)

MICROS_BY_SPELLING: dict[str | int | dt.timedelta, int] = {
    "1s": 1_000_000,
    "250ms": 250_000,
    "5m": 300_000_000,
    "2h": 7_200_000_000,
    "1d": 86_400_000_000,
    "1500us": 1_500,
    "1": 1_000_000,
    " 2 s ": 2_000_000,
    "1S": 1_000_000,
    3: 3_000_000,
    dt.timedelta(minutes=1): 60_000_000,
    dt.timedelta(milliseconds=1500): 1_500_000,
}

UNINTELLIGIBLE_DURATIONS: dict[str, object] = {
    "empty": "",
    "word": "soon",
    "unknown unit": "1 fortnight",
    "negative string": "-1s",
    "trailing digits": "1s2",
    "unit only": "s",
    "infinity": float("inf"),
    "nan": float("nan"),
    "negative int": -1,
    "boolean": True,
    "none": None,
    "list": list(),
}

IMPOSSIBLE_AMOUNTS: dict[str, object] = {
    "zero": 0,
    "negative": -1,
    "above maximum": MAX_AMOUNT + 1,
    "fractional": 1.5,
    "string": "10",
}

OUT_OF_RANGE_PERIODS = {
    "one microsecond": "1us",
    "just under a millisecond": "999us",
    "above maximum": f"{MAX_PERIOD_US + 1}us",
}


@pytest.fixture(
    params=list(MICROS_BY_SPELLING.items()),
    ids=[repr(spelling) for spelling in MICROS_BY_SPELLING],
)
def spelled_duration(request: pytest.FixtureRequest) -> tuple[str | int | dt.timedelta, int]:
    spelling_and_micros: tuple[str | int | dt.timedelta, int] = request.param
    return spelling_and_micros


@pytest.fixture(params=list(UNINTELLIGIBLE_DURATIONS.values()), ids=list(UNINTELLIGIBLE_DURATIONS))
def unintelligible_duration(request: pytest.FixtureRequest) -> object:
    # Deep-copied because one value is a list, which a test could otherwise mutate.
    duration: object = copy.deepcopy(request.param)
    return duration


@pytest.fixture(params=list(IMPOSSIBLE_AMOUNTS.values()), ids=list(IMPOSSIBLE_AMOUNTS))
def impossible_amount(request: pytest.FixtureRequest) -> object:
    amount: object = request.param
    return amount


@pytest.fixture(params=list(OUT_OF_RANGE_PERIODS.values()), ids=list(OUT_OF_RANGE_PERIODS))
def out_of_range_period(request: pytest.FixtureRequest) -> str:
    period: str = request.param
    return period


@pytest.fixture
def every_policy() -> list[Policy]:
    policies: list[Policy] = [
        FixedWindowPolicy(10, ONE_SECOND),
        SlidingLogPolicy(10, ONE_SECOND),
        SlidingCounterPolicy(10, ONE_SECOND),
        TokenBucketPolicy(10, 10, ONE_SECOND),
        LeakyBucketPolicy(10, ONE_SECOND),
    ]
    return policies


def test_the_clerks_agree_on_what_a_duration_means(
    spelled_duration: tuple[str | int | dt.timedelta, int],
) -> None:
    """
    Given: A duration spelled as a unit string, a bare number, or a timedelta.
    When:  It is converted to microseconds.
    Then:  Every spelling yields the same agreed value.
    """
    spelling, expected = spelled_duration

    actual = duration_to_micros(spelling)

    assert actual == expected


def test_a_float_duration_means_its_decimal_spelling() -> None:
    """
    Given: Float durations of 0.1 and 0.3 seconds.
    When:  They are converted to microseconds.
    Then:  They mean their decimal spelling, not the nearest binary value (N6).
    """
    assert duration_to_micros(0.1) == 100_000
    assert duration_to_micros(0.3) == 300_000


def test_a_remainder_lengthens_the_period_rather_than_shortening_it() -> None:
    """
    Given: A duration with a fractional microsecond.
    When:  It is converted to microseconds.
    Then:  It rounds up, the stricter reading of N per T (T6).
    """
    expected = 2

    actual = duration_to_micros("0.0000015s")

    assert actual == expected


def test_the_auditors_reject_an_unintelligible_duration(unintelligible_duration: object) -> None:
    """
    Given: A value that is not an intelligible, finite, non-negative duration.
    When:  It is converted to microseconds.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        duration_to_micros(unintelligible_duration)  # ty: ignore[invalid-argument-type]


def test_a_limit_states_a_rate_without_choosing_an_algorithm() -> None:
    """
    Given: A limit of 10 per second.
    When:  Its amount, period, and string form are read.
    Then:  They state the rate without naming an algorithm.
    """
    limit = Limit(10, per="1s")

    assert limit.amount == 10
    assert limit.period_us == 1_000_000
    assert str(limit) == "10 per 1s"


def test_a_limit_defaults_to_one_second() -> None:
    """
    Given: A limit with no period given.
    When:  Its period is read.
    Then:  It is one second.
    """
    expected = 1_000_000

    actual = Limit(5).period_us

    assert actual == expected


def test_lu_tze_will_not_accept_a_boolean_as_a_rate() -> None:
    """
    Given: A boolean amount.
    When:  A limit is built with it.
    Then:  InvalidPolicy is raised, because bool is an int subclass and Limit(True)
           would otherwise silently mean 1 per second (N5).
    """
    with pytest.raises(InvalidPolicy, match="bool"):
        Limit(True, per="1s")


def test_an_impossible_amount_is_refused(impossible_amount: object) -> None:
    """
    Given: An amount that is non-positive, too large, or not an integer.
    When:  A limit is built with it.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        Limit(impossible_amount, per="1s")  # ty: ignore[invalid-argument-type]


def test_a_period_outside_the_supported_range_is_refused(out_of_range_period: str) -> None:
    """
    Given: A period below a millisecond or above the maximum.
    When:  A limit is built with it.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        Limit(10, per=out_of_range_period)


def test_the_shortest_supported_period_is_exactly_a_millisecond() -> None:
    """
    Given: A limit with a one-millisecond period.
    When:  Its period is read.
    Then:  It is exactly the minimum supported period.
    """
    expected = MIN_PERIOD_US

    actual = Limit(1, per="1ms").period_us

    assert actual == expected


def test_every_policy_carries_a_stable_algorithm_id_and_state_version(
    every_policy: list[Policy],
) -> None:
    """
    Given: One policy of each built-in algorithm.
    When:  Their algorithm ids and state versions are read.
    Then:  The ids cover every algorithm and each version is 1, since these strings
           and numbers are part of the on-disk contract (I4, I5).
    """
    expected = set(Algorithms)

    actual = {policy.algorithm for policy in every_policy}

    assert actual == expected
    assert all(policy.state_version == 1 for policy in every_policy)


def test_a_fixed_window_offset_must_fall_inside_its_window() -> None:
    """
    Given: Fixed-window offsets just inside and exactly one window.
    When:  Policies are built with them.
    Then:  The inside offset is accepted and a whole-window offset is refused,
           because alignment is to the epoch plus an offset, and an offset of a
           whole window is the same window shifted nowhere (T8).
    """
    FixedWindowPolicy(10, ONE_SECOND, DurationMicros(999_999))

    with pytest.raises(InvalidPolicy, match="epoch_offset_us"):
        FixedWindowPolicy(10, ONE_SECOND, DurationMicros(1_000_000))


def test_a_sliding_counter_refuses_a_weighting_no_executor_could_compute_exactly() -> None:
    """
    Given: A modest sliding counter, and one at the maximum amount and period.
    When:  The policies are built.
    Then:  The modest one is accepted and the maximal one refused, because
           amount * period must stay inside the double-precision integer range (N3).
    """
    SlidingCounterPolicy(1000, DurationMicros(60 * 1_000_000))

    with pytest.raises(InvalidPolicy, match=str(MAX_EXACT_INT)):
        SlidingCounterPolicy(MAX_AMOUNT, DurationMicros(MAX_PERIOD_US))


def test_a_token_bucket_starts_full_unless_told_otherwise() -> None:
    """
    Given: Token buckets with default and with zero initial tokens.
    When:  Their starting tokens are read.
    Then:  The default starts at capacity and the explicit one at zero, so the
           difference is observable (P4).
    """
    assert TokenBucketPolicy(10, 10, ONE_SECOND).starting_tokens == 10
    assert TokenBucketPolicy(10, 10, ONE_SECOND, 0).starting_tokens == 0


def test_a_token_bucket_cannot_start_with_more_than_it_holds() -> None:
    """
    Given: Initial tokens above the bucket's capacity.
    When:  A token bucket is built with them.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy, match="initial_tokens"):
        TokenBucketPolicy(10, 10, ONE_SECOND, initial_tokens=11)


def test_a_leaky_bucket_grants_no_burst_by_default() -> None:
    """
    Given: Leaky buckets with default and with explicit burst tolerance.
    When:  Their burst tolerance and capacity are read.
    Then:  The default tolerates no early arrival, unlike a token bucket (P7), and
           the largest single cost is one period's amount whatever the tolerance,
           because weighting and tolerance are specified separately (P4).
    """
    expected = (0, 10, 10)

    actual = (
        LeakyBucketPolicy(10, ONE_SECOND).burst_tolerance,
        LeakyBucketPolicy(10, ONE_SECOND).capacity,
        LeakyBucketPolicy(10, ONE_SECOND, burst_tolerance=5).capacity,
    )

    assert actual == expected


def test_policies_are_immutable() -> None:
    """
    Given: A sliding-log policy.
    When:  One of its fields is assigned.
    Then:  AttributeError is raised.
    """
    policy = SlidingLogPolicy(10, ONE_SECOND)

    with pytest.raises(AttributeError):
        policy.amount = 11  # ty: ignore[invalid-assignment]


if __name__ == "__main__":
    pass
else:
    pass
