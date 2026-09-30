"""Policy normalization, clocks, and the public-seconds boundary.

Normalization turns what a vendor wrote into integers without inventing
capacity; the clocks keep the three time domains apart; budgets translate the
facade's ``timeout`` exactly as contract B2 says.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import datetime as dt
from fractions import Fraction

import pytest
from hypothesis import given
from hypothesis import strategies as st

from procrastinators.clocks import (
    Deadline,
    LocalEpochClock,
    MonotonicClock,
    SystemClock,
    budget_for_timeout,
    seconds_to_micros,
)
from procrastinators.errors import InvalidPolicy
from procrastinators.models import (
    MAX_AMOUNT,
    MAX_DURATION_US,
    MAX_PERIOD_US,
    MIN_PERIOD_US,
    Algorithms,
    DurationMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    Limit,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.policies import normalize_limit, normalize_limits, resolve_algorithm
from procrastinators.protocols import AdmissionClock, DeadlineClock
from procrastinators.testing import FakeTimeline

ONE_SECOND = DurationMicros(1_000_000)

NORMALIZED = {
    Algorithms.FIXED_WINDOW: FixedWindowPolicy(10, ONE_SECOND),
    Algorithms.SLIDING_LOG: SlidingLogPolicy(10, ONE_SECOND),
    Algorithms.SLIDING_COUNTER: SlidingCounterPolicy(10, ONE_SECOND),
    Algorithms.TOKEN_BUCKET: TokenBucketPolicy(10, 1, DurationMicros(100_000)),
    Algorithms.LEAKY_BUCKET: LeakyBucketPolicy(10, ONE_SECOND),
}


@pytest.fixture(params=list(NORMALIZED), ids=[algorithm.value for algorithm in NORMALIZED])
def algorithm(request: pytest.FixtureRequest) -> Algorithms:
    chosen: Algorithms = request.param
    return chosen


def test_ten_per_second_normalizes_per_algorithm(algorithm: Algorithms) -> None:
    """
    Given: Ten per second.
    When:  It is normalized under each algorithm, by member and by string value.
    Then:  Each yields its documented policy; the token bucket refills one token per
           100 ms, the finest exact spelling of the rate.
    """
    expected = NORMALIZED[algorithm]

    actual = normalize_limit(Limit(10, per="1s"), algorithm)

    assert actual == expected
    assert normalize_limit(Limit(10, per="1s"), algorithm.value) == expected


@pytest.mark.parametrize(
    ("amount", "per", "refill"),
    [
        (3, "1s", (3, 1_000_000)),
        (1_000_000, "1s", (1_000, 1_000)),
        (7, "1ms", (7, 1_000)),
        (MAX_AMOUNT, "1ms", (MAX_AMOUNT, 1_000)),
    ],
    ids=["coprime", "scaled to the minimum period", "at the minimum", "largest amount"],
)
def test_a_token_bucket_rate_is_exact(amount: int, per: str, refill: tuple[int, int]) -> None:
    """
    Given: Rates whose reduced form is coprime, finer than a millisecond, or at the bounds.
    When:  They are normalized to token buckets.
    Then:  The refill is the reduced rate scaled only up to the minimum period, and
           equals the rate exactly.
    """
    limit = Limit(amount, per=per)
    expected = refill

    policy = normalize_limit(limit, Algorithms.TOKEN_BUCKET)
    assert isinstance(policy, TokenBucketPolicy)
    actual = (policy.refill_amount, policy.refill_period_us)

    assert actual == expected
    assert Fraction(policy.refill_amount, policy.refill_period_us) == Fraction(
        amount, limit.period_us
    )


@given(
    amount=st.integers(min_value=1, max_value=MAX_AMOUNT),
    period=st.integers(min_value=MIN_PERIOD_US, max_value=MAX_PERIOD_US),
)
def test_every_token_bucket_normalization_is_exact_and_valid(amount: int, period: int) -> None:
    """
    Given: Any supported amount and period.
    When:  They are normalized to a token bucket.
    Then:  The refill rate equals the limit's rate exactly, the refill period is at
           least the minimum, and no refill exceeds capacity.
    """
    limit = Limit(amount, per=dt.timedelta(microseconds=period))

    policy = normalize_limit(limit, Algorithms.TOKEN_BUCKET)

    assert isinstance(policy, TokenBucketPolicy)
    assert Fraction(policy.refill_amount, policy.refill_period_us) == Fraction(amount, period)
    assert policy.refill_period_us >= MIN_PERIOD_US
    assert policy.refill_amount <= policy.capacity


def test_algorithm_options_reach_their_policy() -> None:
    """
    Given: An epoch offset, a partial initial balance with a larger capacity, and a
           burst tolerance.
    When:  Each is passed to the algorithm it applies to.
    Then:  The policies carry them.
    """
    limit = Limit(10, per="1s")
    expected = (
        FixedWindowPolicy(10, ONE_SECOND, DurationMicros(250_000)),
        TokenBucketPolicy(20, 1, DurationMicros(100_000), initial_tokens=5),
        LeakyBucketPolicy(10, ONE_SECOND, burst_tolerance=3),
    )

    actual = (
        normalize_limit(limit, Algorithms.FIXED_WINDOW, {"epoch_offset": "250ms"}),
        normalize_limit(limit, Algorithms.TOKEN_BUCKET, {"capacity": 20, "initial_tokens": 5}),
        normalize_limit(limit, Algorithms.LEAKY_BUCKET, {"burst_tolerance": 3}),
    )

    assert actual == expected


@pytest.mark.parametrize(
    ("algorithm", "options", "message"),
    [
        (Algorithms.SLIDING_LOG, {"burst_tolerance": 1}, "do not apply"),
        (Algorithms.TOKEN_BUCKET, {"initial_tokens": 11}, "initial_tokens"),
        (Algorithms.TOKEN_BUCKET, {"capacity": True}, "bool"),
        (Algorithms.LEAKY_BUCKET, {"burst_tolerance": 1.5}, "integer"),
        (Algorithms.FIXED_WINDOW, {"epoch_offset": "1s"}, "epoch_offset_us"),
        (Algorithms.FIXED_WINDOW, {"epoch_offset": object()}, "duration"),
    ],
    ids=[
        "inapplicable",
        "initial above capacity",
        "bool capacity",
        "float tolerance",
        "offset of a whole period",
        "offset of the wrong type",
    ],
)
def test_impossible_options_are_refused(
    algorithm: Algorithms, options: dict[str, object], message: str
) -> None:
    """
    Given: An option that does not apply, is out of range, or has the wrong type.
    When:  A limit is normalized with it.
    Then:  InvalidPolicy names the problem.
    """
    with pytest.raises(InvalidPolicy, match=message):
        normalize_limit(Limit(10, per="1s"), algorithm, options)


def test_an_unknown_algorithm_is_refused_not_guessed() -> None:
    """
    Given: An algorithm name that is not built in.
    When:  It is resolved.
    Then:  InvalidPolicy lists the built-ins and says custom algorithms bring their own policy.
    """
    with pytest.raises(InvalidPolicy, match="custom algorithm"):
        resolve_algorithm("hogswatch.teatime")


def test_a_sliding_counter_that_overflows_exactness_is_refused() -> None:
    """
    Given: A limit whose amount times period exceeds 2**53 - 1.
    When:  It is normalized to a sliding counter.
    Then:  InvalidPolicy is raised rather than computing differently in Lua (N3).
    """
    with pytest.raises(InvalidPolicy, match="amount \\* period_us"):
        normalize_limit(Limit(MAX_AMOUNT, per="1d"), Algorithms.SLIDING_COUNTER)


def test_limits_normalize_in_order_and_need_at_least_one() -> None:
    """
    Given: Two limits, and then none.
    When:  They are normalized together.
    Then:  Order is kept (positional names follow it); an empty list is refused.
    """
    expected = (SlidingLogPolicy(10, ONE_SECOND), SlidingLogPolicy(500, DurationMicros(60_000_000)))

    actual = normalize_limits([Limit(10, per="1s"), Limit(500, per="1m")])

    assert actual == expected
    with pytest.raises(InvalidPolicy, match="at least one"):
        normalize_limits([])


def test_a_non_limit_is_refused() -> None:
    """
    Given: A bare number where a Limit belongs.
    When:  It is normalized.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy, match="expected a Limit"):
        normalize_limit(10)  # ty: ignore[invalid-argument-type]


def test_real_clocks_satisfy_their_protocols() -> None:
    """
    Given: The monotonic, system, and local epoch clocks.
    When:  They are checked against the clock protocols and read twice.
    Then:  Each satisfies its protocol, and the monotonic and local epoch clocks never
           go backwards.
    """
    monotonic = MonotonicClock()
    local = LocalEpochClock()

    assert isinstance(monotonic, DeadlineClock)
    assert isinstance(SystemClock(), AdmissionClock)
    assert isinstance(local, AdmissionClock)
    assert monotonic.now() <= monotonic.now()
    assert local.now() <= local.now()


def test_the_local_epoch_clock_aligns_to_the_epoch_and_ignores_wall_steps() -> None:
    """
    Given: A local epoch clock anchored at a known wall time and monotonic reading.
    When:  Monotonic time advances by 1.5 s while the wall clock is stepped back.
    Then:  It reads the anchor plus 1.5 s: epoch-aligned (T8) and never backwards (T7).
    """
    readings = {"wall": 1_700_000_000_000_000_000, "monotonic": 42_000}

    def wall_ns() -> int:
        return readings["wall"]

    def monotonic_ns() -> int:
        return readings["monotonic"]

    clock = LocalEpochClock(wall_ns=wall_ns, monotonic_ns=monotonic_ns)
    readings["monotonic"] += 1_500_000_000
    readings["wall"] -= 3_600_000_000_000
    expected = 1_700_000_000_000_000 + 1_500_000

    actual = clock.now()

    assert actual == expected


def test_a_clock_before_the_epoch_is_refused() -> None:
    """
    Given: A wall clock reading before 1970.
    When:  A local epoch clock is built from it.
    Then:  InvalidPolicy is raised rather than a negative timestamp stored.
    """

    def before_epoch() -> int:
        return -1_000

    with pytest.raises(InvalidPolicy, match="outside the supported range"):
        LocalEpochClock(wall_ns=before_epoch)


SECONDS = {
    "zero": (0, 0),
    "fraction": (0.1, 100_000),
    "string": ("250ms", 250_000),
    "timedelta": (dt.timedelta(seconds=2), 2_000_000),
    "sub-microsecond rounds up": (0.0000001, 1),
}


@pytest.mark.parametrize(("value", "micros"), list(SECONDS.values()), ids=list(SECONDS))
def test_public_seconds_cross_the_boundary_exactly(value: object, micros: int) -> None:
    """
    Given: Seconds written as a number, a string, or a timedelta.
    When:  They are converted at the public boundary.
    Then:  They are exact microseconds, a sub-microsecond remainder rounding up (T3, T6).
    """
    expected = micros

    actual = seconds_to_micros(value, what="timeout")  # ty: ignore[invalid-argument-type]

    assert actual == expected


@pytest.mark.parametrize(
    ("value", "message"),
    [(-1, "negative"), (True, "bool"), (float("nan"), "finite"), (MAX_DURATION_US, "at most")],
    ids=["negative", "bool", "nan", "too long"],
)
def test_nonsense_seconds_are_refused(value: object, message: str) -> None:
    """
    Given: A negative, boolean, non-finite, or overlong duration.
    When:  It is converted.
    Then:  InvalidPolicy names the argument and the problem.
    """
    with pytest.raises(InvalidPolicy, match=f"timeout.*{message}|{message}"):
        seconds_to_micros(value, what="timeout")  # ty: ignore[invalid-argument-type]


def test_zero_is_refused_where_it_means_unbounded() -> None:
    """
    Given: A zero storage timeout.
    When:  It is converted with zero disallowed.
    Then:  InvalidPolicy says it must be positive (B3).
    """
    with pytest.raises(InvalidPolicy, match="positive"):
        seconds_to_micros(0, what="storage_timeout", allow_zero=False)


def test_timeout_none_waits_indefinitely_with_bounded_storage_calls() -> None:
    """
    Given: timeout=None and a two-second storage timeout.
    When:  The budget is built.
    Then:  No deadline, waiting allowed, and every storage call still bounded (B2, B3).
    """
    timeline = FakeTimeline()
    expected = (None, True, 2_000_000, 2_000_000)

    budget = budget_for_timeout(None, clock=timeline.deadline_clock, storage_timeout=2)
    actual = (
        budget.deadline_us,
        budget.wait_for_quota,
        budget.storage_timeout_us,
        budget.lock_timeout_us,
    )

    assert actual == expected


def test_timeout_zero_is_exactly_one_attempt() -> None:
    """
    Given: timeout=0 at monotonic time 5 s.
    When:  The budget is built.
    Then:  The deadline is now and quota is not waited for (B2).
    """
    timeline = FakeTimeline(monotonic_us=5_000_000)
    expected = (5_000_000, False)

    budget = budget_for_timeout(0, clock=timeline.deadline_clock)
    actual = (budget.deadline_us, budget.wait_for_quota)

    assert actual == expected


def test_a_positive_timeout_is_a_local_deadline() -> None:
    """
    Given: timeout=30 at monotonic time 5 s, and a separate lock timeout.
    When:  The budget is built.
    Then:  The deadline is 35 s of local monotonic time, and the lock timeout is its own.
    """
    timeline = FakeTimeline(monotonic_us=5_000_000)
    expected = (35_000_000, True, 250_000)

    budget = budget_for_timeout(30, clock=timeline.deadline_clock, lock_timeout="250ms")
    actual = (budget.deadline_us, budget.wait_for_quota, budget.lock_timeout_us)

    assert actual == expected


def test_a_deadline_counts_down_and_caps_other_waits() -> None:
    """
    Given: A deadline three seconds away.
    When:  Two seconds pass, then two more.
    Then:  One second remains and caps a five-second storage call; then none remains
           and the deadline has expired.
    """
    timeline = FakeTimeline()
    deadline = Deadline.after(DurationMicros(3_000_000), timeline.deadline_clock)
    expected = [(1_000_000, 1_000_000, False), (0, 0, True)]

    actual = list()
    for _ in range(2):
        timeline.advance(2_000_000)
        actual.append(
            (deadline.remaining(), deadline.cap(DurationMicros(5_000_000)), deadline.expired())
        )

    assert actual == expected


def test_no_deadline_never_expires_and_caps_nothing() -> None:
    """
    Given: A deadline built from a budget with none.
    When:  A long time passes.
    Then:  Nothing remains to count, it never expires, and it leaves a duration alone.
    """
    timeline = FakeTimeline()
    budget = budget_for_timeout(None, clock=timeline.deadline_clock)
    deadline = Deadline.from_budget(budget, timeline.deadline_clock)
    expected = (None, False, 5_000_000)

    timeline.advance(10**12)
    actual = (deadline.remaining(), deadline.expired(), deadline.cap(DurationMicros(5_000_000)))

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
