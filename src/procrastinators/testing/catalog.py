"""The shared conformance traces and scenarios.

Every expected verdict and retry delay here was worked out by hand from the
reference semantics in ``docs/source/contracts.md`` §17; the arithmetic is shown in
the notes where it is not obvious. They are the same for every executor: the
reference Python evaluators, native Redis scripts, and third-party backends
all run exactly these traces (contract N4).

All times are offsets from :data:`T0`, which is aligned to 100 seconds, so
fixed windows and sliding-counter windows of one or ten seconds begin exactly
at ``T0``.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Final

from procrastinators.models import (
    USECS_PER_SECOND,
    Algorithms,
    DurationMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.testing.clocks import DEFAULT_EPOCH_US
from procrastinators.testing.traces import (
    Attempt,
    Burst,
    Covers,
    Expectation,
    Finish,
    Scenario,
    Trace,
    TraceRule,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from procrastinators.models import PolicySpec
else:
    pass

__all__ = [
    "ALGORITHM_TRACES",
    "COMPOSITION_TRACES",
    "CONFLICTING_PROBES",
    "PROBES",
    "SCENARIOS",
    "T0",
    "TRACES",
    "traces_for",
]

T0: Final = DEFAULT_EPOCH_US
"""The epoch every trace starts from: 2023-11-14T22:13:20Z, a multiple of 100 s."""

_S: Final = USECS_PER_SECOND


def _second(count: int = 1) -> DurationMicros:
    duration = DurationMicros(count * _S)
    return duration


def _allowed(
    at: int, cost: int = 1, *, rules: tuple[str, ...] | None = None, note: str = ""
) -> Attempt:
    attempt = Attempt(at, True, cost, rules=rules, note=note)
    return attempt


def _denied(
    at: int,
    retry_after_us: int,
    cost: int = 1,
    *,
    rules: tuple[str, ...] | None = None,
    blocking: tuple[str, ...] | None = None,
    note: str = "",
) -> Attempt:
    attempt = Attempt(at, False, cost, retry_after_us, rules, blocking, note)
    return attempt


#
# Fixed window: count per aligned window [start, start + period), where
# start = (now - offset) // period * period + offset; a denial waits for the
# window's end.

_FIXED_THREE = TraceRule("fixed", FixedWindowPolicy(3, _second()))

FIXED_WINDOW_TRACES: Final = (
    Trace(
        name="fixed_window.basics",
        description="Three per aligned second: weights, denials, the exact boundary, and idling.",
        rules=(_FIXED_THREE,),
        steps=(
            _allowed(T0, note="first admission"),
            _denied(T0 + 100_000, 900_000, 3, note="1 + 3 > 3; the window ends at T0 + 1 s"),
            _allowed(T0 + 100_000, 2, note="the denied cost of 3 consumed nothing"),
            _denied(T0 + 200_000, 800_000, note="full"),
            _denied(T0 + 999_999, 1, note="the window's last microsecond"),
            _allowed(T0 + _S, 3, note="windows start inclusive: [T0 + 1 s, T0 + 2 s)"),
            _denied(T0 + _S + 1, 999_999),
            _allowed(T0 + 5_500_000, 3, note="idle windows accumulate nothing"),
            _denied(T0 + 5_600_000, 400_000),
        ),
        contracts=("P1", "P2", "P4", "A5", "T8"),
        covers=frozenset(
            {
                Covers.FIRST_ADMISSION,
                Covers.FULL_CAPACITY,
                Covers.WEIGHTED,
                Covers.DENIAL,
                Covers.RETRY_TIMING,
                Covers.EXACT_BOUNDARY,
                Covers.IDLE_REFILL,
            }
        ),
    ),
    Trace(
        name="fixed_window.boundary_burst",
        description=(
            "Up to twice the amount across a boundary is correct fixed-window behavior, "
            "not a violation to be judged by a rolling-window rule."
        ),
        rules=(_FIXED_THREE,),
        steps=(
            _allowed(T0 + 999_000, 3),
            _allowed(T0 + _S, 3, note="six admitted within one millisecond"),
            _denied(T0 + _S, 1_000_000),
        ),
        contracts=("P4",),
        covers=frozenset({Covers.EXACT_BOUNDARY, Covers.FULL_CAPACITY}),
    ),
    Trace(
        name="fixed_window.epoch_offset",
        description="Windows align to the epoch plus the configured offset, not the whole second.",
        rules=(TraceRule("fixed", FixedWindowPolicy(2, _second(), DurationMicros(250_000))),),
        steps=(
            _allowed(T0 + 200_000, 2, note="window [T0 - 750 ms, T0 + 250 ms)"),
            _denied(T0 + 200_000, 50_000),
            _allowed(T0 + 250_000, 2, note="the offset boundary starts a new window"),
            _denied(T0 + _S, 250_000, note="a whole-second window would have reset here"),
        ),
        contracts=("T8", "P1"),
        covers=frozenset({Covers.EXACT_BOUNDARY, Covers.RETRY_TIMING}),
    ),
    Trace(
        name="fixed_window.long_running",
        description="Entry charges before the body runs; exit refunds nothing.",
        rules=(TraceRule("fixed", FixedWindowPolicy(2, _second())),),
        steps=(
            _allowed(T0),
            _allowed(T0),
            _denied(T0, 1_000_000, note="a third entrant while both bodies still run"),
            Finish(0, T0 + 100_000),
            Finish(1, T0 + 100_000),
            _denied(T0 + 200_000, 800_000, note="exits refunded nothing"),
            _allowed(T0 + _S),
            _allowed(T0 + _S),
        ),
        contracts=("A2", "A3", "W5"),
        covers=frozenset({Covers.LONG_RUNNING}),
    ),
)


#
# Sliding log: sum of costs in (now - period, now]. A denial waits until the
# oldest events whose expiry frees enough room have left: event e leaves at
# e.at + period.

_LOG_THREE = TraceRule("log", SlidingLogPolicy(3, _second()))

SLIDING_LOG_TRACES: Final = (
    Trace(
        name="sliding_log.basics",
        description=(
            "Three in any rolling second: weights, exact expiry, and oldest-blocking delays."
        ),
        rules=(_LOG_THREE,),
        steps=(
            _allowed(T0, note="first admission"),
            _denied(T0 + 100_000, 900_000, 3, note="the T0 entry must leave: T0 + 1 s"),
            _allowed(T0 + 100_000, 2, note="the denied cost of 3 consumed nothing"),
            _denied(T0 + 200_000, 800_000),
            _denied(T0 + 999_999, 1),
            _allowed(T0 + _S, note="an entry exactly one period old has left"),
            _denied(T0 + _S, 100_000, 2, note="freeing 2 needs the T0 + 100 ms entry to leave"),
            _denied(T0 + 1_050_000, 950_000, 3, note="freeing 3 needs both remaining entries"),
            _allowed(T0 + 1_100_000, 2),
            _allowed(T0 + 10 * _S, 3, note="idling leaves no credit beyond the amount"),
            _denied(T0 + 10 * _S + 1, 999_999),
        ),
        contracts=("P1", "P2", "P4", "P6", "A5"),
        covers=frozenset(
            {
                Covers.FIRST_ADMISSION,
                Covers.FULL_CAPACITY,
                Covers.WEIGHTED,
                Covers.DENIAL,
                Covers.RETRY_TIMING,
                Covers.EXACT_BOUNDARY,
                Covers.IDLE_REFILL,
            }
        ),
    ),
    Trace(
        name="sliding_log.no_boundary_burst",
        description="Unlike a fixed window, no burst is admitted across a boundary.",
        rules=(_LOG_THREE,),
        steps=(
            _allowed(T0 + 999_000, 3),
            _denied(T0 + _S, 999_000),
            _denied(T0 + 1_998_999, 1),
            _allowed(T0 + 1_999_000, 3),
        ),
        contracts=("P1", "P4"),
        covers=frozenset({Covers.EXACT_BOUNDARY, Covers.RETRY_TIMING}),
    ),
    Trace(
        name="sliding_log.long_running",
        description="Entry charges before the body runs; slow bodies do not delay expiry.",
        rules=(TraceRule("log", SlidingLogPolicy(2, _second())),),
        steps=(
            _allowed(T0),
            _allowed(T0),
            _denied(T0, 1_000_000, note="a third entrant while both bodies still run"),
            Finish(0, T0 + 100_000),
            Finish(1, T0 + 100_000),
            _denied(T0 + 200_000, 800_000, note="exits refunded nothing"),
            _allowed(T0 + _S),
            _allowed(T0 + _S),
        ),
        contracts=("A2", "A3", "W5"),
        covers=frozenset({Covers.LONG_RUNNING}),
    ),
)


#
# Token bucket: refill_amount tokens at each whole refill_period after the
# anchor, capped at capacity; a full bucket moves its anchor to now. First use
# starts with the configured tokens and anchors at that instant, admitted or
# not. A denial waits for ceil(missing / refill_amount) refills after the
# (refilled) anchor.

TOKEN_BUCKET_TRACES: Final = (
    Trace(
        name="token_bucket.basics",
        description="Capacity three, one token per second, starting full.",
        rules=(TraceRule("bucket", TokenBucketPolicy(3, 1, _second())),),
        steps=(
            _allowed(T0, note="first use: 3 tokens, anchored at T0"),
            _allowed(T0, 2, note="empty now"),
            _denied(T0 + 500_000, 500_000, note="the first refill is at T0 + 1 s"),
            _denied(T0 + 999_999, 1),
            _allowed(T0 + _S, note="one refill; anchor T0 + 1 s"),
            _denied(T0 + _S, 2_000_000, 2, note="two tokens need two refills"),
            _allowed(T0 + 10 * _S, 3, note="nine refills capped at capacity: burst after idle"),
            _denied(T0 + 10 * _S, 1_000_000, note="full bucket re-anchored at T0 + 10 s"),
            _denied(T0 + 12_500_000, 500_000, 3, note="two refills; the third is at T0 + 13 s"),
            _allowed(T0 + 13 * _S, 3, note="the denial committed nothing, yet refills continued"),
        ),
        contracts=("P4", "P7", "T6", "A5"),
        covers=frozenset(
            {
                Covers.FIRST_ADMISSION,
                Covers.FULL_CAPACITY,
                Covers.WEIGHTED,
                Covers.DENIAL,
                Covers.RETRY_TIMING,
                Covers.EXACT_BOUNDARY,
                Covers.IDLE_REFILL,
            }
        ),
    ),
    Trace(
        name="token_bucket.initially_empty",
        description=(
            "An initially empty bucket admits nothing at first, and its refill clock starts "
            "at the first attempt even though that attempt is denied."
        ),
        rules=(TraceRule("bucket", TokenBucketPolicy(2, 1, _second(), initial_tokens=0)),),
        steps=(
            _denied(T0, 1_000_000, note="first use: no tokens, anchored at T0"),
            _allowed(T0 + _S, note="only if the denied first attempt recorded its anchor"),
            _denied(T0 + _S, 1_000_000),
            _denied(T0 + 1_500_000, 500_000),
        ),
        contracts=("P5", "P4"),
        covers=frozenset({Covers.INITIAL_STATE, Covers.FIRST_ADMISSION}),
    ),
    Trace(
        name="token_bucket.drained_bucket_is_not_forgotten",
        description=(
            "A drained bucket is not indistinguishable from an unused one, so it must not be "
            "forgotten and come back full."
        ),
        rules=(TraceRule("bucket", TokenBucketPolicy(3, 1, _second())),),
        steps=(
            _allowed(T0, 3),
            _denied(T0 + 2_500_000, 500_000, 3, note="two refills so far, not a fresh bucket"),
            _allowed(T0 + 3 * _S, 3),
        ),
        contracts=("P5", "L9"),
        covers=frozenset({Covers.INITIAL_STATE, Covers.IDLE_REFILL}),
    ),
    Trace(
        name="token_bucket.long_running",
        description="Entry charges before the body runs; exit refunds nothing.",
        rules=(TraceRule("bucket", TokenBucketPolicy(2, 2, _second())),),
        steps=(
            _allowed(T0),
            _allowed(T0),
            _denied(T0, 1_000_000, note="a third entrant while both bodies still run"),
            Finish(0, T0 + 100_000),
            Finish(1, T0 + 100_000),
            _denied(T0 + 200_000, 800_000, note="exits refunded nothing"),
            _allowed(T0 + _S),
            _allowed(T0 + _S),
        ),
        contracts=("A2", "A3", "W5"),
        covers=frozenset({Covers.LONG_RUNNING}),
    ),
)


#
# Leaky bucket (virtual scheduling): the next eligible time TAT starts at the
# first attempt. Admit when TAT - now <= tolerance, where tolerance =
# floor(burst_tolerance * period / amount); then TAT = max(TAT, now) +
# ceil(cost * period / amount). A denial waits TAT - tolerance - now.

LEAKY_BUCKET_TRACES: Final = (
    Trace(
        name="leaky_bucket.basics",
        description="Two per second strictly paced: 500 ms apart, weights, and no idle credit.",
        rules=(TraceRule("pacing", LeakyBucketPolicy(2, _second())),),
        steps=(
            _allowed(T0, note="first admission; next eligible T0 + 500 ms"),
            _denied(T0 + 100_000, 400_000),
            _allowed(T0 + 500_000, note="eligible exactly on time"),
            _denied(T0 + 999_999, 1),
            _allowed(T0 + _S, 2, note="one operation of cost 2 pushes the next by 1 s"),
            _denied(T0 + 1_500_000, 500_000),
            _allowed(T0 + 10 * _S),
            _denied(T0 + 10 * _S, 500_000, note="idling earned no credit"),
        ),
        contracts=("P4", "P7", "A5"),
        covers=frozenset(
            {
                Covers.FIRST_ADMISSION,
                Covers.FULL_CAPACITY,
                Covers.WEIGHTED,
                Covers.DENIAL,
                Covers.RETRY_TIMING,
                Covers.EXACT_BOUNDARY,
                Covers.IDLE_REFILL,
            }
        ),
    ),
    Trace(
        name="leaky_bucket.fractional_interval",
        description=(
            "Three per second: each cost's interval is rounded up once per operation, "
            "never shortening a required wait."
        ),
        rules=(TraceRule("pacing", LeakyBucketPolicy(3, _second())),),
        steps=(
            _allowed(T0, note="ceil(1 s / 3) = 333 334 us"),
            _denied(T0 + 333_333, 1),
            _allowed(T0 + 333_334),
            _allowed(T0 + 666_668, 2, note="ceil(2 s / 3) = 666 667 us, not 2 * 333 334"),
            _denied(T0 + 1_333_334, 1),
            _allowed(T0 + 1_333_335),
        ),
        contracts=("T6", "P4"),
        covers=frozenset({Covers.ROUNDING, Covers.EXACT_BOUNDARY, Covers.WEIGHTED}),
    ),
    Trace(
        name="leaky_bucket.burst_tolerance",
        description=(
            "A tolerance of two lets two operations arrive early — three at once — "
            "however long the bucket idled."
        ),
        rules=(TraceRule("pacing", LeakyBucketPolicy(1, _second(), burst_tolerance=2)),),
        steps=(
            _allowed(T0),
            _allowed(T0, note="one second early"),
            _allowed(T0, note="two seconds early: the tolerance"),
            _denied(T0, 1_000_000),
            _allowed(T0 + _S),
            _denied(T0 + _S, 1_000_000),
            _allowed(T0 + 100 * _S),
            _allowed(T0 + 100 * _S),
            _allowed(T0 + 100 * _S),
            _denied(T0 + 100 * _S, 1_000_000, note="a long idle still grants only 1 + tolerance"),
        ),
        contracts=("P4", "P7"),
        covers=frozenset({Covers.IDLE_REFILL, Covers.FULL_CAPACITY}),
    ),
    Trace(
        name="leaky_bucket.long_running",
        description="Entry charges before the body runs; exit refunds nothing.",
        rules=(TraceRule("pacing", LeakyBucketPolicy(2, _second(), burst_tolerance=1)),),
        steps=(
            _allowed(T0),
            _allowed(T0, note="tolerance 500 ms"),
            _denied(T0, 500_000, note="a third entrant while both bodies still run"),
            Finish(0, T0 + 100_000),
            Finish(1, T0 + 100_000),
            _denied(T0 + 200_000, 300_000, note="exits refunded nothing"),
            _allowed(T0 + _S),
            _allowed(T0 + _S),
        ),
        contracts=("A2", "A3", "W5"),
        covers=frozenset({Covers.LONG_RUNNING}),
    ),
)


#
# Sliding counter: epoch-aligned windows; with e = now - window start, admit
# when floor(previous * (period - e) / period) + current + cost <= amount.
# A denial waits for the first microsecond at which that holds with no other
# admissions, rolling current into previous at the next window if needed.

SLIDING_COUNTER_TRACES: Final = (
    Trace(
        name="sliding_counter.basics",
        description="Four per second, weighted by the previous window.",
        rules=(TraceRule("counter", SlidingCounterPolicy(4, _second())),),
        steps=(
            _allowed(T0, 3, note="first admission"),
            _allowed(T0 + 500_000, note="current window full"),
            _denied(
                T0 + 600_000,
                400_001,
                note="next window: floor(4 * 999 999 / 1 000 000) = 3 at T0 + 1 s + 1 us",
            ),
            _denied(T0 + _S, 1, note="at the rollover the previous window weighs in fully"),
            _allowed(T0 + _S + 1),
            _denied(T0 + 1_250_000, 1, note="floor(4 * 0.75) + 1 + 1 = 5"),
            _denied(T0 + 1_400_000, 100_001, 2, note="needs floor(4 * r) <= 1: r < 0.5"),
            _allowed(T0 + 1_500_001, 2, note="floor(4 * 0.499 999) + 1 + 2 = 4"),
            _allowed(T0 + 5 * _S, 4, note="two idle windows reset both counters"),
        ),
        contracts=("P4", "N3", "A5"),
        covers=frozenset(
            {
                Covers.FIRST_ADMISSION,
                Covers.FULL_CAPACITY,
                Covers.WEIGHTED,
                Covers.DENIAL,
                Covers.RETRY_TIMING,
                Covers.EXACT_BOUNDARY,
                Covers.IDLE_REFILL,
            }
        ),
    ),
    Trace(
        name="sliding_counter.approximation",
        description=(
            "The counter is approximate: it admits eight within one rolling second here, "
            "which is why it must never stand in for a sliding log."
        ),
        rules=(TraceRule("counter", SlidingCounterPolicy(4, _second())),),
        steps=(
            _allowed(T0 + 999_999, 4),
            _allowed(T0 + 1_900_000, 4, note="floor(4 * 0.1) = 0 of the previous window"),
            _denied(T0 + 1_900_000, 100_001),
        ),
        contracts=("P4",),
        covers=frozenset({Covers.WEIGHTED, Covers.RETRY_TIMING}),
    ),
    Trace(
        name="sliding_counter.long_running",
        description="Entry charges before the body runs; exit refunds nothing.",
        rules=(TraceRule("counter", SlidingCounterPolicy(2, _second())),),
        steps=(
            _allowed(T0),
            _allowed(T0),
            _denied(T0, 1_000_001, note="a third entrant while both bodies still run"),
            Finish(0, T0 + 100_000),
            Finish(1, T0 + 100_000),
            _denied(T0 + 200_000, 800_001, note="exits refunded nothing"),
            _allowed(T0 + 1_500_000, note="floor(2 * 0.5) + 0 + 1 = 2"),
            _denied(T0 + 1_500_000, 1),
        ),
        contracts=("A2", "A3", "W5"),
        covers=frozenset({Covers.LONG_RUNNING}),
    ),
)


#
# Composition: every constraint evaluated against one time sample; all admit
# or nothing is debited; the delay is the largest blocking delay.

COMPOSITION_TRACES: Final = (
    Trace(
        name="composition.later_rule_denies",
        description=(
            "The account rule sorts first and would admit, the endpoint rule sorts later and "
            "denies; the account must not be debited."
        ),
        rules=(
            TraceRule("account", SlidingLogPolicy(3, _second()), scope="ankh"),
            TraceRule("endpoint", SlidingLogPolicy(1, _second()), scope="ankh.orders"),
        ),
        steps=(
            _allowed(T0),
            _denied(T0 + 100_000, 900_000, blocking=("endpoint",)),
            _denied(T0 + 200_000, 800_000, blocking=("endpoint",)),
            _allowed(
                T0 + 300_000, 2, rules=("account",), note="account holds 1 + 2: no partial debit"
            ),
            _denied(T0 + 400_000, 600_000, rules=("account",), blocking=("account",)),
        ),
        contracts=("A6", "C1", "C5"),
        covers=frozenset({Covers.COMPOSITION, Covers.DENIAL}),
    ),
    Trace(
        name="composition.largest_delay",
        description="When several rules deny, the delay is the largest of theirs.",
        rules=(
            TraceRule("window", FixedWindowPolicy(1, _second()), scope="ankh"),
            TraceRule("log", SlidingLogPolicy(1, _second(2)), scope="ankh.orders"),
        ),
        steps=(
            _allowed(T0 + 500_000),
            _denied(T0 + 600_000, 1_900_000, blocking=("log", "window")),
            _denied(T0 + _S, 1_500_000, blocking=("log",), note="the window has reset"),
            _allowed(T0 + 2_500_000),
        ),
        contracts=("C7", "A6"),
        covers=frozenset({Covers.COMPOSITION, Covers.RETRY_TIMING}),
    ),
    Trace(
        name="composition.exact_limit_with_pacing",
        description=(
            "A sliding log and a leaky bucket compose in one admission; when only the pacing "
            "denies, the log is not debited."
        ),
        rules=(
            TraceRule("exact", SlidingLogPolicy(3, _second()), scope="ankh"),
            TraceRule("pacing", LeakyBucketPolicy(3, _second()), scope="ankh"),
        ),
        steps=(
            _allowed(T0),
            _denied(T0 + 100_000, 233_334, blocking=("pacing",)),
            _allowed(T0 + 333_334),
            _allowed(T0 + 666_668, note="a debit at T0 + 100 ms would have filled the log"),
            _denied(T0 + _S, 2, blocking=("pacing",)),
            _allowed(T0 + 1_000_002),
        ),
        contracts=("P8", "A6"),
        covers=frozenset({Covers.COMPOSITION, Covers.ROUNDING}),
    ),
    Trace(
        name="composition.duplicates_charge_once",
        description="Naming the same constraint twice charges it once.",
        rules=(TraceRule("account", SlidingLogPolicy(2, _second()), scope="ankh"),),
        steps=(
            _allowed(T0, rules=("account", "account")),
            _allowed(T0, rules=("account", "account")),
            _denied(T0, 1_000_000, rules=("account", "account")),
        ),
        contracts=("C2",),
        covers=frozenset({Covers.COMPOSITION}),
    ),
)


ALGORITHM_TRACES: Final[Mapping[str, tuple[Trace, ...]]] = {
    Algorithms.FIXED_WINDOW: FIXED_WINDOW_TRACES,
    Algorithms.SLIDING_LOG: SLIDING_LOG_TRACES,
    Algorithms.TOKEN_BUCKET: TOKEN_BUCKET_TRACES,
    Algorithms.LEAKY_BUCKET: LEAKY_BUCKET_TRACES,
    Algorithms.SLIDING_COUNTER: SLIDING_COUNTER_TRACES,
}
"""The single-rule traces of each built-in algorithm."""

TRACES: Final = (
    *FIXED_WINDOW_TRACES,
    *SLIDING_LOG_TRACES,
    *TOKEN_BUCKET_TRACES,
    *LEAKY_BUCKET_TRACES,
    *SLIDING_COUNTER_TRACES,
    *COMPOSITION_TRACES,
)
"""Every trace."""


def traces_for(algorithm_id: str) -> tuple[Trace, ...]:
    """The single-rule traces for ``algorithm_id``; empty for a third-party algorithm.

    :param algorithm_id: An algorithm id.
    """
    traces = tuple(ALGORITHM_TRACES.get(algorithm_id, tuple()))
    return traces


#
# Failures demonstrated against existing rate limiters, as finite scenarios.
# Each records what every policy should do, so a fixed window is not judged by
# a sliding log's rule.

SCENARIOS: Final = (
    Scenario(
        name="ratelimit.boundary_burst",
        source="tomasbasham/ratelimit: limits(calls=10, period=10)",
        description="Ten calls just before a ten-second boundary, ten just after.",
        bursts=(Burst(T0 + 9 * _S, 10), Burst(T0 + 10_001_000, 10)),
        expectations=(
            Expectation(
                "fixed_window",
                FixedWindowPolicy(10, _second(10)),
                (10, 10),
                "twenty in a second is correct for aligned ten-second windows",
            ),
            Expectation(
                "sliding_log",
                SlidingLogPolicy(10, _second(10)),
                (10, 0),
                "no rolling ten seconds holds more than ten",
            ),
            Expectation(
                "sliding_counter",
                SlidingCounterPolicy(10, _second(10)),
                (10, 1),
                "floor(10 * 0.9999) = 9 leaves room for one: approximate by design",
            ),
            Expectation(
                "token_bucket",
                TokenBucketPolicy(10, 1, _second()),
                (10, 1),
                "a full bucket bursts ten, then one token has refilled",
            ),
            Expectation(
                "leaky_bucket",
                LeakyBucketPolicy(10, _second(10)),
                (1, 1),
                "strict pacing admits one per second",
            ),
        ),
        contracts=("P4",),
    ),
    Scenario(
        name="ratelimiter.charge_on_exit",
        source="RazerM/ratelimiter: RateLimiter(10, 1) with ten-second bodies",
        description="Fifty-one entrants at once, each holding the limiter for ten seconds.",
        bursts=(Burst(T0, 51, body_us=10 * _S),),
        expectations=(
            Expectation(
                "fixed_window",
                FixedWindowPolicy(10, _second()),
                (10,),
                "charged on entry, so the eleventh entrant is refused",
            ),
            Expectation(
                "sliding_log",
                SlidingLogPolicy(10, _second()),
                (10,),
                "charged on entry, so the eleventh entrant is refused",
            ),
            Expectation(
                "sliding_counter",
                SlidingCounterPolicy(10, _second()),
                (10,),
                "charged on entry, so the eleventh entrant is refused",
            ),
            Expectation(
                "token_bucket",
                TokenBucketPolicy(10, 10, _second()),
                (10,),
                "charged on entry, so the eleventh entrant is refused",
            ),
            Expectation(
                "leaky_bucket",
                LeakyBucketPolicy(10, _second()),
                (1,),
                "strict pacing admits one at a time",
            ),
        ),
        contracts=("A2", "A3"),
    ),
    Scenario(
        name="limit.replenish_after_completion",
        source="enricobacis/limit: limit(10, 1) with ten-second bodies",
        description=(
            "Ten entrants a second for three seconds, each body running ten seconds: capacity "
            "returns on schedule while the bodies are still running."
        ),
        bursts=(
            Burst(T0, 10, body_us=10 * _S),
            Burst(T0 + _S, 10, body_us=10 * _S),
            Burst(T0 + 2 * _S, 10, body_us=10 * _S),
        ),
        expectations=(
            Expectation(
                "fixed_window",
                FixedWindowPolicy(10, _second()),
                (10, 10, 10),
                "each second is a new window",
            ),
            Expectation(
                "sliding_log",
                SlidingLogPolicy(10, _second()),
                (10, 10, 10),
                "admissions age from when they were recorded, not when bodies finish",
            ),
            Expectation(
                "sliding_counter",
                SlidingCounterPolicy(10, _second()),
                (10, 0, 10),
                "at each rollover the full previous window weighs in; it is approximate",
            ),
            Expectation(
                "token_bucket",
                TokenBucketPolicy(10, 10, _second()),
                (10, 10, 10),
                "ten tokens refill each second",
            ),
            Expectation(
                "leaky_bucket",
                LeakyBucketPolicy(10, _second()),
                (1, 1, 1),
                "strict pacing admits one per tenth of a second",
            ),
        ),
        contracts=("W5", "A2"),
    ),
    Scenario(
        name="example.replacement_token_bucket",
        source="a hand-written replacement: RateLimiter(10, 1), starting empty",
        description=(
            "One call, a ten-second idle, then two bursts nine tenths of a second apart. The "
            "replacement bucket admitted ten after idling and nine more 0.9 s later."
        ),
        bursts=(Burst(T0, 1), Burst(T0 + 10 * _S, 12), Burst(T0 + 10_900_000, 12)),
        expectations=(
            Expectation(
                "token_bucket",
                TokenBucketPolicy(10, 1, DurationMicros(100_000), initial_tokens=0),
                (0, 10, 9),
                "idle time accumulates up to capacity: exactly the measured behavior",
            ),
            Expectation(
                "sliding_log",
                SlidingLogPolicy(10, _second()),
                (1, 10, 0),
                "no rolling second holds more than ten",
            ),
            Expectation(
                "fixed_window",
                FixedWindowPolicy(10, _second()),
                (1, 10, 0),
                "both later bursts fall in one aligned second",
            ),
            Expectation(
                "sliding_counter",
                SlidingCounterPolicy(10, _second()),
                (1, 10, 0),
                "both later bursts fall in one window after an empty one",
            ),
            Expectation(
                "leaky_bucket",
                LeakyBucketPolicy(10, _second()),
                (1, 1, 1),
                "no idle credit: one per tenth of a second",
            ),
        ),
        contracts=("P4", "P7"),
    ),
)
"""Failures demonstrated against existing rate limiters, one expectation per policy.

Its fifth point — sleeping while holding a lock — concerns waiters, not
admission, and is covered by contract W1 and the recording sleepers.
"""


PROBES: Final[Mapping[str, PolicySpec]] = {
    Algorithms.FIXED_WINDOW: FixedWindowPolicy(1, _second()),
    Algorithms.SLIDING_LOG: SlidingLogPolicy(1, _second()),
    Algorithms.TOKEN_BUCKET: TokenBucketPolicy(1, 1, _second()),
    Algorithms.LEAKY_BUCKET: LeakyBucketPolicy(1, _second()),
    Algorithms.SLIDING_COUNTER: SlidingCounterPolicy(1, _second()),
}
"""A capacity-one policy per algorithm.

At one instant the first attempt admits and the next denies.

The failure-injection checks use them to tell "nothing was committed" from
"the debit stands".
"""

CONFLICTING_PROBES: Final[Mapping[str, PolicySpec]] = {
    Algorithms.FIXED_WINDOW: FixedWindowPolicy(2, _second()),
    Algorithms.SLIDING_LOG: SlidingLogPolicy(2, _second()),
    Algorithms.TOKEN_BUCKET: TokenBucketPolicy(2, 1, _second()),
    Algorithms.LEAKY_BUCKET: LeakyBucketPolicy(2, _second()),
    Algorithms.SLIDING_COUNTER: SlidingCounterPolicy(2, _second()),
}
"""For each probe, a policy of the same algorithm with different parameters."""


if __name__ == "__main__":
    pass
else:
    pass
