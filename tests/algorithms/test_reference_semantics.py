"""Specific behaviors of the reference algorithms that traces alone do not pin down.

Safe-forget horizons, the default burst difference between the two buckets,
weighted single entries, fail-closed truncation, numeric bounds, and the
purity rule: no algorithm module may import a clock, a lock, a sleep, or I/O.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import ast
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

import procrastinators.algorithms
from procrastinators.algorithms import (
    FixedWindow,
    LeakyBucket,
    SlidingCounter,
    SlidingLog,
    TokenBucket,
)
from procrastinators.errors import InvalidCost, InvalidPolicy
from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    MAX_DURATION_US,
    MAX_PERIOD_US,
    MAX_TIMESTAMP_US,
    AdmissionRequest,
    Constraint,
    DurationMicros,
    EpochMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    QuotaIdentity,
    RuleId,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.protocols import LogEvent, RuleState, StateRepresentation
from procrastinators.state import UNUSED, MaterializedView, plan_admission
from procrastinators.testing import DEFAULT_EPOCH_US

if TYPE_CHECKING:
    from procrastinators.algorithms import ReferenceAlgorithm
    from procrastinators.models import PolicySpec
    from procrastinators.protocols import Algorithm
    from procrastinators.state import AdmissionPlan
else:
    pass

T0 = EpochMicros(DEFAULT_EPOCH_US)
SECOND = DurationMicros(1_000_000)
RULE = RuleId(QuotaIdentity("ankh-morpork", "post-office"), "clacks")


class _Rule:
    """Drives one rule through ``plan_admission``, keeping its state between attempts."""

    def __init__(self, algorithm: Algorithm[Any], policy: PolicySpec) -> None:
        self.algorithm = algorithm
        self.constraint = Constraint(RULE, policy, policy_fingerprint(policy))
        self.state: RuleState = UNUSED

    def resolve(self, constraint: Constraint) -> Algorithm[Any]:
        del constraint
        return self.algorithm

    def attempt(self, now: int, cost: int = 1) -> AdmissionPlan:
        plan = plan_admission(
            AdmissionRequest((self.constraint,), cost),
            {RULE: self.state},
            self.resolve,
            EpochMicros(now),
        )
        self.state = plan.writes.get(RULE, self.state)
        return plan

    def admitted(self, now: int, attempts: int, cost: int = 1) -> int:
        count = sum(1 for _ in range(attempts) if self.attempt(now, cost).admitted)
        return count


def test_vimes_finds_token_and_leaky_buckets_differ_after_idling() -> None:
    """
    Given: A token bucket and a leaky bucket, both five per second, idle for ten seconds.
    When:  Ten attempts arrive at once at each.
    Then:  The token bucket spends its banked burst of five; the strictly paced leaky
           bucket admits one and asks the next to wait a fifth of a second (P7).
    """
    expected = {"token_bucket": 5, "leaky_bucket": 1, "leaky_retry": 200_000}
    token = _Rule(TokenBucket(), TokenBucketPolicy(5, 5, SECOND))
    leaky = _Rule(LeakyBucket(), LeakyBucketPolicy(5, SECOND))
    token.attempt(T0)
    leaky.attempt(T0)
    later = T0 + 10 * SECOND

    actual = {
        "token_bucket": token.admitted(later, 10),
        "leaky_bucket": leaky.admitted(later, 10),
        "leaky_retry": leaky.attempt(later).retry_after_us,
    }

    assert actual == expected


@pytest.mark.parametrize(
    ("algorithm", "policy", "horizon"),
    [
        (FixedWindow(), FixedWindowPolicy(3, SECOND), T0 + SECOND),
        (SlidingLog(), SlidingLogPolicy(3, SECOND), T0 + 250_000 + SECOND),
        (TokenBucket(), TokenBucketPolicy(3, 1, SECOND), T0 + 250_000 + SECOND),
        (LeakyBucket(), LeakyBucketPolicy(4, SECOND), T0 + 250_000 + 250_000),
        (SlidingCounter(), SlidingCounterPolicy(3, SECOND), T0 + 2 * SECOND),
    ],
    ids=["fixed_window", "sliding_log", "token_bucket", "leaky_bucket", "sliding_counter"],
)
def test_each_algorithm_reports_when_its_state_stops_mattering(
    algorithm: Algorithm[Any], policy: PolicySpec, horizon: int
) -> None:
    """
    Given: A rule admitting one unit a quarter of a second into an aligned second.
    When:  The admitting transition is inspected.
    Then:  Its safe-forget horizon is where contract L9 puts it for that algorithm:
           the window's end, the newest entry leaving, the bucket refilling, the
           schedule passing, or two windows on.
    """
    rule = _Rule(algorithm, policy)

    actual = rule.attempt(T0 + 250_000).transitions[0].safe_forget_after_us

    assert actual == horizon


def test_rincewind_never_forgets_a_bucket_that_started_empty() -> None:
    """
    Given: A token bucket configured to start empty, and one that starts full.
    When:  Each admits once.
    Then:  Only the full-starting bucket ever becomes forgettable: re-creating the
           empty-starting one would hand back fewer tokens than it had refilled to,
           which is distinguishable, so P5 forbids forgetting it.
    """
    empty = _Rule(TokenBucket(), TokenBucketPolicy(2, 1, SECOND, initial_tokens=0))
    full = _Rule(TokenBucket(), TokenBucketPolicy(2, 1, SECOND))
    empty.attempt(T0)

    actual = (
        empty.attempt(T0 + SECOND).transitions[0].safe_forget_after_us,
        full.attempt(T0).transitions[0].safe_forget_after_us,
    )

    assert actual == (None, T0 + SECOND)


def test_a_denied_first_attempt_still_starts_the_refill_clock() -> None:
    """
    Given: A token bucket that starts empty.
    When:  Its first attempt is denied.
    Then:  The initial balance and refill anchor are recorded anyway (P5), so the
           next attempt one period later is admitted rather than restarting the clock.
    """
    rule = _Rule(TokenBucket(), TokenBucketPolicy(2, 1, SECOND, initial_tokens=0))

    first = rule.attempt(T0)
    second = rule.attempt(T0 + SECOND)

    assert (first.admitted, first.retry_after_us, second.admitted) == (False, SECOND, True)
    assert dict(rule.state.scalars) == {"tokens": 0, "anchor": T0 + SECOND}


def test_a_weighted_admission_is_one_log_entry() -> None:
    """
    Given: A sliding log of a hundred per second.
    When:  One operation of cost forty is admitted.
    Then:  The log holds one entry weighing forty, not forty entries (P6).
    """
    rule = _Rule(SlidingLog(), SlidingLogPolicy(100, SECOND))

    rule.attempt(T0, cost=40)

    assert rule.state.events == (LogEvent(T0, 40),)


def test_a_sliding_log_prunes_expired_entries_even_when_denying() -> None:
    """
    Given: A full sliding log of two per second, whose oldest entry is exactly one period old.
    When:  An attempt of cost two, which still cannot fit, arrives.
    Then:  It is denied until the remaining entry leaves, and the expired entry is
           pruned: an entry exactly ``period`` old has left the window (P1), and
           pruning consumes nothing (A5).
    """
    rule = _Rule(SlidingLog(), SlidingLogPolicy(2, SECOND))
    rule.attempt(T0)
    rule.attempt(T0 + 500_000)

    plan = rule.attempt(T0 + SECOND, cost=2)

    assert (plan.admitted, plan.retry_after_us) == (False, 500_000)
    assert rule.state.events == (LogEvent(EpochMicros(T0 + 500_000), 1),)


def test_a_truncated_history_is_denied_rather_than_guessed() -> None:
    """
    Given: A view marked truncated, as a backend reports when more entries are live
           than the requirements allow materializing.
    When:  The sliding log evaluates it.
    Then:  It denies for one whole period, however little the visible entries weigh (S6).
    """
    policy = SlidingLogPolicy(3, SECOND)
    view = MaterializedView(
        RULE,
        scalars=dict(),
        events=(LogEvent(T0, 1),),
        declared=frozenset(),
        exists=True,
        truncated=True,
    )

    transition = SlidingLog().evaluate(policy, view, EpochMicros(T0 + 1), 1)

    assert (transition.admitted, transition.retry_after_us) == (False, SECOND)


def test_the_sliding_counter_weights_the_previous_window_down_with_floor() -> None:
    """
    Given: A sliding counter of ten per second whose previous window admitted ten.
    When:  Attempts arrive 0.3 s and 0.35 s into the next window.
    Then:  At 0.3 s the previous count weighs floor(10 * 0.7) = 7, so three fit; at
           0.35 s it weighs floor(6.5) = 6 and one more fits (E5).
    """
    rule = _Rule(SlidingCounter(), SlidingCounterPolicy(10, SECOND))
    rule.admitted(T0, 10)

    actual = (rule.admitted(T0 + 1_300_000, 5), rule.admitted(T0 + 1_350_000, 5))

    assert actual == (3, 1)


def test_hundred_year_windows_keep_their_arithmetic_exact() -> None:
    """
    Given: A sliding log allowing one admission per hundred years.
    When:  It admits once and is asked again a microsecond later.
    Then:  The denial waits exactly the rest of the century, within the duration range.
    """
    rule = _Rule(SlidingLog(), SlidingLogPolicy(1, DurationMicros(MAX_PERIOD_US)))
    rule.attempt(T0)

    plan = rule.attempt(T0 + 1)

    assert plan.retry_after_us == MAX_PERIOD_US - 1
    assert plan.retry_after_us <= MAX_DURATION_US


def test_a_leaky_bucket_refuses_to_schedule_past_the_timestamp_range() -> None:
    """
    Given: A leaky bucket of one per hundred years, near the end of the timestamp range.
    When:  An admission would move its schedule past the largest supported timestamp.
    Then:  It raises ``InvalidPolicy`` instead of writing an out-of-range value (N2).
    """
    rule = _Rule(LeakyBucket(), LeakyBucketPolicy(1, DurationMicros(MAX_PERIOD_US)))

    with pytest.raises(InvalidPolicy, match="timestamp range"):
        rule.attempt(MAX_TIMESTAMP_US - 1)


@pytest.mark.parametrize(
    ("policy_type", "arguments"),
    [
        (TokenBucketPolicy, (2**31 - 1, 1, DurationMicros(MAX_PERIOD_US))),
        (LeakyBucketPolicy, (1, DurationMicros(MAX_PERIOD_US), 3)),
    ],
    ids=["token_bucket_fill_time", "leaky_bucket_run_ahead"],
)
def test_bucket_policies_whose_delays_exceed_the_duration_range_are_rejected(
    policy_type: type, arguments: tuple[int, ...]
) -> None:
    """
    Given: A token bucket that would take millennia to refill, and a leaky bucket whose
           tolerance lets its schedule run centuries ahead.
    When:  Either policy is constructed.
    Then:  ``InvalidPolicy`` is raised: a retry delay no executor can represent is
           rejected up front (N2, N3).
    """
    with pytest.raises(InvalidPolicy, match="beyond the supported"):
        policy_type(*arguments)


def test_evaluate_rejects_a_cost_above_capacity(reference: ReferenceAlgorithm) -> None:
    """
    Given: Any reference algorithm, called directly with a cost above its capacity.
    When:  It evaluates.
    Then:  ``InvalidCost`` is raised rather than a denial with an unbounded delay (P3).
    """
    policies: dict[str, PolicySpec] = {
        "fixed_window": FixedWindowPolicy(2, SECOND),
        "sliding_log": SlidingLogPolicy(2, SECOND),
        "token_bucket": TokenBucketPolicy(2, 1, SECOND),
        "leaky_bucket": LeakyBucketPolicy(2, SECOND),
        "sliding_counter": SlidingCounterPolicy(2, SECOND),
    }
    view = MaterializedView(
        RULE, scalars=dict(), events=(), declared=frozenset(), exists=False, truncated=False
    )

    with pytest.raises(InvalidCost):
        reference.evaluate(policies[reference.id], view, T0, 3)


def test_validate_rejects_another_algorithms_policy(reference: ReferenceAlgorithm) -> None:
    """
    Given: Any reference algorithm.
    When:  It is asked to validate a policy of a different algorithm.
    Then:  ``InvalidPolicy`` names the type it evaluates.
    """
    stranger = (
        SlidingLogPolicy(1, SECOND)
        if reference.id != "sliding_log"
        else FixedWindowPolicy(1, SECOND)
    )

    with pytest.raises(InvalidPolicy, match=reference.policy_type.__name__):
        reference.validate(stranger)


def test_only_the_sliding_log_needs_an_event_log(reference: ReferenceAlgorithm) -> None:
    """
    Given: Any reference algorithm and a policy for it.
    When:  Its state requirements are read.
    Then:  The sliding log asks for one period of at most ``amount`` entries; the
           others ask for scalars only, so a constant-state store can host them.
    """
    policies: dict[str, PolicySpec] = {
        "fixed_window": FixedWindowPolicy(2, SECOND),
        "sliding_log": SlidingLogPolicy(2, SECOND),
        "token_bucket": TokenBucketPolicy(2, 1, SECOND),
        "leaky_bucket": LeakyBucketPolicy(2, SECOND),
        "sliding_counter": SlidingCounterPolicy(2, SECOND),
    }

    requirements = reference.requirements(policies[reference.id])

    if reference.id == "sliding_log":
        assert requirements.representation == StateRepresentation.EVENT_LOG
        assert requirements.events is not None
        assert (requirements.events.horizon_us, requirements.events.max_events) == (SECOND, 2)
    else:
        assert requirements.representation == StateRepresentation.SCALARS
        assert requirements.events is None


_FORBIDDEN_MODULES = frozenset(
    {"asyncio", "io", "os", "random", "select", "socket", "sqlite3", "threading", "time"}
)


def test_lu_tze_finds_no_clock_lock_or_io_in_any_algorithm_module() -> None:
    """
    Given: Every module in ``procrastinators.algorithms``.
    When:  Their imports and calls are read from source.
    Then:  None imports a clock, lock, sleep, randomness, or I/O module, and none
           calls ``open``: evaluation is pure (contract R8).
    """
    package = procrastinators.algorithms.__file__
    assert package is not None
    directory = Path(package).parent
    expected: dict[str, list[str]] = dict()
    found = dict()
    for module in sorted(directory.glob("*.py")):
        tree = ast.parse(module.read_text())
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            (node.module or "").split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        opened = any(
            isinstance(node, ast.Call) and getattr(node.func, "id", None) == "open"
            for node in ast.walk(tree)
        )
        if problems := sorted(imported & _FORBIDDEN_MODULES) + (["open()"] if opened else []):
            found[module.name] = problems
        else:
            pass

    actual = found

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
