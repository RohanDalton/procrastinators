"""Property checks of the reference algorithms over random histories.

Three independent kinds of evidence, each over hundreds of generated
histories with small and very large periods, weighted costs, and repeated
denials:

* **Agreement with the oracles.** ``tests/oracles.py`` finds each retry delay by
  searching forward for the first admitting microsecond; the reference
  evaluators use closed forms. Identical verdicts and delays at every step mean
  the closed forms round exactly as contract §17 requires.
* **Guarantees.** Whatever the reference admits satisfies its policy's own
  guarantee (contract P4).
* **Safe forgetting.** Past the horizon an evaluator reports, forgetting the
  state changes no decision (contract L9), and states survive their codec.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from procrastinators.algorithms import (
    FixedWindow,
    LeakyBucket,
    SlidingCounter,
    SlidingLog,
    TokenBucket,
)
from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    MAX_DURATION_US,
    MAX_EXACT_INT,
    MAX_PERIOD_US,
    MIN_PERIOD_US,
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
from procrastinators.state import UNUSED, plan_admission
from procrastinators.testing import (
    DEFAULT_EPOCH_US,
    EvaluatorHost,
    FakeTimeline,
    fixed_window_violations,
    pacing_violations,
    rolling_window_violations,
    token_bucket_violations,
)
from tests.oracles import (
    FixedWindowOracle,
    LeakyBucketOracle,
    SlidingCounterOracle,
    SlidingLogOracle,
    TokenBucketOracle,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from procrastinators.algorithms import ReferenceAlgorithm
    from procrastinators.models import PolicySpec
    from procrastinators.protocols import Algorithm, RuleState
    from procrastinators.state import AdmissionPlan
    from procrastinators.testing import Violation
else:
    pass

RULE = RuleId(QuotaIdentity("unseen-university", "high-energy-magic"), "hex")

# Room between the trace epoch and the largest timestamp, split so that even a
# history of maximal gaps leaves half the range for schedules running ahead.
_MAX_GAP = (MAX_EXACT_INT - DEFAULT_EPOCH_US) // 120

periods = st.one_of(
    st.integers(min_value=MIN_PERIOD_US, max_value=3_000_000),
    st.integers(min_value=MIN_PERIOD_US, max_value=MAX_PERIOD_US),
    st.sampled_from([MIN_PERIOD_US, 1_000_003, MAX_PERIOD_US]),
).map(DurationMicros)
amounts = st.integers(min_value=1, max_value=7)


@st.composite
def fixed_windows(draw: st.DrawFn) -> FixedWindowPolicy:
    period = draw(periods)
    offset = draw(st.integers(min_value=0, max_value=period - 1))
    policy = FixedWindowPolicy(draw(amounts), period, DurationMicros(offset))
    return policy


@st.composite
def sliding_logs(draw: st.DrawFn) -> SlidingLogPolicy:
    policy = SlidingLogPolicy(draw(amounts), draw(periods))
    return policy


@st.composite
def token_buckets(draw: st.DrawFn) -> TokenBucketPolicy:
    capacity = draw(amounts)
    refill = draw(st.integers(min_value=1, max_value=4))
    # Filling an empty bucket must take a representable duration (N3).
    fill_limit = MAX_DURATION_US // -(-capacity // refill)
    policy = TokenBucketPolicy(
        capacity=capacity,
        refill_amount=refill,
        refill_period_us=draw(periods.filter(lambda period: period <= fill_limit)),
        initial_tokens=draw(st.none() | st.integers(min_value=0, max_value=capacity)),
    )
    return policy


@st.composite
def leaky_buckets(draw: st.DrawFn) -> LeakyBucketPolicy:
    # A twelve-year period with a burst of five still runs the schedule no
    # further ahead than the timestamp range these histories leave free.
    policy = LeakyBucketPolicy(
        draw(amounts),
        draw(periods.filter(lambda period: period <= MAX_PERIOD_US // 8)),
        burst_tolerance=draw(st.integers(0, 5)),
    )
    return policy


@st.composite
def sliding_counters(draw: st.DrawFn) -> SlidingCounterPolicy:
    amount = draw(amounts)
    period = draw(periods.filter(lambda period: period * 7 <= MAX_EXACT_INT))
    policy = SlidingCounterPolicy(amount, period)
    return policy


@st.composite
def schedules(draw: st.DrawFn, policy: PolicySpec) -> list[tuple[int, int]]:
    """``(gap, cost)`` attempts, with gaps scaled to the policy's period."""
    period = getattr(policy, "period_us", None) or policy.refill_period_us  # ty: ignore[unresolved-attribute]
    ceiling = min(3 * period, _MAX_GAP)
    gaps = st.one_of(
        st.just(0),
        st.integers(min_value=0, max_value=min(period // 4, ceiling)),
        st.integers(min_value=0, max_value=ceiling),
        st.sampled_from([min(period, ceiling), max(0, min(period, ceiling) - 1)]),
    )
    costs = st.integers(min_value=1, max_value=policy.capacity)
    schedule = draw(st.lists(st.tuples(gaps, costs), min_size=1, max_size=50))
    return schedule


def _constraint(policy: PolicySpec) -> Constraint:
    constraint = Constraint(RULE, policy, policy_fingerprint(policy))
    return constraint


class _Model:
    """One rule evaluated by ``plan_admission`` alone, recording state and horizon."""

    def __init__(self, algorithm: Algorithm[Any], policy: PolicySpec) -> None:
        self.algorithm = algorithm
        self.constraint = _constraint(policy)
        self.state: RuleState = UNUSED
        self.horizon: EpochMicros | None = None

    def resolve(self, constraint: Constraint) -> Algorithm[Any]:
        del constraint
        return self.algorithm

    def plan(self, now: int, cost: int, state: RuleState | None = None) -> AdmissionPlan:
        request = AdmissionRequest((self.constraint,), cost)
        chosen = self.state if state is None else state
        plan = plan_admission(request, {RULE: chosen}, self.resolve, EpochMicros(now))
        return plan

    def step(self, now: int, cost: int) -> AdmissionPlan:
        plan = self.plan(now, cost)
        if RULE in plan.writes:
            self.state = plan.writes[RULE]
            self.horizon = plan.transitions[0].safe_forget_after_us
        else:
            pass
        return plan


def _agree_and_collect(
    reference: ReferenceAlgorithm,
    oracle: Algorithm[Any],
    policy: PolicySpec,
    schedule: Sequence[tuple[int, int]],
) -> tuple[list[tuple[int, int]], _Model, int]:
    """Run ``schedule`` on the reference model and the oracle host; assert every step agrees."""
    timeline = FakeTimeline()
    host = EvaluatorHost([oracle], clock=timeline.epoch_clock)
    model = _Model(reference, policy)
    admitted = list()
    for index, (gap, cost) in enumerate(schedule):
        timeline.advance(gap)
        now = timeline.peek_epoch()
        expected = host.admit(AdmissionRequest((model.constraint,), cost))
        actual = model.step(now, cost).decision(expected.admission if expected.allowed else None)
        assert (actual.allowed, actual.retry_after_us) == (
            expected.allowed,
            expected.retry_after_us,
        ), f"step {index} at +{now - DEFAULT_EPOCH_US} µs cost {cost} of {schedule}"
        if actual.allowed:
            admitted.append((now, cost))
        else:
            pass
    return admitted, model, timeline.peek_epoch()


_PROPERTY_SETTINGS = settings(
    max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)


def _check(
    reference: ReferenceAlgorithm,
    oracle: Algorithm[Any],
    policy: PolicySpec,
    schedule: Sequence[tuple[int, int]],
    violations: Callable[[list[tuple[int, int]]], tuple[Violation, ...]],
    probe: tuple[int, int],
) -> None:
    admitted, model, last = _agree_and_collect(reference, oracle, policy, schedule)
    expected_violations: tuple[Violation, ...] = tuple()

    assert violations(admitted) == expected_violations
    assert reference.codec.decode(reference.codec.encode(model.state)) == model.state
    if model.horizon is not None:
        extra, cost = probe
        later = max(last, model.horizon) + extra
        kept = model.plan(later, min(cost, policy.capacity))
        forgotten = model.plan(later, min(cost, policy.capacity), UNUSED)
        assert (kept.admitted, kept.retry_after_us) == (
            forgotten.admitted,
            forgotten.retry_after_us,
        ), "forgetting state past its horizon changed a decision (L9)"
    else:
        pass


probes = st.tuples(st.integers(min_value=0, max_value=10**9), st.integers(1, 7))


@_PROPERTY_SETTINGS
@given(data=st.data(), probe=probes)
def test_fixed_window_agrees_with_its_oracle(data: st.DataObject, probe: tuple[int, int]) -> None:
    """
    Given: A random fixed-window policy and a schedule of weighted attempts.
    When:  The reference evaluator and the search-based oracle both run it.
    Then:  Every verdict and retry delay agrees, the admissions respect each aligned
           window, and forgetting state past its horizon changes nothing.
    """
    policy = data.draw(fixed_windows())
    schedule = data.draw(schedules(policy))

    def violations(admitted: list[tuple[int, int]]) -> tuple[Violation, ...]:
        found = fixed_window_violations(
            admitted, policy.amount, policy.period_us, policy.epoch_offset_us
        )
        return found

    _check(FixedWindow(), FixedWindowOracle(), policy, schedule, violations, probe)


@_PROPERTY_SETTINGS
@given(data=st.data(), probe=probes)
def test_sliding_log_agrees_with_its_oracle(data: st.DataObject, probe: tuple[int, int]) -> None:
    """
    Given: A random sliding-log policy and a schedule of weighted attempts.
    When:  The reference evaluator and the search-based oracle both run it.
    Then:  Every verdict and retry delay agrees, no rolling window ever holds more
           than ``amount``, and forgetting state past its horizon changes nothing.
    """
    policy = data.draw(sliding_logs())
    schedule = data.draw(schedules(policy))

    def violations(admitted: list[tuple[int, int]]) -> tuple[Violation, ...]:
        found = rolling_window_violations(admitted, policy.amount, policy.period_us)
        return found

    _check(SlidingLog(), SlidingLogOracle(), policy, schedule, violations, probe)


@_PROPERTY_SETTINGS
@given(data=st.data(), probe=probes)
def test_token_bucket_agrees_with_its_oracle(data: st.DataObject, probe: tuple[int, int]) -> None:
    """
    Given: A random token-bucket policy, full or not at the start, and a schedule.
    When:  The reference evaluator and the search-based oracle both run it.
    Then:  Every verdict and retry delay agrees, the admissions stay inside the
           burst-plus-refill envelope, and forgetting past a horizon changes nothing.
    """
    policy = data.draw(token_buckets())
    schedule = data.draw(schedules(policy))

    def violations(admitted: list[tuple[int, int]]) -> tuple[Violation, ...]:
        found = token_bucket_violations(
            admitted, policy.capacity, policy.refill_amount, policy.refill_period_us
        )
        return found

    _check(TokenBucket(), TokenBucketOracle(), policy, schedule, violations, probe)


@_PROPERTY_SETTINGS
@given(data=st.data(), probe=probes)
def test_leaky_bucket_agrees_with_its_oracle(data: st.DataObject, probe: tuple[int, int]) -> None:
    """
    Given: A random leaky-bucket policy with or without burst tolerance, and a schedule.
    When:  The reference evaluator and the search-based oracle both run it.
    Then:  Every verdict and retry delay agrees, the admissions respect the pace,
           and forgetting state past its horizon changes nothing.
    """
    policy = data.draw(leaky_buckets())
    schedule = data.draw(schedules(policy))

    def violations(admitted: list[tuple[int, int]]) -> tuple[Violation, ...]:
        found = pacing_violations(admitted, policy.amount, policy.period_us, policy.burst_tolerance)
        return found

    _check(LeakyBucket(), LeakyBucketOracle(), policy, schedule, violations, probe)


@_PROPERTY_SETTINGS
@given(data=st.data(), probe=probes)
def test_sliding_counter_agrees_with_its_oracle(
    data: st.DataObject, probe: tuple[int, int]
) -> None:
    """
    Given: A random sliding-counter policy and a schedule of weighted attempts.
    When:  The reference evaluator and the search-based oracle both run it.
    Then:  Every verdict and closed-form retry delay agrees with the searched one,
           no aligned window exceeds ``amount``, and forgetting changes nothing.
    """
    policy = data.draw(sliding_counters())
    schedule = data.draw(schedules(policy))

    def violations(admitted: list[tuple[int, int]]) -> tuple[Violation, ...]:
        found = fixed_window_violations(admitted, policy.amount, policy.period_us)
        return found

    _check(SlidingCounter(), SlidingCounterOracle(), policy, schedule, violations, probe)


if __name__ == "__main__":
    pass
else:
    pass
