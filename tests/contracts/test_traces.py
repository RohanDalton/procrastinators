"""The shared traces are right, complete, and independent of any implementation.

The expected values in the catalog were computed by hand from the contract's
closed forms. Here they are replayed against the naive oracles in
``tests/oracles.py``, which find retry delays by searching forward in time. The
two methods share nothing but the contract, so agreement means the traces say
what the contract says.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import ast
import dataclasses
import re
from typing import TYPE_CHECKING

import pytest

from procrastinators.models import Algorithms, canonical_constraints
from procrastinators.testing import (
    ALGORITHM_TRACES,
    COMPOSITION_TRACES,
    SCENARIOS,
    TRACES,
    Attempt,
    BackendSubject,
    Covers,
    EvaluatorHost,
    FakeTimeline,
    Finish,
    Trace,
    TraceRule,
    run_trace,
)
from procrastinators.testing.harness import DEFAULT_NAMESPACE
from procrastinators.testing.traces import TraceStep
from tests.oracles import all_oracles

if TYPE_CHECKING:
    from pathlib import Path
else:
    pass

EVERY_TRACE = (
    *TRACES,
    *(
        scenario.trace_for(expectation)
        for scenario in SCENARIOS
        for expectation in scenario.expectations
    ),
)

REQUIRED_COVERAGE = frozenset(
    {
        Covers.FIRST_ADMISSION,
        Covers.FULL_CAPACITY,
        Covers.EXACT_BOUNDARY,
        Covers.IDLE_REFILL,
        Covers.WEIGHTED,
        Covers.DENIAL,
        Covers.RETRY_TIMING,
        Covers.LONG_RUNNING,
    }
)


@pytest.fixture(params=EVERY_TRACE, ids=[trace.name for trace in EVERY_TRACE])
def trace(request: pytest.FixtureRequest) -> Trace:
    chosen: Trace = request.param
    return chosen


@pytest.fixture(params=list(Algorithms), ids=[algorithm.value for algorithm in Algorithms])
def algorithm_id(request: pytest.FixtureRequest) -> str:
    chosen: str = request.param
    return chosen


@pytest.fixture(scope="session")
def contract_rules(project_root: Path) -> frozenset[str]:
    """Every numbered rule in docs/source/contracts.md."""
    text = (project_root / "docs" / "source" / "contracts.md").read_text()
    rules = frozenset(re.findall(r"^\*\*([A-Z]\d+)\.\*\*", text, flags=re.MULTILINE))
    return rules


def test_the_auditors_agree_with_every_hand_computed_trace(trace: Trace) -> None:
    """
    Given: A hand-computed trace or compiled scenario.
    When:  It is replayed against the search-based oracles on the evaluator host.
    Then:  Every verdict, retry delay, blocking rule, and timestamp matches.
    """
    timeline = FakeTimeline()
    host = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock)

    report = run_trace(trace, BackendSubject(host), timeline)

    report.raise_for_mismatches()


def test_a_miscounted_trace_is_caught() -> None:
    """
    Given: The sliding-log basics trace with one retry delay off by a microsecond.
    When:  It is replayed against the oracles.
    Then:  Exactly that step is reported, so the replay is evidence rather than ceremony.
    """
    original = ALGORITHM_TRACES[Algorithms.SLIDING_LOG][0]
    index = 3
    step = original.steps[index]
    assert isinstance(step, Attempt)
    assert step.retry_after_us is not None
    miscounted = dataclasses.replace(
        original,
        steps=(
            *original.steps[:index],
            dataclasses.replace(step, retry_after_us=step.retry_after_us + 1),
            *original.steps[index + 1 :],
        ),
    )
    timeline = FakeTimeline()
    host = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock)
    expected = [index]

    report = run_trace(miscounted, BackendSubject(host), timeline)
    actual = [mismatch.step for mismatch in report.mismatches]

    assert actual == expected


def test_every_algorithm_has_the_required_traces(algorithm_id: str) -> None:
    """
    Given: One of the five algorithms.
    When:  The coverage of its traces is collected.
    Then:  It includes first admission, full capacity, exact boundaries, idle refill,
           weighted admission, denial, retry timing, and long-running work.
    """
    expected: frozenset[Covers] = frozenset()
    covered = frozenset().union(*(chosen.covers for chosen in ALGORITHM_TRACES[algorithm_id]))

    actual = REQUIRED_COVERAGE - covered

    assert actual == expected


def test_every_algorithm_trace_uses_only_its_algorithm(algorithm_id: str) -> None:
    """
    Given: The traces filed under one algorithm.
    When:  The algorithms their rules use are collected.
    Then:  Only that algorithm appears.
    """
    expected = {frozenset({algorithm_id})}

    actual = {chosen.algorithms for chosen in ALGORITHM_TRACES[algorithm_id]}

    assert actual == expected


def test_a_later_rule_denies_where_an_earlier_one_would_admit() -> None:
    """
    Given: The composition traces.
    When:  Each denial's blocking rules are compared with the canonical rule order.
    Then:  At least one denial is blocked only by a rule that sorts after a rule that
           did not block, which is the case a partial debit would get wrong (A6).
    """
    found = list()
    for chosen in COMPOSITION_TRACES:
        by_label = {rule.label: rule for rule in chosen.rules}
        for step in chosen.steps:
            if not isinstance(step, Attempt) or step.allowed or step.blocking is None:
                continue
            else:
                pass
            requested = step.rules or tuple(by_label)
            ordered = canonical_constraints(
                [by_label[label].constraint(DEFAULT_NAMESPACE) for label in requested]
            )
            label_of = chosen.labels_of(DEFAULT_NAMESPACE)
            order = [label_of[constraint.rule] for constraint in ordered]
            first_blocking = min(order.index(label) for label in step.blocking)
            if first_blocking > 0:
                found.append((chosen.name, step.at))
            else:
                pass

    assert found


def test_every_scenario_records_all_five_policies() -> None:
    """
    Given: The scenarios of failures demonstrated against existing rate limiters.
    When:  The algorithms of their expectations are collected.
    Then:  Every scenario states the outcome under all five algorithms, rather than
           judging every policy by a sliding log.
    """
    expected = {scenario.name: frozenset(Algorithms) for scenario in SCENARIOS}

    actual = {
        scenario.name: frozenset(
            expectation.policy.algorithm for expectation in scenario.expectations
        )
        for scenario in SCENARIOS
    }

    assert actual == expected


def test_traces_cite_only_rules_that_exist(contract_rules: frozenset[str]) -> None:
    """
    Given: Every trace and scenario.
    When:  The contract rules they cite are compared with docs/source/contracts.md.
    Then:  Each cited rule exists.
    """
    expected: set[str] = set()
    cited = {rule for chosen in TRACES for rule in chosen.contracts}
    cited |= {rule for scenario in SCENARIOS for rule in scenario.contracts}

    actual = cited - contract_rules

    assert actual == expected


def test_expected_traces_import_no_implementation(project_root: Path) -> None:
    """
    Given: The modules that define traces and scenarios.
    When:  Their imports are read from the syntax tree.
    Then:  None imports an algorithm, a backend, or the state planner: the expected
           values cannot have been produced by the code they check.
    """
    expected: set[str] = set()
    forbidden = ("procrastinators.algorithms", "procrastinators.backends", "procrastinators.state")
    imported: set[str] = set()
    for name in ("catalog.py", "traces.py"):
        tree = ast.parse((project_root / "src" / "procrastinators" / "testing" / name).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
            elif isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            else:
                pass

    actual = {module for module in imported if module.startswith(forbidden)}

    assert actual == expected


MALFORMED_STEPS: dict[str, tuple[TraceStep, ...]] = {
    "no steps": (),
    "out of order": (Attempt(2, True), Attempt(1, True)),
    "unknown rule": (Attempt(1, True, rules=("nobody",)),),
    "finish before its attempt": (Finish(0, 1),),
    "finish of a denial": (Attempt(1, False), Finish(0, 2)),
}


@pytest.fixture(params=list(MALFORMED_STEPS))
def malformed(request: pytest.FixtureRequest) -> tuple[TraceStep, ...]:
    steps = MALFORMED_STEPS[request.param]
    return steps


def test_a_malformed_trace_cannot_be_built(malformed: tuple[TraceStep, ...]) -> None:
    """
    Given: Trace fields that break a structural rule.
    When:  The trace is constructed.
    Then:  ValueError is raised.
    """
    rule = TraceRule("vimes", ALGORITHM_TRACES[Algorithms.SLIDING_LOG][0].rules[0].policy)
    with pytest.raises(ValueError, match="trace"):
        Trace(name="malformed", description="", rules=(rule,), steps=malformed)


def test_an_allowed_attempt_names_no_retry() -> None:
    """
    Given: An allowed attempt with a retry delay.
    When:  It is constructed.
    Then:  ValueError is raised.
    """
    with pytest.raises(ValueError, match="allowed attempt"):
        Attempt(1, True, retry_after_us=5)


if __name__ == "__main__":
    pass
else:
    pass
