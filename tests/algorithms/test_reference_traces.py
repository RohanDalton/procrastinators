"""The reference algorithms reproduce every shared trace and scenario.

The traces were computed by hand from contract §17 before these evaluators
existed, so passing them is evidence the evaluators say what the contract
says, not merely what they happened to be written to do.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

import pytest

from procrastinators.algorithms import builtin_specs, reference_algorithms
from procrastinators.models import Algorithms
from procrastinators.protocols import Algorithm, StateRepresentation
from procrastinators.registry import Registry
from procrastinators.testing import (
    SCENARIOS,
    TRACES,
    BackendSubject,
    CheckStatus,
    check_algorithm,
    run_trace,
)

if TYPE_CHECKING:
    from procrastinators.algorithms import ReferenceAlgorithm
    from procrastinators.testing import EvaluatorHost, FakeTimeline, Trace
else:
    pass

SCENARIO_TRACES = tuple(
    scenario.trace_for(expectation)
    for scenario in SCENARIOS
    for expectation in scenario.expectations
)


def test_each_reference_algorithm_passes_its_conformance_traces(
    reference: ReferenceAlgorithm,
) -> None:
    """
    Given: One reference algorithm.
    When:  ``check_algorithm`` runs every trace and scenario for it, evaluating each
           step twice to catch hidden state.
    Then:  Every check passes and none is skipped.
    """
    report = check_algorithm(reference)

    report.raise_for_failures(allow_skips=False)
    assert report.results
    assert all(result.status is CheckStatus.PASSED for result in report.results)


@pytest.mark.parametrize("trace", TRACES, ids=[trace.name for trace in TRACES])
def test_granny_weatherwax_runs_every_shared_trace_on_the_reference_host(
    trace: Trace, reference_host: EvaluatorHost, timeline: FakeTimeline
) -> None:
    """
    Given: All five reference algorithms on one host, including composed traces
           that mix algorithms in a single request.
    When:  A shared trace is run with exact retry delays and timestamps checked.
    Then:  Every step matches, so composition evaluates every rule at one time
           sample and debits nothing when a later rule denies.
    """
    report = run_trace(trace, BackendSubject(reference_host), timeline)

    report.raise_for_mismatches()


@pytest.mark.parametrize("trace", SCENARIO_TRACES, ids=[trace.name for trace in SCENARIO_TRACES])
def test_every_demonstrated_failure_scenario_gets_its_policy_outcome(
    trace: Trace, reference_host: EvaluatorHost, timeline: FakeTimeline
) -> None:
    """
    Given: The failures demonstrated against existing rate limiters, each compiled
           under every policy with that policy's correct outcome.
    When:  Each is run on the reference host.
    Then:  Every policy admits exactly what its own guarantee allows.
    """
    report = run_trace(trace, BackendSubject(reference_host), timeline, check_retry=False)

    report.raise_for_mismatches()


def test_every_reference_algorithm_satisfies_the_algorithm_protocol(
    reference: ReferenceAlgorithm,
) -> None:
    """
    Given: One reference algorithm.
    When:  It is checked against the structural ``Algorithm`` protocol.
    Then:  It conforms, and its codec version is its state version.
    """
    assert isinstance(reference, Algorithm)
    assert reference.codec.version == reference.state_version


def test_builtin_specs_register_all_five_with_their_representations() -> None:
    """
    Given: The built-in algorithm registrations.
    When:  They are registered and each algorithm is built from its registry.
    Then:  All five ids resolve, the sliding log alone needs an event log, and each
           built algorithm reports the id it was registered under.
    """
    expected = {
        Algorithms.FIXED_WINDOW: StateRepresentation.SCALARS,
        Algorithms.SLIDING_LOG: StateRepresentation.EVENT_LOG,
        Algorithms.TOKEN_BUCKET: StateRepresentation.SCALARS,
        Algorithms.LEAKY_BUCKET: StateRepresentation.SCALARS,
        Algorithms.SLIDING_COUNTER: StateRepresentation.SCALARS,
    }
    registry = Registry()
    for spec in builtin_specs():
        registry.register_algorithm(spec)

    actual = {
        algorithm_id: registry.algorithm_spec(algorithm_id).representation
        for algorithm_id in registry.algorithm_ids
    }

    assert actual == expected
    assert all(registry.algorithm(name).id == name for name in registry.algorithm_ids)


def test_reference_algorithms_returns_one_of_each() -> None:
    """
    Given: The reference algorithm factory.
    When:  It is called.
    Then:  It returns exactly the five built-in ids.
    """
    expected = set(Algorithms)

    actual = {algorithm.id for algorithm in reference_algorithms()}

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
