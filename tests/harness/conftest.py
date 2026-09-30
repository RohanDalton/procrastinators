"""Fixtures for the conformance-tool tests.

Timelines, injectors, and hosts are mutable, so they are function-scoped: each
test starts at the same instant with nothing armed and nothing stored.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import pytest

from procrastinators.models import AdmissionRequest, DurationMicros, SlidingLogPolicy
from procrastinators.testing import EvaluatorHost, FakeTimeline, FaultInjector, TraceRule
from procrastinators.testing.harness import DEFAULT_NAMESPACE
from tests.oracles import all_oracles


@pytest.fixture
def timeline() -> FakeTimeline:
    fresh = FakeTimeline()
    return fresh


@pytest.fixture
def injector() -> FaultInjector:
    fresh = FaultInjector()
    return fresh


@pytest.fixture
def host(timeline: FakeTimeline, injector: FaultInjector) -> EvaluatorHost:
    """The oracles on an evaluator host, observed by ``injector``."""
    backend = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock, observer=injector)
    return backend


@pytest.fixture
def one_per_second() -> AdmissionRequest:
    """A request against a fresh rule that admits one per rolling second."""
    rule = TraceRule("mustrum", SlidingLogPolicy(1, DurationMicros(1_000_000)), scope="unseen")
    request = AdmissionRequest((rule.constraint(DEFAULT_NAMESPACE),))
    return request


if __name__ == "__main__":
    pass
else:
    pass
