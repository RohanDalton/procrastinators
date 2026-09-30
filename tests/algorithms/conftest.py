"""Fixtures for the reference-algorithm tests.

Algorithms are stateless and cheap, so each test gets fresh instances; the
timeline and host are mutable and function-scoped.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

import pytest

from procrastinators.algorithms import (
    FixedWindow,
    LeakyBucket,
    SlidingCounter,
    SlidingLog,
    TokenBucket,
    reference_algorithms,
)
from procrastinators.testing import EvaluatorHost, FakeTimeline

if TYPE_CHECKING:
    from procrastinators.algorithms import ReferenceAlgorithm
else:
    pass

REFERENCE_TYPES: dict[str, type[ReferenceAlgorithm]] = {
    "fixed_window": FixedWindow,
    "sliding_log": SlidingLog,
    "token_bucket": TokenBucket,
    "leaky_bucket": LeakyBucket,
    "sliding_counter": SlidingCounter,
}


@pytest.fixture(params=sorted(REFERENCE_TYPES))
def reference(request: pytest.FixtureRequest) -> ReferenceAlgorithm:
    """Each of the five reference algorithms in turn."""
    algorithm = REFERENCE_TYPES[request.param]()
    return algorithm


@pytest.fixture
def timeline() -> FakeTimeline:
    fresh = FakeTimeline()
    return fresh


@pytest.fixture
def reference_host(timeline: FakeTimeline) -> EvaluatorHost:
    """Every reference algorithm on one evaluator host, reading ``timeline``."""
    host = EvaluatorHost(reference_algorithms(), clock=timeline.epoch_clock)
    return host


if __name__ == "__main__":
    pass
else:
    pass
