"""Shared value objects for the contract tests.

Every fixture here is a frozen value object, so session scope is safe: no test
can mutate what another test sees.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import pytest

from procrastinators import BackendIdentity, QuotaIdentity, RuleId


@pytest.fixture(scope="session")
def ankh() -> QuotaIdentity:
    identity = QuotaIdentity("discworld", "ankh-morpork")
    return identity


@pytest.fixture(scope="session")
def quirm() -> QuotaIdentity:
    identity = QuotaIdentity("discworld", "quirm")
    return identity


@pytest.fixture(scope="session")
def burst_rule(ankh: QuotaIdentity) -> RuleId:
    rule = RuleId(ankh, "burst")
    return rule


@pytest.fixture(scope="session")
def daily_rule(ankh: QuotaIdentity) -> RuleId:
    rule = RuleId(ankh, "daily")
    return rule


@pytest.fixture(scope="session")
def memory_identity() -> BackendIdentity:
    identity = BackendIdentity("memory", "unseen-university", "discworld")
    return identity


if __name__ == "__main__":
    pass
else:
    pass
