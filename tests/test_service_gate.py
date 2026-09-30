"""A required service job cannot pass by skipping, while a laptop run still can.

Each test runs a small inner pytest session with the service gate plugin, the
way a CI job for one backend would.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import pytest

SERVICE_TESTS = """
import pytest


@pytest.mark.service("redis")
def test_the_clacks_tower_is_dark(service_available):
    service_available("redis", False, "nobody home")


@pytest.mark.service("redis")
def test_the_clacks_tower_answers(service_available):
    service_available("redis", lambda: True)
"""


@pytest.fixture
def clacks(pytester: pytest.Pytester) -> pytest.Pytester:
    """An inner project with one unreachable and one reachable Redis test."""
    pytester.makepyfile(test_clacks=SERVICE_TESTS)
    return pytester


def test_an_unreachable_service_skips_on_a_laptop(clacks: pytest.Pytester) -> None:
    """
    Given: One unreachable and one reachable Redis test, with nothing required.
    When:  The inner session runs.
    Then:  The unreachable test is skipped, the reachable one passes, and the run passes.
    """
    result = clacks.runpytest("-p", "tests.service_gate")

    result.assert_outcomes(passed=1, skipped=1)
    assert result.ret == pytest.ExitCode.OK


def test_an_unreachable_required_service_fails(clacks: pytest.Pytester) -> None:
    """
    Given: The same tests, with Redis required.
    When:  The inner session runs.
    Then:  The unreachable test fails instead of skipping.
    """
    result = clacks.runpytest("-p", "tests.service_gate", "--require-service", "redis")

    result.assert_outcomes(passed=1, failed=1)
    result.stdout.fnmatch_lines(["*required service redis is unreachable: nobody home*"])


def test_a_required_job_with_no_passing_tests_fails(clacks: pytest.Pytester) -> None:
    """
    Given: Redis required, but every Redis test deselected.
    When:  The inner session runs.
    Then:  The session fails and says the required service had no passing tests,
           rather than reporting "no tests ran" as success.
    """
    result = clacks.runpytest(
        "-p", "tests.service_gate", "--require-service", "redis", "-k", "no_such_test"
    )

    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(["*required services had no passing tests: redis*"])


def test_the_requirement_can_come_from_the_environment(
    clacks: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: Valkey required through the environment, and no Valkey tests at all.
    When:  The inner session runs.
    Then:  The session fails naming valkey, though every Redis test behaved.
    """
    monkeypatch.setenv("PROCRASTINATORS_REQUIRE_SERVICES", " valkey , ")

    result = clacks.runpytest("-p", "tests.service_gate", "-k", "answers")

    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(["*required services had no passing tests: valkey*"])


if __name__ == "__main__":
    pass
else:
    pass
