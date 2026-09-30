"""The type checker must enforce the time-domain separation (T1, T2).

Contract T2 says a client's local deadline must never be sent to a remote
authority as a comparable timestamp. That is only worth writing down if
something checks it, so this runs ty over a snippet that makes the mistake
and asserts it is rejected.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.typing.conftest import TypeCheckRunner
else:
    pass

CONFUSED = """
from procrastinators.models import DurationMicros, EpochMicros, MonotonicMicros


def record_admission(at: EpochMicros) -> None: ...


def sleep_for(duration: DurationMicros) -> None: ...


deadline = MonotonicMicros(1_000)
record_admission(deadline)          # E: local deadline sent as authority time
record_admission(1_000)             # E: a bare int is in no domain at all
sleep_for(EpochMicros(1_000))       # E: an instant is not a length of time
record_admission(EpochMicros(1_000))
sleep_for(DurationMicros(1_000))
"""


def test_the_type_checker_keeps_the_clocks_apart(run_type_checker: TypeCheckRunner) -> None:
    """
    Given: A snippet that passes values across time domains on three marked lines.
    When:  ty checks the snippet.
    Then:  Exactly the three marked lines are rejected; the two correct calls
           beneath them still type check.
    """
    expected = [12, 13, 14]
    report = run_type_checker(CONFUSED)
    actual = report.error_lines
    assert actual == expected, report.output


def test_the_right_domains_are_accepted(run_type_checker: TypeCheckRunner) -> None:
    """
    Given: The same snippet with every marked (wrong-domain) line removed.
    When:  ty checks the snippet.
    Then:  No line is rejected.
    """
    correct_only = "\n".join(line for line in CONFUSED.splitlines() if "# E:" not in line)
    expected: list[int] = list()
    report = run_type_checker(correct_only)
    actual = report.error_lines
    assert actual == expected, report.output


if __name__ == "__main__":
    pass
else:
    pass
