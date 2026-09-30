"""Every goal requirement is traced to contracts that exist.

Phase 3: "Cross-check every requirement in goal.md against a contract and a
planned acceptance test."
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import re
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path
else:
    pass


@pytest.fixture(scope="session")
def goal_requirements(project_root: Path) -> tuple[str, ...]:
    """The bullet points under "Requirements" in goal.md, verbatim."""
    text = (project_root / "goal.md").read_text()
    section = text.split("## Requirements", 1)[1].split("\n## ", 1)[0]
    requirements = tuple(
        line.removeprefix("* ").strip() for line in section.splitlines() if line.startswith("* ")
    )
    return requirements


@pytest.fixture(scope="session")
def traceability_rows(project_root: Path) -> dict[str, list[str]]:
    """Each table row of docs/source/traceability.md: first cell mapped to all cells."""
    text = (project_root / "docs" / "source" / "traceability.md").read_text()
    rows = dict()
    for line in text.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if line.startswith("|") and len(cells) == 4 and not set(cells[0]) <= {"-", " "}:
            rows[cells[0]] = cells
        else:
            pass
    return rows


@pytest.fixture(scope="session")
def contract_rules(project_root: Path) -> frozenset[str]:
    text = (project_root / "docs" / "source" / "contracts.md").read_text()
    rules = frozenset(re.findall(r"^\*\*([A-Z]\d+)\.\*\*", text, flags=re.MULTILINE))
    return rules


def test_every_goal_requirement_is_traced(
    goal_requirements: tuple[str, ...], traceability_rows: dict[str, list[str]]
) -> None:
    """
    Given: The requirements listed in goal.md.
    When:  They are looked up in the traceability table.
    Then:  Every one has a row.
    """
    expected: list[str] = list()

    actual = [
        requirement for requirement in goal_requirements if requirement not in traceability_rows
    ]

    assert goal_requirements
    assert actual == expected


def test_every_row_cites_existing_contracts_and_plans_a_test(
    traceability_rows: dict[str, list[str]], contract_rules: frozenset[str]
) -> None:
    """
    Given: Each row of the traceability table.
    When:  Its cited rules and planned tests are read.
    Then:  It cites at least one rule, every cited rule exists in docs/source/contracts.md,
           and it names a planned acceptance test by phase.
    """
    expected: dict[str, list[str]] = dict()
    problems = dict()
    for requirement, (_, contracts, _, planned) in traceability_rows.items():
        if requirement == "Requirement":
            continue
        else:
            pass
        cited = re.findall(r"\*\*([A-Z]\d+)\*\*", contracts)
        issues = [f"unknown rule {rule}" for rule in cited if rule not in contract_rules]
        if not cited:
            issues.append("cites no rule")
        else:
            pass
        if "Phase" not in planned:
            issues.append("plans no acceptance test")
        else:
            pass
        if issues:
            problems[requirement] = issues
        else:
            pass

    actual = problems

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
