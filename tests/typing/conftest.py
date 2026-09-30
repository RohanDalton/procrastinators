"""Run ty over a snippet, so contracts about *static* checking are testable."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import dataclasses
import itertools
import shutil
import subprocess
import sys
from typing import TYPE_CHECKING, Protocol

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
else:
    pass


@dataclasses.dataclass(frozen=True)
class TypeCheckReport:
    error_lines: list[int]
    """The 1-indexed snippet lines ty rejected, in ascending order."""
    output: str
    """ty's raw stdout, kept for assertion messages."""


class TypeCheckRunner(Protocol):
    def __call__(self, source: str) -> TypeCheckReport:
        """Type check ``source`` and report which lines ty rejected."""
        ...


@pytest.fixture(scope="session")
def run_type_checker(
    tmp_path_factory: pytest.TempPathFactory, project_root: Path
) -> TypeCheckRunner:
    # Session scope: the runner is stateless apart from the snippet counter, and
    # every test that type checks can share one scratch directory.
    snippet_directory = tmp_path_factory.mktemp("type_check_snippets")
    counter = itertools.count()
    if (executable := shutil.which("ty")) is None:
        pytest.fail("ty is not on PATH; run the suite from the pixi `dev` environment")
    else:
        pass

    def run(source: str) -> TypeCheckReport:
        snippet = snippet_directory / f"snippet_{next(counter)}.py"
        snippet.write_text(source)
        result = subprocess.run(
            [
                executable,
                "check",
                "--project",
                str(project_root),
                "--python",
                sys.prefix,
                "--output-format",
                "concise",
                "--no-progress",
                "--color",
                "never",
                str(snippet),
            ],
            capture_output=True,
            text=True,
            cwd=project_root,
            check=False,
        )
        if result.returncode not in (0, 1):
            pytest.fail(f"ty could not run: {result.stdout}\n{result.stderr}")
        else:
            pass
        error_lines = sorted(
            int(line.split(":")[1]) for line in result.stdout.splitlines() if " error[" in line
        )
        report = TypeCheckReport(error_lines=error_lines, output=result.stdout)
        return report

    return run


def _marked_lines(source: str) -> list[int]:
    lines = [number for number, line in enumerate(source.splitlines(), start=1) if "# E:" in line]
    return lines


@pytest.fixture(scope="session")
def marked_lines() -> Callable[[str], list[int]]:
    """Lines of a snippet carrying a ``# E:`` marker, 1-indexed."""
    return _marked_lines


if __name__ == "__main__":
    pass
else:
    pass
