"""Phase 0 acceptance: the package imports quietly and promises nothing yet.

Lu-Tze sweeps the floor without anyone noticing he was there. Importing
``procrastinators`` should be the same: no files written, no threads swept into
existence, no sockets opened.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import json
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

import procrastinators

if TYPE_CHECKING:
    from pathlib import Path
else:
    pass

# Run the import under an audit hook in a fresh interpreter: by the time this
# test module is collected, ``procrastinators`` is already in ``sys.modules``,
# so the interesting moment has passed.
_AUDITED_IMPORT = """
import json, os, sys, threading

violations = list()
WRITE_FLAGS = 0
for name in ("O_WRONLY", "O_RDWR", "O_APPEND", "O_CREAT", "O_TRUNC"):
    WRITE_FLAGS |= getattr(os, name, 0)

watched = {
    "socket.connect", "socket.bind", "socket.__new__",
    "os.mkdir", "os.rename", "os.remove", "os.rmdir", "os.link", "os.symlink",
    "shutil.copyfile", "shutil.move",
    "subprocess.Popen",
}

def hook(event, args):
    if event == "open":
        path, mode, flags = args
        if (mode and any(character in mode for character in "wxa+")) or (flags & WRITE_FLAGS):
            violations.append(f"open(write): {path!r}")
        else:
            pass
    elif event in watched:
        violations.append(f"{event}: {args!r}")
    else:
        pass

before = {thread.ident for thread in threading.enumerate()}
sys.addaudithook(hook)

import procrastinators  # noqa: F401
import procrastinators.algorithms  # noqa: F401
import procrastinators.backends.executor  # noqa: F401
import procrastinators.backends.memory  # noqa: F401
import procrastinators.backends.sqlite  # noqa: F401
import procrastinators.builtins  # noqa: F401
import procrastinators.capabilities  # noqa: F401
import procrastinators.clocks  # noqa: F401
import procrastinators.config  # noqa: F401
import procrastinators.keys  # noqa: F401
import procrastinators.limiter  # noqa: F401
import procrastinators.policies  # noqa: F401
import procrastinators.registry  # noqa: F401
import procrastinators.state  # noqa: F401
import procrastinators.testing  # noqa: F401
import procrastinators.waiting  # noqa: F401

after = [thread for thread in threading.enumerate() if thread.ident not in before]
print(json.dumps({"violations": violations, "threads": [thread.name for thread in after]}))
"""

VOCABULARY_NAMES = (
    "Limit",
    "Algorithms",
    "Decision",
    "Admission",
    "AcquireTimeout",
    "RateLimiter",
    "idempotent_key",
)


@pytest.fixture(scope="session")
def import_audit(project_root: Path) -> dict[str, list[str]]:
    """What a fresh interpreter did while importing the package, per the audit hook."""
    result = subprocess.run(
        # -B: bytecode caching is the interpreter mutating the tree, not the library.
        [sys.executable, "-B", "-c", _AUDITED_IMPORT],
        capture_output=True,
        text=True,
        cwd=project_root,
        check=True,
    )
    report: dict[str, list[str]] = json.loads(result.stdout.splitlines()[-1])
    return report


def test_lu_tze_leaves_no_footprints_on_import(import_audit: dict[str, list[str]]) -> None:
    """
    Given: A fresh interpreter with an audit hook watching writes, sockets, and processes.
    When:  ``procrastinators``, every foundation module, and the conformance tools
           are imported.
    Then:  Nothing watched happened and no new thread was started.
    """
    expected: dict[str, list[str]] = {"violations": list(), "threads": list()}
    actual = import_audit
    assert actual == expected


def test_configuration_is_resolved_through_from_config() -> None:
    """
    Given: The facade.
    When:  It is searched for its configuration entry point.
    Then:  ``from_config`` exists and is a class method, resolving configuration layers
           rather than standing in for them.
    """
    assert callable(procrastinators.RateLimiter.from_config)
    assert isinstance(procrastinators.RateLimiter.__dict__["from_config"], classmethod)


def test_the_vocabulary_is_exported() -> None:
    """
    Given: The core vocabulary names.
    When:  The package's public interface is inspected.
    Then:  Every name is listed in ``__all__`` and resolves as an attribute.
    """
    expected: list[str] = list()
    unlisted = [name for name in VOCABULARY_NAMES if name not in procrastinators.__all__]
    unresolved = [name for name in VOCABULARY_NAMES if not hasattr(procrastinators, name)]
    assert unlisted == expected
    assert unresolved == expected


def test_every_exported_name_resolves() -> None:
    """
    Given: The package's ``__all__``.
    When:  Each listed name is looked up on the package.
    Then:  Every one resolves.
    """
    expected: list[str] = list()
    actual = [name for name in procrastinators.__all__ if not hasattr(procrastinators, name)]
    assert actual == expected


def test_the_package_ships_its_typing_marker(project_root: Path) -> None:
    """
    Given: The package source tree.
    When:  It is checked for a PEP 561 marker.
    Then:  ``py.typed`` is present as a file.
    """
    assert (project_root / "src" / "procrastinators" / "py.typed").is_file()


if __name__ == "__main__":
    pass
else:
    pass
