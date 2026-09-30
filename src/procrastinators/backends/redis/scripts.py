"""The Lua scripts a Redis or Valkey store runs, identified by content.

Scripts run by ``EVALSHA``. A server that does not know one — after a restart,
a failover, or ``SCRIPT FLUSH`` — answers ``NOSCRIPT`` without running
anything, and the transport loads it and tries once more. Because the digest is
the script's content, two library versions with different scripts never run
each other's code by name; the key layout version each script checks guards
the data they share.

Every script starts with a ``#!lua`` line. That makes the server refuse a
writing script before it starts when memory is exhausted, instead of failing
at its first write with earlier writes already applied, and it requires Redis
7 or Valkey 7.2 or later.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import functools
import hashlib
import importlib.resources
from dataclasses import dataclass, field
from typing import Literal, TypeAlias

__all__ = ["Script", "ScriptName", "packaged"]

ScriptName: TypeAlias = Literal["admit", "read", "defer", "apply"]
"""The shipped scripts: atomic admission (the native executors), a consistent
read-only view, cooldown extension, and a compare-and-swap administrative write."""


@dataclass(frozen=True, slots=True)
class Script:
    """One Lua script and its ``SHA1`` digest."""

    name: str
    """The script's file name, without ``.lua``."""

    source: str
    """The script's text."""

    sha: str = field(init=False)
    """The digest ``EVALSHA`` names the script by."""

    def __post_init__(self) -> None:
        digest = hashlib.sha1(self.source.encode(), usedforsecurity=False).hexdigest()
        object.__setattr__(self, "sha", digest)


@functools.cache
def packaged(name: ScriptName) -> Script:
    """The script shipped as ``lua/<name>.lua`` beside this module, read on first use.

    :param name: The script's name.
    """
    path = importlib.resources.files("procrastinators.backends.redis").joinpath(
        "lua", f"{name}.lua"
    )
    script = Script(name, path.read_text(encoding="utf-8"))
    return script


if __name__ == "__main__":
    pass
else:
    pass
