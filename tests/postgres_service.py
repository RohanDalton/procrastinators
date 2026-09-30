"""Where the PostgreSQL service tests find their server.

``PROCRASTINATORS_POSTGRES_URL`` names it; the default is the port
``just services`` publishes. A test isolates itself in a schema of its own,
dropped when it ends, so the server may be shared with anything else.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import os
from typing import Final, LiteralString, cast

DEFAULT_URL: Final = "postgresql://procrastinators:procrastinators@localhost:15432/procrastinators"
"""The server ``just services`` starts."""

POSTGRES_URL: Final = os.environ.get("PROCRASTINATORS_POSTGRES_URL", DEFAULT_URL)
"""The server's address."""


def statement(text: str) -> LiteralString:
    """``text`` as psycopg's types want a query; names in it are the test's own."""
    checked = cast("LiteralString", text)
    return checked


def reachable(url: str = POSTGRES_URL) -> bool:
    """Whether the server at ``url`` accepts a connection within two seconds."""
    import psycopg

    try:
        with psycopg.connect(url, connect_timeout=2):
            pass
    except psycopg.Error:
        answered = False
    else:
        answered = True
    return answered


if __name__ == "__main__":
    pass
else:
    pass
