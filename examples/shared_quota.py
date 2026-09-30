"""
Several functions, and separately built limiters, drawing on one quota.

Every function decorated with the same limiter spends the same quota, and a
second limiter with the same key on the same database file spends it too:
quota belongs to the key and the authority, not to a Python object.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import tempfile
from pathlib import Path

from procrastinators import AcquireTimeout, Limit, RateLimiter, idempotent_key

KEY = idempotent_key({"vendor": "ankh-morpork-times", "dataset": "headlines"})


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        backend = f"sqlite:///{Path(directory) / 'quota.sqlite3'}"
        limiter = RateLimiter(key=KEY, limits=[Limit(3, per="1h")], backend=backend, timeout=0)

        @limiter
        def fetch_headlines() -> str:
            return "Patrician Declares Holiday"

        @limiter
        def fetch_obituaries() -> str:
            return "None today, surprisingly"

        print(fetch_headlines())
        print(fetch_obituaries())
        with limiter as admission:
            print(f"a context manager charged {admission.cost} at {admission.admitted_at} µs")

        # Built independently, but same key, same rates, same file: same quota.
        colleague = RateLimiter(key=KEY, limits=[Limit(3, per="1h")], backend=backend, timeout=0)
        try:
            colleague.acquire()
        except AcquireTimeout:
            print("the colleague's limiter found the shared hourly quota already spent")
        else:
            raise SystemExit("quota was not shared")
        limiter.close()
        colleague.close()


if __name__ == "__main__":
    main()
