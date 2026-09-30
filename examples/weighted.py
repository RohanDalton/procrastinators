"""
Weighted operations: one batch call that costs as much as many small ones.

A vendor allows 100 records per minute, whether fetched singly or in batches.
Each acquisition charges its cost once, as one atomic admission.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import tempfile
from pathlib import Path

from procrastinators import Limit, RateLimiter, idempotent_key

KEY = idempotent_key({"vendor": "bank-of-ankh-morpork", "endpoint": "ledger"})


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        limiter = RateLimiter(
            key=KEY,
            limits=[Limit(100, per="1m")],
            backend=f"sqlite:///{Path(directory) / 'quota.sqlite3'}",
        )

        with limiter(cost=60):
            print("fetched a batch of 60 records")

        @limiter(cost=30)
        def fetch_thirty() -> str:
            return "fetched a batch of 30 records"

        print(fetch_thirty())
        
        decision = limiter.try_acquire(cost=20)
        if decision.allowed:
            result = "allowed"
        else:
            result = "denied"
        retry = decision.retry_after_us / 1e6 

        print(f"A batch of 20 is {result}; " f"retry in {retry} s")
        
        limiter.close()


if __name__ == "__main__":
    main()
