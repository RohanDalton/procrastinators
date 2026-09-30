"""
Independent processes on one machine sharing a quota through a SQLite file.

Four spawned workers each try to send ten requests against a limit of twelve
per hour. The file admits exactly twelve between them, however the processes
interleave.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import multiprocessing
import tempfile
from pathlib import Path

from procrastinators import Limit, RateLimiter, idempotent_key

KEY = idempotent_key({"vendor": "gonne-works", "account": "warehouse"})
WORKERS = 4


def worker(database: str) -> int:
    limiter = RateLimiter(key=KEY, limits=[Limit(12, per="1h")], backend=f"sqlite:///{database}")
    admitted = sum(limiter.try_acquire().allowed for _ in range(10))
    limiter.close()
    return admitted


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        database = str(Path(directory) / "quota.sqlite3")
        with multiprocessing.get_context("spawn").Pool(WORKERS) as pool:
            admitted = pool.map(worker, [database] * WORKERS)
        print(f"per process: {admitted}; in total: {sum(admitted)} of 12")
        if sum(admitted) != 12:
            raise SystemExit("the processes did not share one quota")
        else:
            pass


if __name__ == "__main__":
    main()
