"""
Workers anywhere sharing a quota through Redis, Valkey, or PostgreSQL.

Four spawned workers each try to send ten requests against a limit of twelve
per hour, through the service named by ``PROCRASTINATORS_EXAMPLE_BACKEND``
(``redis://localhost:16379/0`` by default, which ``just services`` starts).
The service admits exactly twelve between them, however the workers
interleave — and would across machines, too. Each run uses a fresh key, so it
never meets an earlier run's quota. With no server reachable the example says
so and exits cleanly, since it needs one to demonstrate anything.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import multiprocessing
import os
import uuid

from procrastinators import Limit, RateLimiter, idempotent_key
from procrastinators.config_models import redact
from procrastinators.errors import BackendUnavailable, ConfigurationError

BACKEND = os.environ.get("PROCRASTINATORS_EXAMPLE_BACKEND", "redis://localhost:16379/0")
SHOWN = redact("backend", BACKEND)
WORKERS = 4


def worker(key: str) -> int:
    limiter = RateLimiter(key=key, limits=[Limit(12, per="1h")], backend=BACKEND)
    admitted = sum(limiter.try_acquire().allowed for _ in range(10))
    limiter.close()
    return admitted


def reachable() -> bool:
    probe = RateLimiter(key=f"probe-{uuid.uuid4().hex}", limits=[Limit(1)], backend=BACKEND)
    try:
        probe.try_acquire()
    except (BackendUnavailable, ConfigurationError) as error:
        print(f"no service at {SHOWN} ({error}); start one with `just services`")
        answered = False
    else:
        answered = True
    finally:
        probe.close()
    return answered


def main() -> None:
    if reachable():
        key = idempotent_key({"vendor": "gonne-works", "run": uuid.uuid4().hex})
        with multiprocessing.get_context("spawn").Pool(WORKERS) as pool:
            admitted = pool.map(worker, [key] * WORKERS)
        print(f"per worker: {admitted}; in total: {sum(admitted)} of 12, through {SHOWN}")
        if sum(admitted) != 12:
            raise SystemExit("the workers did not share one quota")
        else:
            pass
    else:
        pass


if __name__ == "__main__":
    main()
