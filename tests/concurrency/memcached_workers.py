"""Work for spawned processes racing on one Memcached item.

Spawned children import this module by name, so everything here is a plain
top-level function of picklable arguments. Each child builds its own store and
handle: nothing is shared with the parent but the server.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import contextlib
from typing import TYPE_CHECKING

from procrastinators.backends.memcached import MemcachedBackend, MemcachedStore
from procrastinators.errors import BackendBusy
from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    AdmissionRequest,
    Constraint,
    DurationMicros,
    FixedWindowPolicy,
    QuotaIdentity,
    RuleId,
)

if TYPE_CHECKING:
    from multiprocessing.queues import Queue
    from multiprocessing.synchronize import Barrier
else:
    pass

HOUR = DurationMicros(3600 * 1_000_000)

AMOUNT = 60
"""The hourly quota every worker races for."""


def hourly() -> Constraint:
    """A fixed window of :data:`AMOUNT` per hour, so a test finishes well inside one window."""
    policy = FixedWindowPolicy(AMOUNT, HOUR)
    built = Constraint(
        RuleId(QuotaIdentity("discworld", "ankh"), "vendor"), policy, policy_fingerprint(policy)
    )
    return built


def contend(
    address: str, prefix: str, attempts: int, barrier: Barrier, results: Queue[int]
) -> None:
    """Try to admit ``attempts`` times; report how many were admitted.

    Contention reported as busy counts as not admitted, as a denial would.

    :param address: The shared server.
    :param prefix: The test's key prefix.
    :param attempts: How many attempts to make.
    :param barrier: Released once every worker is ready, so they race.
    :param results: Receives the number admitted.
    """
    handle = MemcachedBackend(MemcachedStore(address, prefix=prefix))
    request = AdmissionRequest((hourly(),))
    admitted = 0
    barrier.wait(60)
    for _ in range(attempts):
        with contextlib.suppress(BackendBusy):
            admitted += handle.admit(request).allowed
    handle.close()
    results.put(admitted)


if __name__ == "__main__":
    pass
else:
    pass
