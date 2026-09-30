"""Work for spawned processes racing on one SQLite file.

Spawned children import this module by name, so everything here is a plain
top-level function of picklable arguments. Each child builds its own store and
handle: nothing is shared with the parent but the file.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import os
from typing import TYPE_CHECKING

from procrastinators.backends.sqlite import SQLiteBackend, SQLiteStore
from procrastinators.errors import PolicyConflict
from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    AdmissionRequest,
    Constraint,
    DurationMicros,
    QuotaIdentity,
    RuleId,
    SlidingLogPolicy,
)
from procrastinators.protocols import ObservationPoint

if TYPE_CHECKING:
    from multiprocessing.queues import Queue
    from multiprocessing.synchronize import Barrier
else:
    pass

HOUR = DurationMicros(3600 * 1_000_000)

KILLED: int = 17
"""The exit status of a worker that died on purpose."""


def hourly(scope: str, name: str, amount: int) -> Constraint:
    """A sliding log of ``amount`` per hour, so a test finishes well inside one window."""
    policy = SlidingLogPolicy(amount, HOUR)
    built = Constraint(
        RuleId(QuotaIdentity("discworld", scope), name), policy, policy_fingerprint(policy)
    )
    return built


VENDOR = hourly("ankh", "vendor", 50)
"""A vendor-wide rule every request is charged to."""

ENDPOINT = hourly("ankh-orders", "endpoint", 30)
"""A tighter rule charged only by composed requests."""

SINGLE = hourly("quirm", "single", 1)
"""A rule of capacity one, which makes a consumed admission visible."""


def contend(path: str, attempts: int, barrier: Barrier, results: Queue[tuple[int, int]]) -> None:
    """Alternate composed and vendor-only attempts; report the admissions of each.

    :param path: The shared database file.
    :param attempts: How many attempts to make in total.
    :param barrier: Released once every worker is ready, so they race.
    :param results: Receives the admitted composed requests, then the admitted
        vendor-only requests.
    """
    handle = SQLiteBackend(SQLiteStore(path))
    composed = AdmissionRequest((VENDOR, ENDPOINT))
    vendor_only = AdmissionRequest((VENDOR,))
    counts = [0, 0]
    barrier.wait(60)
    for attempt in range(attempts):
        kind = attempt % 2
        request = composed if kind == 0 else vendor_only
        counts[kind] += handle.admit(request).allowed
    handle.close()
    results.put((counts[0], counts[1]))


def die_at(path: str, point: str) -> None:
    """Admit once on :data:`SINGLE`, killing this process at ``point``.

    The process exits with :data:`KILLED` from inside the admission, with no
    cleanup at all, as a worker killed by the operating system would.

    :param path: The shared database file.
    :param point: The :class:`~procrastinators.protocols.ObservationPoint` value to die at.
    """
    target = ObservationPoint(point)

    def observer(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is target:
            os._exit(KILLED)
        else:
            pass

    handle = SQLiteBackend(SQLiteStore(path), observer=observer)
    handle.admit(AdmissionRequest((SINGLE,)))


def after_restart(path: str) -> tuple[bool, bool]:
    """In a fresh process: is :data:`SINGLE` spent, and is a changed policy refused?

    :param path: The shared database file.
    :returns: Whether an admission was allowed, then whether a changed policy conflicted.
    """
    handle = SQLiteBackend(SQLiteStore(path))
    allowed = handle.admit(AdmissionRequest((SINGLE,))).allowed
    try:
        handle.admit(AdmissionRequest((hourly("quirm", "single", 2),)))
    except PolicyConflict:
        conflicted = True
    else:
        conflicted = False
    handle.close()
    result = (allowed, conflicted)
    return result


if __name__ == "__main__":
    pass
else:
    pass
