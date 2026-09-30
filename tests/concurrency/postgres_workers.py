"""Work for spawned processes racing on one PostgreSQL database.

Spawned children import this module by name, so everything here is a plain
top-level function of picklable arguments. Each child builds its own store and
handle: nothing is shared with the parent but the database.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import os
from typing import TYPE_CHECKING

from procrastinators.backends.postgres import PostgresBackend, PostgresStore
from procrastinators.models import AdmissionRequest
from procrastinators.protocols import ObservationPoint
from tests.concurrency.sqlite_workers import ENDPOINT, KILLED, SINGLE, VENDOR

if TYPE_CHECKING:
    from multiprocessing.queues import Queue
    from multiprocessing.synchronize import Barrier
else:
    pass

__all__ = ["ENDPOINT", "KILLED", "SINGLE", "VENDOR", "admit_single", "contend", "die_at"]


def _handle(url: str, schema: str) -> PostgresBackend:
    handle = PostgresBackend(PostgresStore(url, schema=schema))
    return handle


def contend(
    url: str, schema: str, attempts: int, barrier: Barrier, results: Queue[tuple[int, int]]
) -> None:
    """Alternate composed and vendor-only attempts; report the admissions of each.

    :param url: The shared database.
    :param schema: The schema holding the test's tables.
    :param attempts: How many attempts to make in total.
    :param barrier: Released once every worker is ready, so they race.
    :param results: Receives the admitted composed, then vendor-only, request counts.
    """
    handle = _handle(url, schema)
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


def die_at(url: str, schema: str, point: str) -> None:
    """Admit once on ``SINGLE``, killing this process at ``point`` without any cleanup.

    :param url: The shared database.
    :param schema: The schema holding the test's tables.
    :param point: The :class:`~procrastinators.protocols.ObservationPoint` value to die at.
    """
    target = ObservationPoint(point)

    def observer(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is target:
            os._exit(KILLED)
        else:
            pass

    handle = PostgresBackend(PostgresStore(url, schema=schema), observer=observer)
    handle.admit(AdmissionRequest((SINGLE,)))


def admit_single(url: str, schema: str) -> bool:
    """In a fresh process: whether one admission on ``SINGLE`` is allowed.

    :param url: The shared database.
    :param schema: The schema holding the test's tables.
    """
    handle = _handle(url, schema)
    allowed = handle.admit(AdmissionRequest((SINGLE,))).allowed
    handle.close()
    return allowed


if __name__ == "__main__":
    pass
else:
    pass
