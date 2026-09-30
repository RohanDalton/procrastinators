"""Work for spawned processes racing on one Redis or Valkey server.

Spawned children import this module by name, so everything here is a plain
top-level function of picklable arguments. Each child builds its own store,
handle, and client: nothing is shared with the parent but the server.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import os
from typing import TYPE_CHECKING

from procrastinators.backends.redis import AsyncRedisBackend, RedisBackend, RedisStore
from procrastinators.models import AdmissionRequest
from procrastinators.protocols import ObservationPoint
from tests.concurrency.sqlite_workers import ENDPOINT, KILLED, SINGLE, VENDOR

if TYPE_CHECKING:
    from multiprocessing.queues import Queue
    from multiprocessing.synchronize import Barrier
else:
    pass


def contend(
    url: str, prefix: str, attempts: int, barrier: Barrier, results: Queue[tuple[int, int]]
) -> None:
    """Alternate composed and vendor-only attempts; report the admissions of each.

    Even-numbered workers use the synchronous handle, odd ones the asynchronous
    handle, so both drivers race on the same keys.

    :param url: The server's address.
    :param prefix: The test's key prefix.
    :param attempts: How many attempts to make in total.
    :param barrier: Released once every worker is ready, so they race.
    :param results: Receives the admitted composed requests, then the admitted
        vendor-only requests.
    """
    composed = AdmissionRequest((VENDOR, ENDPOINT))
    vendor_only = AdmissionRequest((VENDOR,))
    requests = [composed if attempt % 2 == 0 else vendor_only for attempt in range(attempts)]
    barrier.wait(60)
    if os.getpid() % 2:
        allowed = asyncio.run(_contend_async(url, prefix, requests))
    else:
        handle = RedisBackend(RedisStore(url, prefix=prefix))
        allowed = [handle.admit(request).allowed for request in requests]
        handle.close()
    counts = (sum(allowed[0::2]), sum(allowed[1::2]))
    results.put(counts)


async def _contend_async(url: str, prefix: str, requests: list[AdmissionRequest]) -> list[bool]:
    handle = AsyncRedisBackend(RedisStore(url, prefix=prefix))
    allowed = [(await handle.admit(request)).allowed for request in requests]
    await handle.aclose()
    return allowed


def die_after_commit(url: str, prefix: str) -> None:
    """Admit once on :data:`SINGLE` and die as soon as the reply arrives, before reporting.

    :param url: The server's address.
    :param prefix: The test's key prefix.
    """

    def observer(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is ObservationPoint.AFTER_COMMIT:
            os._exit(KILLED)
        else:
            pass

    handle = RedisBackend(RedisStore(url, prefix=prefix), observer=observer)
    handle.admit(AdmissionRequest((SINGLE,)))


def admit_single(url: str, prefix: str) -> tuple[bool, int]:
    """In a fresh process: admit :data:`SINGLE` once.

    :param url: The server's address.
    :param prefix: The test's key prefix.
    :returns: Whether it was admitted, and the retry delay if not.
    """
    handle = RedisBackend(RedisStore(url, prefix=prefix))
    decision = handle.admit(AdmissionRequest((SINGLE,)))
    handle.close()
    result = (decision.allowed, decision.retry_after_us)
    return result


if __name__ == "__main__":
    pass
else:
    pass
