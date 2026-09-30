"""Where the Redis and Valkey service tests find their servers.

``PROCRASTINATORS_REDIS_URL`` and ``PROCRASTINATORS_VALKEY_URL`` name them;
the defaults are the ports ``just services`` publishes. A test isolates itself
with a fresh key prefix, so the servers may be shared with anything else.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import itertools
import os
import uuid
from typing import Final

SERVICE_URLS: Final = {
    "redis": os.environ.get("PROCRASTINATORS_REDIS_URL", "redis://localhost:16379/0"),
    "valkey": os.environ.get("PROCRASTINATORS_VALKEY_URL", "valkey://localhost:16380/0"),
}
"""The address of each service, by service name."""

CLUSTER_URL: Final = os.environ.get("PROCRASTINATORS_REDIS_CLUSTER_URL", "")
"""A Redis Cluster node's address, when a cluster is available."""

RUN_PREFIX: Final = f"procrastinators-test-{uuid.uuid4().hex[:12]}"
"""Starts every key this test run writes, so the run can remove them when it ends."""

_prefixes = itertools.count()


def fresh_prefix() -> str:
    """A key prefix no other test, run, or process uses."""
    prefix = f"{RUN_PREFIX}-{next(_prefixes)}"
    return prefix


def remove_run_keys(url: str) -> None:
    """Delete every key this run wrote on the server at ``url``, if it answers."""
    import redis

    client = redis.Redis.from_url(_driver_address(url), socket_timeout=2)
    try:
        for key in client.scan_iter(match=f"{RUN_PREFIX}*", count=1000):
            client.unlink(key)
    finally:
        client.close()


def _driver_address(url: str) -> str:
    address = url.replace("valkey://", "redis://", 1).replace("valkeys://", "rediss://", 1)
    return address


def reachable(url: str) -> bool:
    """Whether a server answers ``PING`` at ``url`` within half a second."""
    import redis
    import redis.exceptions

    client = redis.Redis.from_url(
        _driver_address(url), socket_timeout=0.5, socket_connect_timeout=0.5
    )
    try:
        answered = bool(client.ping())
    except (redis.exceptions.RedisError, OSError):
        answered = False
    finally:
        client.close()
    return answered


if __name__ == "__main__":
    pass
else:
    pass
