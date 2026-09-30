"""The Redis and Valkey backend: one deployment coordinating workers on any machine.

A :class:`~procrastinators.backends.redis.store.RedisStore` names a
deployment — a server or a Redis Cluster — and handles address it:
:class:`~procrastinators.backends.redis.backend.RedisBackend` with the driver's
synchronous client, :class:`~procrastinators.backends.redis.backend.AsyncRedisBackend`
with its native asyncio client. Any number of
handles, processes, and machines reaching the same deployment and namespace
obey its quotas together. Redis 7 and Valkey 7.2 or later are supported, and
the service jobs run against both.

**Native executors.** Admission is one Lua script, run atomically by the
server: it samples the server's ``TIME``, checks every rule's policy
metadata, evaluates every rule against that one sample, and writes every debit
or none (contracts A1, T4, A6). Each built-in algorithm is a separate Lua
implementation of the reference evaluator's contract, registered as a native
executor and checked against the same traces (N4, Y5). A custom Python
algorithm cannot run inside the server and is refused at construction.

**No rollback.** Redis does not undo a script that fails part-way. The
admission script therefore reads and checks everything — policy conflicts,
foreign layouts, malformed state, out-of-range schedules — before its first
write, and refuses with a reply rather than an error. A failure after that, or
a reply lost in transit, is reported as
:exc:`~procrastinators.errors.IndeterminateAdmission`: never permission,
never a refund.

**Time.** Authority time is the server's clock, clamped against the latest time
stored for each rule so that it never runs backwards for that rule's state
(T7). Forward jumps are not detected: the deployment is trusted to keep its
clock disciplined, including across a failover to a replica whose clock
differs. Fixed windows align to the Unix epoch by that clock (T8).

**Expiry.** A rule's state expires at its safe-forget horizon (L9), when a
vanished key and the state it held mean the same thing. Policy metadata and
migration records never expire, so a disagreement between workers stays
detectable after the quota state is gone (L10). Cooldowns expire when they end.

**Deployment assumptions.** Quota state is only as durable as the deployment:

* *No eviction.* A server that evicts keys under memory pressure forgets quota
  history early, and a forgotten key reads as unused quota. A handle refuses a
  server whose ``maxmemory-policy`` is not ``noeviction`` unless told to accept
  it; a server that will not report its policy is trusted.
* *Persistence.* A restart without persistence forgets every admission. Use
  AOF with ``appendfsync everysec`` or ``always`` according to how much loss a
  crash may cause.
* *Failover.* Replication is asynchronous, so a failover can lose admissions
  that were acknowledged, even with ``WAIT``. A strict deployment fences the
  old primary and lets uncertain quota history drain — a period of each
  policy — before trusting the new one.

**Cluster.** In a Redis Cluster a script touches one hash slot. A rule's keys
are tagged with its coordination domain, or its scope when it declares none,
so the rules of one scope always compose; rules of several scopes compose when
they share one domain, and otherwise are refused before anything is sent (C4,
I6). Every worker must declare a rule's domain the same way: a rule already
admitted without its domain is refused with a policy conflict when it arrives
with one. On a single server a domain places nothing. A cooldown lives with
its scope; admitting rules placed elsewhere reads it just before the script,
and can miss one applied during that round trip.
"""

__author__ = "Rohan B. Dalton"

from procrastinators.backends.redis.backend import AsyncRedisBackend, RedisBackend, redis_backend
from procrastinators.backends.redis.layout import KeyLayout, key_slot
from procrastinators.backends.redis.store import (
    REDIS_CAPABILITIES,
    RedisStore,
    native_executor_specs,
)

__all__ = [
    "REDIS_CAPABILITIES",
    "AsyncRedisBackend",
    "KeyLayout",
    "RedisBackend",
    "RedisStore",
    "key_slot",
    "native_executor_specs",
    "redis_backend",
]


if __name__ == "__main__":
    pass
else:
    pass
