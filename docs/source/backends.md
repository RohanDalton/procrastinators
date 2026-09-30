# Backends

A backend is the authority that decides admission atomically and records it.
Choose it by **who must share a quota** and **what losing state would cost**:
the algorithm, the facade, and the waiting behavior are the same everywhere,
and unsupported combinations are refused when a limiter is constructed, never
degraded at the first acquisition (**Y2**).

| Backend | Address | Coordinates | Survives | Extra |
| --- | --- | --- | --- | --- |
| Memory | `memory://[name]` | Threads and tasks of one process | Nothing | — |
| SQLite | `sqlite://[/path]` | Processes on one machine | Process crashes; power loss with `synchronous=full` | — |
| Redis, Valkey | `redis://`, `rediss://`, `valkey://`, `valkeys://` | Any worker reaching the deployment | What its persistence and failover keep | `redis` or `valkey` |
| PostgreSQL | `postgresql://`, `postgres://` | Any worker reaching the database | What `synchronous_commit` and replication keep | `postgres` |
| Memcached | `memcached://host:port` | Any worker reaching the server | **Nothing reliably** — best effort | `memcached` |

`pip install "procrastinators[redis]"` installs a driver; registering a family
imports none, so an unused backend costs nothing.

## Capability matrix

What each backend declares through its
{class}`~procrastinators.models.Capabilities`, and therefore what capability
validation accepts:

| | Memory | SQLite | Redis / Valkey | PostgreSQL | Memcached |
| --- | --- | --- | --- | --- | --- |
| Fixed window | ✓ | ✓ | ✓ native | ✓ | ✓ |
| Sliding log | ✓ | ✓ | ✓ native | ✓ | — |
| Sliding counter | ✓ | ✓ | ✓ native | ✓ | ✓ |
| Token bucket | ✓ | ✓ | ✓ native | ✓ | ✓ |
| Leaky bucket | ✓ | ✓ | ✓ native | ✓ | ✓ |
| Third-party Python algorithms | ✓ | ✓ | — (**Y5**) | ✓ | ✓ scalar state |
| Synchronous | ✓ | ✓ | ✓ | ✓ | ✓ |
| Asynchronous | ✓ never blocks the loop | ✓ dedicated executor | ✓ native asyncio | ✓ native asyncio | ✓ dedicated executor |
| Coordination | `in_process` | `local_machine` | `shared_service` | `shared_service` | `shared_service` |
| Durability | `ephemeral` | `local_durable` | `service_durable` | `service_durable` | `best_effort` |
| Atomic composition | ✓ | ✓ | ✓ | ✓ | — |
| Shared cooldowns | ✓ | ✓ | ✓ | ✓ | — |
| Policy administration | ✓ | ✓ | ✓ | ✓ | — |
| Observation points | all four | all four | before sending, after commit | all four | all four |

"Native" means a separate implementation of the algorithm runs inside the
server — a Lua script — registered as a native executor for its algorithm id,
state version, and numeric range, and checked against the same traces as the
reference evaluator (**N4**, **Y5**). Every other backend runs the reference
evaluator inside its own transaction or compare-and-swap.

**Memcached is not a durable shared store.** Its state can vanish at any
moment, and a vanished rule admits its initial allowance again. A limiter
refuses it unless the caller writes `accept_best_effort=True` (**Y4**).

## What failures mean

Every backend reports failures the same way, whatever caused them:

| Error | Nothing was committed? | A waiter retries it? |
| --- | --- | --- |
| `BackendBusy` | Yes — contention, never a denial (**O2**) | Yes, within budget |
| `BackendUnavailable` | Yes | Yes, within budget |
| `IndeterminateAdmission` | **Unknown** — the admission may have happened | No: the body does not run and nothing is refunded (**O4**) |
| `PolicyConflict` | Yes | No |
| `StateCorruption` | Yes; the malformed state is left untouched | No |

A reply lost after an admission reached the server is the one case no backend
can resolve: retrying could admit twice and refunding could hand out capacity
the policy never granted, so neither happens.

## Redis and Valkey

```python
limiter = RateLimiter(
    key=idempotent_key({"vendor": "ankh"}),
    limits=[Limit(10, per="1s"), Limit(500, per="1m")],
    backend="redis://quota.internal:6379/0",
)
```

Credentials in the address reach the driver and never the backend's identity,
diagnostics, or `explain()` output; keep them out of configuration files and
supply the address through `PROCRASTINATORS_BACKEND` instead (**G9**). Query
parameters `prefix` (default `procrastinators`), `cluster=true`, and
`require_noeviction=false` configure the store; any other parameter is passed
to the driver.

Admission is one Lua script: it samples the server's `TIME`, checks every
rule's policy metadata, evaluates every rule against that one instant, and
writes every debit or none. Redis does not roll a script back when it fails,
so the script reads and checks everything before its first write and refuses
with a reply rather than an error. Scripts are sent by digest and reloaded
transparently after a restart or `SCRIPT FLUSH`, and they carry a key-layout
version that another library version would refuse rather than reinterpret.
Redis 7 or Valkey 7.2, or later, is required.

### Deployment assumptions

Quota state is exactly as durable as the deployment, so these are part of the
guarantee, not advice:

* **No eviction.** Set `maxmemory-policy noeviction`. Any other policy can
  delete a rule's keys before its safe-forget horizon, and a missing key reads
  as unused quota. A handle checks the policy before its first call and
  refuses an evicting server; `require_noeviction=false` accepts the weaker
  guarantee knowingly. A server that will not report its policy (`CONFIG`
  renamed or denied) is trusted, and must be configured correctly.
* **Persistence.** Without persistence a restart forgets every admission. Use
  AOF: `appendfsync always` loses no acknowledged admission to a crash,
  `everysec` up to about a second of them.
* **Failover.** Replication is asynchronous; a failover can lose admissions
  that were acknowledged, even with `WAIT`. A strict deployment fences the old
  primary and lets uncertain history drain — one period of the longest policy
  — before trusting the new one. A write sent to a demoted primary is refused
  (`READONLY`) before anything runs, and is safely retried.
* **Clock.** Authority time is the server's clock, clamped per rule so it never
  runs backwards for that rule's state (**T7**). Forward jumps are not
  detected: keep the servers' clocks disciplined, including replicas that may
  be promoted. Fixed windows align to the Unix epoch by that clock (**T8**).
* **Expiry.** A rule's state expires at its safe-forget horizon (**L9**); its
  policy metadata never does, so disagreement between workers stays detectable
  after the quota state is gone (**L10**). Cooldowns expire when they end.

A minimal `redis.conf` for a quota server:

```text
maxmemory-policy noeviction
appendonly yes
appendfsync everysec
```

### Redis Cluster

`redis://node:7000?cluster=true` addresses a cluster, where one script can
touch only one hash slot. All rule and cooldown keys under one prefix share one cluster tag.
Consequently, rules across scopes that declare one coordination domain compose
in one script, and a cooldown is checked atomically with admission. A rule keeps the same keys regardless of
which coordination domain a caller declares. The tradeoff is that cluster
quota traffic concentrates on one shard; capacity planning should account
for this.

The cluster layout differs from earlier phase 11 builds that tagged keys by
scope or domain. Existing quota keys from those builds are not read by this
layout. Drain or explicitly migrate that state before switching a live
deployment to this version.

## PostgreSQL

```python
limiter = RateLimiter(
    key=idempotent_key({"vendor": "ankh"}),
    limits=[Limit(10, per="1s")],
    backend="postgresql://etl@db.internal:5432/quota?prefix=etl_",
)
```

Admission is one transaction: it creates missing rule rows (`INSERT … ON
CONFLICT DO NOTHING`), locks scope and rule rows in canonical order so
admissions never deadlock one another, reads `clock_timestamp()` after the
locks are held — `now()` would be the transaction's start — and commits before
reporting success. A deadlock or serialization failure, which rolled
everything back, is retried within the budget; nothing is retried once
`COMMIT` was sent. The query parameters `prefix` and `schema` place the
tables; others go to libpq.

Deployment assumptions:

* **Durability** follows `synchronous_commit` and replication. `off`, or a
  failover to an asynchronous replica, can lose admissions that were reported.
* **Clock.** The database server's clock is the authority, clamped per rule.
* **Privileges.** The first user creates the tables, serialized by an advisory
  lock; later users need only read and write them. Tables written by another
  schema version are refused.
* **Cleanup.** State past its horizon is ignored by admission and removed by
  `sweep()`; policy metadata is kept.

## Memcached

```python
limiter = RateLimiter(
    key=idempotent_key({"vendor": "ankh"}),
    limits=[Limit(10, per="1s")],
    algorithm="token_bucket",
    backend="memcached://cache.internal:11211",
    accept_best_effort=True,
)
```

Each rule is one bounded item updated by compare-and-swap. Only constant-state
algorithms are offered, one rule per limiter, with no cooldowns or policy
administration. A swap that keeps losing to other workers ends as
`BackendBusy`, which the waiter retries with backoff.

What *best effort* means here:

* **Eviction and restarts forget quota.** A missing item reads as never used,
  so it admits its initial allowance again. A cache miss is not proof that no
  quota was consumed.
* **Metadata is evicted with state.** A disagreement between workers' policies
  is detected only while the item survives.
* **Client clocks.** Memcached has no clock a client can read, so each client's
  wall clock is the authority, clamped per item. A client running fast admits
  more than the policy allows, by up to its skew. Keep clocks synchronized.

Choose Memcached when an occasional burst after an eviction is acceptable and
a durable store is not available.

## Measurements

`benchmarks/contention.py` measures admissions against any address: a *hot
key* (every worker on one rule) and a *composed* quota (every worker on a
vendor rule plus one of four endpoint rules). Each worker is a thread with a
handle of its own.

The numbers below come from one development machine running every service in
a rootless container. Loopback round trips there took about 300 µs, and a
PostgreSQL commit about 14 ms of container-storage `fsync`. They show the
*shape* of each backend's costs, not what it can do on production hardware.
A token bucket, whose state stays constant, measures the cost of one
admission:

| Backend | Contention | Workers | Admissions/s | p50 µs | p99 µs |
| --- | --- | --- | ---: | ---: | ---: |
| memory | hot key | 1 | 9,251 | 81 | 471 |
| memory | hot key | 8 | 4,306 | 1,534 | 14,264 |
| memory | composed | 8 | 4,105 | 1,456 | 12,243 |
| sqlite | hot key | 1 | 2,507 | 351 | 1,214 |
| sqlite | hot key | 8 | 2,024 | 384 | 57,489 |
| sqlite | composed | 8 | 1,419 | 550 | 108,221 |
| redis | hot key | 1 | 670 | 1,087 | 6,602 |
| redis | hot key | 8 | 1,355 | 4,901 | 20,162 |
| redis | composed | 8 | 1,180 | 5,989 | 20,942 |
| valkey | hot key | 1 | 863 | 854 | 6,017 |
| valkey | hot key | 8 | 1,163 | 5,788 | 22,390 |
| valkey | composed | 8 | 1,001 | 6,318 | 29,147 |
| postgresql | hot key | 1 | 53 | 17,114 | 58,140 |
| postgresql | hot key | 8 | 61 | 126,409 | 205,148 |
| postgresql | composed | 8 | 34 | 225,010 | 405,270 |
| memcached | hot key | 1 | 1,118 | 671 | 3,876 |
| memcached | hot key | 8 | 432 | 12,071 | 80,617 |

What they show:

* **Redis and Valkey scale with workers** on a hot key and on a composed quota:
  the server runs one script at a time, and more clients overlap their round
  trips. The admission script itself took 170–250 µs of server time here.
* **SQLite serializes writers.** Throughput holds, but the tail grows under
  contention as writers wait for the file's lock.
* **PostgreSQL is bound by commit latency** on this machine; hot-key
  admissions queue behind one row lock. Each admission takes about a dozen
  round trips; batching them is future work.
* **Memcached's compare-and-swap loses under a hot key**: with eight workers,
  seven admissions in three seconds exhausted their swap rounds and ended as
  `BackendBusy`.
* **A busy sliding log costs the reference evaluators more than the native
  executor.** Memory, SQLite, and PostgreSQL load a rule's live log for each
  admission, so their cost grows with it: with a thousand live entries, a
  memory admission took about 800 µs and a SQLite one 3.5 ms, while Redis stayed
  near 750 µs by keeping running totals. Indexed aggregates for the SQL
  backends are future work, to be adopted only where they preserve the same
  observations (see the state access boundary in
  {mod}`procrastinators.protocols`).
