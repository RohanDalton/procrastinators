# Requirement traceability

Every requirement in `goal.md` (repository root), mapped to the
[contract rules](contracts.md) that define it, the evidence that exists now,
and the acceptance tests that will prove it. The Requirements rows quote
`goal.md` verbatim, spelling included, so `tests/test_traceability.py` can
check that none is missing and that every cited rule exists.

"Planned" names the phase from `IMPLEMENTATION_PLAN.md` and the test directory
where the acceptance test will live.

## Goal statement

| Requirement | Contracts | Evidence now | Planned acceptance tests |
| --- | --- | --- | --- |
| Rate limits configured per vendor/dataset, endpoint, or API key. | **I1**, **I2**, **I7**, **I8**, **I9** | `idempotent_key`, HMAC keys, `scope_constraints`; `tests/foundations/test_keys.py` with cross-process golden fixtures; `RateLimiter.combine` composes vendor, account, and endpoint limiters atomically (`tests/contracts/test_facade.py`) | Phase 12 HTTP scope mapping (`tests/integrations/`) |
| Running multiple processes at once can't violate a rate limit. | **A1**, **A2**, **A6**, **Y3** | Conformance suite and traces (`procrastinators.testing`); threads, tasks, and mixed sync/async handles racing on one memory store admit exactly the quota, with no partial composed debit (`tests/concurrency/test_memory_races.py`); spawned processes racing on one SQLite file admit exactly the quota with no partial composed debit, and workers killed before or after commit leave the declared outcome (`tests/concurrency/test_sqlite_processes.py`); spawned sync and async processes on one Redis or Valkey server, one PostgreSQL database, or one Memcached server admit exactly the quota, with no partial composed debit where composition is offered (`tests/concurrency/test_redis_processes.py`, `tests/concurrency/test_postgres_processes.py`, `tests/backends/test_memcached.py`) | Phase 12 clean-install runs of every advertised service and mode |
| `idempotent_key(config)` then `RateLimiter(algorithim=..., key=key)` used as `with limiter as limit:` | **I2**, **I9**, **A2**, **A3**, **L6** | The goal's and the design's examples run against an explicit memory backend (`tests/contracts/test_facade.py`); `from_config` supplies limits and backend from configuration (`tests/config/test_from_config.py`); without a backend a limiter uses the shared SQLite file (`tests/backends/test_sqlite.py`) | Phase 12 release validation |

## Requirements

| Requirement | Contracts | Evidence now | Planned acceptance tests |
| --- | --- | --- | --- |
| Support for Token Bucket, Leaky Bucket, Fixed Window, Sliding Log, and Sliding Counter algorithims. | **P1**, **P4**, **P5**, **P7**, **E1**, **E2**, **E3**, **E4**, **E5** | All five reference algorithms pass `check_algorithm` and agree with the search-based oracles over random histories (`tests/algorithms/`); the memory, SQLite, and PostgreSQL backends, sync and async, pass `check_backend` with no skips (`tests/backends/test_memory.py`, `tests/backends/test_sqlite.py`, `tests/backends/test_postgres.py`); native Lua executors for all five pass every trace and scenario on Redis and Valkey and agree with the reference evaluators over random histories to the edges of the numeric range (`tests/backends/test_redis.py`); Memcached passes every constant-state trace (`tests/backends/test_memcached.py`) | Phase 12 published compatibility tables and release validation |
| Should support synchronous and asynchronous limiting. | **T2**, **O5**, **W4**, **Y1** | One waiting plan run by sync and async runners; heartbeat, unrelated-key, and cancellation tests (`tests/concurrency/test_waiting.py`); the async memory handle never blocks the loop on its lock (`tests/backends/test_memory.py`); the async SQLite handle runs storage on a bounded dedicated executor, keeps the loop responsive, and settles cancelled work before closing (`tests/concurrency/test_sqlite_async.py`, `tests/backends/test_executor.py`); native asyncio drivers for Redis, Valkey, and PostgreSQL pass the asynchronous conformance suite, and an admission cancelled in flight propagates the cancellation and may have committed (`tests/backends/test_redis.py`, `tests/backends/test_postgres.py`) | Phase 12 clean-install runs of every advertised mode |
| Should support in-processes limits between methods/functions, async limitinng coroutines, and between processes via file/database/redis/memcache/valkey/... | **Y3**, **Y4**, **L8**, **H5** | In-process memory backend with fork refusal (`tests/backends/test_memory.py`); SQLite file coordination across spawned and forked processes and restarts (`tests/backends/test_sqlite.py`, `tests/concurrency/test_sqlite_processes.py`); service-job gate (`tests/test_service_gate.py`); Redis and Valkey, a Redis Cluster, PostgreSQL, and explicitly best-effort Memcached, each against a real server (`tests/backends/`, `tests/concurrency/`, `-m service`); capability matrix and deployment assumptions ([Backends](backends.md)) | Phase 12 clean-install tests of each extra |
| Should be easy for users to change configuration of their limiters via arguments -> environment variable specified defaults -> project defaults -> team/org level defaults. | **G1**, **G2**, **G3**, **G4**, **G5**, **G6** | Table-driven precedence across all five layers, unset versus explicit null, wholesale rate replacement, unknown-field and environment rejection (`tests/config/test_resolution.py`) | Phase 12 release validation of configured deployments |
| Project and team/org defaults should be saveabvle and readable using PlatformDirs. | **G7**, **G9**, **G10** | PlatformDirs locations, TOML round trip, atomic save with revision checks, password refusal, and redacted `explain` (`tests/config/test_files.py`, `tests/config/test_resolution.py`) | Phase 12 clean-install tests of saved configuration |
| Limiters should be able to be used as decorators, or context managers. | **A2**, **A3**, **L6**, **L7** | Contexts, invocations, and decorators with metadata preserved, generator rejection, no refund on exceptions, and per-call admissions (`tests/contracts/test_facade.py`, `tests/concurrency/test_waiting.py`); decorators and contexts on configured limiters (`tests/config/test_from_config.py`) | Phase 12 HTTP integration contexts |
| The design should make ergonomics for the users a top priority. | **I3**, **B2**, **O1** | `RateLimiter(key=..., limits=[Limit(10, per="1s")], backend="memory://")` with contexts, decorators, and `try_acquire` (`tests/contracts/test_facade.py`); runnable examples checked from a clean install (`examples/`) | Phase 12 release validation |
| Users should be able to implement their own algorithims if they want. | **Y5**, **Y6**, **Y7**, **I10** | A registered third-party algorithm enforced through the facade and the memory backend (`tests/contracts/test_facade.py`, `tests/backends/test_memory.py`); explicit `Registry` (`tests/foundations/test_registry.py`) | Phase 12 published custom-algorithm example running `check_algorithm` |
| Users should be able to implemnet their own backends if they want. | **A1**, **Y1**, **Y2**, **H1**, **H2**, **H4** | `BackendCase` and `check_backend`; nine deliberately broken backends each caught (`tests/contracts/test_checkpoint.py`) | Phase 12 published custom-backend example running `check_backend` |

## Checkpoint findings

Reviewing the models, protocols, and traces together at the Phase 3 checkpoint
changed three contracts:

- **P4** — a strictly paced leaky bucket's largest single cost is `amount`, not
  `max(1, burst_tolerance)`. Weighting and tolerance are independent: a weighted
  operation pays by pushing the next eligible time out, and the tolerance says
  how early an operation may start.
- **P5**, **A5** — a rule's initial state is established at its first attempt
  and recorded even when that attempt is denied. `Algorithm.initial_changes`
  now receives the first attempt's authority time.
- **P5** previously cited **C4** for when state may be forgotten; the rule it
  meant is **L9**.
