# Procrastinators

Rate limiting for ETL libraries and pipelines: several algorithms, several
backends, sync and async, and an ergonomic facade.

> **Status: distributed ETL release.** Phases 0-11 of [the implementation plan](IMPLEMENTATION_PLAN.md)
> are in place: the normative specification and its typed models, the protocols
> a custom algorithm or backend implements, the conformance harness, all five
> reference algorithms, an atomic in-process memory backend, a SQLite backend
> that coordinates every process on one machine (the default), the
> `RateLimiter` facade with sync and async waiting, contexts, decorators,
> composition, cooldowns, and diagnostics, layered configuration with
> PlatformDirs persistence (`from_config`), and service backends for workers on
> any machine: Redis and Valkey with native Lua executors, PostgreSQL, and a
> deliberately best-effort Memcached. HTTP integrations are not implemented yet.
> [Backends](docs/source/backends.md) compares what each guarantees.
>
> Read [docs/source/contracts.md](docs/source/contracts.md) for what the library
> guarantees, [docs/source/api.md](docs/source/api.md) for the public surface,
> [the design](DESIGN.md) for why, and [goals](goal.md) for the requirements.

```python
from procrastinators import Limit, RateLimiter, idempotent_key

limiter = RateLimiter(
    key=idempotent_key({"vendor": "ankh", "dataset": "orders"}),
    limits=[Limit(10, per="1s"), Limit(500, per="1m")],
)

with limiter:  # shared with every process of this user, through a SQLite file
    fetch_page()
```

See [examples/](examples/) for shared quotas, coroutines, multiple processes,
weighted operations, and configuration provenance.

## Development

See [docs/source/development.md](docs/source/development.md) for the local check commands and
the difference between the unit suite and service integration tests.
