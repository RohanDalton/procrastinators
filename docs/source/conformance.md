# Conformance tools

`procrastinators.testing` is how an algorithm or backend proves it keeps the
[contract](contracts.md). The same tools check the built-ins, so a third-party
implementation is held to exactly the standard the shipped ones are. They are
test support: nothing in them enforces a limit, and the main package never
imports them.

## Checking a backend

Describe how to build the backend on a fake timeline, with an observer, and
which guarantees it claims:

```python
from procrastinators.testing import BackendCase, Guarantee, check_backend


def build(timeline, observer):
    return MyBackend(clock=timeline.epoch_clock, observer=observer)


def test_my_backend_conforms():
    report = check_backend(BackendCase("my backend", build))
    report.raise_for_failures(allow_skips=False)
```

The suite builds a fresh backend for every check and reports each as passed,
failed, or skipped with a reason. A check is skipped only when the backend's
capabilities or claimed guarantees make it inapplicable, and
`allow_skips=False` turns any skip into a failure, so a CI job cannot pass by
checking nothing. `check_async_backend` with an `AsyncBackendCase` runs the
same checks, awaited.

| Guarantee | What the suite checks |
| --- | --- |
| `REFERENCE_DECISIONS` | Every applicable shared trace and scenario gives the expected verdicts (**N4**). |
| `EXACT_RETRY_DELAYS` | …and the exact retry delays and blocking rules. |
| `AUTHORITY_TIMESTAMPS` | Admissions carry the injected clock's time, sampled after the lock (**T4**). |
| `OBSERVATION_POINTS` | Failures and cancellations injected at each point are reported as **H2** requires, commit nothing before the commit, and refund nothing after it. |
| `POLICY_CONFLICTS` | The same rule under another policy raises `PolicyConflict` and resets nothing (**I4**). |
| `LIFECYCLE` | Closing is idempotent and later calls raise `ClosedResource` (**L1**, **L5**). |

A backend that implements none of the built-in algorithms passes `probe` and
`conflicting_probe` policies of its own: a capacity-one policy, and another of
the same algorithm with different parameters.

### What the suite is required to catch

The checkpoint (**H4**) is only evidence if the suite can fail, so
`tests/contracts/test_checkpoint.py` runs it against deliberately broken
backends. Each must fail the checks aimed at its flaw:

| Broken backend | Caught by |
| --- | --- |
| Records admissions when the body exits | The long-running traces and the charge-on-exit scenario |
| Debits each composed rule in turn | The composition traces |
| Reports success after an unknown commit | `failure_after_commit` |
| Refunds an unknown commit | `failure_after_commit` |
| Samples time before taking its lock | `time_sampled_after_lock` |
| Reports cancellation as unavailability | `cancellation_before_commit`, `cancellation_after_commit` |
| Ignores stored fingerprints | `policy_conflict` |
| Keeps working after close | `lifecycle` |
| Evicts a drained bucket after a second idle | `token_bucket.drained_bucket_is_not_forgotten` |

## Checking an algorithm

```python
from procrastinators.testing import check_algorithm

check_algorithm(MyAlgorithm()).raise_for_failures()
```

The algorithm runs inside `EvaluatorHost`, which performs the whole admission
sequence of **A1** around it, and every evaluation is repeated and compared, so
an evaluator that reads a clock or keeps state on itself fails. A built-in
algorithm id runs its shared traces and scenarios; a third-party algorithm
passes its own `traces`.

## Writing traces

A `Trace` is data: rules with policies, then `Attempt` and `Finish` steps in
time order, each attempt with its expected verdict and, for a denial, its exact
retry delay. Work the expected values out by hand from the contract — not by
running the implementation being tested — and note the arithmetic where it is
not obvious. `tests/oracles.py` shows the independent cross-check used for the
shared traces: naive models that find each retry delay by searching forward in
time.

A `Scenario` is coarser: bursts of identical attempts and, per policy, how many
each burst admits. `Scenario.trace_for` compiles one policy's expectation into a
trace. The four scenarios in `SCENARIOS` record failures demonstrated against
existing rate limiters, stating what every policy should do rather than judging
all of them by a sliding log.

## Deterministic concurrency and failures

| Tool | Use |
| --- | --- |
| `FakeTimeline` | One instant read through an epoch clock, an async epoch clock, and a deadline clock. `advance` moves both; `step_epoch` moves only the wall clock. |
| `RecordingSleeper`, `AsyncRecordingSleeper` | Record each wait and advance the timeline. `during` acts while the caller sleeps; `lock_held` turns sleeping under a lock into a violation (**W1**). |
| `FaultInjector` | An observer that records every point and, when armed, raises or runs a callable there. |
| `Pause` | An armed action that holds a thread at a point until released, for choosing an interleaving instead of hoping for one. |
| `ScriptedBackend`, `AsyncScriptedBackend` | Replay chosen decisions and exceptions, for testing waiters and the facade without an algorithm. |
| `rolling_window_violations` and friends | Check that a history of admissions — from racing threads, say — satisfies a policy's guarantee. |

## Required service jobs

Service tests are marked with the service they need and ask whether it is
reachable:

```python
@pytest.mark.service("redis")
def test_granny_weatherwax_holds_the_line(service_available):
    service_available("redis", redis_is_reachable, "REDIS_URL not set")
    ...
```

Locally, an unreachable server skips the test. Each backend's CI job names its
service, which makes an unreachable server a failure and fails the session if
no test of that service passed (**H5**):

| Job | Command | Server |
| --- | --- | --- |
| `service-redis` | `pytest -m service --require-service redis` | `PROCRASTINATORS_REDIS_URL` |
| `service-valkey` | `pytest -m service --require-service valkey` | `PROCRASTINATORS_VALKEY_URL` |
| `service-redis-cluster` | `pytest -m service --require-service redis-cluster` | `PROCRASTINATORS_REDIS_CLUSTER_URL` |
| `service-postgres` | `pytest -m service --require-service postgres` | `PROCRASTINATORS_POSTGRES_URL` |
| `service-memcached` | `pytest -m service --require-service memcached` | `PROCRASTINATORS_MEMCACHED_URL` |

`PROCRASTINATORS_REQUIRE_SERVICES=redis,valkey` is equivalent to passing both
options. Every Redis test runs against Redis and against Valkey, so the two
jobs exercise the same suite on both servers.

### Native executors

A backend that runs an algorithm natively — a Lua script, a stored procedure —
proves it agrees with the reference evaluator the same way the Redis and
Valkey executors do: the whole backend suite with the store's time replaced by
the timeline, so every trace and scenario runs at exact instants, plus a
property test running random histories of random policies, from the smallest
to the edges of the supported numeric range, on the native store and on a
memory store side by side, comparing every decision field
(`tests/backends/test_redis.py`). A native executor is registered only with the
names of the traces it passes (**Y5**).
