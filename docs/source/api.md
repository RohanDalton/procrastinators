# Public API specification

The facade's public signatures, reviewed against [the contracts](contracts.md).
Rule references like **A2** point at that document.

`RateLimiter`, `Invocation`, and `idempotent_key` are importable from
`procrastinators`. The in-process memory backend and the SQLite backend, which
coordinates every process on one machine and is the default, need nothing
installed; Redis, Valkey, PostgreSQL, and best-effort Memcached backends need
their driver's extra. [Backends](backends.md) compares what each guarantees.

---

## Construction

```python
class RateLimiter:
    def __init__(
        self,
        *,
        key: str,
        limits: Sequence[Limit | PolicySpec] | None = None,
        rules: Mapping[str, Limit | PolicySpec] | None = None,
        algorithm: Algorithms | str | None = None,
        backend: str | SyncBackend | AsyncBackend | None = None,
        namespace: str = "default",
        options: Mapping[str, object] | None = None,
        timeout: Seconds | None = None,
        storage_timeout: Seconds = 5.0,
        accept_best_effort: bool = False,
        on_event: DiagnosticsCallback | None = None,
        registry: Registry | None = None,
        clock: DeadlineClock | None = None,
        sleeper: Sleeper | None = None,
        async_sleeper: AsyncSleeper | None = None,
    ) -> None: ...
```

`Seconds` is a number of seconds, a duration string such as `"250ms"`, or a
`timedelta`.

| Argument | Meaning |
| --- | --- |
| `key` | Stable quota key, normally from `idempotent_key({...})`. With `namespace` it forms the quota identity (**I2**). |
| `limits` | Rates for this scope. Positional rule ids `#0`, `#1`, … follow list order, so reordering is a policy conflict (**I3**). An entry may be a `Limit` or a policy object, which is how a registered third-party algorithm is used (**Y7**). |
| `rules` | Explicit rule names instead of positional ones. Recommended for managed configuration. Mutually exclusive with `limits`. |
| `algorithm` | One policy choice for every `Limit` in the scope; the sliding log when `None`. A policy object names its own algorithm, and naming a different one here is an `InvalidPolicy`. |
| `backend` | An address (`"sqlite://"`, `"sqlite:///quota.sqlite3"`, `"memory://"`, `"memory://ankh"`, `"redis://host:6379/0"`, `"valkey://host"`, `"postgresql://host/db"`, `"memcached://host:11211"`) or an already-constructed backend. An address is resolved through `registry`; its handles are **owned** and closed by the limiter. A backend object is **borrowed** and never closed by the limiter (**L2**). `None` means `"sqlite://"`, the shared per-user database file; memory is never chosen silently, because it coordinates nothing beyond one process. |
| `options` | Algorithm options applied to every `Limit`: `epoch_offset` (fixed window), `capacity` and `initial_tokens` (token bucket), `burst_tolerance` (leaky bucket). |
| `timeout` | Default quota-wait budget. `None` waits indefinitely, `0` attempts once, positive values bound the whole attempt (**B2**). |
| `storage_timeout` | Per-storage-call cap. Never `None`, whatever `timeout` says (**B3**). |
| `accept_best_effort` | Accept a backend whose state may vanish through eviction, such as Memcached. Never implied, and never read from configuration: a cache miss is not proof that no quota was consumed (**Y4**). |
| `on_event` | Diagnostics callback, invoked outside critical sections; cannot change outcomes (**D2**). |
| `registry` | Resolves algorithm ids and backend families; the process-wide `default_registry()` when `None`. |
| `clock`, `sleeper`, `async_sleeper` | Local monotonic time and the two ways of waiting; real ones when `None`. Injected by tests to run on fake time. |

Construction validates policies, rule identities, and the backend's
capabilities for every mode it offers, and raises `ConfigurationError`,
`InvalidPolicy`, or `UnsupportedCapability`. It opens no connection and starts
no thread: a limiter that has not been used yet has done nothing.

An address gives a limiter both modes, because the backend family's factory
builds a sync and an async handle on one authority. A backend object offers
only its own mode; calling the other raises `UnsupportedCapability` rather than
wrapping blocking calls in the event loop.

`memory://` names the process-wide store called `default`, and `memory://<name>`
the store called `<name>`, so separately constructed limiters naming the same
store share its quotas. Stores are per process: a forked child resolves a fresh
store, and a store object carried across a fork refuses to serve (**L8**).

## Backends

| Address | Coordinates | Survives restart | Notes |
| --- | --- | --- | --- |
| `memory://`, `memory://<name>` | Threads and coroutines in one process (`IN_PROCESS`) | No (`EPHEMERAL`) | Explicit opt-in only. |
| `sqlite://` | Every process of one OS user on one machine (`LOCAL_MACHINE`) | Yes (`LOCAL_DURABLE`) | The default: `limits.sqlite3` in PlatformDirs' `user_state_path("procrastinators")`. |
| `sqlite:///relative/path`, `sqlite:////absolute/path` | Every process reaching that file on one machine | Yes | A relative path resolves once, against the current directory at construction. |

The SQLite backend admits inside `BEGIN IMMEDIATE`, which takes the write lock
before anything is read, and commits before reporting success (**A1**, **A2**).
Authority time is the machine's wall clock, clamped against the latest time any
process recorded in the file (**T7**). Connections run with `synchronous=FULL`
and write-ahead logging, so an acknowledged admission survives a process crash
and, on storage that honors `fsync`, a power loss. A file held by another
writer past the request's lock budget is `BackendBusy`, never a denial (**O2**).

The guarantee is limited to **local filesystems**: SQLite's locking is not
reliable on network filesystems, so a database there is unsupported, and
workers on different machines need a service backend. Workers run by different
OS users need an explicitly shared path they can all write. Relative paths,
`:memory:`, hosts, queries, and fragments in `sqlite://` addresses are refused
or resolved as described above rather than guessed at.

Asynchronous SQLite calls run one at a time on a dedicated worker thread per
handle (`DedicatedExecutor`), bounded in queued work; quota waiting stays on the
calling task, so the event loop is never blocked (**W4**). A call cancelled
before its transaction starts consumes nothing; one cancelled after may still
commit, which is possible consumed capacity and never permission (**O5**), and
`aclose()` waits for such work to settle (**L4**). After `fork`, a handle opens
a fresh connection (and executor) in the child and coordinates with its parent
through the file (**L8**).

## Configured limiters

```python
@classmethod
def from_config(
    cls,
    profile: str,
    *,
    key: str,
    project: str | None = None,
    organization: str | None = None,
    project_file: str | os.PathLike[str] | None = None,
    organization_file: str | os.PathLike[str] | None = None,
    environ: Mapping[str, str] | None = None,
    locations: ConfigLocations | None = None,
    on_event: DiagnosticsCallback | None = None,
    registry: Registry | None = None,
    clock: DeadlineClock | None = None,
    sleeper: Sleeper | None = None,
    async_sleeper: AsyncSleeper | None = None,
    **overrides: object,
) -> RateLimiter: ...

@property
def config(self) -> ResolvedConfig | None: ...
```

Resolves `profile` once through every layer, strongest first: `overrides`,
`PROCRASTINATORS_*` environment defaults, the selected project, the selected
organization, and library defaults (**G1**), then builds the limiter through the
ordinary constructor, so the backend's capabilities are checked before the
first admission. `overrides` are configuration fields only — `limits`, `rules`,
`algorithm`, `backend`, `namespace`, `options`, `timeout`, `storage_timeout` —
and any other name is a `ConfigurationError` (**G5**). The limiter never re-reads
configuration (**G6**); `limiter.config` keeps the resolution, and
`limiter.config.explain()` says which layer supplied each setting, secrets
redacted (**G7**). A directly constructed limiter's `config` is `None`.

`key` is always explicit, never derived from configuration, so editing a
profile's rates meets the stored policy as a `PolicyConflict` rather than
creating fresh quota (**G8**). There is no default vendor rate: a profile that
supplies none, with none passed, is a `ConfigurationError`.
[Configuration](configuration.md) describes the layers, files, and environment
variables.

## Keys

```python
def idempotent_key(scope: Mapping[str, object], *, secret: bytes | None = None) -> str: ...
```

Derives the stable quota key a limiter's `key` argument expects
from a scope mapping — vendor, dataset, endpoint, credential *reference* — and
never from rates (**I2**). The same mapping gives the same key in every process
(**I8**, **I9**); `secret` makes it an HMAC for credential-derived identity.
Unsupported values raise `InvalidIdentity`.

```python
key = idempotent_key({"vendor": "ankh", "dataset": "orders"})
```

## Composition

```python
@classmethod
def combine(
    cls,
    *limiters: RateLimiter,
    timeout: Seconds | None = ...,
    storage_timeout: Seconds = ...,
    accept_best_effort: bool = ...,
    on_event: DiagnosticsCallback | None = ...,
) -> RateLimiter: ...
```

Returns a limiter whose every acquisition is one atomic admission of all
participating constraints (**C1**). Identical constraints deduplicate (**C2**);
conflicting ones raise `PolicyConflict` (**C3**); constraints on different
authorities raise `UnsupportedCapability` (**C6**), and incompatible
coordination domains raise `PolicyConflict` (**C4**). Composition is checked at
`combine` time, not at the first acquisition. The combined limiter borrows its
parts' backends, and takes its defaults from the first limiter unless given.

## Acquiring

```python
def acquire(self, cost: int = 1, timeout: Seconds | None = ...) -> Admission: ...
async def acquire_async(self, cost: int = 1, timeout: Seconds | None = ...) -> Admission: ...

def try_acquire(self, cost: int = 1) -> Decision: ...
async def try_acquire_async(self, cost: int = 1) -> Decision: ...
```

| Method | Behavior |
| --- | --- |
| `acquire` | Waits for capacity and returns proof of one committed acquisition. Raises `AcquireTimeout` when the deadline passes, having consumed nothing (**B5**). |
| `acquire_async` | The same meaning, always awaitable. Never blocks the event loop; an unrelated key keeps running (**W4**). |
| `try_acquire` | Exactly one atomic attempt, no quota waiting. Returns a `Decision`; storage work still has its own timeout. |
| `try_acquire_async` | Awaitable counterpart. |

Sync and async are separate methods rather than one returning either. `cost`
must be a positive integer within the rule's capacity, or `InvalidCost` is
raised before any waiting (**P3**).

Waiting follows **W1**–**W3**: every attempt is one atomic backend admission,
sleeps happen between attempts with nothing held, each sleep is exactly the
reported delay (no jitter is added), and a denial whose delay outlasts the
deadline times out at once rather than sleeping to the deadline (**K5**).
Contention and unavailability are retried with a capped backoff within the
budget; an indeterminate commit is raised at once and never retried (**B4**,
**O4**).

A returned `Decision` with `allowed=True` **has already been charged**. Code
that checks it and then calls `acquire` charges twice:

```python
decision = limiter.try_acquire(cost=1)
if decision.allowed:
    fetch_page()          # already charged; do not acquire again
```

## Contexts

```python
def __enter__(self) -> Admission: ...
def __exit__(self, *exc: object) -> None: ...
async def __aenter__(self) -> Admission: ...
async def __aexit__(self, *exc: object) -> None: ...
```

Entry acquires and returns the `Admission`. Exit does nothing at all: it does
not refund quota (**A3**) and does not close storage (**L6**). Exceptions
propagate untouched.

Each entry owns its own result. The limiter holds no "current admission", so
concurrent and reentrant use cannot overwrite another call's (**L7**).

## Invocation options

```python
def __call__(
    self,
    *,
    cost: int = 1,
    timeout: Seconds | None = ...,
) -> Invocation: ...
```

Returns an immutable wrapper usable three ways, leaving the limiter unchanged:

```python
with limiter(cost=5, timeout=30):        # sync context
    fetch_batch()

async with limiter(cost=5):              # async context
    await fetch_batch_async()

@limiter(cost=5)                         # decorator
def fetch_batch(): ...
```

## Decorators

```python
@overload
def __call__(self, func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]: ...
@overload
def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...
```

`@limiter` and `@limiter(cost=…)` both work. The wrapper preserves
`functools.wraps` metadata and picks the sync or async acquisition path from
the decorated function, so an async function is never wrapped in a blocking
acquire.

Generator and async-generator functions are **rejected** with
`UnsupportedCapability`. Construction and iteration have different admission
points, and charging at construction would limit how often generators are
created rather than how often requests are made. Acquire inside the loop,
around each actual request.

## Inspection and cooldowns

```python
def inspect(self) -> Snapshot: ...
async def inspect_async(self) -> Snapshot: ...

def defer_for(
    self, duration: Seconds, *, scope: QuotaIdentity | None = None, reason: str = ""
) -> Cooldown: ...
async def defer_for_async(
    self, duration: Seconds, *, scope: QuotaIdentity | None = None, reason: str = ""
) -> Cooldown: ...
```

`inspect` is advisory and never a reservation (**R7**). Sleeping on a snapshot
and then executing without acquiring is a misuse the type name is chosen to
discourage. A rule the backend has never seen is reported with the algorithm
`"unused"`.

`defer_for` extends a shared cooldown with `max(existing, new)` (**K2**). It is
the supported path for a vendor's `Retry-After`: it fabricates no quota events
and replays no application work (**K4**). `scope` defaults to the limiter's
scope; a combined limiter spanning several scopes must name one. Raises
`UnsupportedCapability` on a backend without cooldowns.

## Closing

```python
def close(self) -> None: ...
async def aclose(self) -> None: ...
```

Idempotent (**L1**). Releases only owned resources; a borrowed backend or client
is left alone (**L2**). Never deletes quota state (**L3**). Waits for owned
outstanding work to settle, including executor work that may still commit
(**L4**). Calls afterwards raise `ClosedResource` (**L5**). `close()` closes an
owned synchronous handle; an owned asynchronous handle needs awaiting, so
`aclose()` closes both.

`close()` is independent of context entry: `with limiter:` acquires quota and
does not close anything.

## What the facade does not expose

Backend clients, connection pools, Lua scripts, SQL schemas, table names, and
key encodings are not part of this contract. A limiter built on memory and one
built on Redis differ in their `backend` argument and their declared
capabilities, and nowhere else in this document. Anything a caller can only do
by reaching through to a driver is a capability that has not been designed yet,
not an escape hatch.

## Errors at a glance

| Raised by | Error | Meaning |
| --- | --- | --- |
| Construction | `ConfigurationError`, `InvalidPolicy`, `UnsupportedCapability` | Settings cannot produce a usable limiter. |
| Any acquisition | `InvalidCost` | Cost is malformed or above capacity. |
| `acquire`, `acquire_async` | `AcquireTimeout` | Deadline passed; nothing consumed. |
| Any acquisition | `PolicyConflict` | Stored policy disagrees with this worker's. |
| Any acquisition | `BackendBusy`, `BackendUnavailable` | Contention or an unreachable authority — never a denial. |
| Any acquisition | `IndeterminateAdmission` | Commit outcome unknown; the body does not run and nothing is refunded. |
| After `close` | `ClosedResource` | The limiter was closed. |
| Async callers | `asyncio.CancelledError` | Propagates unchanged; never translated into a quota error (**O5**). |
