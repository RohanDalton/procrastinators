# Procrastinators behavior contract

This is the normative specification. Where it and the design (`DESIGN.md` at
the repository root) differ, this document governs implementations and tests;
the design remains the architectural rationale. Conformance tests, algorithm traces, and backend
acceptance suites are written against the statements here.

Every rule below is numbered so tests and review comments can cite it. A later
discovery may revise a rule, but the revision must update this document, the
conformance expectations, and the affected implementations together.

Key words **must**, **must not**, **may**, and **should** carry their usual
weight. "The authority" means whichever store decides admission: the memory
instance, the SQLite file, the Redis deployment, and so on.

---

## 1. Admission

**A1.** *Admission is one atomic check-and-commit performed by the backend.*
Inside a single transaction or critical section the backend validates policy
metadata, obtains the time, prunes obsolete state, evaluates every rule, and
commits every debit only if every rule admits. Nothing observable happens
between the check and the commit.

**A2.** *Quota is consumed before user code runs.* The caller receives control
only after the commit succeeded. Charging on completion would let every
concurrent entrant pass the check before any of them recorded anything.

**A3.** *There is no refund.* Exiting a context, raising an exception, an HTTP
error, and cancellation after commit all leave the quota consumed.
:class:`Admission` therefore carries no release operation. The library charges
attempts, not successes.

**A4.** *A retry is a new acquisition.* Application code that sends a second
request acquires again. The core never retries application code.

**A5.** *Denial consumes nothing.* A denied attempt must not debit any rule,
must not extend any cooldown, and must not lengthen anyone's wait. It **may**
prune state that can no longer affect a decision, and it **must** record a
never-used rule's initial state (**P5**), which consumes nothing.

**A6.** *Composition is all-or-nothing.* If any constraint in a request denies,
no constraint is debited. Because nothing is ever committed provisionally,
there is no partial debit to compensate for.

**A7.** *Admission timestamps are the guarantee.* The library controls when
admissions are recorded, not when packets reach a vendor. A paused process,
network buffering, another application sharing the credential, or a different
server-side policy can still produce a 429.

---

## 2. Time

**T1.** *Three time domains exist and are never interchanged.*

| Domain | Type | Meaning |
| --- | --- | --- |
| Authority epoch time | `EpochMicros` | Microseconds since the Unix epoch, read by the admission authority. The only domain for stored state and admission timestamps. |
| Local monotonic time | `MonotonicMicros` | Microseconds from an arbitrary local origin. Deadlines and elapsed measurement only. |
| Duration | `DurationMicros` | A length of time with no origin. Periods, retry delays, timeouts. |

**T2.** *A client deadline is local monotonic time and must never be sent to a
remote authority as a comparable timestamp.* The three types are distinct at
the type level so this is a static error, not a production incident.

**T3.** *All internal time is integer microseconds.* Conversion between public
seconds and internal microseconds happens at documented boundaries: the `Limit`
constructor, the facade's `timeout` arguments, and configuration loading.

**T4.** *Timestamps are obtained inside the critical section, after locks are
acquired.* An operation that queued for a lock must not commit a timestamp read
before it waited.

**T5.** *Time is re-read after every wait.* A waiter must never advance its
notion of the current time by the amount it intended to sleep.

**T6.** *Rounding is conservative.* Replenishment is never rounded up and a
required delay is never rounded down. A period with a sub-microsecond remainder
rounds up, lengthening it, because for `N` per `T` a longer `T` is stricter.

**T7.** *Backwards movement of authority time is clamped* against the last
observed value. This is conservative and may reduce throughput. Clamping cannot
repair arbitrary forward jumps; a backend that tolerates them must document its
trusted-clock assumption.

**T8.** *Fixed windows align to the Unix epoch plus a configured offset*, never
to process startup. Two workers that started at different times must agree on
where a boundary falls. Alignment additionally assumes the configured epoch
matches the vendor's.

---

## 3. Policies and boundaries

**P1.** *Interval conventions.* Rolling intervals are `(now - period, now]`.
Fixed intervals are `[start, start + period)`. An event exactly `period` old has
left a rolling window.

**P2.** *A cost is a positive integer* describing one atomic operation. It does
not space out several physical requests made inside the caller's body.

**P3.** *An impossible cost is an error, not a denial.* A cost above a rule's
capacity raises `InvalidCost` at request construction, before any waiting,
because denying it would imply an unbounded retry delay.

**P4.** *Per-algorithm guarantees.*

| Algorithm | Guarantee | Notes |
| --- | --- | --- |
| Sliding log | At most `amount` cost units in every rolling `period`. | Exact. Retains one timestamp/cost entry per admission still in a window. The default. |
| Fixed window | At most `amount` per aligned interval. | Admits up to `2 * amount` across a boundary. That is correct fixed-window behavior and must not be judged by a rolling-window test. |
| Sliding counter | `floor(previous * remaining / period) + current + cost <= amount`. | Approximate. Does **not** promise a rolling bound. Explicit opt-in only. |
| Token bucket | Refill `refill_amount` per `refill_period`, capped at `capacity`. | Burst-plus-refill envelope. `initial_tokens` defaults to `capacity`. |
| Leaky bucket | Pacing at `amount / period`, no accumulated idle credit by default. | Admitted cost `c` advances the next eligible time by `c / rate`. `burst_tolerance` defaults to 0 and governs how early an operation may start, not how large it may be: the largest single cost is `amount` whatever the tolerance. |

**P5.** *Initial state is explicit and is not re-derivable by forgetting.* A
token bucket configured to start empty must not be reset to "unused" after an
idle period and then treated as initially full. State may only be forgotten
once the rule's state is genuinely indistinguishable from its configured
initial state — see **L9**. A rule's initial state is established at its first
attempt, at that attempt's authority time, and is recorded whether the attempt
is admitted or denied: otherwise an initially empty bucket would restart its
refill clock on every denied retry and never fill.

**P6.** *Retained state is bounded.* One weighted operation of cost `c` stores
one entry of weight `c`, never `c` entries.

**P7.** *Token bucket and leaky bucket differ by default* and must not be
substituted for each other. Their burst semantics are part of their public
configuration.

**P8.** *Exact enforcement and smooth pacing compose* — a sliding log plus a
leaky bucket in one atomic admission — rather than one silently replacing the
other.

---

## 4. Numeric bounds

**N1.** *Supported ranges.*

| Quantity | Range |
| --- | --- |
| `amount`, `capacity` | 1 to 2³¹ − 1 |
| `cost` | 1 to 2³¹ − 1 |
| `period` | 1 millisecond to 100 years |
| Timestamps, durations | 0 to 2⁵³ − 1 microseconds |

**N2.** *Out-of-range values are rejected, never wrapped or saturated.*

**N3.** *2⁵³ − 1 is the exactness ceiling.* The binding constraint is the Lua
interpreter embedded in Redis, whose numbers are IEEE doubles. Every
intermediate value a native executor must compute exactly stays at or below it.
`SlidingCounterPolicy` therefore also requires `amount * period_us <= 2⁵³ − 1`,
because its weighting multiplies a count by a remaining-time value. For the same
reason every retry delay a policy can produce must be a representable duration:
`TokenBucketPolicy` requires `ceil(capacity / refill_amount) * refill_period_us`
— the time to refill an empty bucket — and `LeakyBucketPolicy` requires
`burst_tolerance * period_us // amount + period_us` — how far its schedule can
run ahead — to be at most 2⁵³ − 1 µs. A leaky bucket whose next admission would
schedule it past the largest timestamp raises `InvalidPolicy` rather than write
an out-of-range value (**N2**).

**N4.** *Native executors produce identical results to the reference evaluator*
within these bounds, verified against shared traces (Phase 3).

**N5.** *Booleans are not integers here.* `True` is rejected wherever a count is
expected, since `Limit(True, per="1s")` is never what anyone meant.

**N6.** *Non-finite floats are rejected.* Float durations are read through their
shortest round-tripping decimal spelling, so `per=0.1` is exactly 100 000 µs.

---

## 5. Identity

Five things are separate, and conflating any two of them causes a specific bug.

**I1.** *Namespace* identifies the environment or quota domain. Workers in
different namespaces never share counters even when their keys match.

**I2.** *Quota identity* is `(namespace, key)`, where the key is a canonical
digest of the caller's scope mapping — vendor, dataset, endpoint, credential
reference. It **must not** include rate values or the full configuration:
changing a rate must address the same stored quota rather than creating an empty
one.

**I3.** *Rule identity* is `(quota identity, rule name)`. Names are explicit
(`"burst"`) or positional (`#0`, `#1`, ...).

- The simple list API assigns positional names in list order and fingerprints
  the complete normalized policy of the scope. Reordering or changing that list
  is therefore a **policy conflict**, not a request for fresh quota.
- Explicit names survive reordering and are recommended for managed
  configuration and persistent named rules.

**I4.** *Policy fingerprint* covers algorithm, parameters, state version, and
schema. It is stored alongside the state and checked atomically during
admission. A mismatch raises `PolicyConflict`.

**I5.** *State version* is the on-disk representation version of an algorithm's
state, bumped when a codec changes. It is part of the fingerprint.

**I6.** *Coordination domain* names the partition that must physically hold a
rule's state, such as a Redis Cluster hash slot. It is not identity; it is a
placement constraint that determines what can be composed atomically.

**I7.** *Scoping is not hierarchy.* A key over `{vendor, dataset, endpoint,
credential}` limits exactly that combination. A vendor-wide quota is a separate
rule, composed alongside it.

**I8.** *Identity is stable across processes and restarts.* Python's `hash()`,
`repr()`, and punctuation replacement are never used to derive it. An ordinary
digest does not protect a low-entropy secret, so credential-derived identity
uses HMAC with an explicitly shared secret, and an account or credential
*reference* is preferred to a raw API key.

**I9.** *Keys come from one canonical, versioned encoding.* `idempotent_key()`
encodes a non-empty string-keyed mapping of `None`, booleans, 64-bit integers,
finite floats, strings, lists, and nested mappings — types kept distinct,
mapping keys sorted by their UTF-8 bytes, list order kept, strings not
Unicode-normalized — and digests it with SHA-256 (`k1-` prefix) or, given a
shared secret of at least 16 bytes, HMAC-SHA-256 (`k1h-`). Anything else is an
`InvalidIdentity`, including values that would need an implicit `str()`. The
encoding is frozen by golden fixtures; changing it is a new version.

**I10.** *Fingerprints cover exactly what changes behavior.* A policy's
fingerprint (`p1-` prefix) digests the fingerprint schema, the algorithm id,
the state version, and the policy's parameters — its dataclass fields, or its
`fingerprint_parameters()` where two spellings behave identically. Equivalent
policies fingerprint identically. Positional rules share one fingerprint of
the whole ordered list, so changing any member or the order conflicts on every
rule (**I3**); named rules are fingerprinted individually.

---

## 6. Outcomes

**O1.** *Each outcome has exactly one class*, so callers can treat them
differently.

| Outcome | Class | Was quota consumed? |
| --- | --- | --- |
| Denied within an attempt | `Decision(allowed=False)` — not an exception | No |
| Deadline expired while waiting | `AcquireTimeout` | No |
| Contention exceeded its budget | `BackendBusy` | No |
| Storage unreachable or refusing | `BackendUnavailable` | No |
| Commit outcome unknown | `IndeterminateAdmission` | **Unknown — assume yes** |
| Stored state undecodable | `StateCorruption` | Unknown |
| Policy disagreement | `PolicyConflict` | No |
| Configuration invalid | `ConfigurationError` / `InvalidPolicy` | No |
| Unsupported combination | `UnsupportedCapability` | No |
| Impossible or malformed cost | `InvalidCost` | No |
| Used after close | `ClosedResource` | No |
| Caller cancelled | `asyncio.CancelledError` — propagates unchanged | Depends; see **O5** |

**O2.** *Contention is not exhaustion.* Lock waits, `SQLITE_BUSY`, and
serialization failures raise `BackendBusy` after bounded retries. Reporting them
as a denial would make a busy database look like a strict vendor.

**O3.** *Failure is closed.* On storage failure, unknown state, or policy
conflict the library denies rather than admits, and never falls back from a
shared backend to private memory. An explicitly configured fail-open mode may
exist and must report that it suspends the rate guarantee.

**O4.** *An unknown commit is surfaced, not guessed.* On
`IndeterminateAdmission` the library does not run the caller's body and does not
refund. Retrying acquisition may conservatively consume additional capacity.
Exactly-once remote acquisition is out of scope.

**O5.** *Cancellation stays cancellation.* `asyncio.CancelledError` propagates
unchanged and is never translated into a quota error. Cancellation before commit
consumes nothing. Cancellation after a commit may have consumed capacity; the
body still does not run, and nothing is refunded. Executor work already
dispatched may commit after its caller is gone — that is possible consumed
capacity, never permission to start the cancelled body.

**O6.** *Driver exceptions are wrapped with their cause preserved.*
`BackendError` accepts a `cause` and sets `__cause__` from it.

**O7.** *There is no shared mutable "last failure".* Every result and error
belongs to the call that produced it.

---

## 7. Results

**R1.** *An allowed decision* carries an `Admission`, names no blocking rules,
and has `retry_after_us == 0`.

**R2.** *A denied decision* carries no `Admission`, names at least one blocking
rule, and has a non-negative, policy-derived `retry_after_us`. These invariants
are enforced in `Decision.__post_init__`, so a contradictory decision cannot be
constructed.

**R3.** *Retry delays are durations, not instants*, so client and authority need
not agree on the time. They come from the same atomic evaluation that produced
the verdict.

**R4.** *Retry delays are advisory.* Another worker may consume the capacity
first. Waiters recheck after sleeping rather than assuming the delay was enough.

**R5.** *Remaining counts are advisory* and explicitly not reservations.

**R6.** *An `Admission` is proof of one completed acquisition*, created only by
a backend after its commit succeeded, identifying the rules charged, the cost,
the authority-epoch timestamp, and the backend.

**R7.** *A `Snapshot` is advisory inspection and never a reservation.* Sleeping
on a snapshot and then executing without acquiring is a misuse.

**R8.** *A `Transition` is a proposal.* An evaluator returns proposed changes, a
retry delay, and a safe-forget horizon; it never commits, locks, sleeps, or
performs I/O. A denied transition may contain only changes marked
`prunes_only`.

---

## 8. Timeout budgets

**B1.** *Four separate budgets*, carried by `OperationBudget`: quota waiting
(the caller's deadline), a per-storage-call timeout, a lock/contention timeout,
and a bounded count of internal retries.

**B2.** *Public `timeout` maps as follows.*

| Value | Meaning |
| --- | --- |
| `None` | Wait indefinitely for quota. Individual storage calls stay bounded. |
| `0` | Exactly one admission attempt, no waiting for capacity. Storage work still has its own timeout. |
| `> 0` | A deadline covering local contention, storage work, and quota waiting together. |

**B3.** *No storage call is ever unbounded*, whatever the quota timeout says.

**B4.** *Only definitely uncommitted transient failures are retried*, within
budget. A failure that might have committed raises `IndeterminateAdmission`
instead.

**B5.** *`AcquireTimeout` means nothing was granted.* It is distinct from
`BackendUnavailable`, which means the authority never answered.

---

## 9. Composition

**C1.** *One atomic admission of every participating constraint.* All admit or
none is consumed (**A6**).

**C2.** *Identical constraints deduplicate.* Same rule, same policy, same
fingerprint collapses to one: naming a quota twice is a convenience, not a
request to be charged twice.

**C3.** *Conflicts are rejected.* Two constraints sharing a rule identity but
disagreeing on policy raise `PolicyConflict`.

**C4.** *Incompatible coordination domains are rejected.* Composed constraints
either all leave the domain unset or all declare the same one. Cross-slot
composition is refused rather than approximated. Redis Cluster places rule
and cooldown keys under one prefix in a shared slot.

**C5.** *Constraints are canonically ordered* by `(namespace, key, rule name)`,
independent of the order the caller passed, so backends acquire locks in a
consistent order and cannot deadlock against each other.

**C6.** *Separate backends cannot compose.* All-or-nothing acquisition across
two authorities is not offered and is not emulated with increment-and-refund.
Backend identity is checked when composing.

**C7.** *The denial delay is the maximum* of the blocked rules' next-eligible
delays at that snapshot, rechecked on waking.

---

## 10. Cooldowns

**K1.** *A cooldown is a shared, atomic pause on a scope*, typically from a
vendor's `Retry-After`.

**K2.** *Extension takes `max(existing, new)`*, so a later, shorter cooldown
cannot shorten one already in force.

**K3.** *Cooldowns participate in ordinary admission* and in expiry. They are
not a separate code path callers must remember to check.

**K4.** *A cooldown fabricates no quota events* and never replays application
work.

**K5.** *A valid server cooldown is not shortened to fit a local deadline.* The
caller times out instead.

**K6.** *Malformed or absent vendor feedback uses a bounded, configurable
fallback*, never an unbounded or invented one.

---

## 11. Lifecycle and cleanup

**L1.** *Closing is idempotent.* Closing twice is not an error.

**L2.** *Only owned resources are closed.* An injected client or executor is
borrowed and is never closed by this library.

**L3.** *Closing releases connections and executors; it never deletes quota
state.*

**L4.** *Closing waits for owned outstanding work to settle*, including executor
work that may still commit.

**L5.** *Calls after close raise `ClosedResource`* rather than reopening.

**L6.** *Context exit neither refunds quota nor closes storage.* Entering a
limiter as a context manager is independent of resource teardown.

**L7.** *The limiter holds no single "current admission".* Each context or
decorator invocation owns its own result, so concurrent and reentrant use cannot
overwrite another call's admission.

**L8.** *Fork is not inherited.* Copied in-process memory must not claim shared
coordination, and network clients and SQLite connections are reopened after a
fork rather than shared across it.

**L9.** *State may be forgotten only past its safe-forget horizon* — the
earliest time it can no longer affect a decision, consistent with the rule's
configured initial state (**P5**). Cooldowns extend that horizon.

**L10.** *Policy metadata outlives quota state.* Cleanup, TTLs, and expiry must
never erase fingerprints, because expiry would then hide a configuration
disagreement between workers.

**L11.** *Active state is never evicted for capacity.* A memory backend under
pressure refuses new state rather than dropping a rule still in use.

**L12.** *Policy changes are explicit migrations* under the same atomic
authority: preserve history where supported, otherwise stop admissions and drain
the old policy's state until neutral before switching. Updating configuration
never silently resets counters, and restarting a worker never resets shared
policy metadata or quota state.

---

## 12. Configuration

**G1.** *Precedence, strongest first*: constructor arguments, `PROCRASTINATORS_*`
environment defaults, the selected project, the selected organization, library
behavior defaults. There is no layer of invented vendor rates.

**G2.** *Unset is not null.* A layer that omits a field falls through; a layer
that sets it to null has explicitly chosen the null meaning. `UNSET` and `None`
are distinct values.

**G3.** *Selection is explicit.* Projects and organizations are chosen through
settings or environment. There is no working-directory discovery and no upward
directory scan: a library's behavior must not depend on where an ETL worker
happened to start.

**G4.** *Limits replace wholesale.* The strongest layer that sets `limits`
supplies all of them; lists are never merged element-wise. Merging would produce
a policy nobody wrote and, because positional rule identity follows list order,
would silently repoint stored rules.

**G5.** *Unknown fields are rejected*, not ignored.

**G6.** *Resolved configuration is immutable* and is applied once. A limiter does
not re-read configuration between acquisitions.

**G7.** *Provenance is retained and redacted on the way out.* `explain()` reports
which layer supplied each value with secrets removed — both by field name and by
stripping userinfo from backend URLs.

**G8.** *Configuration changes cause a policy conflict or an explicit migration*,
never a new implicit quota identity (**I2**, **L12**).

**G9.** *Distributable configuration stores credential references, not
passwords.*

**G10.** *Saves are atomic*: write and validate a temporary file, then replace
the destination, with a lock or expected revision to prevent lost updates.

---

## 13. Waiting

**W1.** *Never sleep while holding a storage lock or transaction.* Sleep outside
every critical section, then attempt atomic admission again.

**W2.** *Recheck after every sleep*, with a fresh authority timestamp (**T5**).

**W3.** *Jitter never shortens a required wait.*

**W4.** *One exhausted key does not block unrelated keys*, and quota waiting does
not block an event loop.

**W5.** *Slow bodies do not delay replenishment.* Admissions age from when they
were recorded, not from when the caller's work finished. Rate and concurrency
are separate controls.

**W6.** *No global FIFO fairness in v1.* Weighted calls may starve under
sustained small-call traffic. Wait metrics and deadlines are exposed so this is
observable.

---

## 14. Diagnostics

**D1.** *Events are immutable and delivered after the critical section ends.*

**D2.** *A diagnostics callback cannot change an admission outcome*, and a
callback that raises must not turn a committed admission into a reported
failure.

**D3.** *Events carry no raw credentials* and no unbounded per-key metric
cardinality.

---

## 15. Capabilities

**Y1.** *A backend declares what it implements*: algorithms, native executors,
sync and async modes, coordination scope, durability, composition, cooldowns,
and policy administration.

**Y2.** *Unsupported combinations are rejected at construction*, not degraded at
runtime.

**Y3.** *Coordination scope is honest.* `IN_PROCESS` does not survive a fork;
`LOCAL_MACHINE` covers one machine's filesystem; `SHARED_SERVICE` covers workers
reaching the same service and namespace.

**Y4.** *Durability is honest.* `BEST_EFFORT` state can vanish through eviction,
so a cache miss is not proof that no quota was consumed, and that weaker
guarantee must be accepted explicitly rather than defaulted into.

**Y5.** *A custom Python algorithm does not automatically run inside a remote
store.* It needs a registered native executor or a transactional adapter with a
proven clock and atomicity contract. A native executor is registered against an
exact algorithm id, state version, and numeric range, and names the conformance
traces it was verified against; a policy outside that range falls back to the
reference evaluator rather than running on an executor nobody checked.

**Y6.** *State representations are negotiated, not assumed.* A backend declares
which shapes of state it can hold — `scalars`, `event_log`, or a third party's
own — and an algorithm declares which it needs. A store that keeps one bounded
item therefore refuses a sliding log at construction instead of at the first
acquisition. This is what makes "users can implement their own algorithms" and
"users can implement their own backends" compose rather than collide.

**Y7.** *A policy is structural.* Anything carrying a stable algorithm id, a
state version, and a capacity can be a constraint's policy. Requiring a
third-party algorithm to subclass a built-in would make extensibility true only
for algorithms shaped like the ones already shipped.

---

## 16. The state access boundary

Where evaluation ends and storage begins, stated once because it is the rule
most easily eroded.

**S1.** *An algorithm declares its needs in advance* through
`StateRequirements`: a representation, a set of named scalars, and — for
log-structured policies — a bounded event window.

**S2.** *The backend collects those observations inside its transaction*,
awaiting I/O where it must, and then calls the synchronous, pure evaluator.
Everything the evaluator will read has already been read.

**S3.** *The evaluator receives an immutable `StateView` and performs no I/O.*
No property, callback, or lazy accessor on that view may reach the database. A
read that escaped the transaction would escape the atomicity the transaction
exists to provide.

**S4.** *An evaluator sees only what it declared.* Anything not named in its
requirements is not available to it, so a backend can know exactly what to load.

**S5.** *Bounded materialized views come first.* Indexed aggregate queries may
replace them later only where they preserve the same observations and measurably
reduce cost.

**S6.** *A view that could not supply everything says so* through `truncated`.
An exact algorithm must then deny and report rather than decide on a partial
history (**O3**).

**S7.** *A view distinguishes "never used" from "used and now zero"* through
`exists`, which is what keeps an initially-empty bucket from becoming a full one
by being forgotten (**P5**).

**S8.** *The evaluator returns a proposal and nothing else.* The backend applies
every rule's changes together or none of them; that is where **A6** is actually
enforced, and why an evaluator able to commit on its own would break it.

---

## 17. Reference evaluation

The exact arithmetic of the five built-in algorithms. The shared traces
(`procrastinators.testing.TRACES`) were computed by hand from these statements,
and every executor — reference evaluator, native script, third-party backend —
must reproduce them (**N4**). All quantities are integers; `now` is authority
epoch time; `//` is floor division; `ceil(a / b)` is `-(-a // b)`.

**E1.** *Fixed window.* `start = (now - offset) // period * period + offset`.
The window's count is the stored count if it was recorded for `start`, else 0.
Admit when `count + cost <= amount`. A denial waits `start + period - now`.

**E2.** *Sliding log.* The live cost is the sum over recorded entries with
`at > now - period`. Admit when `live + cost <= amount`, recording one entry
`(now, cost)`. A denial walks the live entries oldest first, freeing each
entry's cost; for the first entry `e` after which `live - freed + cost <=
amount`, it waits `e.at + period - now`. A truncated view denies (**S6**).

**E3.** *Token bucket.* First use records `tokens = initial_tokens` (capacity
when unset) and `anchor = now`. At each evaluation, `refills = (now - anchor) //
refill_period` and `tokens += refills * refill_amount`; if that reaches
`capacity`, `tokens = capacity` and `anchor = now` (a full bucket accrues no
further credit), else `anchor += refills * refill_period`. Admit when `cost <=
tokens`. A denial waits `anchor + ceil((cost - tokens) / refill_amount) *
refill_period - now`, computed from the refilled values.

**E4.** *Leaky bucket.* Let `interval(c) = ceil(c * period / amount)` and
`tolerance = burst_tolerance * period // amount`. The next eligible time `tat`
of a never-used rule is `now`. Admit when `tat - now <= tolerance`, recording
`tat = max(tat, now) + interval(cost)`. A denial waits `tat - tolerance - now`.
Rounding each operation's interval up, never the tolerance, keeps the pace
conservative (**T6**).

**E5.** *Sliding counter.* Windows are epoch-aligned: `start = now // period *
period`, `elapsed = now - start`. The stored counts roll forward: the current
window's counts as stored; one window later, `previous = current` and `current =
0`; two or more, both 0. Admit when `previous * (period - elapsed) // period +
current + cost <= amount`. A denial waits until the first microsecond at which
that inequality holds with no other admissions: within this window when
`current + cost <= amount`, otherwise in the next window with `previous =
current`, and at the latest at the start of the window after.

**E6.** *One sample, every rule.* A request evaluates every constraint at the
same `now`, including after one denies, so the reported delay is the maximum
(**C7**). The next-eligible delays of **E1**–**E5** assume no other activity and
are advisory (**R4**).

**E7.** *Safe-forget horizons.* Each evaluation reports the earliest time its
rule's state stops affecting any decision (**L9**), after which forgetting it is
indistinguishable from keeping it:

| Algorithm | Horizon |
| --- | --- |
| Fixed window | The end of the counted window, `start + period`. |
| Sliding log | The newest entry's time plus `period`. |
| Token bucket | When the bucket is full again, `anchor + ceil((capacity - tokens) / refill_amount) * refill_period` — but only when the policy also starts full. A bucket configured to start with fewer tokens is **never** forgotten, since re-creating it would restore the smaller initial balance (**P5**). |
| Leaky bucket | The theoretical arrival time `tat`. |
| Sliding counter | Two periods after the start of the counted window. |

A policy whose state never becomes neutral cannot be migrated by draining
(**L12**); its migration waits, and completing it is refused.

---

## 18. Observation points and conformance

**H1.** *Four observation points* name where a race or failure can land in one
admission: `BEFORE_LOCK` (nothing read), `AFTER_LOAD` (inside the critical
section, time sampled, state loaded), `BEFORE_COMMIT` (every rule admitted,
nothing written), and `AFTER_COMMIT` (durable, caller not yet told). The last
two are reached only when admitting. A backend whose critical section runs
inside the server, as a script does, can report only the points outside it —
`BEFORE_LOCK` and `AFTER_COMMIT` — and does not claim the others; its failures
are placed instead by faults in transit: a reply lost, delayed, or replaced by
a refusal after the request left.

**H2.** *An exception at a point has a fixed meaning.* Before the commit it
committed nothing: a library error propagates unchanged and anything else
becomes `BackendUnavailable` with its cause. After the commit it becomes
`IndeterminateAdmission`, never a decision (**O4**). Cancellation propagates
unchanged at every point (**O5**).

**H3.** *Expected outcomes are independent of implementations.* The traces and
scenarios are data computed by hand from §17 and cross-checked by independent
search-based models; the modules that define them import no algorithm, backend,
or state planner.

**H4.** *The conformance suite must be able to fail.* It is required to catch a
backend that charges on exit, one that debits an earlier rule before a later one
denies, and one that reports permission (or a refund) after an unknown commit —
as well as stale timestamps (**T4**), translated cancellation (**O5**), ignored
fingerprints (**I4**), forgotten active state (**P5**), and use after close
(**L5**).

**H5.** *A required service job cannot pass by skipping.* Service tests may
skip locally when their server is unreachable. A job that requires a service
fails the test instead, and fails the session if that service had no passing
test.
