"""The ``RateLimiter`` facade: construction, contexts, decorators, composition, and closing.

Waiting behavior is in ``tests/concurrency/test_waiting.py`` and failure
handling in ``tests/failures/test_waiting_failures.py``. Here, the public
surface of ``docs/source/api.md`` — starting with the design's own examples,
run against an explicitly selected memory backend.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import dataclasses
import inspect
from typing import TYPE_CHECKING

import pytest

from procrastinators import (
    Admission,
    Algorithms,
    Decision,
    Invocation,
    Limit,
    RateLimiter,
    Snapshot,
    idempotent_key,
)
from procrastinators.backends.memory import MemoryBackend, MemoryStore
from procrastinators.builtins import builtin_registry
from procrastinators.errors import (
    ClosedResource,
    ConfigurationError,
    InvalidCost,
    InvalidPolicy,
    PolicyConflict,
    UnsupportedCapability,
)
from procrastinators.models import (
    CooldownEvent,
    DurationMicros,
    QuotaIdentity,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.protocols import AlgorithmSpec, StateRepresentation
from procrastinators.testing import ScriptedBackend, allow
from tests.doubles import ClacksAlgorithm, ClacksPolicy
from tests.limiters import QUIRM

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from tests.limiters import Rig
else:
    pass

SECOND = DurationMicros(1_000_000)


def fetch_page() -> str:
    return "page"


async def fetch_page_async() -> str:
    await asyncio.sleep(0)
    return "page"


def test_the_designs_api_example_runs_on_an_explicit_memory_backend() -> None:
    """
    Given: The design's example limiter — two positional limits, the sliding log, and
           a key from ``idempotent_key`` — on ``memory://ankh-example``.
    When:  It is used as a context, an async context, a decorator, an invocation, a
           single attempt, and an awaited acquisition.
    Then:  Every form admits and returns what the API specifies (Phase 7 acceptance).
    """
    limiter = RateLimiter(
        key=idempotent_key({"vendor": "ankh", "account": "warehouse"}),
        limits=[Limit(10, per="1s"), Limit(500, per="1m")],
        algorithm=Algorithms.SLIDING_LOG,
        backend="memory://ankh-example",
    )

    with limiter as admission:
        fetched = fetch_page()

    @limiter
    def fetch_one_page() -> str:
        return fetch_page()

    with limiter(cost=5, timeout=30):
        batch = fetch_page()

    decision = limiter.try_acquire(cost=1)

    async def fetch_async() -> tuple[str, Admission]:
        async with limiter as entered:
            page = await fetch_page_async()
        awaited = await limiter.acquire_async(cost=1, timeout=30)
        assert awaited.cost == 1
        return page, entered

    async_page, async_admission = asyncio.run(fetch_async())

    assert isinstance(admission, Admission)
    assert admission.charged == limiter.rules
    assert (fetched, fetch_one_page(), batch, async_page) == ("page",) * 4
    assert isinstance(decision, Decision)
    assert decision.allowed
    assert async_admission.admission_id != admission.admission_id


def test_the_goal_example_runs_with_a_fixed_window() -> None:
    """
    Given: The goal statement's example: a key from a vendor/dataset mapping and the
           fixed-window algorithm, with a limit and a backend chosen explicitly.
    When:  It is entered as ``with limiter as limit:``.
    Then:  ``limit`` is the admission, charged against a fixed-window rule.
    """
    config = {"vendor": "ankh", "dataset": "orders"}
    key = idempotent_key(config)
    limiter = RateLimiter(
        algorithm=Algorithms.FIXED_WINDOW,
        key=key,
        limits=[Limit(5, per="1s")],
        backend="memory://goal-example",
    )

    with limiter as limit:
        pass

    assert limit.cost == 1
    assert [constraint.algorithm for constraint in limiter.constraints] == [Algorithms.FIXED_WINDOW]


def test_limiters_naming_one_memory_store_share_its_quota() -> None:
    """
    Given: Two separately constructed limiters on ``memory://`` with the same key.
    When:  Each acquires once against a rule of two per second, then one tries again.
    Then:  The third attempt is denied: an address names a shared process-wide store.
    """
    key = idempotent_key({"vendor": "klatch", "dataset": "coffee"})
    first = RateLimiter(key=key, limits=[Limit(2, per="1h")], backend="memory://")
    second = RateLimiter(key=key, limits=[Limit(2, per="1h")], backend="memory://")

    verdicts = [first.try_acquire().allowed, second.try_acquire().allowed]

    assert verdicts == [True, True]
    assert not first.try_acquire().allowed


@pytest.mark.parametrize(
    ("arguments", "error", "match"),
    [
        ({}, ConfigurationError, "no limits"),
        ({"limits": [Limit(1)], "rules": {"a": Limit(1)}}, ConfigurationError, "not both"),
        ({"limits": [Limit(1)], "backend": "etcd://ankh"}, UnsupportedCapability, "etcd"),
        ({"limits": [Limit(1)], "backend": "no-scheme"}, ConfigurationError, "family"),
        ({"limits": [Limit(1)], "backend": object()}, ConfigurationError, "backend"),
        ({"limits": []}, ConfigurationError, "empty"),
        ({"limits": "10/s"}, ConfigurationError, "sequence"),
        ({"rules": {}}, ConfigurationError, "non-empty"),
        ({"limits": [10]}, InvalidPolicy, "Limit or a policy"),
        ({"limits": [Limit(1)], "algorithm": "discworld.octarine"}, InvalidPolicy, "unknown"),
        ({"limits": [Limit(1)], "options": {"capacity": 3}}, InvalidPolicy, "do not apply"),
        ({"limits": [Limit(1)], "timeout": -1}, InvalidPolicy, "negative"),
        ({"limits": [Limit(1)], "storage_timeout": 0}, InvalidPolicy, "positive"),
        ({"limits": [Limit(1)], "key": ""}, InvalidPolicy, "empty"),
        (
            {"limits": [SlidingLogPolicy(1, SECOND)], "algorithm": Algorithms.TOKEN_BUCKET},
            InvalidPolicy,
            "already names",
        ),
        (
            {"limits": [SlidingLogPolicy(1, SECOND)], "options": {"x": 1}},
            InvalidPolicy,
            "only to Limit",
        ),
    ],
)
def test_construction_rejects_what_cannot_become_a_limiter(
    rig: Rig, arguments: dict[str, object], error: type[Exception], match: str
) -> None:
    """
    Given: Arguments that are missing, contradictory, malformed, or unsupported.
    When:  A limiter is constructed.
    Then:  The specific error is raised at construction, never at first use (Y2).
    """
    arguments = {"limits": None, **arguments}

    with pytest.raises(error, match=match):
        rig.limiter(**arguments)


def test_construction_opens_nothing_and_admits_nothing(rig: Rig) -> None:
    """
    Given: A freshly constructed limiter.
    When:  The store is examined before any acquisition.
    Then:  No rule has state and no authority time was read.
    """
    rig.limiter()

    assert (rig.store.active_rules, rig.timeline.epoch_reads) == (0, 0)


def test_named_rules_survive_reordering_where_positional_ones_conflict(rig: Rig) -> None:
    """
    Given: One scope configured first with named rules, then the same rules reordered;
           and another configured with a positional list, then that list reordered.
    When:  Each configuration acquires.
    Then:  Reordered named rules keep their identities (I3); a reordered positional
           list is a policy conflict, not fresh quota.
    """
    burst, daily = Limit(2, per="1s"), Limit(100, per="1d")
    rig.limiter(limits=None, rules={"burst": burst, "daily": daily}).acquire()
    rig.limiter(limits=None, rules={"daily": daily, "burst": burst}).acquire()
    rig.limiter(key=QUIRM, limits=[burst, daily]).acquire()

    assert {rule.name for rule in rig.limiter(limits=None, rules={"burst": burst}).rules} == {
        "burst"
    }
    with pytest.raises(PolicyConflict):
        rig.limiter(key=QUIRM, limits=[daily, burst]).acquire()


def test_raising_a_rate_is_a_conflict_not_fresh_quota(rig: Rig) -> None:
    """
    Given: A key used under two per second.
    When:  A worker starts with five per second for the same key.
    Then:  Its acquisition is a ``PolicyConflict``: changing a rate keeps the quota
           identity (I2) and needs an explicit migration (L12).
    """
    rig.limiter().acquire()

    with pytest.raises(PolicyConflict):
        rig.limiter(limits=[Limit(5, per="1s")]).acquire()


def test_lu_tze_does_not_refund_a_failed_request(rig: Rig) -> None:
    """
    Given: A limiter of two per second.
    When:  Two bodies run inside its context and both raise.
    Then:  The exceptions propagate untouched and both admissions stay charged, so a
           third attempt is denied (A3): the library charges attempts, not successes.
    """
    limiter = rig.limiter()
    for _ in range(2):
        with pytest.raises(ConnectionError), limiter:
            raise ConnectionError("vendor said no")

    assert not limiter.try_acquire().allowed


def test_leaving_a_context_closes_nothing(rig: Rig) -> None:
    """
    Given: A limiter used as a context manager.
    When:  The block exits.
    Then:  The limiter and its backend are still open (L6).
    """
    limiter = rig.limiter()
    with limiter:
        pass

    assert not limiter.closed
    assert limiter.try_acquire().allowed


def test_nested_entries_each_own_their_admission(rig: Rig) -> None:
    """
    Given: One limiter entered twice, one context inside the other.
    When:  Both admissions are compared.
    Then:  They are distinct acquisitions; the limiter holds no current admission (L7).
    """
    limiter = rig.limiter()

    with limiter as outer, limiter as inner:
        pass

    assert outer.admission_id != inner.admission_id
    assert not hasattr(limiter, "admission")


def test_calling_a_decorated_function_twice_acquires_twice(rig: Rig) -> None:
    """
    Given: A function decorated with a limiter of two per second.
    When:  It is called twice, as a retry would.
    Then:  Each call acquired: a retry is a new acquisition (A4).
    """
    limiter = rig.limiter()

    @limiter
    def send() -> int:
        return 200

    assert [send(), send()] == [200, 200]
    assert not limiter.try_acquire().allowed


def test_decorators_preserve_metadata_and_pick_the_matching_path(rig: Rig) -> None:
    """
    Given: A plain function and a coroutine function, decorated with and without options.
    When:  The wrappers are inspected and called.
    Then:  Names, docstrings, and ``__wrapped__`` survive; the coroutine's wrapper is a
           coroutine function, so it never blocks on a sync acquire.
    """
    limiter = rig.limiter(limits=[Limit(10, per="1s")])

    @limiter
    def fetch(page: int) -> int:
        """Fetch one page."""
        return page

    @limiter(cost=3)
    async def fetch_async(page: int) -> int:
        """Fetch one page, asynchronously."""
        return page

    assert (fetch.__name__, fetch.__doc__) == ("fetch", "Fetch one page.")
    assert fetch.__wrapped__ is not None  # ty: ignore[unresolved-attribute]
    assert inspect.iscoroutinefunction(fetch_async)
    assert fetch_async.__name__ == "fetch_async"
    assert fetch(7) == 7
    assert asyncio.run(fetch_async(8)) == 8
    snapshot = limiter.inspect()
    assert snapshot.rules[0].remaining == 10 - 1 - 3


def test_generator_functions_cannot_be_decorated(rig: Rig) -> None:
    """
    Given: A generator function and an async generator function.
    When:  Each is decorated.
    Then:  ``UnsupportedCapability`` is raised: creating and iterating a generator
           happen at different times, so the caller acquires inside the loop.
    """
    limiter = rig.limiter()

    def pages() -> Iterator[int]:
        yield 1

    async def pages_async() -> AsyncIterator[int]:
        yield 1

    with pytest.raises(UnsupportedCapability, match="generator"):
        limiter(pages)
    with pytest.raises(UnsupportedCapability, match="generator"):
        limiter(cost=2)(pages_async)


def test_an_invocation_is_immutable_and_leaves_the_limiter_unchanged(rig: Rig) -> None:
    """
    Given: A limiter and an invocation of it with cost two.
    When:  The invocation is used as a context, and its fields are reassigned.
    Then:  Two units are charged, reassignment fails, and the limiter's own entry
           still charges one.
    """
    limiter = rig.limiter(limits=[Limit(4, per="1s")])
    invocation = limiter(cost=2, timeout=0)

    with invocation as admission:
        pass
    with limiter as default:
        pass

    assert isinstance(invocation, Invocation)
    assert (admission.cost, default.cost) == (2, 1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        invocation.cost = 3  # ty: ignore[invalid-assignment]


def test_an_impossible_cost_fails_before_any_waiting(rig: Rig) -> None:
    """
    Given: A limiter of two per second.
    When:  A cost of three is acquired, decorated, or given to an invocation.
    Then:  ``InvalidCost`` is raised immediately each time, and nothing slept (P3).
    """
    limiter = rig.limiter()

    with pytest.raises(InvalidCost):
        limiter.acquire(cost=3)
    with pytest.raises(InvalidCost):
        limiter(cost=3)
    with pytest.raises(InvalidCost):
        limiter.try_acquire(cost=0)

    assert rig.sleeper.sleeps == tuple()


def test_an_allowed_try_acquire_has_already_been_charged(rig: Rig) -> None:
    """
    Given: A limiter of two per second.
    When:  ``try_acquire`` is called three times.
    Then:  Two decisions allow and carry admissions; the third denies with a retry
           delay and consumed nothing, as inspection confirms.
    """
    limiter = rig.limiter()

    decisions = [limiter.try_acquire() for _ in range(3)]

    assert [decision.allowed for decision in decisions] == [True, True, False]
    assert decisions[2].retry_after_us == SECOND
    assert limiter.inspect().rules[0].remaining == 0


def test_the_auditors_compose_vendor_account_and_endpoint_limits(rig: Rig) -> None:
    """
    Given: Vendor-wide, account, and endpoint limiters on one store, combined.
    When:  The combined limiter acquires until the tightest rule denies.
    Then:  Each acquisition charged all three rules atomically (C1), and the denial
           debited none of them (A6).
    """
    vendor = rig.limiter(key=idempotent_key({"vendor": "ankh"}), limits=[Limit(5, per="1s")])
    account = rig.limiter(
        key=idempotent_key({"vendor": "ankh", "account": "a"}), limits=[Limit(3, per="1s")]
    )
    endpoint = rig.limiter(
        key=idempotent_key({"vendor": "ankh", "endpoint": "/orders"}),
        limits=[Limit(10, per="1s")],
    )
    combined = RateLimiter.combine(vendor, account, endpoint)

    decisions = [combined.try_acquire() for _ in range(4)]
    remaining = [snapshot.remaining for snapshot in combined.inspect().rules]

    assert [decision.allowed for decision in decisions] == [True, True, True, False]
    assert decisions[3].blocking == account.rules
    assert sorted(remaining) == sorted([5 - 3, 0, 10 - 3])  # ty: ignore[invalid-argument-type]


def test_combining_a_limiter_with_itself_charges_once(rig: Rig) -> None:
    """
    Given: A limiter combined with itself.
    When:  The combined limiter's constraints are read and it acquires.
    Then:  Identical constraints deduplicated (C2): one charge per acquisition.
    """
    limiter = rig.limiter()
    combined = RateLimiter.combine(limiter, limiter)

    combined.acquire()

    assert combined.constraints == limiter.constraints
    assert limiter.inspect().rules[0].remaining == 1


def test_combining_conflicting_or_foreign_limiters_is_refused(rig: Rig) -> None:
    """
    Given: Two limiters naming one rule with different policies, and a limiter on
           another store.
    When:  Each pair is combined.
    Then:  The conflict raises ``PolicyConflict`` (C3) and the foreign store
           ``UnsupportedCapability`` (C6), both at ``combine`` time.
    """
    two, five = rig.limiter(), rig.limiter(limits=[Limit(5, per="1s")])
    foreign = RateLimiter(key=QUIRM, limits=[Limit(1)], backend="memory://elsewhere")

    with pytest.raises(PolicyConflict):
        RateLimiter.combine(two, five)
    with pytest.raises(UnsupportedCapability, match="one store"):
        RateLimiter.combine(two, foreign)
    with pytest.raises(ConfigurationError):
        RateLimiter.combine()


def test_closing_is_idempotent_and_final(rig: Rig) -> None:
    """
    Given: A limiter built from an address, which owns its handles.
    When:  It is closed twice, then used.
    Then:  Closing twice is fine (L1), every call afterwards raises ``ClosedResource``
           (L5), and the store's state survives for other limiters (L3).
    """
    limiter = rig.limiter()
    limiter.acquire()
    limiter.close()
    limiter.close()

    for call in (limiter.acquire, limiter.try_acquire, limiter.inspect, limiter.__enter__):
        with pytest.raises(ClosedResource):
            call()
    with pytest.raises(ClosedResource):
        asyncio.run(limiter.acquire_async())
    assert rig.limiter().inspect().rules[0].remaining == 1


def test_a_borrowed_backend_is_never_closed(rig: Rig) -> None:
    """
    Given: A limiter given a backend object, and a combined limiter built from it.
    When:  Both limiters are closed, synchronously and asynchronously.
    Then:  The backend object stays open (L2).
    """
    handle = MemoryBackend(rig.store)
    limiter = rig.limiter(backend=handle)
    combined = RateLimiter.combine(limiter)

    combined.close()
    limiter.close()
    asyncio.run(limiter.aclose())

    assert not handle.closed


def test_aclose_closes_every_owned_handle(rig: Rig) -> None:
    """
    Given: A limiter built from an address, which owns a sync and an async handle.
    When:  It is closed asynchronously.
    Then:  Both handles are closed.
    """
    limiter = rig.limiter()
    handles = (limiter._handles.sync, limiter._handles.async_)

    asyncio.run(limiter.aclose())

    assert all(handle is not None and handle.closed for handle in handles)  # ty: ignore[unresolved-attribute]


def test_a_single_mode_backend_refuses_the_other_mode(rig: Rig) -> None:
    """
    Given: A limiter on a synchronous backend object.
    When:  Its async methods are called.
    Then:  ``UnsupportedCapability`` explains that wrapping blocking calls would block
           the event loop.
    """
    limiter = rig.limiter(backend=MemoryBackend(rig.store))

    with pytest.raises(UnsupportedCapability, match="asynchronous"):
        asyncio.run(limiter.acquire_async())


def test_inspection_reports_every_rule_without_consuming(rig: Rig) -> None:
    """
    Given: A limiter of two positional limits, used once.
    When:  It is inspected synchronously and asynchronously.
    Then:  Both snapshots describe both rules with one unit consumed, and nothing more
           was charged (R7).
    """
    limiter = rig.limiter(limits=[Limit(2, per="1s"), Limit(5, per="1m")])
    limiter.acquire()

    snapshot = limiter.inspect()
    async_snapshot = asyncio.run(limiter.inspect_async())

    assert isinstance(snapshot, Snapshot)
    assert [rule.remaining for rule in snapshot.rules] == [1, 4]
    assert [rule.remaining for rule in async_snapshot.rules] == [1, 4]


def test_defer_for_pauses_every_worker_on_the_scope(rig: Rig) -> None:
    """
    Given: Two limiters on one key, one of which applies a vendor's three-second
           ``Retry-After``.
    When:  The other tries to acquire.
    Then:  It is denied for the cooldown's length, a ``CooldownEvent`` was delivered,
           and no quota event was fabricated (K3, K4).
    """
    worker, other = rig.limiter(), rig.limiter()

    cooldown = worker.defer_for(3, reason="429 Too Many Requests")
    decision = other.try_acquire()

    assert cooldown.until == rig.timeline.peek_epoch() + 3 * SECOND
    assert (decision.allowed, decision.retry_after_us) == (False, 3 * SECOND)
    assert any(isinstance(event, CooldownEvent) for event in rig.events)
    assert other.inspect().rules[0].remaining == 2


def test_defer_for_async_and_scope_selection(rig: Rig) -> None:
    """
    Given: A combined limiter spanning two scopes.
    When:  A cooldown is applied without a scope, then asynchronously with one.
    Then:  Without a scope it is refused as ambiguous; with one, only that scope pauses.
    """
    ankh, quirm = rig.limiter(), rig.limiter(key=QUIRM)
    combined = RateLimiter.combine(ankh, quirm)
    scope = QuotaIdentity("default", QUIRM)

    with pytest.raises(ConfigurationError, match="scope="):
        combined.defer_for(1)
    asyncio.run(combined.defer_for_async(1, scope=scope))

    assert ankh.try_acquire().allowed
    assert not quirm.try_acquire().allowed


def test_defer_for_needs_a_backend_with_cooldowns(rig: Rig) -> None:
    """
    Given: A limiter on a scripted backend, which implements no cooldowns.
    When:  A cooldown is applied.
    Then:  ``UnsupportedCapability`` is raised.
    """
    backend = ScriptedBackend([allow()], clock=rig.timeline.epoch_clock)
    limiter = rig.limiter(backend=backend)

    with pytest.raises(UnsupportedCapability, match="cooldowns"):
        limiter.defer_for(1)


def test_a_registered_third_party_algorithm_works_through_the_facade(rig: Rig) -> None:
    """
    Given: A registry with a third-party algorithm registered, and a limiter given its
           policy object.
    When:  The limiter acquires.
    Then:  The custom algorithm enforces the policy like a built-in (Y7).
    """
    registry = builtin_registry()
    registry.register_algorithm(
        AlgorithmSpec("discworld.clacks", 1, StateRepresentation.EVENT_LOG, ClacksAlgorithm)
    )
    store = MemoryStore(clock=rig.timeline.epoch_clock)
    store.host([ClacksAlgorithm()])
    limiter = rig.limiter(
        limits=[ClacksPolicy(1, SECOND)], registry=registry, backend=MemoryBackend(store)
    )

    assert [limiter.try_acquire().allowed, limiter.try_acquire().allowed] == [True, False]


def test_an_unregistered_algorithm_is_refused_at_construction(rig: Rig) -> None:
    """
    Given: A policy object for an algorithm no registry knows.
    When:  A limiter is built with it.
    Then:  ``UnsupportedCapability`` is raised: configuration cannot import code.
    """
    with pytest.raises(UnsupportedCapability, match="not registered"):
        rig.limiter(limits=[ClacksPolicy(1, SECOND)])


def test_algorithm_options_reach_the_policy(rig: Rig) -> None:
    """
    Given: A token-bucket limiter of ten per second starting empty.
    When:  It is constructed and tried at once.
    Then:  Its policy starts with no tokens, so the first attempt is denied.
    """
    limiter = rig.limiter(
        limits=[Limit(10, per="1s")],
        algorithm=Algorithms.TOKEN_BUCKET,
        options={"initial_tokens": 0},
    )

    policy = limiter.constraints[0].policy

    assert isinstance(policy, TokenBucketPolicy)
    assert policy.starting_tokens == 0
    assert not limiter.try_acquire().allowed


if __name__ == "__main__":
    pass
else:
    pass
