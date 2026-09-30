"""The Memcached backend: conformance, CAS contention, best effort, failures, and lifecycle.

Tests marked ``service("memcached")`` need a real server, found through
``PROCRASTINATORS_MEMCACHED_URL`` (default ``memcached://127.0.0.1:11311``);
each uses a key prefix of its own, so tests never see one another's items.
The rest need no server.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import contextlib
import itertools
import multiprocessing
import os
import threading
import uuid
from typing import TYPE_CHECKING

import pytest

from procrastinators.backends.memcached import (
    MEMCACHED_CAPABILITIES,
    MIN_CAS_ROUNDS,
    AsyncMemcachedBackend,
    MemcachedBackend,
    MemcachedStore,
    expiry_seconds,
    memcached_backend,
)
from procrastinators.capabilities import Mode
from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    IndeterminateAdmission,
    PolicyConflict,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.limiter import RateLimiter
from procrastinators.models import (
    AdmissionRequest,
    Constraint,
    Durability,
    DurationMicros,
    EpochMicros,
    FixedWindowPolicy,
    Limit,
    OperationBudget,
    Ownership,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.protocols import ObservationPoint
from procrastinators.testing import (
    AsyncBackendCase,
    BackendCase,
    CheckStatus,
    FakeTimeline,
    check_async_backend,
    check_backend,
)
from tests.backends.conftest import SECOND, DelayedCloseExecutor, constraint
from tests.concurrency import memcached_workers

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from pymemcache.client.base import Client

    from procrastinators.testing import ConformanceReport
    from tests.service_gate import ServiceGate
else:
    pass

URL = os.environ.get("PROCRASTINATORS_MEMCACHED_URL", "memcached://127.0.0.1:11311")

THREADS = 12
WORKERS = 4
ATTEMPTS = 40
JOIN_S = 60

EXPECTED_SKIP = ("does not implement", "does not support composition")

BAD_ADDRESSES = {
    "other scheme": "redis://127.0.0.1:6379",
    "credentials": "memcached://rincewind:luggage@127.0.0.1:11211",
    "path": "memcached://127.0.0.1:11211/0",
    "unknown setting": "memcached://127.0.0.1:11211?binary=1",
    "bad port": "memcached://127.0.0.1:eleven",
}


def _reachable() -> bool:
    store = MemcachedStore(URL)
    client = store.connect()
    try:
        client.version()
    except (OSError, ConnectionError):
        reachable = False
    else:
        reachable = True
    finally:
        client.close()
    return reachable


@pytest.fixture
def memcached(service_available: ServiceGate) -> str:
    """The server's address, once it is known to answer."""
    service_available("memcached", _reachable, f"nothing answers at {URL}")
    return URL


@pytest.fixture
def prefix() -> str:
    """A key prefix no other test uses."""
    fresh = f"test-{uuid.uuid4().hex}"
    return fresh


@pytest.fixture
def mc_store(memcached: str, prefix: str, timeline: FakeTimeline) -> MemcachedStore:
    fresh = MemcachedStore(memcached, prefix=prefix, clock=timeline.epoch_clock)
    return fresh


@pytest.fixture
def mc_backend(mc_store: MemcachedStore) -> Iterator[MemcachedBackend]:
    handle = MemcachedBackend(mc_store, namespace="discworld")
    yield handle
    handle.close()


@pytest.fixture
def raw(memcached: str) -> Iterator[Client]:
    """A plain client, standing in for another worker or for the server's own whims."""
    client = MemcachedStore(memcached).connect()
    yield client
    client.close()


@pytest.fixture
def window() -> Constraint:
    """A fixed window of two per hour on the ``ankh`` scope."""
    built = constraint("ankh", "window", FixedWindowPolicy(2, DurationMicros(3600 * SECOND)))
    return built


@pytest.fixture
def conformance_prefixes(memcached: str) -> Callable[[], str]:
    """A fresh key prefix per conformance check."""
    run = uuid.uuid4().hex
    counter = itertools.count()

    def fresh() -> str:
        prefix = f"conformance-{run}-{next(counter)}"
        return prefix

    return fresh


@pytest.fixture(params=list(BAD_ADDRESSES))
def bad_address(request: pytest.FixtureRequest) -> str:
    address = BAD_ADDRESSES[request.param]
    return address


def _assert_conforms(report: ConformanceReport) -> None:
    unexpected = [
        result
        for result in report.skipped
        if not result.detail.startswith(EXPECTED_SKIP)
        or ("sliding_log" not in result.detail and "composition" not in result.detail)
    ]
    assert report.failures == tuple()
    assert unexpected == list()
    assert report.status_of("observation_order") is CheckStatus.PASSED
    assert report.status_of("trace:token_bucket.basics") is CheckStatus.PASSED


#
# No server needed.


def test_constructing_a_store_and_handle_touches_nothing() -> None:
    """
    Given: An address where nothing listens.
    When:  A store and both handles are constructed.
    Then:  Nothing fails: no connection is made until a handle is used.
    """
    store = MemcachedStore("memcached://127.0.0.1:1")

    handles = (MemcachedBackend(store), AsyncMemcachedBackend(store))

    assert [handle.identity.authority for handle in handles] == ["127.0.0.1:1"] * 2


def test_cancelled_async_close_finishes_owned_executor_cleanup(
    delayed_close_executor: DelayedCloseExecutor,
    event_loop_runner: asyncio.Runner,
) -> None:
    """
    Given: An async Memcached handle whose worker cleanup is waiting.
    When:  The caller awaiting close is cancelled.
    Then:  A later close waits for the original cleanup and the executor stops.
    """
    backend = AsyncMemcachedBackend(MemcachedStore("memcached://127.0.0.1:1"))
    backend._executor = delayed_close_executor  # ty: ignore[invalid-assignment]

    async def scenario() -> None:
        closing = asyncio.create_task(backend.aclose())
        await delayed_close_executor.started.wait()
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert backend.closed
        assert not delayed_close_executor.closed
        delayed_close_executor.proceed.set()
        await backend.aclose()

    event_loop_runner.run(scenario())

    assert delayed_close_executor.closed


def test_handles_declare_honest_capabilities() -> None:
    """
    Given: A synchronous and an asynchronous handle.
    When:  Their capabilities are read.
    Then:  Each offers its own mode, best-effort durability, constant state only,
           and neither composition, cooldowns, nor administration.
    """
    store = MemcachedStore()
    sync = MemcachedBackend(store).capabilities
    asynchronous = AsyncMemcachedBackend(store).capabilities

    assert (sync.supports_sync, sync.supports_async) == (True, False)
    assert (asynchronous.supports_sync, asynchronous.supports_async) == (False, True)
    assert sync.durability is Durability.BEST_EFFORT
    assert sync.state_representations == frozenset({"scalars"})
    assert "sliding_log" not in sync.algorithms
    assert not sync.supports_composition
    assert not sync.supports_cooldowns
    assert not sync.supports_policy_administration
    assert sync.algorithms == MEMCACHED_CAPABILITIES.algorithms


def test_the_identity_names_host_and_port_only() -> None:
    """
    Given: A factory address with a port and a prefix.
    When:  A handle is made from it.
    Then:  Its identity includes a credential-free digest of the key prefix.
    """
    handle = memcached_backend(
        "memcached://cache.example:22122?prefix=quirm", mode=Mode.SYNC, namespace="discworld"
    )
    same_prefix = MemcachedStore("memcached://cache.example:22122", prefix="quirm")
    other_prefix = MemcachedStore("memcached://cache.example:22122", prefix="ankh")

    assert handle.identity == same_prefix.identity("discworld")
    assert handle.identity != other_prefix.identity("discworld")
    assert "quirm" not in handle.identity.authority
    assert handle.store.key(constraint("ankh", "x", SlidingLogPolicy(1, SECOND)).rule).startswith(
        "quirm:"
    )


def test_the_default_port_is_11211() -> None:
    """
    Given: An address without a port.
    When:  A store is made from it.
    Then:  It addresses port 11211.
    """
    expected = "cache.example:11211"

    actual = MemcachedStore("memcached://cache.example").authority

    assert actual == expected


def test_malformed_addresses_are_refused(bad_address: str) -> None:
    """
    Given: An address the ``memcached`` family cannot use.
    When:  A handle is made from it.
    Then:  ConfigurationError is raised.
    """
    with pytest.raises(ConfigurationError):
        memcached_backend(bad_address, mode=Mode.SYNC)


def test_keys_are_bounded_digests() -> None:
    """
    Given: Rules whose names are long and full of spaces and braces.
    When:  Their keys are computed.
    Then:  Each is short, printable, free of spaces, and distinct.
    """
    store = MemcachedStore()
    rules = [
        constraint("a b" * 100, f"{{rule}} {index}", SlidingLogPolicy(1, SECOND)).rule
        for index in range(3)
    ]

    keys = [store.key(rule) for rule in rules]

    assert len(set(keys)) == len(keys)
    assert all(len(key) <= 250 and " " not in key and key.isprintable() for key in keys)


@pytest.mark.parametrize(
    ("horizon", "now", "expected"),
    [
        (None, 0, 0),
        (1, 0, 2),
        (SECOND, 0, 2),
        (SECOND + 1, 0, 3),
        (5, 10, 1),
        (30 * 24 * 3600 * SECOND - SECOND, 0, 30 * 24 * 3600),
    ],
    ids=["never", "a microsecond", "a second", "just over", "past", "thirty days"],
)
def test_expiry_rounds_up_to_whole_seconds(horizon: int | None, now: int, expected: int) -> None:
    """
    Given: A safe-forget horizon within thirty days, or none.
    When:  The item's expiry is computed.
    Then:  It is the time to the horizon rounded up to whole seconds plus a second
           for the server's coarse clock, or zero for never.
    """
    actual = expiry_seconds(
        None if horizon is None else EpochMicros(horizon), EpochMicros(now), wall_seconds=0
    )

    assert actual == expected


def test_a_long_expiry_becomes_an_absolute_time_with_margin() -> None:
    """
    Given: A horizon thirty-one days away, and a wall clock at one million seconds.
    When:  The item's expiry is computed.
    Then:  It is an absolute Unix time on the wall clock, a second past the horizon.
    """
    days = 31 * 24 * 3600
    expected = 1_000_000 + days + 1

    actual = expiry_seconds(EpochMicros(days * SECOND), EpochMicros(0), wall_seconds=1_000_000.5)

    assert actual == expected


def test_a_sliding_log_is_refused_at_construction() -> None:
    """
    Given: A limiter asking for a sliding log on a memcached handle, best effort accepted.
    When:  It is constructed.
    Then:  UnsupportedCapability is raised before any connection is attempted.
    """
    with pytest.raises(UnsupportedCapability, match="cannot hold"):
        RateLimiter(
            key="ankh",
            limits=[Limit(2, per="1s")],
            algorithm="sliding_log",
            backend=MemcachedBackend(MemcachedStore("memcached://127.0.0.1:1")),
            accept_best_effort=True,
        )


def test_a_limiter_must_accept_best_effort_explicitly() -> None:
    """
    Given: A limiter on a memcached handle that does not accept best effort.
    When:  It is constructed.
    Then:  UnsupportedCapability is raised naming the weaker guarantee (Y4).
    """
    with pytest.raises(UnsupportedCapability, match="best-effort"):
        RateLimiter(
            key="ankh",
            limits=[Limit(2, per="1s")],
            algorithm="fixed_window",
            backend=MemcachedBackend(MemcachedStore("memcached://127.0.0.1:1")),
        )


def test_composed_requests_are_refused_before_anything_is_sent(window: Constraint) -> None:
    """
    Given: A request composing two rules, on a handle whose server is unreachable.
    When:  It is admitted.
    Then:  UnsupportedCapability is raised, not a connection error.
    """
    other = constraint("quirm", "window", FixedWindowPolicy(1, SECOND))
    handle = MemcachedBackend(MemcachedStore("memcached://127.0.0.1:1"))

    with pytest.raises(UnsupportedCapability, match="compose"):
        handle.admit(AdmissionRequest((window, other)))


def test_an_unreachable_server_is_unavailable_not_a_denial(window: Constraint) -> None:
    """
    Given: A handle on an address where nothing listens.
    When:  It admits.
    Then:  BackendUnavailable is raised: nothing was committed, and no private
           fallback state was consulted.
    """
    handle = MemcachedBackend(MemcachedStore("memcached://127.0.0.1:1", timeout=0.5))

    with pytest.raises(BackendUnavailable):
        handle.admit(AdmissionRequest((window,)))


def test_cooldowns_and_administration_are_not_offered() -> None:
    """
    Given: A memcached handle.
    When:  Cooldown and administration methods are looked up.
    Then:  They do not exist, rather than existing and raising.
    """
    handle = MemcachedBackend(MemcachedStore())

    assert not hasattr(handle, "defer_for")
    assert not hasattr(handle, "begin_migration")


#
# A real server.


@pytest.mark.service("memcached")
def test_the_memcached_backend_passes_the_conformance_suite(
    conformance_prefixes: Callable[[], str],
) -> None:
    """
    Given: A synchronous handle with a fresh prefix per check, claiming every guarantee.
    When:  The conformance suite runs.
    Then:  Nothing fails, and only sliding-log and composition checks are skipped.
    """

    def build(timeline: FakeTimeline, observer: object) -> MemcachedBackend:
        handle = MemcachedBackend(
            MemcachedStore(URL, prefix=conformance_prefixes(), clock=timeline.epoch_clock),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = check_backend(BackendCase("memcached", build))

    _assert_conforms(report)


@pytest.mark.service("memcached")
def test_the_async_memcached_backend_passes_the_conformance_suite(
    conformance_prefixes: Callable[[], str],
) -> None:
    """
    Given: An asynchronous handle with a fresh prefix per check.
    When:  The asynchronous conformance suite runs, every call on the executor.
    Then:  Nothing fails, and only sliding-log and composition checks are skipped.
    """

    def build(timeline: FakeTimeline, observer: object) -> AsyncMemcachedBackend:
        handle = AsyncMemcachedBackend(
            MemcachedStore(URL, prefix=conformance_prefixes(), clock=timeline.epoch_clock),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = asyncio.run(check_async_backend(AsyncBackendCase("async memcached", build)))

    _assert_conforms(report)


@pytest.mark.service("memcached")
def test_racing_threads_admit_exactly_the_quota(memcached: str, prefix: str) -> None:
    """
    Given: Twelve threads, each with its own handle, racing on a window of 60 per hour.
    When:  Each makes 40 attempts.
    Then:  Exactly 60 are admitted: every lost swap was retried, never overwritten.
    """
    policy = FixedWindowPolicy(60, DurationMicros(3600 * SECOND))
    request = AdmissionRequest((constraint("ankh", "vendor", policy),))
    barrier = threading.Barrier(THREADS)
    counts = [0] * THREADS

    def race(index: int) -> None:
        handle = MemcachedBackend(MemcachedStore(memcached, prefix=prefix))
        barrier.wait()
        for _ in range(ATTEMPTS):
            with contextlib.suppress(BackendBusy):
                counts[index] += handle.admit(request).allowed
        handle.close()

    threads = [threading.Thread(target=race, args=(index,)) for index in range(THREADS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(JOIN_S)

    assert sum(counts) == 60


@pytest.mark.service("memcached")
def test_spawned_processes_share_one_quota(memcached: str, prefix: str) -> None:
    """
    Given: Four spawned processes racing on one fixed window of 60 per hour.
    When:  They make 160 attempts between them.
    Then:  Exactly 60 are admitted: the server's CAS, not shared memory, coordinates them.
    """
    spawn = multiprocessing.get_context("spawn")
    barrier = spawn.Barrier(WORKERS)
    queue = spawn.Queue()
    workers = [
        spawn.Process(
            target=memcached_workers.contend, args=(memcached, prefix, ATTEMPTS, barrier, queue)
        )
        for _ in range(WORKERS)
    ]
    for worker in workers:
        worker.start()
    results = [queue.get(timeout=JOIN_S) for _ in workers]
    for worker in workers:
        worker.join(JOIN_S)

    assert sum(results) == memcached_workers.AMOUNT


@pytest.mark.service("memcached")
def test_a_denied_first_attempt_records_initial_state(
    mc_backend: MemcachedBackend, timeline: FakeTimeline
) -> None:
    """
    Given: A token bucket of two that starts empty and refills one per second.
    When:  The first attempt is denied, and another comes a second later.
    Then:  The second is admitted: the first recorded the empty bucket and its
           refill anchor rather than restarting the clock (P5).
    """
    bucket = constraint("quirm", "bucket", TokenBucketPolicy(2, 1, SECOND, initial_tokens=0))
    request = AdmissionRequest((bucket,))

    first = mc_backend.admit(request)
    timeline.advance(SECOND)
    second = mc_backend.admit(request)

    assert (first.allowed, second.allowed) == (False, True)


@pytest.mark.service("memcached")
def test_handles_on_one_server_share_quota(
    mc_store: MemcachedStore, mc_backend: MemcachedBackend, window: Constraint
) -> None:
    """
    Given: Two handles on one store, and a window of two per hour.
    When:  Each admits once, then the first tries again.
    Then:  The third attempt is denied.
    """
    other = MemcachedBackend(mc_store, namespace="discworld")
    request = AdmissionRequest((window,))

    verdicts = [mc_backend.admit(request).allowed, other.admit(request).allowed]
    verdicts.append(mc_backend.admit(request).allowed)
    other.close()

    assert verdicts == [True, True, False]


@pytest.mark.service("memcached")
def test_an_evicted_item_hands_out_its_initial_allowance_again(
    mc_store: MemcachedStore, mc_backend: MemcachedBackend, window: Constraint, raw: Client
) -> None:
    """
    Given: A window of two per hour, spent, whose item the server then evicts.
    When:  The rule is admitted again.
    Then:  It is allowed: a cache miss reads as unused quota. This is the weaker
           guarantee a caller accepts with ``accept_best_effort`` (Y4).
    """
    request = AdmissionRequest((window,))
    mc_backend.admit(request)
    mc_backend.admit(request)
    spent = mc_backend.admit(request).allowed

    raw.delete(mc_store.key(window.rule), noreply=False)
    after_eviction = mc_backend.admit(request).allowed

    assert (spent, after_eviction) == (False, True)


@pytest.mark.service("memcached")
def test_missing_state_inspects_as_unused(mc_backend: MemcachedBackend, window: Constraint) -> None:
    """
    Given: A rule that was never admitted, or whose item was evicted.
    When:  It is inspected.
    Then:  It reports ``unused``, with nothing remaining claimed.
    """
    (snapshot,) = mc_backend.inspect((window.rule,)).rules

    assert (snapshot.algorithm, snapshot.remaining) == ("unused", None)


@pytest.mark.service("memcached")
def test_a_changed_policy_conflicts_while_the_item_survives(
    mc_backend: MemcachedBackend, window: Constraint
) -> None:
    """
    Given: A rule admitted under one policy.
    When:  The same rule is admitted under another, then under the first again.
    Then:  The second raises PolicyConflict and the third still sees the first's
           admission: the conflict reset nothing.
    """
    changed = constraint("ankh", "window", FixedWindowPolicy(3, DurationMicros(3600 * SECOND)))
    request = AdmissionRequest((window,))
    mc_backend.admit(request)

    with pytest.raises(PolicyConflict):
        mc_backend.admit(AdmissionRequest((changed,)))
    remaining = mc_backend.admit(request).allowed

    assert remaining


@pytest.mark.service("memcached")
def test_endless_contention_exhausts_the_retry_budget(
    mc_store: MemcachedStore, window: Constraint, raw: Client
) -> None:
    """
    Given: A window of two with one admission, and another writer that rewrites
           the item just before every swap.
    When:  A wanted is admitted.
    Then:  BackendBusy is raised after exactly the bounded rounds, and the item
           still holds one admission: contention is not a denial and not permission (O2).
    """
    key = mc_store.key(window.rule)
    wanted = AdmissionRequest((window,))
    plain = MemcachedBackend(mc_store)
    plain.admit(wanted)
    swaps = list()

    def interfere(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is ObservationPoint.BEFORE_COMMIT:
            raw.set(key, raw.get(key), noreply=False)
            swaps.append(point)
        else:
            pass

    handle = MemcachedBackend(mc_store, observer=interfere)

    with pytest.raises(BackendBusy):
        handle.admit(wanted)
    handle.close()
    verdicts = [plain.admit(wanted).allowed, plain.admit(wanted).allowed]
    plain.close()

    assert len(swaps) == MIN_CAS_ROUNDS
    assert verdicts == [True, False]


@pytest.mark.service("memcached")
def test_contention_retries_until_a_swap_lands(
    mc_store: MemcachedStore, window: Constraint, raw: Client
) -> None:
    """
    Given: Another worker that admits once, just before this handle's first swap.
    When:  This handle admits on a window of two.
    Then:  It is admitted on its second round, and a third attempt is denied:
           both admissions counted, neither overwrote the other.
    """
    other = MemcachedBackend(mc_store)
    wanted = AdmissionRequest((window,))
    interfered = list()

    def interfere(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is ObservationPoint.BEFORE_COMMIT and not interfered:
            interfered.append(other.admit(wanted).allowed)
        else:
            pass

    handle = MemcachedBackend(mc_store, observer=interfere)

    allowed = handle.admit(wanted).allowed
    third = other.admit(wanted).allowed
    handle.close()
    other.close()

    assert (interfered, allowed, third) == ([True], True, False)


@pytest.mark.service("memcached")
def test_a_lost_reply_after_commit_is_indeterminate(
    mc_store: MemcachedStore, window: Constraint
) -> None:
    """
    Given: A connection that fails right after an admitting swap landed.
    When:  A request is admitted, then admitted again.
    Then:  The first raises IndeterminateAdmission, and the debit stands: a window
           of two has one left.
    """

    def lose(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is ObservationPoint.AFTER_COMMIT:
            raise ConnectionResetError("reply lost")
        else:
            pass

    handle = MemcachedBackend(mc_store, observer=lose)
    request = AdmissionRequest((window,))
    with pytest.raises(IndeterminateAdmission):
        handle.admit(request)
    handle.close()
    plain = MemcachedBackend(mc_store)

    verdicts = [plain.admit(request).allowed, plain.admit(request).allowed]
    plain.close()

    assert verdicts == [True, False]


@pytest.mark.service("memcached")
def test_an_item_over_the_size_limit_is_never_written(
    memcached: str, prefix: str, window: Constraint, raw: Client
) -> None:
    """
    Given: A store whose item size limit is smaller than one item.
    When:  A request is admitted.
    Then:  StateCorruption is raised before anything is written.
    """
    store = MemcachedStore(memcached, prefix=prefix, max_item_size=90)
    handle = MemcachedBackend(store)

    with pytest.raises(StateCorruption, match="item size limit"):
        handle.admit(AdmissionRequest((window,)))
    handle.close()

    assert raw.get(store.key(window.rule)) is None


@pytest.mark.service("memcached")
def test_a_malformed_item_fails_closed(
    mc_store: MemcachedStore, mc_backend: MemcachedBackend, window: Constraint, raw: Client
) -> None:
    """
    Given: An item that is not a procrastinators item.
    When:  Its rule is admitted.
    Then:  StateCorruption is raised; it is never read as an empty bucket.
    """
    raw.set(mc_store.key(window.rule), b"not an item", noreply=False)

    with pytest.raises(StateCorruption):
        mc_backend.admit(AdmissionRequest((window,)))


@pytest.mark.service("memcached")
def test_items_expire_at_their_horizon(
    mc_store: MemcachedStore, mc_backend: MemcachedBackend, raw: Client
) -> None:
    """
    Given: A fixed window of one per second, admitted once.
    When:  The item's remaining lifetime is asked of the server.
    Then:  It expires within three seconds: its state stops mattering when the window ends.
    """
    rule = constraint("ankh", "short", FixedWindowPolicy(1, SECOND))
    mc_backend.admit(AdmissionRequest((rule,)))

    lifetime = raw.raw_command(f"mg {mc_store.key(rule.rule)} t", b"\r\n")

    assert lifetime.startswith(b"HD t")
    assert 0 < int(lifetime.split(b"t", 1)[1]) <= 3


@pytest.mark.service("memcached")
def test_closing_a_handle_leaves_items_to_others(
    mc_store: MemcachedStore, window: Constraint
) -> None:
    """
    Given: A handle that admitted once, then closed twice.
    When:  It is used again, and another handle admits on the same rule.
    Then:  The closed handle raises ClosedResource; the other still sees the admission.
    """
    handle = MemcachedBackend(mc_store)
    request = AdmissionRequest((window,))
    handle.admit(request)
    handle.close()
    handle.close()

    with pytest.raises(ClosedResource):
        handle.admit(request)
    other = MemcachedBackend(mc_store)
    verdicts = [other.admit(request).allowed, other.admit(request).allowed]
    other.close()

    assert verdicts == [True, False]


@pytest.mark.service("memcached")
def test_a_borrowed_executor_is_left_running(mc_store: MemcachedStore, window: Constraint) -> None:
    """
    Given: An asynchronous handle given an executor it does not own.
    When:  It admits and is closed.
    Then:  It reports the executor as borrowed and leaves it open.
    """
    from procrastinators.backends.executor import DedicatedExecutor

    executor = DedicatedExecutor()
    handle = AsyncMemcachedBackend(mc_store, executor=executor)

    async def use() -> bool:
        allowed = (await handle.admit(AdmissionRequest((window,)))).allowed
        await handle.aclose()
        return allowed

    allowed = asyncio.run(use())
    closed = executor.closed
    executor.close()

    assert (allowed, handle.ownership.executor, closed) == (True, Ownership.BORROWED, False)


@pytest.mark.service("memcached")
def test_a_limiter_accepting_best_effort_admits_through_memcached(
    memcached: str, prefix: str
) -> None:
    """
    Given: A limiter of two per hour on a memcached handle, best effort accepted.
    When:  It tries three times.
    Then:  Two are admitted and the third denied.
    """
    limiter = RateLimiter(
        key="ankh",
        limits=[Limit(2, per="1h")],
        algorithm="fixed_window",
        backend=MemcachedBackend(MemcachedStore(memcached, prefix=prefix)),
        accept_best_effort=True,
    )

    verdicts = [limiter.try_acquire().allowed for _ in range(3)]

    assert verdicts == [True, True, False]


def test_the_minimum_cas_rounds_is_generous() -> None:
    """
    Given: The documented bound on compare-and-swap rounds.
    When:  It is compared with the default contention retries.
    Then:  It allows more, so ordinary races land rather than surface as contention.
    """
    assert OperationBudget().max_contention_retries + 1 < MIN_CAS_ROUNDS


if __name__ == "__main__":
    pass
else:
    pass
