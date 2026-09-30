"""The Redis and Valkey backend: native executors, expiry, failures, and lifecycle.

Every service test runs twice, against a real Redis and a real Valkey server,
each in a key prefix of its own, removed when the session ends. Time is a fake
timeline injected in place of the server's clock wherever a test needs exact
instants; nothing expires under an injected clock, so the few tests about
expiry use the server's clock and wait briefly in real time.
Tests that need no server — addresses, key layout, capabilities, cluster slot
checks — are unmarked and always run.

Separate processes racing on one server live in ``tests/concurrency``.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from redis.crc import key_slot as driver_key_slot

from procrastinators.backends.memory import MemoryBackend, MemoryStore
from procrastinators.backends.redis import (
    REDIS_CAPABILITIES,
    AsyncRedisBackend,
    KeyLayout,
    RedisBackend,
    RedisStore,
    key_slot,
    redis_backend,
)
from procrastinators.backends.redis.scripts import packaged
from procrastinators.backends.redis.store import Call
from procrastinators.builtins import builtin_registry
from procrastinators.capabilities import Mode
from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    IndeterminateAdmission,
    InvalidPolicy,
    PolicyConflict,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.keys import policy_fingerprint
from procrastinators.limiter import RateLimiter
from procrastinators.models import (
    MAX_AMOUNT,
    AdmissionRequest,
    Algorithms,
    Constraint,
    CoordinationScope,
    Durability,
    DurationMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    Limit,
    OperationBudget,
    Ownership,
    QuotaIdentity,
    RuleId,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.protocols import (
    AlgorithmSpec,
    AsyncBackend,
    MigrationStatus,
    SupportsAsyncCooldown,
    SupportsAsyncPolicyAdministration,
    SupportsCooldown,
    SupportsPolicyAdministration,
    SyncBackend,
)
from procrastinators.testing import (
    AsyncBackendCase,
    BackendCase,
    FakeTimeline,
    check_async_backend,
    check_backend,
)
from procrastinators.testing.conformance import ALL_GUARANTEES, CheckStatus, Guarantee
from tests.backends.conftest import SECOND, constraint
from tests.backends.redis_proxy import Fault, FaultyProxy, faulty_proxy
from tests.doubles import ClacksAlgorithm, ClacksPolicy
from tests.redis_service import CLUSTER_URL, fresh_prefix, reachable

if TYPE_CHECKING:
    from collections.abc import Iterator

    from redis import Redis

    from procrastinators.models import Decision, PolicySpec
    from tests.service_gate import ServiceGate

else:
    pass

CLAIMS = ALL_GUARANTEES - {Guarantee.OBSERVATION_POINTS}
"""Everything but observation points: loading and committing happen inside one script."""

OBSERVED_CHECKS = frozenset(
    {
        "observation_order",
        "failure_before_commit:before_lock",
        "failure_before_commit:after_load",
        "failure_before_commit:before_commit",
        "failure_after_commit",
        "cancellation_before_commit",
        "cancellation_after_commit",
        "time_sampled_after_lock",
    }
)
"""The checks that need an observer inside the transaction, which a script cannot call."""

MILLISECOND = DurationMicros(1_000)

GOOD_ADDRESSES = {
    "plain": ("redis://cache:6380/2", "redis", "cache:6380/2"),
    "credentials": ("redis://rincewind:luggage@cache/0", "redis", "cache:6379/0"),
    "tls": ("rediss://cache", "redis", "cache:6379/0"),
    "valkey": ("valkey://cache/3", "valkey", "cache:6379/3"),
    "valkey tls": ("valkeys://:secret@cache:7000", "valkey", "cache:7000/0"),
    "cluster": ("redis://cache:7000?cluster=true", "redis", "cluster:cache:7000"),
}

BAD_ADDRESSES = {
    "scheme": "memcached://cache",
    "database": "redis://cache/orders",
    "cluster database": "redis://cache/2?cluster=true",
    "flag": "redis://cache?cluster=perhaps",
    "prefix brace": "redis://cache?prefix=a{b}",
    "fragment": "redis://cache#ankh",
}


#
# Fixtures.


@pytest.fixture(params=list(GOOD_ADDRESSES))
def good_address(request: pytest.FixtureRequest) -> tuple[str, str, str]:
    """An address, the family it names, and its credential-free authority."""
    address = GOOD_ADDRESSES[request.param]
    return address


@pytest.fixture(params=list(BAD_ADDRESSES))
def bad_address(request: pytest.FixtureRequest) -> str:
    address = BAD_ADDRESSES[request.param]
    return address


@pytest.fixture
def redis_store(redis_url: str, timeline: FakeTimeline) -> RedisStore:
    """A store in a fresh key prefix, reading the fake timeline."""
    fresh = RedisStore(redis_url, prefix=fresh_prefix(), clock=timeline.epoch_clock)
    return fresh


@pytest.fixture
def redis_backend_handle(redis_store: RedisStore) -> Iterator[RedisBackend]:
    handle = RedisBackend(redis_store, namespace="discworld")
    yield handle
    handle.close()


@pytest.fixture
def raw(redis_url: str) -> Iterator[Redis]:
    """A plain client on the same server, standing in for an operator or another program."""
    import redis

    client = redis.Redis.from_url(redis_url.replace("valkey://", "redis://", 1))
    yield client
    client.close()


def _admit(handle: SyncBackend, *constraints: Constraint, cost: int = 1) -> Decision:
    decision = handle.admit(AdmissionRequest(constraints, cost=cost))
    return decision


def _text(value: object) -> str:
    text = value.decode() if isinstance(value, bytes) else str(value)
    return text


def _store_keys(store: RedisStore, rule_constraint: Constraint) -> tuple[str, str, str]:
    keys = store.rule_keys(rule_constraint).all
    return keys


#
# Addresses, identity, layout, and capabilities: no server needed.


def test_addresses_name_an_authority_without_credentials(
    good_address: tuple[str, str, str],
) -> None:
    """
    Given: A redis, rediss, valkey, or valkeys address, possibly with credentials.
    When:  A handle is built from it.
    Then:  Its identity names the family and host:port/db, never the credentials.
    """
    address, family, authority = good_address
    expected = (family, authority)

    handle = redis_backend(address, mode=Mode.SYNC, namespace="discworld")
    actual = (handle.identity.family, handle.identity.authority)

    assert actual == expected
    assert "luggage" not in str(handle.identity)
    assert "secret" not in str(handle.identity)


def test_redis_key_prefix_is_part_of_authority_identity() -> None:
    """
    Given: Stores on one server with matching and different key prefixes.
    When:  Their identities are compared.
    Then:  Only matching prefixes identify one admission authority.
    """
    first = RedisStore("redis://cache/0", prefix="orders")
    same = RedisStore("redis://cache/0", prefix="orders")
    other = RedisStore("redis://cache/0", prefix="payments")

    assert first.identity("discworld") == same.identity("discworld")
    assert first.identity("discworld") != other.identity("discworld")
    assert "orders" not in first.identity("discworld").authority


def test_malformed_addresses_are_refused(bad_address: str) -> None:
    """
    Given: An address naming another family, a non-numeric or cluster database, an
           unreadable flag, a brace in the prefix, or a fragment.
    When:  A handle is built from it.
    Then:  ConfigurationError is raised before anything connects.
    """
    with pytest.raises(ConfigurationError):
        redis_backend(bad_address, mode=Mode.SYNC)


def test_constructing_a_handle_connects_to_nothing() -> None:
    """
    Given: An address where nothing listens.
    When:  A store and both handles are constructed and closed.
    Then:  Nothing is raised: no client exists until a handle is first used.
    """
    store = RedisStore("redis://127.0.0.1:1/0")

    RedisBackend(store).close()
    asyncio.run(AsyncRedisBackend(store).aclose())


def test_handles_declare_honest_capabilities() -> None:
    """
    Given: A synchronous and an asynchronous handle.
    When:  Their capabilities are read.
    Then:  Each offers its own mode, shared-service coordination, service durability,
           composition, cooldowns, administration, and a native executor for every
           built-in algorithm and nothing else.
    """
    store = RedisStore()
    sync = RedisBackend(store).capabilities
    async_ = AsyncRedisBackend(store).capabilities
    expected_algorithms = frozenset(str(algorithm) for algorithm in Algorithms)

    assert (sync.supports_sync, sync.supports_async) == (True, False)
    assert (async_.supports_sync, async_.supports_async) == (False, True)
    for capabilities in (sync, async_):
        assert capabilities.coordination is CoordinationScope.SHARED_SERVICE
        assert capabilities.durability is Durability.SERVICE_DURABLE
        assert capabilities.supports_composition
        assert capabilities.supports_cooldowns
        assert capabilities.supports_policy_administration
        assert capabilities.native_executors == expected_algorithms
        assert capabilities.algorithms == expected_algorithms
    assert isinstance(RedisBackend(store), SyncBackend)
    assert isinstance(RedisBackend(store), SupportsCooldown)
    assert isinstance(RedisBackend(store), SupportsPolicyAdministration)
    assert isinstance(AsyncRedisBackend(store), AsyncBackend)
    assert isinstance(AsyncRedisBackend(store), SupportsAsyncCooldown)
    assert isinstance(AsyncRedisBackend(store), SupportsAsyncPolicyAdministration)


def test_every_family_registers_a_native_executor_per_algorithm() -> None:
    """
    Given: The built-in registry.
    When:  Each Redis and Valkey family is asked for the executor of each algorithm.
    Then:  One is registered, verified against that algorithm's traces.
    """
    registry = builtin_registry()
    policies = {
        Algorithms.FIXED_WINDOW: FixedWindowPolicy(10, SECOND),
        Algorithms.SLIDING_LOG: SlidingLogPolicy(10, SECOND),
        Algorithms.SLIDING_COUNTER: SlidingCounterPolicy(10, SECOND),
        Algorithms.TOKEN_BUCKET: TokenBucketPolicy(10, 1, SECOND),
        Algorithms.LEAKY_BUCKET: LeakyBucketPolicy(10, SECOND),
    }

    for family in ("redis", "rediss", "valkey", "valkeys"):
        assert registry.backend_spec(family).capabilities == REDIS_CAPABILITIES
        for algorithm, policy in policies.items():
            spec = registry.native_executor(family, constraint("ankh", "rule", policy))
            assert spec is not None
            assert spec.algorithm_id == algorithm
            assert spec.conformance_traces


def test_a_custom_algorithm_is_refused_before_anything_is_sent() -> None:
    """
    Given: A registered third-party algorithm, which has no native executor.
    When:  A limiter enforcing it on a Redis address is constructed.
    Then:  UnsupportedCapability is raised at construction: a Python evaluator does not
           run inside the server (Y2, Y5).
    """
    registry = builtin_registry()
    registry.register_algorithm(
        AlgorithmSpec(ClacksAlgorithm.id, ClacksPolicy.state_version, "event_log", ClacksAlgorithm)
    )

    with pytest.raises(UnsupportedCapability, match=ClacksAlgorithm.id):
        RateLimiter(
            key="ankh",
            limits=[ClacksPolicy(1, SECOND)],
            backend="redis://127.0.0.1:1/0",
            registry=registry,
        )


@pytest.mark.parametrize(
    "key",
    ["foo", "{user1000}.following", "{user1000}.followers", "{}foo", "foo{}{bar}", "foo{{bar}}"],
)
def test_key_slots_agree_with_redis_cluster(key: str) -> None:
    """
    Given: Keys with and without hash tags, including empty and nested braces.
    When:  Their slots are computed.
    Then:  They agree with the driver's independent implementation of Redis Cluster's rule.
    """
    expected = driver_key_slot(key.encode())

    actual = key_slot(key)

    assert actual == expected


def test_names_cannot_forge_a_hash_tag_or_a_separator() -> None:
    """
    Given: Rules whose names hold braces and separators, and ones that would collide
           if those were kept verbatim.
    When:  Their keys are laid out.
    Then:  Every rule has its own keys, each carrying exactly one hash tag.
    """
    layout = KeyLayout("p")
    scopes = (
        QuotaIdentity("a:b", "c"),
        QuotaIdentity("a", "b:c"),
        QuotaIdentity("{x}", "}y{"),
    )
    keys = [layout.rule_keys(RuleId(scope, "r"), KeyLayout.scope_tag(scope)) for scope in scopes]

    assert len({rule_keys.meta for rule_keys in keys}) == len(scopes)
    for rule_keys in keys:
        assert rule_keys.meta.count("{") == 1
        assert rule_keys.meta.count("}") == 1


def test_cluster_rules_and_cooldowns_share_one_atomic_slot() -> None:
    """
    Given: A cluster store and rules of two scopes sharing a coordination domain.
    When:  Their admission is built.
    Then:  Both rules and both cooldowns are read in one atomic hash slot.
    """
    store = RedisStore("redis://cache:7000", cluster=True)
    request = AdmissionRequest(
        tuple(
            Constraint(
                plain.rule,
                plain.policy,
                plain.fingerprint,
                "vendor",
            )
            for plain in (
                constraint("ankh", "vendor", SlidingLogPolicy(5, SECOND)),
                constraint("quirm", "vendor", SlidingLogPolicy(5, SECOND)),
            )
        )
    )

    store.validate(request)
    operation = store.admit(request, store.identity("discworld"))
    call = next(operation)

    assert call.admission is request
    assert len(call.keys) == 8
    assert len({key_slot(key) for key in call.keys}) == 1


def test_cluster_composition_across_scopes_requires_a_domain() -> None:
    """
    Given: A cluster store and rules of two scopes without a domain.
    When:  They are composed.
    Then:  The request is rejected as the contract requires.
    """
    store = RedisStore("redis://cache:7000", cluster=True)
    request = AdmissionRequest(
        (
            constraint("ankh", "vendor", SlidingLogPolicy(5, SECOND)),
            constraint("quirm", "vendor", SlidingLogPolicy(5, SECOND)),
        )
    )

    with pytest.raises(UnsupportedCapability, match="coordination domain"):
        store.validate(request)


def test_cluster_rule_placement_is_independent_of_domain() -> None:
    """
    Given: One rule described with two different coordination domains.
    When:  Its keys are built on a cluster store.
    Then:  Both descriptions address one quota rather than separate counters.
    """
    store = RedisStore("redis://cache:7000", cluster=True)
    plain = constraint("ankh", "vendor", SlidingLogPolicy(1, SECOND))
    first = Constraint(plain.rule, plain.policy, plain.fingerprint, "first")
    second = Constraint(plain.rule, plain.policy, plain.fingerprint, "second")

    assert store.rule_keys(first) == store.rule_keys(second)
    assert store.rule_keys(first) == store.rule_keys(plain)


@pytest.mark.parametrize(
    ("unsafe_second_shard", "require_noeviction"),
    [(False, True), (True, True), (True, False)],
)
def test_cluster_eviction_policy_is_checked_on_each_called_shard(
    unsafe_second_shard: bool,
    require_noeviction: bool,
) -> None:
    """
    Given: A cluster operation that sends calls to two hash slots.
    When:  The handle executes it.
    Then:  Each call's serving shard is checked before its command runs.
    """
    store = RedisStore("redis://cache:7000", cluster=True, require_noeviction=require_noeviction)
    backend = RedisBackend(store)
    checked = list()
    sent = list()

    def execute(operation: object, *, timeout_us: int, before_call: object = None) -> object:
        try:
            call = next(operation)  # ty: ignore[invalid-argument-type]
            while True:
                if before_call is not None:
                    before_call(call)  # ty: ignore[call-non-callable]
                else:
                    pass
                if call.command[:2] == ("CONFIG", "GET"):
                    checked.append(call.slot)
                    policy = (
                        b"allkeys-lru" if unsafe_second_shard and call.slot == 2 else b"noeviction"
                    )
                    reply: object = [b"maxmemory-policy", policy]
                else:
                    sent.append(call.slot)
                    reply = b"PONG"
                call = operation.send(reply)  # ty: ignore[unresolved-attribute]
        except StopIteration as stop:
            result = stop.value
        return result

    def calls() -> object:
        yield Call(1, command=("PING",))
        yield Call(2, command=("PING",))
        return "done"

    backend._active = SimpleNamespace(execute=execute)  # ty: ignore[invalid-assignment]
    if unsafe_second_shard and require_noeviction:
        with pytest.raises(UnsupportedCapability, match="allkeys-lru"):
            backend._execute(calls())  # ty: ignore[invalid-argument-type]
        assert sent == [1]
    else:
        result = backend._execute(calls())  # ty: ignore[invalid-argument-type]
        assert result == "done"
        assert sent == [1, 2]

    assert checked == ([1, 2] if require_noeviction else list())


def test_sync_close_waits_for_an_in_flight_redis_operation() -> None:
    """
    Given: A Redis command holding a transport reference.
    When:  Its handle closes on another thread.
    Then:  The client is closed only after the command has settled.
    """
    store = RedisStore("redis://cache", require_noeviction=False)
    backend = RedisBackend(store)
    started = threading.Event()
    proceed = threading.Event()
    closed = threading.Event()
    close_calls = list()

    def execute(_operation: object, *, timeout_us: int, before_call: object = None) -> str:
        started.set()
        assert proceed.wait(2)
        return "done"

    def close_client() -> None:
        close_calls.append(True)
        closed.set()

    client = SimpleNamespace(close=close_client)
    backend._active = SimpleNamespace(execute=execute, client=client)  # ty: ignore[invalid-assignment]
    worker = threading.Thread(target=backend._execute, args=(store.check_eviction(),))
    closer = threading.Thread(target=backend.close)
    second_closer = threading.Thread(target=backend.close)
    worker.start()
    try:
        assert started.wait(2)
        closer.start()
        second_closer.start()
        assert not closed.wait(0.05)
    finally:
        proceed.set()
        worker.join(2)
        if closer.ident is not None:
            closer.join(2)
        else:
            pass
        if second_closer.ident is not None:
            second_closer.join(2)
        else:
            pass

    assert not worker.is_alive()
    assert not closer.is_alive()
    assert not second_closer.is_alive()
    assert closed.is_set()
    assert close_calls == [True]


def test_async_close_waits_for_an_in_flight_redis_operation() -> None:
    """
    Given: An async Redis command holding a transport reference.
    When:  Its handle is closed during the command.
    Then:  The client is closed only after the command has settled.
    """
    store = RedisStore("redis://cache", require_noeviction=False)
    backend = AsyncRedisBackend(store)

    async def scenario() -> None:
        started = asyncio.Event()
        proceed = asyncio.Event()
        closed = asyncio.Event()

        async def execute(
            _operation: object, *, timeout_us: int, before_call: object = None
        ) -> str:
            started.set()
            await proceed.wait()
            return "done"

        async def close_client() -> None:
            closed.set()

        client = SimpleNamespace(aclose=close_client)
        backend._active = SimpleNamespace(execute=execute, client=client)  # ty: ignore[invalid-assignment]
        running = asyncio.create_task(backend._execute(store.check_eviction()))
        await started.wait()
        closing = asyncio.create_task(backend.aclose())
        await asyncio.sleep(0)
        assert not closed.is_set()
        proceed.set()
        assert await running == "done"
        await closing
        assert closed.is_set()

    asyncio.run(scenario())


def test_a_cluster_store_composes_one_scope_or_one_domain() -> None:
    """
    Given: A cluster store; two rules of one scope; and rules of two scopes sharing
           one coordination domain.
    When:  Requests composing each are validated.
    Then:  Both are accepted: their keys share one hash slot.
    """
    store = RedisStore("redis://cache:7000", cluster=True)
    one_scope = AdmissionRequest(
        (
            constraint("ankh", "burst", SlidingLogPolicy(5, SECOND)),
            constraint("ankh", "sustained", SlidingLogPolicy(50, DurationMicros(60 * SECOND))),
        )
    )
    policy = SlidingLogPolicy(5, SECOND)
    one_domain = AdmissionRequest(
        tuple(
            Constraint(
                RuleId(QuotaIdentity("discworld", scope), "vendor"),
                policy,
                policy_fingerprint(policy),
                "ankh-morpork",
            )
            for scope in ("ankh", "quirm")
        )
    )

    store.validate(one_scope)
    store.validate(one_domain)


#
# Conformance and the native executors.


def test_the_backend_passes_the_conformance_suite(redis_url: str) -> None:
    """
    Given: A synchronous handle per check, in a fresh prefix, on the fake timeline.
    When:  The conformance suite runs every trace, scenario, conflict, and lifecycle check.
    Then:  Every check passes; only those needing an observer inside the script skip.
    """

    def build(timeline: FakeTimeline, observer: object) -> RedisBackend:
        handle = RedisBackend(
            RedisStore(redis_url, prefix=fresh_prefix(), clock=timeline.epoch_clock),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = check_backend(BackendCase("redis", build, guarantees=CLAIMS))

    report.raise_for_failures()
    assert {result.name for result in report.skipped} == OBSERVED_CHECKS


def test_the_async_backend_passes_the_conformance_suite(redis_url: str) -> None:
    """
    Given: An asynchronous handle per check, on the driver's asyncio client.
    When:  The asynchronous conformance suite runs.
    Then:  Every check passes; only those needing an observer inside the script skip.
    """

    def build(timeline: FakeTimeline, observer: object) -> AsyncRedisBackend:
        handle = AsyncRedisBackend(
            RedisStore(redis_url, prefix=fresh_prefix(), clock=timeline.epoch_clock),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = asyncio.run(check_async_backend(AsyncBackendCase("async redis", build, CLAIMS)))

    report.raise_for_failures()
    assert {result.name for result in report.skipped} == OBSERVED_CHECKS
    assert all(result.status is not CheckStatus.FAILED for result in report.results)


_PERIODS = st.sampled_from(
    [
        MILLISECOND,
        DurationMicros(7 * MILLISECOND),
        SECOND,
        DurationMicros(3600 * SECOND),
        DurationMicros(3_153_600_000_000_000),
    ]
)


@st.composite
def _policies(draw: st.DrawFn) -> PolicySpec:
    """A valid policy of any algorithm, from tiny to the edges of the supported range."""
    amount = draw(st.one_of(st.integers(1, 5), st.integers(1, 100), st.just(MAX_AMOUNT)))
    period = draw(_PERIODS)
    kind = draw(st.sampled_from(sorted(Algorithms)))
    try:
        if kind == Algorithms.FIXED_WINDOW:
            policy: PolicySpec = FixedWindowPolicy(
                amount, period, DurationMicros(draw(st.integers(0, period - 1)))
            )
        elif kind == Algorithms.SLIDING_LOG:
            policy = SlidingLogPolicy(min(amount, 64), period)
        elif kind == Algorithms.SLIDING_COUNTER:
            policy = SlidingCounterPolicy(amount, period)
        elif kind == Algorithms.TOKEN_BUCKET:
            policy = TokenBucketPolicy(
                amount,
                draw(st.integers(1, amount)),
                period,
                initial_tokens=draw(st.none() | st.integers(0, amount)),
            )
        else:
            policy = LeakyBucketPolicy(amount, period, draw(st.integers(0, MAX_AMOUNT)))
    except InvalidPolicy:
        policy = SlidingLogPolicy(3, SECOND)
    return policy


_STEPS = st.lists(
    st.tuples(
        st.one_of(st.integers(0, 3), st.integers(0, 2_000_000), st.integers(0, 10**13)),
        st.floats(0, 1),
        st.booleans(),
    ),
    min_size=1,
    max_size=25,
)


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(first=_policies(), second=_policies(), steps=_STEPS)
def test_native_executors_agree_with_the_reference_evaluators(
    redis_url: str, first: PolicySpec, second: PolicySpec, steps: list[tuple[int, float, bool]]
) -> None:
    """
    Given: Two random policies of any algorithms, from tiny to the edges of the
           supported range, and a random history of waits, costs, and composition.
    When:  The same history runs on the native executors and on the reference
           evaluators behind a memory store, reading one fake timeline.
    Then:  Every decision agrees: verdict, blocking rules, retry delay, remainders,
           and admission time (N4).
    """
    timeline = FakeTimeline()
    native = RedisBackend(
        RedisStore(redis_url, prefix=fresh_prefix(), clock=timeline.epoch_clock),
        namespace="discworld",
    )
    reference = MemoryBackend(MemoryStore(clock=timeline.epoch_clock), namespace="discworld")
    rules = (constraint("ankh", "first", first), constraint("ankh", "second", second))
    capacity = min(rule.capacity for rule in rules)
    try:
        for wait, share, composed in steps:
            timeline.advance(wait)
            chosen = rules if composed else rules[:1]
            cost = max(1, round(share * min(rule.capacity for rule in chosen)))
            request = AdmissionRequest(chosen, cost=min(cost, capacity) if composed else cost)
            expected = reference.admit(request)
            actual = native.admit(request)

            assert actual.allowed == expected.allowed
            assert actual.blocking == expected.blocking
            assert actual.retry_after_us == expected.retry_after_us
            assert actual.remaining == expected.remaining
            assert actual.observed_at == expected.observed_at
    finally:
        native.close()


def test_weighted_admissions_store_one_event_each(
    redis_backend_handle: RedisBackend, redis_store: RedisStore, raw: Redis
) -> None:
    """
    Given: A sliding log of 100 per minute.
    When:  Admissions of cost 40 and 30 are made.
    Then:  The event log holds two members, one per admission, not 70 (P6).
    """
    weighted = constraint("ankh", "weighted", SlidingLogPolicy(100, DurationMicros(60 * SECOND)))

    _admit(redis_backend_handle, weighted, cost=40)
    _admit(redis_backend_handle, weighted, cost=30)

    assert raw.zcard(_store_keys(redis_store, weighted)[2]) == 2


def test_a_composed_denial_debits_no_rule(
    redis_backend_handle: RedisBackend, two_per_second: Constraint
) -> None:
    """
    Given: A roomy rule and a rule of capacity one composed together, the second spent.
    When:  The composition is attempted again.
    Then:  It is denied by the tight rule alone and the roomy rule's remainder is untouched.
    """
    tight = constraint("quirm", "tight", SlidingLogPolicy(1, DurationMicros(60 * SECOND)))
    _admit(redis_backend_handle, tight)

    denied = _admit(redis_backend_handle, two_per_second, tight)
    roomy = _admit(redis_backend_handle, two_per_second)

    assert not denied.allowed
    assert denied.blocking == (tight.rule,)
    assert roomy.allowed
    assert roomy.remaining[0].units == 1


#
# Expiry, metadata, restarts, and conflicts.


def test_state_expires_at_its_horizon_and_metadata_never(redis_url: str, raw: Redis) -> None:
    """
    Given: A sliding log of five per second, admitted once on the server's clock.
    When:  The keys' time to live is read.
    Then:  State and events expire about a second out, at the safe-forget horizon,
           while policy metadata never expires (L9, L10).
    """
    store = RedisStore(redis_url, prefix=fresh_prefix())
    handle = RedisBackend(store)
    rule = constraint("ankh", "ttl", SlidingLogPolicy(5, SECOND))
    _admit(handle, rule)
    handle.close()
    meta, state, events = _store_keys(store, rule)

    assert raw.pttl(meta) == -1
    assert 900 <= raw.pttl(state) <= 1001
    assert 900 <= raw.pttl(events) <= 1001


def test_a_conflict_is_detected_after_the_state_expired(redis_url: str, raw: Redis) -> None:
    """
    Given: A rule admitted under one policy, on the server's clock, whose state has
           since expired on the server.
    When:  The rule is admitted under a different policy.
    Then:  PolicyConflict is raised: metadata outlived the quota state (L10).
    """
    store = RedisStore(redis_url, prefix=fresh_prefix())
    handle = RedisBackend(store)
    original = constraint("ankh", "brief", SlidingLogPolicy(1, DurationMicros(20 * MILLISECOND)))
    changed = constraint("ankh", "brief", SlidingLogPolicy(2, DurationMicros(20 * MILLISECOND)))
    _admit(handle, original)
    time.sleep(0.1)

    expired = raw.exists(_store_keys(store, original)[1]) == 0
    with pytest.raises(PolicyConflict):
        _admit(handle, changed)
    handle.close()

    assert expired


def test_quota_and_policy_survive_a_restart(
    redis_url: str, timeline: FakeTimeline, two_per_second: Constraint, raw: Redis
) -> None:
    """
    Given: A rule used to capacity through one handle, then the handle closed, every
           connection dropped, and the server's script cache flushed, as a restart would.
    When:  A new store and handle address the same prefix.
    Then:  The quota is still spent, the script is reloaded transparently, and another
           policy for the rule is still a conflict.
    """
    prefix = fresh_prefix()
    first = RedisBackend(RedisStore(redis_url, prefix=prefix, clock=timeline.epoch_clock))
    admitted = [_admit(first, two_per_second).allowed for _ in range(2)]
    first.close()
    raw.script_flush()
    raw.client_kill_filter(skipme=True, _type="normal")
    restarted = RedisBackend(RedisStore(redis_url, prefix=prefix, clock=timeline.epoch_clock))
    changed = constraint("ankh", "burst", SlidingLogPolicy(5, SECOND))

    after_restart = _admit(restarted, two_per_second)
    with pytest.raises(PolicyConflict):
        _admit(restarted, changed)
    restarted.close()

    assert admitted == [True, True]
    assert not after_restart.allowed


def test_a_denied_first_attempt_records_initial_state(
    redis_url: str, timeline: FakeTimeline, empty_bucket: Constraint
) -> None:
    """
    Given: A token bucket that starts empty, denied on its first attempt.
    When:  A new handle tries again one refill period later.
    Then:  The refill clock started at the first attempt, so one token is available (P5).
    """
    prefix = fresh_prefix()
    first = RedisBackend(RedisStore(redis_url, prefix=prefix, clock=timeline.epoch_clock))
    denied = _admit(first, empty_bucket)
    first.close()
    timeline.advance(SECOND)
    second = RedisBackend(RedisStore(redis_url, prefix=prefix, clock=timeline.epoch_clock))

    admitted = _admit(second, empty_bucket)
    second.close()

    assert not denied.allowed
    assert admitted.allowed


def test_authority_time_never_runs_backwards_for_a_rule(
    redis_backend_handle: RedisBackend, timeline: FakeTimeline
) -> None:
    """
    Given: A sliding log of one per second admitted once, then the clock stepped back
           ten seconds.
    When:  The rule is attempted again.
    Then:  Time is clamped at the last observed instant: the attempt is denied rather
           than seeing the old entry as expired, and is stamped no earlier (T7).
    """
    rule = constraint("ankh", "clamped", SlidingLogPolicy(1, SECOND))
    first = _admit(redis_backend_handle, rule)
    timeline.step_epoch(-DurationMicros(10 * SECOND))

    again = _admit(redis_backend_handle, rule)

    assert first.allowed
    assert not again.allowed
    assert again.observed_at == first.observed_at
    assert again.retry_after_us == SECOND


def test_server_time_is_the_authority_by_default(redis_url: str) -> None:
    """
    Given: A store with no injected clock.
    When:  A rule is admitted.
    Then:  The admission is stamped with the server's clock, which this host shares.
    """
    handle = RedisBackend(RedisStore(redis_url, prefix=fresh_prefix()))
    before = time.time_ns() // 1_000

    decision = _admit(handle, constraint("ankh", "server", SlidingLogPolicy(1, SECOND)))
    handle.close()

    assert decision.admission is not None
    assert abs(decision.admission.admitted_at - before) < 5 * SECOND


#
# Cooldowns, inspection, and administration.


def test_cooldowns_reach_every_handle_on_the_server(
    redis_url: str, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: Two handles on separate stores and clients sharing one prefix.
    When:  One pauses the scope for five seconds, and later for one second.
    Then:  The other is denied for the remaining pause, the shorter pause does not
           shorten it, and admission resumes once it ends (K2, K3).
    """
    prefix = fresh_prefix()
    first = RedisBackend(RedisStore(redis_url, prefix=prefix, clock=timeline.epoch_clock))
    second = RedisBackend(RedisStore(redis_url, prefix=prefix, clock=timeline.epoch_clock))
    scope = two_per_second.rule.scope

    cooldown = first.defer_for(scope, DurationMicros(5 * SECOND), reason="429")
    timeline.advance(SECOND)
    kept = first.defer_for(scope, SECOND, reason="later")
    paused = _admit(second, two_per_second)
    timeline.advance(4 * SECOND)
    resumed = _admit(second, two_per_second)
    first.close()
    second.close()

    assert kept == cooldown
    assert cooldown.reason == "429"
    assert not paused.allowed
    assert paused.retry_after_us == 4 * SECOND
    assert resumed.allowed


def test_inspection_reports_what_this_process_knows(
    redis_backend_handle: RedisBackend, two_per_second: Constraint, redis_url: str
) -> None:
    """
    Given: A rule admitted once through this handle, and a stranger store sharing the prefix.
    When:  Both inspect it, together with a rule never seen.
    Then:  This handle reports the remainder; the stranger reports the algorithm only;
           the unseen rule reports ``unused``, and nothing is consumed (R7).
    """
    _admit(redis_backend_handle, two_per_second)
    unseen = constraint("quirm", "never", SlidingLogPolicy(1, SECOND)).rule
    stranger = RedisBackend(RedisStore(redis_url, prefix=redis_backend_handle.store.layout.prefix))

    snapshot = redis_backend_handle.inspect((two_per_second.rule, unseen))
    distant = stranger.inspect((two_per_second.rule,))
    stranger.close()
    still_one_left = _admit(redis_backend_handle, two_per_second)

    known, never = snapshot.rules
    assert known.remaining == 1
    assert known.algorithm == Algorithms.SLIDING_LOG
    assert never.algorithm == "unused"
    assert distant.rules[0].remaining is None
    assert distant.rules[0].algorithm == Algorithms.SLIDING_LOG
    assert still_one_left.allowed


def test_a_migration_drains_before_installing_the_new_policy(
    redis_backend_handle: RedisBackend, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule with live state, and a migration begun towards a new policy.
    When:  Admissions are attempted while it drains, then after it is complete.
    Then:  Draining refuses both policies, completion is refused until the state is
           neutral, and afterwards only the new policy is admitted (L12).
    """
    handle = redis_backend_handle
    _admit(handle, two_per_second)
    new = constraint("ankh", "burst", SlidingLogPolicy(5, SECOND))
    rule = two_per_second.rule

    begun = handle.begin_migration(
        rule, to_fingerprint=new.fingerprint, to_state_version=new.state_version
    )
    with pytest.raises(PolicyConflict):
        _admit(handle, new)
    with pytest.raises(PolicyConflict):
        handle.complete_migration(rule)
    timeline.advance(SECOND)
    ready = handle.migration_status(rule)
    completed = handle.complete_migration(rule)
    with pytest.raises(PolicyConflict):
        _admit(handle, two_per_second)
    admitted = _admit(handle, new)

    assert begun.status is MigrationStatus.DRAINING
    assert ready is not None
    assert ready.status is MigrationStatus.READY
    assert completed.status is MigrationStatus.COMPLETE
    assert admitted.allowed
    stored = handle.stored_policy(rule)
    assert stored is not None
    assert stored.fingerprint == new.fingerprint


def test_state_that_never_drains_cannot_be_migrated(
    redis_backend_handle: RedisBackend, timeline: FakeTimeline, empty_bucket: Constraint
) -> None:
    """
    Given: A token bucket that starts empty, whose state therefore never becomes neutral.
    When:  A migration is begun and completion attempted a long time later.
    Then:  Completion is refused: live conversion is unsupported (L12).
    """
    handle = redis_backend_handle
    _admit(handle, empty_bucket)
    new = constraint("quirm", "bucket", TokenBucketPolicy(4, 1, SECOND, initial_tokens=0))
    handle.begin_migration(
        empty_bucket.rule, to_fingerprint=new.fingerprint, to_state_version=new.state_version
    )
    timeline.advance(3600 * SECOND)

    with pytest.raises(PolicyConflict, match="drain"):
        handle.complete_migration(empty_bucket.rule)


#
# Corruption, layout, and deployment checks.


def test_corrupt_state_fails_closed_and_writes_nothing(
    redis_backend_handle: RedisBackend, redis_store: RedisStore, raw: Redis
) -> None:
    """
    Given: A rule whose stored count was overwritten with text.
    When:  It is admitted.
    Then:  StateCorruption is raised and neither its state nor its metadata changed.
    """
    rule = constraint("ankh", "corrupt", FixedWindowPolicy(5, SECOND))
    _admit(redis_backend_handle, rule)
    meta, state, _ = _store_keys(redis_store, rule)
    raw.hset(state, "v:count", "many")
    before = (raw.hgetall(meta), raw.hgetall(state))

    with pytest.raises(StateCorruption, match="malformed"):
        _admit(redis_backend_handle, rule)

    assert (raw.hgetall(meta), raw.hgetall(state)) == before


def test_a_key_of_the_wrong_type_is_state_corruption(
    redis_backend_handle: RedisBackend, redis_store: RedisStore, raw: Redis
) -> None:
    """
    Given: A plain string where a rule's state hash belongs.
    When:  The rule is admitted and inspected.
    Then:  StateCorruption is raised both times.
    """
    rule = constraint("ankh", "typed", SlidingLogPolicy(5, SECOND))
    raw.set(_store_keys(redis_store, rule)[1], "surprise")

    with pytest.raises(StateCorruption):
        _admit(redis_backend_handle, rule)
    with pytest.raises(StateCorruption):
        redis_backend_handle.inspect((rule.rule,))


def test_a_foreign_key_layout_is_refused(
    redis_backend_handle: RedisBackend, redis_store: RedisStore, raw: Redis
) -> None:
    """
    Given: A rule whose metadata was written by a library with another key layout.
    When:  It is admitted.
    Then:  UnsupportedCapability is raised rather than the data being reinterpreted.
    """
    rule = constraint("ankh", "foreign", SlidingLogPolicy(5, SECOND))
    raw.hset(_store_keys(redis_store, rule)[0], mapping={"lay": "99", "fp": rule.fingerprint})

    with pytest.raises(UnsupportedCapability, match="layout"):
        _admit(redis_backend_handle, rule)


def test_an_evicting_server_is_refused_unless_accepted(
    redis_url: str, raw: Redis, two_per_second: Constraint
) -> None:
    """
    Given: A server running maxmemory-policy volatile-lru, which could evict state keys.
    When:  A handle admits, then one told to accept the weaker guarantee does.
    Then:  The first raises UnsupportedCapability naming the policy; the second admits.
    """
    original = _text(raw.config_get("maxmemory-policy")["maxmemory-policy"])
    raw.config_set("maxmemory-policy", "volatile-lru")
    try:
        strict = RedisBackend(RedisStore(redis_url, prefix=fresh_prefix()))
        lenient = RedisBackend(
            RedisStore(redis_url, prefix=fresh_prefix(), require_noeviction=False)
        )
        with pytest.raises(UnsupportedCapability, match="volatile-lru"):
            _admit(strict, two_per_second)
        admitted = _admit(lenient, two_per_second)
        strict.close()
        lenient.close()
    finally:
        raw.config_set("maxmemory-policy", original)

    assert admitted.allowed


#
# Failures in transit.


@dataclass(frozen=True, slots=True)
class Proxied:
    """A proxy in front of the server, and stores addressing the server through it or not."""

    proxy: FaultyProxy
    url: str
    prefix: str
    timeline: FakeTimeline

    def store(self, *, direct: bool = False) -> RedisStore:
        """A store in the test's prefix, through the proxy unless ``direct``."""
        address = self.url if direct else self.proxy.url
        store = RedisStore(address, prefix=self.prefix, clock=self.timeline.epoch_clock)
        return store


@pytest.fixture
def proxied(redis_url: str, timeline: FakeTimeline) -> Iterator[Proxied]:
    with faulty_proxy(redis_url.replace("valkey://", "redis://", 1)) as proxy:
        yield Proxied(proxy, redis_url, fresh_prefix(), timeline)


def test_a_server_that_cannot_be_reached_commits_nothing() -> None:
    """
    Given: An address where nothing listens.
    When:  An admission is attempted.
    Then:  BackendUnavailable is raised, which a waiter may retry: nothing was sent.
    """
    handle = RedisBackend(RedisStore("redis://127.0.0.1:1/0", socket_timeout=0.5))

    with pytest.raises(BackendUnavailable, match="nothing was sent"):
        _admit(handle, constraint("ankh", "unreachable", SlidingLogPolicy(1, SECOND)))
    handle.close()


def test_a_lost_reply_after_commit_is_indeterminate_and_not_refunded(
    proxied: Proxied,
) -> None:
    """
    Given: A rule of capacity one, its script already loaded, and the proxy set to lose
           the next admission's reply after the server has run it.
    When:  The admission is attempted, then attempted again directly.
    Then:  The first raises IndeterminateAdmission, never permission; the second is
           denied, because the lost admission did commit (O4).
    """
    proxy = proxied.proxy
    single = constraint("quirm", "single", SlidingLogPolicy(1, DurationMicros(60 * SECOND)))
    probe = constraint("quirm", "probe", SlidingLogPolicy(9, DurationMicros(60 * SECOND)))
    through = RedisBackend(proxied.store())
    _admit(through, probe)
    proxy.arm(Fault.LOSE_REPLY, trigger=packaged("admit").sha.encode())

    with pytest.raises(IndeterminateAdmission) as raised:
        _admit(through, single)
    direct = RedisBackend(proxied.store(direct=True))
    after = _admit(direct, single)
    through.close()
    direct.close()

    assert raised.value.rules == (single.rule,)
    assert not after.allowed


def test_a_reply_past_the_storage_timeout_is_indeterminate(
    proxied: Proxied,
) -> None:
    """
    Given: The proxy set to hold the next admission's reply for two seconds.
    When:  An admission with a half-second storage budget is attempted.
    Then:  IndeterminateAdmission is raised, and the admission did commit.
    """
    proxy = proxied.proxy
    single = constraint("quirm", "slow", SlidingLogPolicy(1, DurationMicros(60 * SECOND)))
    through = RedisBackend(proxied.store())
    _admit(through, constraint("quirm", "warm", SlidingLogPolicy(9, DurationMicros(60 * SECOND))))
    proxy.arm(Fault.DELAY_REPLY, trigger=packaged("admit").sha.encode(), delay_s=2)
    budget = OperationBudget(storage_timeout_us=DurationMicros(500_000))

    with pytest.raises(IndeterminateAdmission):
        through.admit(AdmissionRequest((single,), budget=budget))
    direct = RedisBackend(proxied.store(direct=True))
    after = _admit(direct, single)
    through.close()
    direct.close()

    assert not after.allowed


@pytest.mark.parametrize(
    ("refusal", "error"),
    [
        (b"-READONLY You can't write against a read only replica.\r\n", BackendUnavailable),
        (b"-OOM command not allowed when used memory > 'maxmemory'.\r\n", BackendUnavailable),
        (b"-MASTERDOWN Link with MASTER is down\r\n", BackendUnavailable),
        (b"-BUSY Redis is busy running a script.\r\n", BackendBusy),
    ],
)
def test_a_refusal_before_the_script_runs_commits_nothing(
    proxied: Proxied, refusal: bytes, error: type[Exception]
) -> None:
    """
    Given: The proxy set to answer the next admission with a refusal a server makes
           before running anything — a replica after failover, memory exhausted, a
           primary down, or another script running.
    When:  The admission is attempted, then attempted again.
    Then:  The first raises a retryable error, never permission; the second is admitted,
           because nothing was committed.
    """
    proxy = proxied.proxy
    single = constraint("quirm", "refused", SlidingLogPolicy(1, DurationMicros(60 * SECOND)))
    through = RedisBackend(proxied.store())
    _admit(through, constraint("quirm", "warm", SlidingLogPolicy(9, DurationMicros(60 * SECOND))))
    proxy.arm(Fault.REFUSE, trigger=packaged("admit").sha.encode(), refusal=refusal)

    with pytest.raises(error):
        _admit(through, single)
    again = _admit(through, single)
    through.close()

    assert again.allowed


def test_an_async_admission_cancelled_in_flight_may_have_committed(
    proxied: Proxied,
) -> None:
    """
    Given: An asynchronous handle and the proxy holding the next admission's reply.
    When:  The admission is cancelled while its reply is outstanding, then the handle
           is used again.
    Then:  The cancellation propagates unchanged, the admission did commit — possible
           consumed capacity, never permission — and the handle still works (O5).
    """
    proxy = proxied.proxy
    single = constraint("quirm", "cancelled", SlidingLogPolicy(1, DurationMicros(60 * SECOND)))
    warm = constraint("quirm", "warm", SlidingLogPolicy(9, DurationMicros(60 * SECOND)))

    async def scenario() -> tuple[bool, bool]:
        handle = AsyncRedisBackend(proxied.store())
        await handle.admit(AdmissionRequest((warm,)))
        fired = proxy.arm(Fault.DELAY_REPLY, trigger=packaged("admit").sha.encode(), delay_s=1)
        task = asyncio.create_task(handle.admit(AdmissionRequest((single,))))
        while not fired.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        after = await handle.admit(AdmissionRequest((single,)))
        works = await handle.admit(AdmissionRequest((warm,)))
        await handle.aclose()
        result = (after.allowed, works.allowed)
        return result

    after_allowed, still_works = asyncio.run(scenario())

    assert not after_allowed
    assert still_works


#
# Concurrency, the facade, and ownership.


def test_threads_sharing_a_handle_admit_exactly_the_quota(
    redis_backend_handle: RedisBackend,
) -> None:
    """
    Given: Eight threads racing 25 attempts each on a rule of 50 per hour.
    When:  They finish.
    Then:  Exactly 50 were admitted.
    """
    rule = constraint("ankh", "raced", SlidingLogPolicy(50, DurationMicros(3600 * SECOND)))
    admitted = list()
    lock = threading.Lock()

    def race() -> None:
        for _ in range(25):
            if _admit(redis_backend_handle, rule).allowed:
                with lock:
                    admitted.append(1)
            else:
                pass

    threads = [threading.Thread(target=race) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(admitted) == 50


def test_async_tasks_admit_exactly_the_quota(redis_url: str) -> None:
    """
    Given: Forty concurrent tasks on one asynchronous handle, a rule of 15 per hour.
    When:  They finish.
    Then:  Exactly 15 were admitted.
    """
    rule = constraint("ankh", "tasks", SlidingLogPolicy(15, DurationMicros(3600 * SECOND)))

    async def scenario() -> int:
        handle = AsyncRedisBackend(RedisStore(redis_url, prefix=fresh_prefix()))
        decisions = await asyncio.gather(
            *(handle.admit(AdmissionRequest((rule,))) for _ in range(40))
        )
        await handle.aclose()
        count = sum(decision.allowed for decision in decisions)
        return count

    assert asyncio.run(scenario()) == 15


def test_limiters_on_one_address_share_its_quota(redis_url: str) -> None:
    """
    Given: Two limiters built from one address and prefix, one used synchronously and
           one asynchronously.
    When:  Each acquires without waiting.
    Then:  Together they admit the quota once, and closing them leaves the server's state.
    """
    address = f"{redis_url}?prefix={fresh_prefix()}"
    sync = RateLimiter(key="ankh", limits=[Limit(2, per="1h")], backend=address, timeout=0)
    async_ = RateLimiter(key="ankh", limits=[Limit(2, per="1h")], backend=address, timeout=0)

    async def acquire_once() -> bool:
        decision = await async_.try_acquire_async()
        await async_.aclose()
        return decision.allowed

    first = sync.try_acquire()
    second = asyncio.run(acquire_once())
    third = sync.try_acquire()
    sync.close()

    assert (first.allowed, second, third.allowed) == (True, True, False)
    assert sync.backend.family in ("redis", "valkey")


def test_an_injected_client_is_borrowed_and_left_open(redis_url: str, raw: Redis) -> None:
    """
    Given: A handle given the caller's own client.
    When:  The handle admits and is closed.
    Then:  It reports the client as borrowed, and the client still works afterwards (L2).
    """
    handle = RedisBackend(RedisStore(redis_url, prefix=fresh_prefix()), client=raw)

    _admit(handle, constraint("ankh", "borrowed", SlidingLogPolicy(1, SECOND)))
    handle.close()

    assert handle.ownership.client is Ownership.BORROWED
    assert raw.ping()


def test_an_owned_client_is_closed_with_its_handle(redis_url: str) -> None:
    """
    Given: A handle that created its own client.
    When:  It is closed twice and then used.
    Then:  Closing is idempotent and later use raises ClosedResource (L1, L5).
    """
    handle = RedisBackend(RedisStore(redis_url, prefix=fresh_prefix()))
    _admit(handle, constraint("ankh", "owned", SlidingLogPolicy(1, SECOND)))

    handle.close()
    handle.close()

    assert handle.ownership.client is Ownership.OWNED
    with pytest.raises(ClosedResource):
        _admit(handle, constraint("ankh", "owned", SlidingLogPolicy(1, SECOND)))


#
# A real cluster, when one is available.


@pytest.fixture
def cluster_url(service_available: ServiceGate) -> str:
    """A Redis Cluster node, from ``PROCRASTINATORS_REDIS_CLUSTER_URL``."""
    service_available(
        "redis-cluster",
        bool(CLUSTER_URL) and reachable(CLUSTER_URL),
        "PROCRASTINATORS_REDIS_CLUSTER_URL names no reachable cluster",
    )
    return CLUSTER_URL


@pytest.mark.service("redis-cluster")
def test_a_cluster_admits_within_a_slot_and_refuses_across(cluster_url: str) -> None:
    """
    Given: A real Redis Cluster; rules of two scopes, plain and sharing a domain.
    When:  Each rule is admitted alone, the domain pair composed, and the plain pair composed.
    Then:  All rules and cooldowns share one slot, and both compositions use one quota.
    """
    handle = RedisBackend(RedisStore(cluster_url, cluster=True, prefix=fresh_prefix()))
    async_handle = AsyncRedisBackend(RedisStore(cluster_url, cluster=True, prefix=fresh_prefix()))
    plain = [
        constraint(scope, "vendor", SlidingLogPolicy(1, DurationMicros(60 * SECOND)))
        for scope in "abcdef"
    ]
    policy = SlidingLogPolicy(1, DurationMicros(60 * SECOND))
    domain = tuple(
        Constraint(
            RuleId(QuotaIdentity("discworld", scope), "vendor"),
            policy,
            policy_fingerprint(policy),
            "ankh",
        )
        for scope in ("ghi", "jkl")
    )

    singles = [_admit(handle, rule).allowed for rule in plain]
    composed = handle.admit(AdmissionRequest(domain))
    again = handle.admit(AdmissionRequest(domain))
    with pytest.raises(UnsupportedCapability):
        handle.admit(AdmissionRequest((plain[0], plain[1])))
    roomy = SlidingLogPolicy(5, DurationMicros(60 * SECOND))
    placed = tuple(
        Constraint(
            RuleId(QuotaIdentity("discworld", scope), "burst"),
            roomy,
            policy_fingerprint(roomy),
            "ankh",
        )
        for scope in ("ghi", "jkl")
    )
    cooled = handle.defer_for(placed[0].rule.scope, SECOND)
    paused = handle.admit(AdmissionRequest(placed))

    async def admit_async() -> bool:
        decision = await async_handle.admit(AdmissionRequest(tuple(plain[:1])))
        await async_handle.aclose()
        return decision.allowed

    held = asyncio.run(admit_async())
    handle.close()

    assert all(singles)
    assert len({key_slot(KeyLayout().scope_tag(rule.rule.scope)) for rule in plain}) > 1
    assert composed.allowed
    assert not again.allowed
    assert cooled.until > 0
    assert not paused.allowed
    assert paused.blocking == (placed[0].rule,)
    assert held


def test_a_rule_is_one_quota_with_or_without_a_domain_on_one_server(
    redis_backend_handle: RedisBackend,
) -> None:
    """
    Given: A fixed window of one per hour on a single server, admitted once without a
           coordination domain.
    When:  The same rule is attempted with a domain, then without.
    Then:  Both are denied: a domain places rules only in a cluster and never splits a
           rule into two quotas (I6).
    """
    policy = FixedWindowPolicy(1, DurationMicros(3600 * SECOND))
    plain = constraint("ankh", "placed", policy)
    placed = Constraint(plain.rule, policy, plain.fingerprint, "ankh-morpork")

    first = _admit(redis_backend_handle, plain)
    with_domain = _admit(redis_backend_handle, placed)
    without = _admit(redis_backend_handle, plain)

    assert (first.allowed, with_domain.allowed, without.allowed) == (True, False, False)


def test_state_outlives_a_clock_that_stepped_back(
    redis_url: str, raw: Redis, two_per_second: Constraint
) -> None:
    """
    Given: A rule whose last observed time is a minute ahead of the server's clock, as
           after the clock stepped back, admitted on server time.
    When:  The state's time to live is read.
    Then:  It lasts past the clamped authority time — over a minute — rather than
           expiring a second from now while authority time still stands at the clamp.
    """
    store = RedisStore(redis_url, prefix=fresh_prefix())
    handle = RedisBackend(store)
    meta, state, _ = _store_keys(store, two_per_second)
    _admit(handle, two_per_second)
    (seconds, micros) = raw.time()
    ahead = (seconds + 60) * 1_000_000 + micros
    raw.hset(meta, "last", str(ahead))

    _admit(handle, two_per_second)
    handle.close()

    assert raw.pttl(state) > 60_000


@pytest.mark.service("redis-cluster")
def test_a_cluster_uses_one_quota_across_domains(cluster_url: str) -> None:
    """
    Given: A real Redis Cluster and a rule admitted without a coordination domain.
    When:  The same rule is attempted with another domain.
    Then:  Its already consumed quota denies the request (I6).
    """
    handle = RedisBackend(RedisStore(cluster_url, cluster=True, prefix=fresh_prefix()))
    policy = FixedWindowPolicy(1, DurationMicros(3600 * SECOND))
    plain = constraint("ankh", "placed", policy)
    placed = Constraint(plain.rule, policy, plain.fingerprint, "ankh-morpork")

    first = _admit(handle, plain)
    second = _admit(handle, placed)
    handle.close()

    assert first.allowed
    assert not second.allowed
