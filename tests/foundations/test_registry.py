"""Explicit registration, and capability checks that need no connection.

Nothing is discovered: the Patrician's clerks accept only what was filed with
them, in person, once.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import dataclasses
import threading
from typing import TYPE_CHECKING, Any, NoReturn

import pytest

from procrastinators.capabilities import (
    CapabilityRequirement,
    Mode,
    require_capabilities,
    require_same_authority,
    unmet_requirements,
)
from procrastinators.errors import (
    ConfigurationError,
    InvalidPolicy,
    PolicyConflict,
    UnsupportedCapability,
)
from procrastinators.models import (
    Algorithms,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Durability,
    DurationMicros,
    FixedWindowPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.protocols import (
    AlgorithmSpec,
    BackendSpec,
    NativeExecutorSpec,
    StateRepresentation,
)
from procrastinators.registry import Registry
from procrastinators.testing import TraceRule
from procrastinators.testing.harness import DEFAULT_NAMESPACE
from tests.oracles import SlidingLogOracle, TokenBucketOracle

if TYPE_CHECKING:
    from procrastinators.models import Constraint, PolicySpec
else:
    pass

ONE_SECOND = DurationMicros(1_000_000)

REDIS_LIKE = Capabilities(
    algorithms=frozenset({Algorithms.SLIDING_LOG, Algorithms.TOKEN_BUCKET}),
    coordination=CoordinationScope.SHARED_SERVICE,
    durability=Durability.SERVICE_DURABLE,
    supports_sync=True,
    supports_async=True,
    supports_composition=True,
    native_executors=frozenset({Algorithms.SLIDING_LOG}),
    state_representations=frozenset(StateRepresentation),
)
MEMCACHED_LIKE = Capabilities(
    algorithms=frozenset({Algorithms.TOKEN_BUCKET}),
    coordination=CoordinationScope.SHARED_SERVICE,
    durability=Durability.BEST_EFFORT,
    state_representations=frozenset({StateRepresentation.SCALARS}),
)


def _no_backend(**options: object) -> NoReturn:
    raise AssertionError("a registry never constructs a backend on its own")


def _sliding_log_spec() -> AlgorithmSpec:
    spec = AlgorithmSpec(Algorithms.SLIDING_LOG, 1, StateRepresentation.EVENT_LOG, SlidingLogOracle)
    return spec


def _token_bucket_spec() -> AlgorithmSpec:
    spec = AlgorithmSpec(Algorithms.TOKEN_BUCKET, 1, StateRepresentation.SCALARS, TokenBucketOracle)
    return spec


BASE_EXECUTOR = NativeExecutorSpec(
    backend_family="redis",
    algorithm_id=Algorithms.SLIDING_LOG,
    state_version=1,
    policy_version=1,
    max_amount=1_000,
    max_cost=10,
    max_period_us=DurationMicros(3_600_000_000),
    conformance_traces=("sliding_log.basics",),
)


def _executor(**overrides: object) -> NativeExecutorSpec:
    spec = dataclasses.replace(BASE_EXECUTOR, **overrides)
    return spec


@pytest.fixture
def registry() -> Registry:
    """A registry holding two algorithms and one backend family."""
    fresh = Registry()
    fresh.register_algorithm(_sliding_log_spec())
    fresh.register_algorithm(_token_bucket_spec())
    fresh.register_backend(BackendSpec("redis", _no_backend, REDIS_LIKE))
    return fresh


def _constraint(policy: PolicySpec, label: str = "vetinari", scope: str = "trace") -> Constraint:
    constraint = TraceRule(label, policy, scope=scope).constraint(DEFAULT_NAMESPACE)
    return constraint


def test_what_was_registered_is_all_there_is(registry: Registry) -> None:
    """
    Given: A registry with two algorithms and one backend family.
    When:  Its contents are listed and an unregistered name is resolved.
    Then:  Exactly those are listed, and the unknown name is refused rather than imported.
    """
    expected = (frozenset({Algorithms.SLIDING_LOG, Algorithms.TOKEN_BUCKET}), frozenset({"redis"}))

    actual = (registry.algorithm_ids, registry.backend_families)

    assert actual == expected
    with pytest.raises(UnsupportedCapability, match="configuration cannot import"):
        registry.algorithm("os.system")
    with pytest.raises(UnsupportedCapability, match="not registered"):
        registry.backend_spec("importlib:import_module")


def test_a_second_registration_under_one_name_is_refused(registry: Registry) -> None:
    """
    Given: Registered algorithms and backends.
    When:  Another registration reuses a name.
    Then:  ConfigurationError: registrations are never silently replaced.
    """
    with pytest.raises(ConfigurationError, match="already registered"):
        registry.register_algorithm(_sliding_log_spec())
    with pytest.raises(ConfigurationError, match="already registered"):
        registry.register_backend(BackendSpec("redis", _no_backend, REDIS_LIKE))


def test_an_algorithm_is_built_once_and_shared(registry: Registry) -> None:
    """
    Given: A registered algorithm.
    When:  It is resolved from several threads at once.
    Then:  Every caller gets the same instance.
    """
    seen = list()

    def resolve() -> None:
        seen.append(registry.algorithm(Algorithms.SLIDING_LOG))

    threads = [threading.Thread(target=resolve) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    expected = 1

    actual = len({id(algorithm) for algorithm in seen})

    assert actual == expected


def test_a_factory_that_builds_the_wrong_algorithm_is_caught() -> None:
    """
    Given: A registration whose factory builds an algorithm with another id.
    When:  It is resolved.
    Then:  ConfigurationError names both.
    """
    registry = Registry()
    registry.register_algorithm(
        AlgorithmSpec(Algorithms.FIXED_WINDOW, 1, StateRepresentation.SCALARS, SlidingLogOracle)
    )

    with pytest.raises(ConfigurationError, match="built 'sliding_log'"):
        registry.algorithm(Algorithms.FIXED_WINDOW)


def test_a_native_executor_serves_only_what_it_was_verified_for(registry: Registry) -> None:
    """
    Given: A Redis sliding-log executor verified up to amount 1 000 and cost 10.
    When:  Constraints inside and outside those bounds, and another family, look for it.
    Then:  Only the in-bounds constraint on Redis is served; the rest fall back (Y5).
    """
    registry.register_native_executor(_executor())
    small = _constraint(SlidingLogPolicy(10, ONE_SECOND))
    large = _constraint(SlidingLogPolicy(5_000, ONE_SECOND))
    expected = (True, False, False, False)

    actual = (
        registry.native_executor("redis", small, cost=1) is not None,
        registry.native_executor("redis", small, cost=11) is not None,
        registry.native_executor("redis", large) is not None,
        registry.native_executor("valkey", small) is not None,
    )

    assert actual == expected


EXECUTOR_MISTAKES = {
    "unregistered family": (dict(backend_family="valkey"), UnsupportedCapability, "not registered"),
    "unregistered algorithm": (
        dict(algorithm_id="hogswatch"),
        UnsupportedCapability,
        "not registered",
    ),
    "undeclared executor": (
        dict(algorithm_id=Algorithms.TOKEN_BUCKET),
        UnsupportedCapability,
        "does not declare",
    ),
    "wrong state version": (dict(state_version=2), UnsupportedCapability, "state version"),
}


@pytest.mark.parametrize(
    ("overrides", "error", "message"), list(EXECUTOR_MISTAKES.values()), ids=list(EXECUTOR_MISTAKES)
)
def test_a_native_executor_must_match_what_is_registered(
    registry: Registry, overrides: dict[str, Any], error: type[Exception], message: str
) -> None:
    """
    Given: An executor naming an unknown family or algorithm, one the family does not
           declare, or another state version.
    When:  It is registered.
    Then:  It is refused with the reason.
    """
    with pytest.raises(error, match=message):
        registry.register_native_executor(_executor(**overrides))


def test_a_native_executor_is_registered_once(registry: Registry) -> None:
    """
    Given: A registered executor.
    When:  The same family, algorithm, and versions are registered again.
    Then:  ConfigurationError.
    """
    registry.register_native_executor(_executor())

    with pytest.raises(ConfigurationError, match="already registered"):
        registry.register_native_executor(_executor(max_cost=20))


def test_validation_passes_what_a_backend_can_serve(registry: Registry) -> None:
    """
    Given: Redis-like capabilities and a composed sliding-log and token-bucket request.
    When:  It is validated for async use.
    Then:  Nothing is raised; no connection was needed.
    """
    constraints = [
        _constraint(SlidingLogPolicy(10, ONE_SECOND), scope="ankh"),
        _constraint(TokenBucketPolicy(10, 1, ONE_SECOND), label="bucket", scope="ankh.orders"),
    ]

    registry.validate(REDIS_LIKE, constraints, mode=Mode.ASYNC)


def test_validation_lists_every_shortfall_at_once(registry: Registry) -> None:
    """
    Given: Memcached-like capabilities and a composed sliding-log request with cooldowns.
    When:  It is validated for async use.
    Then:  One UnsupportedCapability lists the missing mode, algorithm, state shape,
           composition, cooldowns, and unaccepted best-effort durability.
    """
    constraints = [
        _constraint(SlidingLogPolicy(10, ONE_SECOND), scope="ankh"),
        _constraint(SlidingLogPolicy(10, ONE_SECOND), label="other", scope="ankh.orders"),
    ]

    with pytest.raises(UnsupportedCapability) as raised:
        registry.validate(MEMCACHED_LIKE, constraints, mode=Mode.ASYNC, cooldowns=True)

    message = str(raised.value)
    for fragment in (
        "asynchronous",
        "sliding_log",
        "event_log",
        "atomically",
        "cooldowns",
        "best-effort",
    ):
        assert fragment in message, fragment


def test_best_effort_must_be_accepted_explicitly(registry: Registry) -> None:
    """
    Given: Memcached-like capabilities and a token-bucket request.
    When:  It is validated without and with accepting best-effort durability.
    Then:  Refused, then accepted (Y4).
    """
    constraints = [_constraint(TokenBucketPolicy(10, 1, ONE_SECOND))]

    with pytest.raises(UnsupportedCapability, match="best-effort"):
        registry.validate(MEMCACHED_LIKE, constraints, mode=Mode.SYNC)
    registry.validate(MEMCACHED_LIKE, constraints, mode=Mode.SYNC, accept_best_effort=True)


def test_validation_needs_registered_algorithms_and_agreeing_constraints(
    registry: Registry,
) -> None:
    """
    Given: An unregistered algorithm; then one rule under two policies; then nothing.
    When:  Each is validated.
    Then:  UnsupportedCapability, PolicyConflict, and InvalidPolicy respectively.
    """
    with pytest.raises(UnsupportedCapability, match="not registered"):
        registry.validate(
            REDIS_LIKE, [_constraint(FixedWindowPolicy(1, ONE_SECOND))], mode=Mode.SYNC
        )
    with pytest.raises(PolicyConflict):
        registry.validate(
            REDIS_LIKE,
            [
                _constraint(SlidingLogPolicy(1, ONE_SECOND)),
                _constraint(SlidingLogPolicy(2, ONE_SECOND)),
            ],
            mode=Mode.SYNC,
        )
    with pytest.raises(InvalidPolicy):
        registry.validate(REDIS_LIKE, [], mode=Mode.SYNC)


def test_coordination_reach_is_ordered() -> None:
    """
    Given: An in-process backend.
    When:  A caller requires local-machine coordination, then in-process.
    Then:  The first is unmet and the second met (Y3).
    """
    in_process = Capabilities(
        algorithms=frozenset({Algorithms.SLIDING_LOG}),
        coordination=CoordinationScope.IN_PROCESS,
        durability=Durability.EPHEMERAL,
    )

    assert unmet_requirements(
        in_process, CapabilityRequirement(coordination=CoordinationScope.LOCAL_MACHINE)
    )
    assert unmet_requirements(in_process, CapabilityRequirement()) == tuple()


def test_a_backend_is_named_in_its_refusal() -> None:
    """
    Given: A backend identity and capabilities it lacks.
    When:  The requirement is enforced.
    Then:  The error names the backend.
    """
    identity = BackendIdentity("memcached", "cache:11211", "discworld")

    with pytest.raises(UnsupportedCapability, match="memcached://cache:11211#discworld"):
        require_capabilities(MEMCACHED_LIKE, CapabilityRequirement(), backend=identity)


@pytest.mark.parametrize("rules", [0, True, 1.5], ids=["zero", "bool", "float"])
def test_a_requirement_composes_at_least_one_rule(rules: object) -> None:
    """
    Given: A nonsense rule count.
    When:  A requirement is built.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy, match="at least one rule"):
        CapabilityRequirement(rules=rules)  # ty: ignore[invalid-argument-type]


def test_composition_needs_one_authority_whatever_the_namespaces() -> None:
    """
    Given: Two handles on one store with different namespaces, and one on another store.
    When:  Authorities are compared.
    Then:  The first pair may compose; adding the stranger is refused (C6).
    """
    orders = BackendIdentity("redis", "cache:6379/0", "orders")
    billing = BackendIdentity("redis", "cache:6379/0", "billing")
    elsewhere = BackendIdentity("redis", "other:6379/0", "orders")
    expected = orders

    actual = require_same_authority([orders, billing])

    assert actual == expected
    with pytest.raises(UnsupportedCapability, match="separate backends"):
        require_same_authority([orders, billing, elsewhere])
    with pytest.raises(UnsupportedCapability, match="at least one"):
        require_same_authority([])


if __name__ == "__main__":
    pass
else:
    pass
