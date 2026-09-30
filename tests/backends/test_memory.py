"""The atomic memory backend: conformance, cooldowns, inspection, migration, and lifecycle.

Concurrency across threads and tasks lives in ``tests/concurrency``; this file
covers what one caller at a time can observe.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import random
import threading
from typing import Never

import pytest

import procrastinators.backends.memory as memory_module
from procrastinators.algorithms import SlidingLog
from procrastinators.backends.memory import (
    AsyncMemoryBackend,
    MemoryBackend,
    MemoryStore,
    memory_backend,
)
from procrastinators.capabilities import Mode
from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    InvalidPolicy,
    PolicyConflict,
    UnsupportedCapability,
)
from procrastinators.keys import policy_fingerprint
from procrastinators.limiter import RateLimiter
from procrastinators.models import (
    AdmissionRequest,
    Constraint,
    CoordinationScope,
    Durability,
    DurationMicros,
    EpochMicros,
    Limit,
    OperationBudget,
    QuotaIdentity,
    SlidingLogPolicy,
)
from procrastinators.protocols import (
    AsyncBackend,
    MigrationStatus,
    SupportsAsyncCooldown,
    SupportsAsyncPolicyAdministration,
    SupportsCooldown,
    SupportsPolicyAdministration,
    SyncBackend,
)
from procrastinators.state import UNUSED
from procrastinators.testing import (
    AsyncBackendCase,
    BackendCase,
    FakeTimeline,
    check_async_backend,
    check_backend,
)
from tests.backends.conftest import SECOND, constraint
from tests.doubles import ClacksAlgorithm, ClacksPolicy

# The conformance suite builds a fresh store per check.


def _sync_case(timeline: FakeTimeline, observer: object) -> MemoryBackend:
    handle = MemoryBackend(
        MemoryStore(clock=timeline.epoch_clock),
        observer=observer,  # ty: ignore[invalid-argument-type]
    )
    return handle


def _async_case(timeline: FakeTimeline, observer: object) -> AsyncMemoryBackend:
    handle = AsyncMemoryBackend(
        MemoryStore(clock=timeline.epoch_clock),
        observer=observer,  # ty: ignore[invalid-argument-type]
    )
    return handle


def test_the_memory_backend_passes_the_whole_conformance_suite() -> None:
    """
    Given: A synchronous memory handle claiming every guarantee.
    When:  The conformance suite runs every trace, scenario, failure, and lifecycle check.
    Then:  Every check passes and none is skipped.
    """
    report = check_backend(BackendCase("memory", _sync_case))

    report.raise_for_failures(allow_skips=False)


def test_the_async_memory_backend_passes_the_whole_conformance_suite() -> None:
    """
    Given: An asynchronous memory handle claiming every guarantee.
    When:  The asynchronous conformance suite runs.
    Then:  Every check passes and none is skipped.
    """
    report = asyncio.run(check_async_backend(AsyncBackendCase("async memory", _async_case)))

    report.raise_for_failures(allow_skips=False)


def test_handles_declare_honest_capabilities(
    backend: MemoryBackend, async_backend: AsyncMemoryBackend
) -> None:
    """
    Given: A synchronous and an asynchronous handle on one store.
    When:  Their capabilities and protocols are read.
    Then:  Each offers exactly its own mode, both are in-process and ephemeral (Y3,
           Y4), and both compose, cool down, and administer policies.
    """
    sync, async_ = backend.capabilities, async_backend.capabilities

    assert (sync.supports_sync, sync.supports_async) == (True, False)
    assert (async_.supports_sync, async_.supports_async) == (False, True)
    assert sync.coordination is CoordinationScope.IN_PROCESS
    assert sync.durability is Durability.EPHEMERAL
    assert sync.supports_composition
    assert sync.supports_cooldowns
    assert sync.supports_policy_administration
    assert isinstance(backend, SyncBackend)
    assert isinstance(backend, SupportsCooldown)
    assert isinstance(backend, SupportsPolicyAdministration)
    assert isinstance(async_backend, AsyncBackend)
    assert isinstance(async_backend, SupportsAsyncCooldown)
    assert isinstance(async_backend, SupportsAsyncPolicyAdministration)
    assert backend.identity == async_backend.identity


def test_handles_on_one_store_share_its_quota(
    store: MemoryStore, one_request: AdmissionRequest
) -> None:
    """
    Given: Two independently constructed handles on one store.
    When:  Each admits once against a rule of two per second, then one tries again.
    Then:  The third attempt is denied: the store, not the handle, holds the quota.
    """
    first, second = MemoryBackend(store), MemoryBackend(store)

    verdicts = [
        first.admit(one_request).allowed,
        second.admit(one_request).allowed,
        first.admit(one_request).allowed,
    ]

    assert verdicts == [True, True, False]


def test_closing_a_handle_keeps_the_store_and_its_state(
    store: MemoryStore, one_request: AdmissionRequest
) -> None:
    """
    Given: A handle that admitted once, then was closed twice.
    When:  It is used again, and another handle on the store admits.
    Then:  The closed handle raises ``ClosedResource`` (L5); the other handle still
           sees the consumed quota, because closing never deletes state (L1, L3).
    """
    closing, surviving = MemoryBackend(store), MemoryBackend(store)
    closing.admit(one_request)
    closing.close()
    closing.close()

    with pytest.raises(ClosedResource):
        closing.admit(one_request)
    assert surviving.admit(one_request).allowed
    assert not surviving.admit(one_request).allowed


def test_named_stores_are_shared_per_name_and_process() -> None:
    """
    Given: The process-wide stores behind ``memory://`` addresses.
    When:  The same and different names are resolved.
    Then:  One name is one store; different names are different stores.
    """
    assert MemoryStore.named("treacle-mine") is MemoryStore.named("treacle-mine")
    assert MemoryStore.named("treacle-mine") is not MemoryStore.named("cable-street")


def test_separate_stores_with_the_same_name_cannot_be_combined() -> None:
    """
    Given: Two separately created memory stores bearing the same display name.
    When:  Limiters on them are combined.
    Then:  The operation is refused before either quota could be bypassed.
    """
    first = RateLimiter(
        key="a", limits=[Limit(1, per="1h")], backend=MemoryBackend(MemoryStore(name="same"))
    )
    second = RateLimiter(
        key="b", limits=[Limit(1, per="1h")], backend=MemoryBackend(MemoryStore(name="same"))
    )

    with pytest.raises(UnsupportedCapability):
        RateLimiter.combine(first, second)


def test_admission_reads_only_the_requested_cooldown_scopes(timeline: FakeTimeline) -> None:
    """
    Given: A store with many unrelated cooldowns.
    When:  One rule is admitted.
    Then:  Admission does not enumerate every cooldown in the shared lock.
    """
    store = MemoryStore(clock=timeline.epoch_clock)
    handle = MemoryBackend(store)
    rule = constraint("target", "burst", SlidingLogPolicy(1, SECOND))
    for index in range(500):
        handle.defer_for(QuotaIdentity("discworld", f"other-{index}"), SECOND)

    class NoItemsCooldowns(dict):
        def items(self) -> Never:
            raise AssertionError("admission scanned unrelated cooldowns")

    store._cooldowns = NoItemsCooldowns(store._cooldowns)

    assert handle.admit(AdmissionRequest((rule,))).allowed


def test_automatic_memory_sweep_checks_a_bounded_number_of_rules(
    timeline: FakeTimeline, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: More than 128 expired rule states and a sweep due on the next commit.
    When:  One more request is admitted.
    Then:  The sweep reclaims at most 128 states while holding the admission lock.
    """
    store = MemoryStore(clock=timeline.epoch_clock, sweep_every=301)
    handle = MemoryBackend(store)
    rules = [
        constraint("ankh", f"rule-{index}", SlidingLogPolicy(1, SECOND)) for index in range(301)
    ]
    for rule in rules[:300]:
        handle.admit(AdmissionRequest((rule,)))

    timeline.advance(SECOND)
    checked = 0
    original = store._state_expiry.discard

    def count(rule: object) -> None:
        nonlocal checked
        checked += 1
        original(rule)  # ty: ignore[invalid-argument-type]

    monkeypatch.setattr(store._state_expiry, "discard", count)
    handle.admit(AdmissionRequest((rules[-1],)))

    assert checked == 128
    assert store.active_rules == 173


def test_expiry_index_updates_and_removes_rules_in_deadline_order() -> None:
    """
    Given: Three expiries whose deadlines are later changed or removed.
    When:  Due entries are read.
    Then:  The index follows their current deadlines without stale entries.
    """
    index = memory_module._ExpiryIndex[str]()
    index.set("late", EpochMicros(30))
    index.set("early", EpochMicros(20))
    index.set("middle", EpochMicros(25))
    index.set("late", EpochMicros(10))

    assert index.first_due(EpochMicros(15)) == "late"
    index.discard("late")
    assert index.first_due(EpochMicros(15)) is None
    index.set("early", EpochMicros(40))
    assert index.first_due(EpochMicros(25)) == "middle"


def test_expiry_index_tracks_current_deadlines_through_many_updates() -> None:
    """
    Given: Repeated additions, deadline changes, and removals of the same keys.
    When:  The next due key is requested after each change.
    Then:  It matches the earliest deadline still in force.
    """
    random_source = random.Random(7)
    index = memory_module._ExpiryIndex[str]()
    current: dict[str, EpochMicros] = dict()
    for step in range(500):
        key = f"rule-{random_source.randrange(20)}"
        if random_source.randrange(4) == 0:
            index.discard(key)
            current.pop(key, None)
        else:
            horizon = EpochMicros(random_source.randrange(1000) * 1000 + step + 1)
            index.set(key, horizon)
            current[key] = horizon
        if current:
            expected = min(current, key=current.__getitem__)
            actual = index.first_due(current[expected])
            assert actual == expected
        else:
            assert index.first_due(EpochMicros(1_000_000)) is None


def test_a_full_store_finds_an_expired_rule_after_many_active_rules(
    timeline: FakeTimeline,
) -> None:
    """
    Given: A full store with many long-lived rules and one short-lived rule.
    When:  The short rule expires and a new rule is admitted.
    Then:  Admission finds that space without scanning every active rule.
    """
    store = MemoryStore(clock=timeline.epoch_clock, max_rules=130, sweep_every=10_000)
    handle = MemoryBackend(store)
    long_policy = SlidingLogPolicy(1, DurationMicros(3600 * SECOND))
    for index in range(129):
        rule = constraint("ankh", f"long-{index}", long_policy)
        handle.admit(AdmissionRequest((rule,)))
    short = constraint("ankh", "short", SlidingLogPolicy(1, SECOND))
    handle.admit(AdmissionRequest((short,)))
    timeline.advance(SECOND)
    incoming = constraint("ankh", "incoming", SlidingLogPolicy(1, SECOND))

    decision = handle.admit(AdmissionRequest((incoming,)))

    assert decision.allowed
    assert store.active_rules == 130


@pytest.mark.parametrize(
    ("address", "store"),
    [("memory://", "default"), ("memory://ankh", "ankh"), ("memory:///ankh", "ankh")],
)
def test_memory_addresses_name_their_store(address: str, store: str) -> None:
    """
    Given: A ``memory`` address.
    When:  It is resolved for both modes.
    Then:  Both handles address the named process-wide store.
    """
    expected = MemoryStore.named(store).authority

    sync = memory_backend(address, mode=Mode.SYNC)
    async_ = memory_backend(address, mode=Mode.ASYNC)

    assert (sync.identity.authority, async_.identity.authority) == (expected, expected)
    assert isinstance(sync, MemoryBackend)
    assert isinstance(async_, AsyncMemoryBackend)


@pytest.mark.parametrize(
    "address",
    ["sqlite:///x", "memory://user:pw@ankh", "memory://ankh?size=3", "memory://ankh:9"],
)
def test_malformed_memory_addresses_are_rejected(address: str) -> None:
    """
    Given: An address that is not a plain ``memory://<name>``.
    When:  It is resolved.
    Then:  ``ConfigurationError`` is raised rather than guessing at a store.
    """
    with pytest.raises(ConfigurationError):
        memory_backend(address, mode=Mode.SYNC)


def test_hosting_a_different_implementation_under_a_hosted_id_is_refused(
    store: MemoryStore,
) -> None:
    """
    Given: A store hosting the reference sliding log.
    When:  Another sliding log instance, then a different class under the same id, is hosted.
    Then:  The same class is accepted and the impostor is refused.
    """
    store.host([SlidingLog()])

    class Impostor(ClacksAlgorithm):
        id = "sliding_log"

    with pytest.raises(ConfigurationError, match="already hosted"):
        store.host([Impostor()])


def test_a_third_party_algorithm_runs_once_hosted(store: MemoryStore) -> None:
    """
    Given: A store hosting a third-party algorithm with its own policy type.
    When:  A request uses that policy.
    Then:  It is admitted and then denied like any built-in (Y7).
    """
    store.host([ClacksAlgorithm()])
    handle = MemoryBackend(store)
    policy = ClacksPolicy(1, SECOND)
    request = AdmissionRequest((constraint("semaphore", "tower", policy),))

    assert [handle.admit(request).allowed, handle.admit(request).allowed] == [True, False]


def test_policy_metadata_outlives_forgotten_state(
    backend: MemoryBackend, store: MemoryStore, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule admitted once, then left until its state is past its horizon and swept.
    When:  The same rule is presented under a different policy.
    Then:  Its state is gone but its fingerprint is not, so the change is a conflict
           rather than fresh quota (L10, I4).
    """
    backend.admit(AdmissionRequest((two_per_second,)))
    timeline.advance(2 * SECOND)
    forgotten = store.sweep()
    changed = Constraint(
        two_per_second.rule,
        SlidingLogPolicy(5, SECOND),
        policy_fingerprint(SlidingLogPolicy(5, SECOND)),
    )

    assert (forgotten, store.state_of(two_per_second.rule)) == (1, UNUSED)
    with pytest.raises(PolicyConflict):
        backend.admit(AdmissionRequest((changed,)))


def test_a_failed_composed_attempt_debits_no_rule(
    backend: MemoryBackend,
    store: MemoryStore,
    two_per_second: Constraint,
    ten_per_minute: Constraint,
) -> None:
    """
    Given: A burst rule and a sustained rule, with the burst rule exhausted.
    When:  Both are requested together.
    Then:  The request is denied by the burst rule alone, and the sustained rule's
           state is exactly what it was before (A6).
    """
    both = AdmissionRequest((two_per_second, ten_per_minute))
    backend.admit(both)
    backend.admit(both)
    before = store.state_of(ten_per_minute.rule)

    decision = backend.admit(both)

    assert decision.blocking == (two_per_second.rule,)
    assert store.state_of(ten_per_minute.rule) == before


def test_authority_time_never_runs_backwards(
    backend: MemoryBackend, timeline: FakeTimeline, one_request: AdmissionRequest
) -> None:
    """
    Given: An injected clock that steps back five seconds between two admissions.
    When:  The second admission is stamped.
    Then:  Its timestamp is clamped to the first: authority time never regresses (T7).
    """
    first = backend.admit(one_request).admission
    timeline.step_epoch(-5 * SECOND)
    second = backend.admit(one_request).admission

    assert first is not None
    assert second is not None
    assert second.admitted_at == first.admitted_at


def test_a_cooldown_holds_every_rule_of_its_scope(
    backend: MemoryBackend,
    store: MemoryStore,
    timeline: FakeTimeline,
    two_per_second: Constraint,
    ten_per_minute: Constraint,
    empty_bucket: Constraint,
) -> None:
    """
    Given: A three-second cooldown on the ``ankh`` scope.
    When:  Its rules, composed with a rule of another scope, are requested one second later.
    Then:  The request is denied by the cooled rules for the two seconds left, and
           nothing is debited; the other scope alone is unaffected (K1, K3, K4).
    """
    backend.defer_for(two_per_second.rule.scope, DurationMicros(3 * SECOND), reason="429")
    timeline.advance(SECOND)
    request = AdmissionRequest((two_per_second, ten_per_minute))

    decision = backend.admit(request)

    assert set(decision.blocking) == {two_per_second.rule, ten_per_minute.rule}
    assert decision.retry_after_us == 2 * SECOND
    assert store.state_of(two_per_second.rule) == UNUSED
    assert backend.admit(AdmissionRequest((empty_bucket,))).blocking == (empty_bucket.rule,)
    assert backend.admit(AdmissionRequest((empty_bucket,))).retry_after_us == SECOND


def test_a_later_shorter_cooldown_does_not_shorten_one_in_force(
    backend: MemoryBackend, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A ten-second cooldown on a scope.
    When:  A two-second cooldown is applied, then a twelve-second one a second later.
    Then:  The first keeps the ten-second end and its reason; the second extends it (K2).
    """
    scope = two_per_second.rule.scope
    long = backend.defer_for(scope, DurationMicros(10 * SECOND), reason="vendor")
    short = backend.defer_for(scope, DurationMicros(2 * SECOND), reason="late")
    timeline.advance(SECOND)
    longer = backend.defer_for(scope, DurationMicros(12 * SECOND), reason="again")

    assert short == long
    assert longer.until == long.until + 3 * SECOND
    assert longer.reason == "again"


def test_a_cooldown_expires_and_admission_resumes(
    backend: MemoryBackend, timeline: FakeTimeline, one_request: AdmissionRequest
) -> None:
    """
    Given: A one-second cooldown on a fresh rule's scope.
    When:  An attempt arrives exactly when it ends.
    Then:  It is admitted: a cooldown ends at its ``until`` instant.
    """
    backend.defer_for(one_request.constraints[0].rule.scope, DurationMicros(SECOND))
    timeline.advance(SECOND)

    assert backend.admit(one_request).allowed


@pytest.mark.parametrize("duration", [-1, True, 1.5])
def test_malformed_cooldown_durations_are_rejected(
    backend: MemoryBackend, two_per_second: Constraint, duration: object
) -> None:
    """
    Given: A negative, boolean, or fractional cooldown duration.
    When:  It is applied.
    Then:  ``InvalidPolicy`` is raised.
    """
    with pytest.raises(InvalidPolicy):
        backend.defer_for(two_per_second.rule.scope, duration)  # ty: ignore[invalid-argument-type]


def test_inspection_is_advisory_and_consumes_nothing(
    backend: MemoryBackend,
    timeline: FakeTimeline,
    two_per_second: Constraint,
    empty_bucket: Constraint,
) -> None:
    """
    Given: A two-per-second rule admitted once, a cooled scope, and a rule never used.
    When:  They are inspected, twice.
    Then:  The used rule reports one unit left and when its state resets, the cooled
           scope reports its end, the unused rule reports ``unused``, and inspecting
           left the remaining unit admissible (R7).
    """
    backend.admit(AdmissionRequest((two_per_second,)))
    cooldown = backend.defer_for(empty_bucket.rule.scope, DurationMicros(SECOND))
    timeline.advance(250_000)

    backend.inspect([two_per_second.rule, empty_bucket.rule])
    snapshot = backend.inspect([two_per_second.rule, empty_bucket.rule])
    used, unused = snapshot.rules

    assert (used.remaining, used.reset_after_us, used.cooldown_until) == (1, 750_000, None)
    assert (unused.algorithm, unused.cooldown_until) == ("unused", cooldown.until)
    assert snapshot.advisory
    assert backend.admit(AdmissionRequest((two_per_second,))).allowed


def test_a_migration_drains_then_installs_the_new_policy(
    backend: MemoryBackend, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule in use under two per second, to be migrated to five per second.
    When:  The migration begins, time passes the old state's horizon, and it completes.
    Then:  Admissions stop with a conflict while draining (L12); the rule becomes
           ready once neutral; after completion the new policy admits with fresh state
           and the old one conflicts.
    """
    new_policy = SlidingLogPolicy(5, SECOND)
    migrated = Constraint(two_per_second.rule, new_policy, policy_fingerprint(new_policy))
    backend.admit(AdmissionRequest((two_per_second,)))
    drains_at = timeline.peek_epoch() + SECOND

    begun = backend.begin_migration(
        two_per_second.rule, to_fingerprint=migrated.fingerprint, to_state_version=1
    )
    with pytest.raises(PolicyConflict, match="migrated"):
        backend.admit(AdmissionRequest((two_per_second,)))
    timeline.advance(SECOND)
    ready = backend.migration_status(two_per_second.rule)
    completed = backend.complete_migration(two_per_second.rule)

    assert begun.status is MigrationStatus.DRAINING
    assert begun.drained_after == drains_at
    assert ready is not None
    assert ready.status is MigrationStatus.READY
    assert completed.status is MigrationStatus.COMPLETE
    assert [backend.admit(AdmissionRequest((migrated,))).allowed for _ in range(6)] == [
        True,
        True,
        True,
        True,
        True,
        False,
    ]
    stored = backend.stored_policy(two_per_second.rule)
    assert stored is not None
    assert stored.fingerprint == migrated.fingerprint
    with pytest.raises(PolicyConflict):
        backend.admit(AdmissionRequest((two_per_second,)))


def test_a_migration_cannot_complete_before_the_state_drains(
    backend: MemoryBackend, two_per_second: Constraint
) -> None:
    """
    Given: A rule with live quota history and a migration just begun.
    When:  Completion is attempted immediately, or a second target is proposed.
    Then:  Both raise ``PolicyConflict``: active history is never deleted early.
    """
    backend.admit(AdmissionRequest((two_per_second,)))
    backend.begin_migration(two_per_second.rule, to_fingerprint="p1-new", to_state_version=1)  # ty: ignore[invalid-argument-type]

    with pytest.raises(PolicyConflict, match="draining"):
        backend.complete_migration(two_per_second.rule)
    with pytest.raises(PolicyConflict, match="another policy"):
        backend.begin_migration(two_per_second.rule, to_fingerprint="p1-other", to_state_version=1)  # ty: ignore[invalid-argument-type]


def test_a_bucket_that_never_becomes_neutral_cannot_be_migrated_live(
    backend: MemoryBackend, timeline: FakeTimeline, empty_bucket: Constraint
) -> None:
    """
    Given: A token bucket configured to start empty, which is never forgettable (P5).
    When:  A migration begins and a long time passes.
    Then:  It stays draining, with no known drain time, and completion is refused.
    """
    backend.admit(AdmissionRequest((empty_bucket,)))
    backend.begin_migration(empty_bucket.rule, to_fingerprint="p1-new", to_state_version=1)  # ty: ignore[invalid-argument-type]
    timeline.advance(3600 * SECOND)

    status = backend.migration_status(empty_bucket.rule)

    assert status is not None
    assert (status.status, status.drained_after) == (MigrationStatus.DRAINING, None)
    with pytest.raises(PolicyConflict):
        backend.complete_migration(empty_bucket.rule)


def test_an_unused_rule_is_ready_to_migrate_at_once(
    backend: MemoryBackend, two_per_second: Constraint
) -> None:
    """
    Given: A rule the store has never seen.
    When:  A migration begins.
    Then:  It is ready immediately, having nothing to drain.
    """
    migration = backend.begin_migration(
        two_per_second.rule, to_fingerprint=two_per_second.fingerprint, to_state_version=1
    )

    assert (migration.status, migration.from_fingerprint) == (MigrationStatus.READY, None)


def test_a_full_store_refuses_new_state_rather_than_evict_active_rules(
    timeline: FakeTimeline,
) -> None:
    """
    Given: A store limited to two rules, both holding live quota.
    When:  A third rule is admitted, then time passes their horizons and it tries again.
    Then:  The third is refused with ``BackendUnavailable`` while the others keep their
           state (L11); once theirs is forgettable, a sweep makes room and it is admitted.
    """
    store = MemoryStore(clock=timeline.epoch_clock, max_rules=2)
    handle = MemoryBackend(store)
    rules = [constraint("ankh", f"rule-{index}", SlidingLogPolicy(1, SECOND)) for index in range(3)]
    for rule in rules[:2]:
        handle.admit(AdmissionRequest((rule,)))

    with pytest.raises(BackendUnavailable, match="refuses new state"):
        handle.admit(AdmissionRequest((rules[2],)))
    assert store.active_rules == 2
    timeline.advance(SECOND)
    assert handle.admit(AdmissionRequest((rules[2],))).allowed
    assert store.active_rules == 1


def test_a_forked_copy_of_a_store_refuses_to_serve(
    backend: MemoryBackend, one_request: AdmissionRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: A store whose process id no longer matches, as after ``fork``.
    When:  A handle admits, and the named store is resolved.
    Then:  The copy raises ``BackendUnavailable`` rather than coordinating with nobody
           (L8), and the child resolves a fresh store of its own.
    """
    parent = MemoryStore.named("wizards")
    real_pid = memory_module.os.getpid()
    monkeypatch.setattr(memory_module.os, "getpid", lambda: real_pid + 1)

    with pytest.raises(BackendUnavailable, match="fork"):
        backend.admit(one_request)
    assert MemoryStore.named("wizards") is not parent


def test_contention_beyond_the_lock_budget_is_busy_not_denied(
    store: MemoryStore, backend: MemoryBackend, two_per_second: Constraint
) -> None:
    """
    Given: Another thread holding the store's lock.
    When:  A handle admits with a one-millisecond lock budget.
    Then:  ``BackendBusy`` is raised, never a denial (O2).
    """
    budget = OperationBudget(lock_timeout_us=DurationMicros(1_000))
    store.acquire(1_000)
    try:
        with pytest.raises(BackendBusy):
            backend.admit(AdmissionRequest((two_per_second,), budget=budget))
    finally:
        store.release()


def test_async_contention_yields_to_the_loop_and_then_gives_up(
    store: MemoryStore, async_backend: AsyncMemoryBackend, two_per_second: Constraint
) -> None:
    """
    Given: Another thread holding the store's lock for longer than the budget.
    When:  An async handle admits alongside a heartbeat task.
    Then:  The heartbeat keeps beating while the admission waits (W4), and the
           admission raises ``BackendBusy``.
    """
    budget = OperationBudget(lock_timeout_us=DurationMicros(50_000))
    request = AdmissionRequest((two_per_second,), budget=budget)
    holding, release = threading.Event(), threading.Event()

    def hold() -> None:
        store.acquire(1_000_000)
        holding.set()
        release.wait(5)
        store.release()

    async def scenario() -> int:
        beats = 0

        async def heartbeat() -> None:
            nonlocal beats
            while True:
                beats += 1
                await asyncio.sleep(0.001)

        beating = asyncio.create_task(heartbeat())
        try:
            with pytest.raises(BackendBusy):
                await async_backend.admit(request)
        finally:
            beating.cancel()
        return beats

    holder = threading.Thread(target=hold)
    holder.start()
    holding.wait(5)
    try:
        beats = asyncio.run(scenario())
    finally:
        release.set()
        holder.join(5)

    assert beats > 5


def test_cancelling_an_async_admission_waiting_for_the_lock_consumes_nothing(
    store: MemoryStore, async_backend: AsyncMemoryBackend, one_request: AdmissionRequest
) -> None:
    """
    Given: An async admission waiting for a lock held elsewhere.
    When:  It is cancelled, the lock is freed, and the rule is admitted twice.
    Then:  ``CancelledError`` propagated and both later admissions succeed (O5).
    """

    async def scenario() -> list[bool]:
        store.acquire(1_000)
        waiting = asyncio.create_task(async_backend.admit(one_request))
        await asyncio.sleep(0.005)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        store.release()
        verdicts = [(await async_backend.admit(one_request)).allowed for _ in range(2)]
        return verdicts

    assert asyncio.run(scenario()) == [True, True]


def test_invalid_store_bounds_are_rejected() -> None:
    """
    Given: A zero or boolean ``max_rules``.
    When:  A store is constructed.
    Then:  ``ConfigurationError`` is raised.
    """
    with pytest.raises(ConfigurationError):
        MemoryStore(max_rules=0)
    with pytest.raises(ConfigurationError):
        MemoryStore(sweep_every=True)


if __name__ == "__main__":
    pass
else:
    pass
