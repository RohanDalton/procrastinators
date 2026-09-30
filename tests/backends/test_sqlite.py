"""The SQLite backend: conformance, persistence, schema, failures, and lifecycle.

Separate processes racing on one file live in ``tests/concurrency``; this file
covers what one process can observe, with time on a fake timeline.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import itertools
import sqlite3
import threading
from typing import TYPE_CHECKING

import platformdirs
import pytest

import procrastinators.backends.sqlite as sqlite_module
from procrastinators.backends.memory import MemoryBackend
from procrastinators.backends.sqlite import (
    AsyncSQLiteBackend,
    SQLiteBackend,
    SQLiteStore,
    default_database_path,
    sqlite_backend,
)
from procrastinators.capabilities import Mode
from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ConfigurationError,
    IndeterminateAdmission,
    PolicyConflict,
    StateCorruption,
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
    Limit,
    OperationBudget,
    SlidingLogPolicy,
)
from procrastinators.protocols import (
    AsyncBackend,
    MigrationStatus,
    ObservationPoint,
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
from tests.backends.conftest import SECOND, DelayedCloseExecutor, constraint
from tests.limiters import ANKH, QUIRM

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from procrastinators.models import Decision
else:
    pass

GOOD_ADDRESSES: dict[str, str] = {
    "relative": "sqlite:///quota.sqlite3",
    "dotted-relative": "sqlite:///./nested/../quota.sqlite3",
    "percent-encoded": "sqlite:///quota%2Esqlite3",
}

BAD_ADDRESSES: dict[str, str] = {
    "another-family": "memory://quota",
    "host": "sqlite://ankh/quota.sqlite3",
    "query": "sqlite:///quota.sqlite3?mode=ro",
    "fragment": "sqlite:///quota.sqlite3#discworld",
    "no-file": "sqlite:///",
    "in-memory": "sqlite:///:memory:",
}


def _admit(backend: SyncBackend, *constraints: Constraint, cost: int = 1) -> Decision:
    decision = backend.admit(AdmissionRequest(constraints, cost))
    return decision


def test_cancelled_async_close_finishes_owned_executor_cleanup(
    sqlite_store: SQLiteStore,
    delayed_close_executor: DelayedCloseExecutor,
    event_loop_runner: asyncio.Runner,
) -> None:
    """
    Given: An async SQLite handle whose worker cleanup is waiting.
    When:  The caller awaiting close is cancelled.
    Then:  A later close waits for the original cleanup and the executor stops.
    """
    backend = AsyncSQLiteBackend(sqlite_store)
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


def _raw(path: Path) -> sqlite3.Connection:
    """A plain connection to the file, standing in for another process."""
    connection = sqlite3.connect(path, isolation_level=None)
    return connection


@pytest.fixture(params=list(GOOD_ADDRESSES))
def good_address(request: pytest.FixtureRequest) -> str:
    """An address naming ``quota.sqlite3`` relative to the current directory."""
    address = GOOD_ADDRESSES[request.param]
    return address


@pytest.fixture(params=list(BAD_ADDRESSES))
def bad_address(request: pytest.FixtureRequest) -> str:
    """An address the ``sqlite`` family must refuse."""
    address = BAD_ADDRESSES[request.param]
    return address


@pytest.fixture
def default_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The default database path, redirected into the test's temporary directory."""
    path = tmp_path / "state" / "procrastinators" / "limits.sqlite3"

    def redirected() -> Path:
        return path

    monkeypatch.setattr(sqlite_module, "default_database_path", redirected)
    return path


@pytest.fixture
def conformance_files(tmp_path: Path) -> Callable[[], Path]:
    """A fresh database path per conformance check."""
    counter = itertools.count()

    def fresh() -> Path:
        path = tmp_path / f"check-{next(counter)}.sqlite3"
        return path

    return fresh


def test_the_sqlite_backend_passes_the_whole_conformance_suite(
    conformance_files: Callable[[], Path],
) -> None:
    """
    Given: A synchronous SQLite handle on a fresh file per check, claiming every guarantee.
    When:  The conformance suite runs every trace, scenario, failure, and lifecycle check.
    Then:  Every check passes and none is skipped.
    """

    def build(timeline: FakeTimeline, observer: object) -> SQLiteBackend:
        handle = SQLiteBackend(
            SQLiteStore(conformance_files(), clock=timeline.epoch_clock),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = check_backend(BackendCase("sqlite", build))

    report.raise_for_failures(allow_skips=False)


def test_the_async_sqlite_backend_passes_the_whole_conformance_suite(
    conformance_files: Callable[[], Path],
) -> None:
    """
    Given: An asynchronous SQLite handle on a fresh file per check.
    When:  The asynchronous conformance suite runs, with every call on the executor.
    Then:  Every check passes and none is skipped, including cancellation raised
           inside a transaction.
    """

    def build(timeline: FakeTimeline, observer: object) -> AsyncSQLiteBackend:
        handle = AsyncSQLiteBackend(
            SQLiteStore(conformance_files(), clock=timeline.epoch_clock),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = asyncio.run(check_async_backend(AsyncBackendCase("async sqlite", build)))

    report.raise_for_failures(allow_skips=False)


def test_handles_declare_honest_capabilities(
    sqlite_backend: SQLiteBackend, async_sqlite_backend: AsyncSQLiteBackend
) -> None:
    """
    Given: A synchronous and an asynchronous handle on one file.
    When:  Their capabilities and protocols are read.
    Then:  Each offers exactly its own mode, both coordinate one machine and survive
           restarts (Y3, Y4), and both compose, cool down, and administer policies.
    """
    sync, async_ = sqlite_backend.capabilities, async_sqlite_backend.capabilities

    assert (sync.supports_sync, sync.supports_async) == (True, False)
    assert (async_.supports_sync, async_.supports_async) == (False, True)
    assert sync.coordination is CoordinationScope.LOCAL_MACHINE
    assert sync.durability is Durability.LOCAL_DURABLE
    assert sync.supports_composition
    assert sync.supports_cooldowns
    assert sync.supports_policy_administration
    assert isinstance(sqlite_backend, SyncBackend)
    assert isinstance(sqlite_backend, SupportsCooldown)
    assert isinstance(sqlite_backend, SupportsPolicyAdministration)
    assert isinstance(async_sqlite_backend, AsyncBackend)
    assert isinstance(async_sqlite_backend, SupportsAsyncCooldown)
    assert isinstance(async_sqlite_backend, SupportsAsyncPolicyAdministration)


def test_handles_on_one_file_share_an_authority(
    sqlite_backend: SQLiteBackend, async_sqlite_backend: AsyncSQLiteBackend, database: Path
) -> None:
    """
    Given: Two handles on one store.
    When:  Their identities are compared.
    Then:  They address the same authority, named by the file's absolute path.
    """
    expected = ("sqlite", str(database))
    actual = (sqlite_backend.identity.family, sqlite_backend.identity.authority)

    assert actual == expected
    assert sqlite_backend.identity.addresses_same_authority(async_sqlite_backend.identity)


def test_constructing_a_store_and_handle_touches_nothing(database: Path) -> None:
    """
    Given: A path where no database exists.
    When:  A store and a handle are constructed, then the handle admits once.
    Then:  Nothing exists on disk until the admission, which creates the file.
    """
    handle = SQLiteBackend(SQLiteStore(database))
    created_early = database.exists()

    decision = _admit(handle, constraint("ankh", "burst", SlidingLogPolicy(2, SECOND)))
    handle.close()

    assert not created_early
    assert decision.allowed
    assert database.is_file()


def test_a_relative_path_is_resolved_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Given: A store built from a relative path.
    When:  The process later changes directory.
    Then:  The store still names the file relative to where it was built.
    """
    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(tmp_path)
    store = SQLiteStore("quota.sqlite3")
    monkeypatch.chdir(tmp_path / "elsewhere")
    expected = tmp_path.resolve() / "quota.sqlite3"

    actual = store.path

    assert actual == expected


def test_the_default_database_is_a_stable_per_user_state_path() -> None:
    """
    Given: The default database location.
    When:  It is computed.
    Then:  It is ``limits.sqlite3`` in the PlatformDirs user state directory, not a
           temporary or cache location, and computing it creates nothing.
    """
    expected = platformdirs.user_state_path("procrastinators") / "limits.sqlite3"

    actual = default_database_path()

    assert actual == expected


def test_addresses_name_files_relative_to_the_current_directory(
    good_address: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: A ``sqlite:///`` address with a relative path.
    When:  A handle is made from it.
    Then:  The handle's store names that path under the current directory.
    """
    monkeypatch.chdir(tmp_path)
    expected = tmp_path.resolve() / "quota.sqlite3"

    handle = sqlite_backend(good_address, mode=Mode.SYNC)

    assert isinstance(handle, SQLiteBackend)
    assert handle.store.path == expected


def test_an_absolute_address_takes_four_slashes(tmp_path: Path) -> None:
    """
    Given: ``sqlite:////<absolute path>``.
    When:  An asynchronous handle is made from it.
    Then:  Its store names exactly that absolute path.
    """
    expected = tmp_path.resolve() / "absolute.sqlite3"

    handle = sqlite_backend(f"sqlite:///{expected}", mode=Mode.ASYNC)

    assert isinstance(handle, AsyncSQLiteBackend)
    assert handle.store.path == expected


def test_the_bare_address_names_the_default_database(default_database: Path) -> None:
    """
    Given: The address ``sqlite://``.
    When:  A handle is made from it.
    Then:  Its store names the default database.
    """
    handle = sqlite_backend("sqlite://", mode=Mode.SYNC)

    assert handle.store.path == default_database


def test_malformed_addresses_are_refused(bad_address: str) -> None:
    """
    Given: An address naming a host, a query, a fragment, no file, an in-memory
           database, or another family.
    When:  The ``sqlite`` factory is asked for a handle.
    Then:  It raises ConfigurationError instead of guessing.
    """
    with pytest.raises(ConfigurationError):
        sqlite_backend(bad_address, mode=Mode.SYNC)


def test_an_in_memory_database_is_refused_as_a_store() -> None:
    """
    Given: SQLite's in-memory database name.
    When:  A store is built on it.
    Then:  ConfigurationError points at the memory backend, since it coordinates nothing.
    """
    with pytest.raises(ConfigurationError, match="memory://"):
        SQLiteStore(":memory:")


def test_a_limiter_without_a_backend_uses_the_shared_default_database(
    default_database: Path,
) -> None:
    """
    Given: A limiter constructed with no backend.
    When:  Its backend and first admission are examined.
    Then:  It addresses the default SQLite file, which coordinates the machine
           rather than the process, and the first admission creates it and its
           directory.
    """
    limiter = RateLimiter(key=ANKH, limits=[Limit(2, per="1s")])
    memory = RateLimiter(key=ANKH, limits=[Limit(2, per="1s")], backend="memory://")

    admission = limiter.acquire(timeout=0)
    limiter.close()

    assert limiter.backend.family == "sqlite"
    assert limiter.backend.authority == str(default_database)
    assert admission.backend == limiter.backend
    assert default_database.is_file()
    assert memory.backend.family == "memory"


def test_limiters_on_one_file_share_its_quota(tmp_path: Path) -> None:
    """
    Given: Two separately constructed limiters naming one database file.
    When:  Each acquires until denied.
    Then:  Together they admit exactly the quota once.
    """
    address = f"sqlite:///{tmp_path / 'shared.sqlite3'}"
    first = RateLimiter(key=QUIRM, limits=[Limit(3, per="1h")], backend=address)
    second = RateLimiter(key=QUIRM, limits=[Limit(3, per="1h")], backend=address)
    expected = [True, True, True, False, False]

    actual = [limiter.try_acquire().allowed for limiter in (first, second, first, second, first)]
    first.close()
    second.close()

    assert actual == expected


def test_an_async_limiter_admits_through_the_executor(tmp_path: Path) -> None:
    """
    Given: A limiter on a database file, used from a coroutine.
    When:  It acquires twice and then tries a third time on a rule of two per hour.
    Then:  The first two are admitted and the third is denied, all without a
           synchronous handle being touched.
    """
    limiter = RateLimiter(
        key=ANKH, limits=[Limit(2, per="1h")], backend=f"sqlite:///{tmp_path / 'async.sqlite3'}"
    )

    async def scenario() -> list[bool]:
        async with limiter:
            pass
        await limiter.acquire_async()
        third = await limiter.try_acquire_async()
        await limiter.aclose()
        results = [True, True, third.allowed]
        return results

    expected = [True, True, False]
    actual = asyncio.run(scenario())

    assert actual == expected


def test_quota_and_policy_survive_a_restart(
    database: Path, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule used to capacity through one handle, which is then closed.
    When:  A new store and handle open the same file, as a restarted worker would.
    Then:  The quota is still spent, and a different policy for the rule is still
           a conflict (L12).
    """
    first = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))
    admitted = [_admit(first, two_per_second).allowed for _ in range(2)]
    first.close()
    restarted = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))
    changed = constraint("ankh", "burst", SlidingLogPolicy(5, SECOND))

    after_restart = _admit(restarted, two_per_second)
    with pytest.raises(PolicyConflict):
        _admit(restarted, changed)
    restarted.close()

    assert admitted == [True, True]
    assert not after_restart.allowed


def test_a_denied_first_attempt_records_initial_state(
    database: Path, timeline: FakeTimeline, empty_bucket: Constraint
) -> None:
    """
    Given: A token bucket that starts empty, denied on its first attempt.
    When:  A restarted worker tries again one refill period later.
    Then:  The refill clock started at the first attempt and survived the restart,
           so one token is available (P5).
    """
    first = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))
    denied = _admit(first, empty_bucket)
    first.close()
    timeline.advance(SECOND)
    restarted = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))

    later = _admit(restarted, empty_bucket)
    restarted.close()

    assert not denied.allowed
    assert later.allowed


def test_a_composed_denial_debits_no_rule(
    sqlite_backend: SQLiteBackend, two_per_second: Constraint
) -> None:
    """
    Given: A roomy rule composed with a rule of capacity one that is already spent.
    When:  The composed request is denied.
    Then:  The roomy rule was not debited: it still admits its full capacity alone (A6).
    """
    roomy = constraint("ankh", "roomy", SlidingLogPolicy(2, SECOND))
    tight = constraint("quirm", "tight", SlidingLogPolicy(1, SECOND))
    _admit(sqlite_backend, tight)

    composed = _admit(sqlite_backend, roomy, tight)
    alone = [_admit(sqlite_backend, roomy).allowed for _ in range(3)]

    assert not composed.allowed
    assert composed.blocking == (tight.rule,)
    assert alone == [True, True, False]


def test_weighted_admissions_store_one_event_each(
    sqlite_backend: SQLiteBackend, database: Path
) -> None:
    """
    Given: A sliding log of 100 per second.
    When:  Two admissions of cost 40 are made.
    Then:  The file holds two event rows, one per admission whatever its cost (P6).
    """
    heavy = constraint("ankh", "heavy", SlidingLogPolicy(100, SECOND))
    _admit(sqlite_backend, heavy, cost=40)
    _admit(sqlite_backend, heavy, cost=40)
    expected = [(40,), (40,)]

    with _raw(database) as connection:
        actual = connection.execute("SELECT cost FROM procrastinators_events").fetchall()

    assert actual == expected


def test_cooldowns_reach_every_handle_on_the_file(
    sqlite_backend: SQLiteBackend,
    database: Path,
    timeline: FakeTimeline,
    two_per_second: Constraint,
) -> None:
    """
    Given: A cooldown applied through one handle, then a shorter one.
    When:  Another store's handle on the same file admits.
    Then:  It is held for the longer cooldown (K2, K3), and admits once it ends.
    """
    scope = two_per_second.rule.scope
    first = sqlite_backend.defer_for(scope, DurationMicros(3 * SECOND), reason="429")
    second = sqlite_backend.defer_for(scope, DurationMicros(SECOND), reason="shorter")
    other = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))

    held = _admit(other, two_per_second)
    timeline.advance(3 * SECOND)
    released = _admit(other, two_per_second)
    other.close()

    assert second == first
    assert not held.allowed
    assert held.retry_after_us == 3 * SECOND
    assert released.allowed


def test_inspection_reports_what_this_process_and_the_file_know(
    sqlite_backend: SQLiteBackend,
    database: Path,
    timeline: FakeTimeline,
    two_per_second: Constraint,
    ten_per_minute: Constraint,
) -> None:
    """
    Given: One admission on a rule, and a second store on the file that has not admitted.
    When:  Both inspect that rule and a rule nobody has used.
    Then:  The admitting store reports the remainder; the other knows the algorithm
           but, holding no policy, cannot say what remains; the unused rule is
           ``unused`` (R7).
    """
    _admit(sqlite_backend, two_per_second)
    other = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))
    rules = (two_per_second.rule, ten_per_minute.rule)

    here = sqlite_backend.inspect(rules)
    there = other.inspect(rules)
    other.close()

    assert [(rule.algorithm, rule.remaining) for rule in here.rules] == [
        ("sliding_log", 1),
        ("unused", None),
    ]
    assert [(rule.algorithm, rule.remaining) for rule in there.rules] == [
        ("sliding_log", None),
        ("unused", None),
    ]
    assert here.rules[0].reset_after_us == SECOND


def test_a_migration_drains_before_installing_the_new_policy(
    sqlite_backend: SQLiteBackend, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule with live state, and a migration begun towards a new policy.
    When:  Admissions are attempted while it drains, then after it is complete.
    Then:  Draining refuses both policies, completion is refused until the state is
           neutral, and afterwards only the new policy is admitted (L12).
    """
    _admit(sqlite_backend, two_per_second)
    new = constraint("ankh", "burst", SlidingLogPolicy(5, SECOND))
    rule = two_per_second.rule

    begun = sqlite_backend.begin_migration(
        rule, to_fingerprint=new.fingerprint, to_state_version=new.state_version
    )
    with pytest.raises(PolicyConflict):
        _admit(sqlite_backend, new)
    with pytest.raises(PolicyConflict):
        sqlite_backend.complete_migration(rule)
    timeline.advance(SECOND)
    ready = sqlite_backend.migration_status(rule)
    completed = sqlite_backend.complete_migration(rule)
    with pytest.raises(PolicyConflict):
        _admit(sqlite_backend, two_per_second)
    admitted = _admit(sqlite_backend, new)

    assert begun.status is MigrationStatus.DRAINING
    assert ready is not None
    assert ready.status is MigrationStatus.READY
    assert completed.status is MigrationStatus.COMPLETE
    assert admitted.allowed
    stored = sqlite_backend.stored_policy(rule)
    assert stored is not None
    assert stored.fingerprint == new.fingerprint


def test_state_that_never_drains_cannot_be_migrated(
    sqlite_backend: SQLiteBackend, timeline: FakeTimeline, empty_bucket: Constraint
) -> None:
    """
    Given: A token bucket configured to start empty, whose state is never neutral.
    When:  A migration is begun and much later completion is attempted.
    Then:  Completion is refused, since forgetting the bucket would refill it (P5, E7).
    """
    _admit(sqlite_backend, empty_bucket)
    rule = empty_bucket.rule
    target = policy_fingerprint(SlidingLogPolicy(1, SECOND))
    sqlite_backend.begin_migration(rule, to_fingerprint=target, to_state_version=1)
    timeline.advance(3600 * SECOND)

    with pytest.raises(PolicyConflict, match="drain"):
        sqlite_backend.complete_migration(rule)


def test_sweeping_forgets_quota_state_but_never_policy(
    sqlite_backend: SQLiteBackend, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule with state and an expired cooldown on another scope.
    When:  Time passes beyond the rule's horizon and the handle sweeps.
    Then:  The state and cooldown are forgotten, the policy metadata is kept, and a
           conflicting policy is still refused (L9, L10).
    """
    _admit(sqlite_backend, two_per_second)
    elsewhere = constraint("quirm", "x", SlidingLogPolicy(1, SECOND)).rule.scope
    sqlite_backend.defer_for(elsewhere, DurationMicros(1))
    timeline.advance(2 * SECOND)

    forgotten = sqlite_backend.sweep()
    stored = sqlite_backend.stored_policy(two_per_second.rule)

    assert forgotten == 1
    assert stored is not None
    assert stored.fingerprint == two_per_second.fingerprint
    with pytest.raises(PolicyConflict):
        _admit(sqlite_backend, constraint("ankh", "burst", SlidingLogPolicy(3, SECOND)))


def test_racing_connections_initialize_a_fresh_file_once(
    database: Path, timeline: FakeTimeline
) -> None:
    """
    Given: Eight threads, each with its own store on one fresh file, released together.
    When:  Each admits once on a rule of five per hour.
    Then:  Every thread gets an answer, the schema is recorded once, and exactly five
           are admitted.
    """
    policy = constraint("ankh", "hourly", SlidingLogPolicy(5, DurationMicros(3600 * SECOND)))
    barrier = threading.Barrier(8)
    results: list[bool] = list()

    def work() -> None:
        handle = SQLiteBackend(SQLiteStore(database, clock=timeline.epoch_clock))
        barrier.wait(10)
        results.append(_admit(handle, policy).allowed)
        handle.close()

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    with _raw(database) as connection:
        versions = connection.execute(
            "SELECT value FROM procrastinators_meta WHERE name = 'schema_version'"
        ).fetchall()

    assert sorted(results) == [False] * 3 + [True] * 5
    assert versions == [(1,)]


def test_a_file_from_another_schema_version_is_refused(
    sqlite_backend: SQLiteBackend, database: Path, two_per_second: Constraint
) -> None:
    """
    Given: A file whose recorded schema version is not this library's.
    When:  A handle first connects to it.
    Then:  UnsupportedCapability refuses it rather than reinterpreting its tables.
    """
    with _raw(database) as connection:
        connection.execute("CREATE TABLE procrastinators_meta (name TEXT PRIMARY KEY, value)")
        connection.execute("INSERT INTO procrastinators_meta VALUES ('schema_version', 99)")

    with pytest.raises(UnsupportedCapability, match="version 99"):
        _admit(sqlite_backend, two_per_second)


def test_a_file_that_is_not_a_database_is_state_corruption(
    sqlite_backend: SQLiteBackend, database: Path, two_per_second: Constraint
) -> None:
    """
    Given: A file of garbage where the database should be.
    When:  A handle first connects to it.
    Then:  StateCorruption is raised; nothing treats it as an empty quota.
    """
    database.write_bytes(b"Hex says no. " * 100)

    with pytest.raises(StateCorruption):
        _admit(sqlite_backend, two_per_second)


def test_corrupt_stored_state_fails_closed(
    sqlite_backend: SQLiteBackend, database: Path, two_per_second: Constraint
) -> None:
    """
    Given: An admission whose event row is then damaged to an impossible cost.
    When:  The rule is admitted again.
    Then:  StateCorruption is raised rather than the damaged row being ignored.
    """
    _admit(sqlite_backend, two_per_second)
    with _raw(database) as connection:
        connection.execute("UPDATE procrastinators_events SET cost = 0")

    with pytest.raises(StateCorruption, match="event cost"):
        _admit(sqlite_backend, two_per_second)


def test_a_held_write_lock_is_contention_not_denial(
    sqlite_backend: SQLiteBackend, database: Path, two_per_second: Constraint
) -> None:
    """
    Given: Another connection holding the file's write lock.
    When:  A request with a 10 ms lock budget is admitted, and again after release.
    Then:  The first raises BackendBusy (O2) and consumed nothing: two admissions
           still fit afterwards.
    """
    _admit(sqlite_backend, constraint("quirm", "warm-up", SlidingLogPolicy(1, SECOND)))
    impatient = AdmissionRequest(
        (two_per_second,), budget=OperationBudget(lock_timeout_us=DurationMicros(10_000))
    )
    holder = _raw(database)
    holder.execute("BEGIN IMMEDIATE")

    with pytest.raises(BackendBusy):
        sqlite_backend.admit(impatient)
    holder.execute("ROLLBACK")
    holder.close()
    after = [_admit(sqlite_backend, two_per_second).allowed for _ in range(3)]

    assert after == [True, True, False]


def test_a_storage_call_past_its_timeout_commits_nothing(
    sqlite_backend: SQLiteBackend, two_per_second: Constraint, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: A storage budget of one microsecond, checked on every SQL step.
    When:  A request is admitted.
    Then:  The call is interrupted and reported unavailable (B3), and nothing was
           committed: two admissions still fit.
    """
    monkeypatch.setattr(sqlite_module, "_PROGRESS_STEPS", 1)
    rushed = AdmissionRequest(
        (two_per_second,), budget=OperationBudget(storage_timeout_us=DurationMicros(1))
    )

    with pytest.raises(BackendUnavailable, match="timeout"):
        sqlite_backend.admit(rushed)
    monkeypatch.setattr(sqlite_module, "_PROGRESS_STEPS", 1_000)
    after = [_admit(sqlite_backend, two_per_second).allowed for _ in range(3)]

    assert after == [True, True, False]


class _CommitFails:
    """A connection whose ``COMMIT`` fails, standing in for a lost disk or response."""

    def __init__(self, connection: sqlite3.Connection, error: sqlite3.Error) -> None:
        self._connection = connection
        self._error = error

    def execute(self, sql: str, *parameters: object) -> sqlite3.Cursor:
        if sql == "COMMIT":
            raise self._error
        else:
            pass
        cursor = self._connection.execute(sql, *parameters)  # ty: ignore[invalid-argument-type]
        return cursor

    def __getattr__(self, name: str) -> object:
        attribute = getattr(self._connection, name)
        return attribute


def _no_observer(point: ObservationPoint, request: AdmissionRequest) -> None:
    del point, request


def test_a_failed_commit_of_an_admission_is_indeterminate(
    sqlite_store: SQLiteStore, sqlite_backend: SQLiteBackend, two_per_second: Constraint
) -> None:
    """
    Given: A connection whose COMMIT fails with an I/O error.
    When:  An admission that would be allowed is committed through it.
    Then:  IndeterminateAdmission is raised, never a decision (O4).
    """
    connection = sqlite_store.connect()
    failing = _CommitFails(connection, sqlite3.OperationalError("disk I/O error"))

    with pytest.raises(IndeterminateAdmission):
        sqlite_store.admit(
            failing,  # ty: ignore[invalid-argument-type]
            AdmissionRequest((two_per_second,)),
            _no_observer,
            sqlite_backend.identity,
        )
    connection.close()


def test_a_busy_commit_is_contention(
    sqlite_store: SQLiteStore, sqlite_backend: SQLiteBackend, two_per_second: Constraint
) -> None:
    """
    Given: A connection whose COMMIT reports the database busy, which leaves the
           transaction uncommitted.
    When:  An admission is committed through it.
    Then:  BackendBusy is raised, and the rolled-back admission consumed nothing.
    """
    error = sqlite3.OperationalError("database is locked")
    error.sqlite_errorcode = 5
    connection = sqlite_store.connect()
    failing = _CommitFails(connection, error)

    with pytest.raises(BackendBusy):
        sqlite_store.admit(
            failing,  # ty: ignore[invalid-argument-type]
            AdmissionRequest((two_per_second,)),
            _no_observer,
            sqlite_backend.identity,
        )
    connection.close()
    after = [_admit(sqlite_backend, two_per_second).allowed for _ in range(3)]

    assert after == [True, True, False]


def test_closing_a_handle_leaves_the_file_to_others(
    database: Path, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: Two handles on one store, one of which admitted and is closed twice.
    When:  The other admits.
    Then:  Closing was idempotent and deleted nothing: the other sees the first's
           admission (L1, L3).
    """
    store = SQLiteStore(database, clock=timeline.epoch_clock)
    closing, staying = SQLiteBackend(store), SQLiteBackend(store)
    _admit(closing, two_per_second)
    closing.close()
    closing.close()

    remaining = [_admit(staying, two_per_second).allowed for _ in range(2)]
    staying.close()

    assert closing.closed
    assert remaining == [True, False]


def test_sqlite_and_memory_are_different_authorities(
    sqlite_backend: SQLiteBackend, backend: MemoryBackend
) -> None:
    """
    Given: A SQLite handle and a memory handle.
    When:  Their identities are compared.
    Then:  They are not the same authority, so they can never be composed (C6).
    """
    assert not sqlite_backend.identity.addresses_same_authority(backend.identity)


if __name__ == "__main__":
    pass
else:
    pass
