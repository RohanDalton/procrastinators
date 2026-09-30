"""The PostgreSQL backend: conformance, contention, persistence, failures, and lifecycle.

Service tests run against the server named by ``PROCRASTINATORS_POSTGRES_URL``,
each in a schema of its own that is dropped afterwards. Separate processes
racing on one database live in ``tests/concurrency``. Tests that need no
server — addresses, identities, capabilities — carry no service marker.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
import time
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from procrastinators.backends.postgres import (
    POSTGRES_CAPABILITIES,
    AsyncPostgresBackend,
    PostgresBackend,
    PostgresStore,
    postgres_backend,
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
from procrastinators.limiter import RateLimiter
from procrastinators.models import (
    AdmissionRequest,
    CoordinationScope,
    Durability,
    DurationMicros,
    FixedWindowPolicy,
    Limit,
    OperationBudget,
    Ownership,
    SlidingLogPolicy,
)
from procrastinators.protocols import (
    AsyncBackend,
    MigrationStatus,
    ObservationPoint,
    StateRepresentation,
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
from tests.backends.conftest import SECOND, constraint
from tests.postgres_service import DEFAULT_URL, statement

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from procrastinators.models import Constraint, Decision
else:
    pass


service = pytest.mark.service("postgres")

HOURLY_SINGLE = constraint("quirm", "single", SlidingLogPolicy(1, DurationMicros(3600 * SECOND)))

GOOD_ADDRESSES: dict[str, tuple[str, str]] = {
    "url": ("postgresql://ridcully:secret@unseen:6543/faculty", "unseen:6543/faculty"),
    "short-scheme": ("postgres://ridcully@unseen/faculty", "unseen:5432/faculty"),
    "default-host": ("postgresql:///faculty", "localhost:5432/faculty"),
    "query-host": ("postgresql:///faculty?host=unseen&port=7654", "unseen:7654/faculty"),
}

BAD_ADDRESSES: dict[str, str] = {
    "another-family": "redis://unseen/0",
    "fragment": "postgresql://unseen/faculty#discworld",
    "no-database": "postgresql://unseen",
    "bad-prefix": "postgresql://unseen/faculty?prefix=Bad-Prefix",
}


def _admit(backend: SyncBackend, *constraints: Constraint, cost: int = 1) -> Decision:
    decision = backend.admit(AdmissionRequest(constraints, cost))
    return decision


@pytest.fixture(params=list(GOOD_ADDRESSES))
def good_address(request: pytest.FixtureRequest) -> tuple[str, str]:
    """An address and the credential-free authority it names."""
    address = GOOD_ADDRESSES[request.param]
    return address


@pytest.fixture(params=list(BAD_ADDRESSES))
def bad_address(request: pytest.FixtureRequest) -> str:
    """An address the ``postgresql`` family must refuse."""
    address = BAD_ADDRESSES[request.param]
    return address


@pytest.fixture
def raw(postgres: str) -> Iterator[psycopg.Connection[Any]]:
    """A plain autocommit connection, standing in for an administrator or another process."""
    with psycopg.connect(postgres, autocommit=True) as connection:
        yield connection


@pytest.fixture
def pg_store(postgres: str, schema: str, timeline: FakeTimeline) -> PostgresStore:
    fresh = PostgresStore(postgres, schema=schema, clock=timeline.epoch_clock)
    return fresh


@pytest.fixture
def pg_backend(pg_store: PostgresStore) -> Iterator[PostgresBackend]:
    handle = PostgresBackend(pg_store, namespace="discworld")
    yield handle
    handle.close()


@pytest.fixture
def conformance_store(
    postgres: str, schema: str, raw: psycopg.Connection[Any]
) -> Callable[[FakeTimeline], PostgresStore]:
    """A store per conformance check on emptied tables, which is far cheaper than new ones.

    The schema version survives, so each store finds the tables already initialized.
    """

    def fresh(timeline: FakeTimeline) -> PostgresStore:
        tables = [
            row[0]
            for row in raw.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = %s AND tablename <> %s",
                (schema, "procrastinators_meta"),
            ).fetchall()
        ]
        if tables:
            listed = ", ".join(f'{schema}."{table}"' for table in tables)
            raw.execute(statement(f"TRUNCATE {listed} CASCADE"))
        else:
            pass
        store = PostgresStore(postgres, schema=schema, clock=timeline.epoch_clock)
        return store

    return fresh


def _waiting_for_a_lock(raw: psycopg.Connection[Any]) -> bool:
    (waiting,) = raw.execute("SELECT count(*) FROM pg_locks WHERE NOT granted").fetchone() or (0,)
    return bool(waiting)


def _until(condition: Callable[[], bool], *, within_s: float = 10.0) -> None:
    deadline = time.monotonic() + within_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("the condition never held")
        else:
            time.sleep(0.02)


#
# No server needed.


def test_handles_declare_honest_capabilities() -> None:
    """
    Given: Sync and async handles on a store that has never connected.
    When:  Their capabilities are read.
    Then:  Both reach every worker on the service, durably, composing, with cooldowns and
           administration, each in its own mode only.
    """
    store = PostgresStore(DEFAULT_URL)
    handles = (PostgresBackend(store), AsyncPostgresBackend(store))

    expected = [
        (CoordinationScope.SHARED_SERVICE, Durability.SERVICE_DURABLE, True, True, True, mode)
        for mode in ((True, False), (False, True))
    ]
    actual = [
        (
            handle.capabilities.coordination,
            handle.capabilities.durability,
            handle.capabilities.supports_composition,
            handle.capabilities.supports_cooldowns,
            handle.capabilities.supports_policy_administration,
            (handle.capabilities.supports_sync, handle.capabilities.supports_async),
        )
        for handle in handles
    ]
    assert actual == expected
    assert POSTGRES_CAPABILITIES.state_representations == frozenset(StateRepresentation)


def test_handles_satisfy_their_protocols() -> None:
    """
    Given: Sync and async handles.
    When:  They are checked against the backend, cooldown, and administration protocols.
    Then:  Each satisfies the protocols of its own mode.
    """
    store = PostgresStore(DEFAULT_URL)
    sync = PostgresBackend(store)
    async_ = AsyncPostgresBackend(store)

    expected = (True, True, True, True, True, True)
    actual = (
        isinstance(sync, SyncBackend),
        isinstance(sync, SupportsCooldown),
        isinstance(sync, SupportsPolicyAdministration),
        isinstance(async_, AsyncBackend),
        isinstance(async_, SupportsAsyncCooldown),
        isinstance(async_, SupportsAsyncPolicyAdministration),
    )
    assert actual == expected


def test_addresses_name_an_authority_without_credentials(good_address: tuple[str, str]) -> None:
    """
    Given: A postgresql address, perhaps with credentials.
    When:  The factory builds a handle for it.
    Then:  Its identity names host, port, database, and table prefix, and never a credential.
    """
    address, authority = good_address
    handle = postgres_backend(address, mode=Mode.SYNC, namespace="discworld")

    expected = ("postgresql", f"{authority}/procrastinators", "discworld")
    actual = (handle.identity.family, handle.identity.authority, handle.identity.namespace)
    assert actual == expected


def test_malformed_addresses_are_refused(bad_address: str) -> None:
    """
    Given: An address the family cannot serve.
    When:  The factory is asked for a handle.
    Then:  ConfigurationError is raised.
    """
    with pytest.raises(ConfigurationError):
        postgres_backend(bad_address, mode=Mode.SYNC)


def test_prefix_and_schema_options_reach_the_store() -> None:
    """
    Given: An address with prefix and schema options.
    When:  The factory builds an async handle.
    Then:  The identity shows them, so differently prefixed stores are different authorities.
    """
    handle = postgres_backend(
        "postgresql://unseen/faculty?prefix=quota&schema=etl&application_name=hex",
        mode=Mode.ASYNC,
    )

    expected = "unseen:5432/faculty/etl.quota"
    actual = handle.identity.authority
    assert actual == expected


def test_constructing_a_store_and_handle_connects_to_nothing() -> None:
    """
    Given: An address where nothing listens.
    When:  A store and both handles are constructed and closed.
    Then:  Nothing fails: no connection is attempted until first use.
    """
    store = PostgresStore("postgresql://localhost:1/nobody")
    PostgresBackend(store).close()
    asyncio.run(AsyncPostgresBackend(store).aclose())


def test_an_injected_connection_must_be_in_autocommit_mode() -> None:
    """
    Given: A connection-like object not in autocommit mode.
    When:  A handle is built to borrow it.
    Then:  ConfigurationError is raised: the backend runs its own transactions.
    """

    class Manual:
        autocommit = False

    with pytest.raises(ConfigurationError):
        PostgresBackend(PostgresStore(DEFAULT_URL), connection=Manual())  # ty: ignore[invalid-argument-type]


#
# Conformance.


@service
def test_the_postgres_backend_passes_the_whole_conformance_suite(
    conformance_store: Callable[[FakeTimeline], PostgresStore],
) -> None:
    """
    Given: A synchronous handle per check on emptied tables, claiming every guarantee.
    When:  The conformance suite runs every trace, scenario, failure, and lifecycle check.
    Then:  Every check passes and none is skipped.
    """

    def build(timeline: FakeTimeline, observer: object) -> PostgresBackend:
        handle = PostgresBackend(
            conformance_store(timeline),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = check_backend(BackendCase("postgres", build))

    report.raise_for_failures(allow_skips=False)


@service
def test_the_async_postgres_backend_passes_the_whole_conformance_suite(
    conformance_store: Callable[[FakeTimeline], PostgresStore],
) -> None:
    """
    Given: An asynchronous handle per check on emptied tables.
    When:  The asynchronous conformance suite runs on the native async driver.
    Then:  Every check passes and none is skipped, including cancellation inside a transaction.
    """

    def build(timeline: FakeTimeline, observer: object) -> AsyncPostgresBackend:
        handle = AsyncPostgresBackend(
            conformance_store(timeline),
            observer=observer,  # ty: ignore[invalid-argument-type]
        )
        return handle

    report = asyncio.run(check_async_backend(AsyncBackendCase("async postgres", build)))

    report.raise_for_failures(allow_skips=False)


#
# Contention.


@service
def test_threads_with_their_own_connections_admit_exactly_the_quota(
    pg_store: PostgresStore, two_per_second: Constraint, ten_per_minute: Constraint
) -> None:
    """
    Given: Eight handles, each with its own connection and transactions, on one store.
    When:  They race composed requests against two per second and ten per minute.
    Then:  Exactly two are admitted, since time stands still: no lost update, no partial debit.
    """
    handles = [PostgresBackend(pg_store, namespace="discworld") for _ in range(8)]
    barrier = threading.Barrier(len(handles))
    admitted: list[bool] = list()

    def race(handle: PostgresBackend) -> None:
        barrier.wait(30)
        for _ in range(5):
            admitted.append(_admit(handle, two_per_second, ten_per_minute).allowed)

    threads = [threading.Thread(target=race, args=(handle,)) for handle in handles]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    for handle in handles:
        handle.close()

    expected = (2, 40)
    actual = (sum(admitted), len(admitted))
    assert actual == expected


@service
def test_racing_first_users_initialize_a_fresh_schema_once(
    postgres: str, schema: str, raw: psycopg.Connection[Any], two_per_second: Constraint
) -> None:
    """
    Given: An empty schema and eight handles that have never connected.
    When:  They all make their first admission at once.
    Then:  Every one succeeds, one schema version is recorded, and two are admitted.
    """
    store = PostgresStore(postgres, schema=schema, clock=FakeTimeline().epoch_clock)
    handles = [PostgresBackend(store, namespace="discworld") for _ in range(8)]
    barrier = threading.Barrier(len(handles))
    outcomes: list[bool | BaseException] = list()

    def first(handle: PostgresBackend) -> None:
        barrier.wait(30)
        try:
            outcomes.append(_admit(handle, two_per_second).allowed)
        except Exception as error:
            outcomes.append(error)

    threads = [threading.Thread(target=first, args=(handle,)) for handle in handles]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    for handle in handles:
        handle.close()
    (versions,) = raw.execute(
        statement(f'SELECT count(*) FROM {schema}."procrastinators_meta" WHERE name = %s'),
        ("schema_version",),
    ).fetchone() or (0,)

    expected = (8, 2, 1)
    actual = (
        sum(isinstance(outcome, bool) for outcome in outcomes),
        sum(outcome is True for outcome in outcomes),
        versions,
    )
    assert actual == expected


@service
def test_a_deadlock_is_retried_within_the_budget(
    pg_backend: PostgresBackend,
    schema: str,
    raw: psycopg.Connection[Any],
    two_per_second: Constraint,
    ten_per_minute: Constraint,
) -> None:
    """
    Given: A composed rule pair, and another session that locks the second rule while the
           admission holds the first, then asks for the first: a deadlock.
    When:  The server breaks the deadlock by aborting the admission's transaction.
    Then:  The admission retries within its contention budget and is admitted, once.
    """
    _admit(pg_backend, two_per_second, ten_per_minute)
    raw.execute("SET deadlock_timeout = '10s'")
    rules = f'{schema}."procrastinators_rules"'
    lock = statement(f"SELECT id FROM {rules} WHERE name = %s FOR UPDATE")
    outcome: list[Decision] = list()

    def admit_both() -> None:
        outcome.append(_admit(pg_backend, two_per_second, ten_per_minute))

    with raw.transaction():
        # Canonical order locks "burst" first, so holding "sustained" crosses it.
        raw.execute(lock, ("sustained",))
        admitting = threading.Thread(target=admit_both)
        admitting.start()
        _until(lambda: _waiting_for_a_lock(raw))
        raw.execute(lock, ("burst",))
    admitting.join(30)
    (events,) = raw.execute(
        statement(f'SELECT count(*) FROM {schema}."procrastinators_events"')
    ).fetchone() or (0,)

    expected = (True, 4)
    actual = (outcome[0].allowed, events)
    assert actual == expected


@service
def test_a_held_row_lock_is_contention_not_denial(
    pg_backend: PostgresBackend,
    schema: str,
    raw: psycopg.Connection[Any],
    two_per_second: Constraint,
) -> None:
    """
    Given: Another session holding the rule's row lock.
    When:  An admission with a 100 ms lock budget is attempted, then the lock is released.
    Then:  The attempt raises BackendBusy, and the quota it did not consume is still whole.
    """
    _admit(pg_backend, two_per_second)
    budget = OperationBudget(lock_timeout_us=DurationMicros(100_000))
    with raw.transaction():
        raw.execute(
            statement(
                f'SELECT id FROM {schema}."procrastinators_rules" WHERE name = %s FOR UPDATE'
            ),
            ("burst",),
        )
        with pytest.raises(BackendBusy):
            pg_backend.admit(AdmissionRequest((two_per_second,), budget=budget))
    decisions = [_admit(pg_backend, two_per_second).allowed for _ in range(2)]

    expected = [True, False]
    actual = decisions
    assert actual == expected


#
# Failures.


@service
def test_a_session_terminated_mid_transaction_commits_nothing(
    postgres: str, schema: str, raw: psycopg.Connection[Any], timeline: FakeTimeline
) -> None:
    """
    Given: A capacity-one rule, and an observer that terminates the admission's server
           session once every rule has admitted, before anything is written.
    When:  The admission continues, then the caller tries again twice.
    Then:  The first attempt is BackendUnavailable, the handle reconnects, and the rule
           admits exactly once afterwards: the server rolled the transaction back.
    """
    armed = [True]

    def terminate(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is ObservationPoint.BEFORE_COMMIT and armed:
            armed.clear()
            raw.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE application_name = %s",
                ("victim",),
            )
        else:
            pass

    store = PostgresStore(
        f"{postgres}?application_name=victim", schema=schema, clock=timeline.epoch_clock
    )
    handle = PostgresBackend(store, observer=terminate)
    with pytest.raises(BackendUnavailable):
        _admit(handle, HOURLY_SINGLE)
    decisions = [_admit(handle, HOURLY_SINGLE).allowed for _ in range(2)]
    handle.close()

    expected = [True, False]
    actual = decisions
    assert actual == expected


class _LosingConnection(psycopg.Connection[Any]):
    """A connection that loses the reply to its next ``COMMIT``, before or after sending it."""

    lose: str | None = None

    def execute(self, query: Any, params: Any = None, **options: Any) -> Any:  # noqa: ANN401
        if query == "COMMIT" and (moment := self.lose) is not None:
            self.lose = None
            if moment == "after":
                super().execute(query, params, **options)
            else:
                pass
            raise psycopg.OperationalError(f"reply lost {moment} sending COMMIT")
        else:
            cursor = super().execute(query, params, **options)
        return cursor


@service
@pytest.mark.parametrize(("moment", "committed"), [("before", False), ("after", True)])
def test_a_lost_commit_reply_is_indeterminate_and_never_admits_twice(
    pg_store: PostgresStore, postgres: str, moment: str, committed: bool
) -> None:
    """
    Given: A capacity-one rule, on a connection that loses the reply to one COMMIT,
           which either did or did not reach the server.
    When:  An admission commits, then the caller tries again.
    Then:  The admission is IndeterminateAdmission either way; the retry is admitted only
           if the lost commit did not happen, so no admission is ever repeated.
    """
    with _LosingConnection.connect(postgres, autocommit=True) as connection:
        handle = PostgresBackend(pg_store, connection=connection)
        _admit(handle, constraint("quirm", "warmup", SlidingLogPolicy(1, SECOND)))
        connection.lose = moment
        with pytest.raises(IndeterminateAdmission):
            _admit(handle, HOURLY_SINGLE)
        retried = _admit(handle, HOURLY_SINGLE).allowed
        handle.close()

    expected = not committed
    actual = retried
    assert actual == expected


@service
def test_a_foreign_schema_version_is_refused(
    pg_store: PostgresStore, schema: str, raw: psycopg.Connection[Any], two_per_second: Constraint
) -> None:
    """
    Given: Tables whose recorded schema version is not this library's.
    When:  A fresh handle connects.
    Then:  UnsupportedCapability is raised rather than the tables being reinterpreted.
    """
    first = PostgresBackend(pg_store)
    _admit(first, two_per_second)
    first.close()
    raw.execute(statement(f'UPDATE {schema}."procrastinators_meta" SET value = 99'))
    handle = PostgresBackend(pg_store)

    with pytest.raises(UnsupportedCapability):
        _admit(handle, two_per_second)


@service
def test_corrupt_stored_state_fails_closed(
    pg_backend: PostgresBackend,
    schema: str,
    raw: psycopg.Connection[Any],
    two_per_second: Constraint,
) -> None:
    """
    Given: A stored event whose cost is impossible.
    When:  The rule is admitted against.
    Then:  StateCorruption is raised, never an empty bucket.
    """
    _admit(pg_backend, two_per_second)
    raw.execute(statement(f'UPDATE {schema}."procrastinators_events" SET cost = 0'))

    with pytest.raises(StateCorruption):
        _admit(pg_backend, two_per_second)


#
# Persistence, policy, cooldowns.


@service
def test_quota_and_policy_survive_a_restart(
    postgres: str, schema: str, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule spent by one store and handle, which are then closed.
    When:  A brand-new store and handle use the same tables, with the same and a changed policy.
    Then:  The quota is still spent, and the changed policy is a conflict, not fresh quota.
    """
    first = PostgresBackend(PostgresStore(postgres, schema=schema, clock=timeline.epoch_clock))
    for _ in range(2):
        _admit(first, two_per_second)
    first.close()
    second = PostgresBackend(PostgresStore(postgres, schema=schema, clock=timeline.epoch_clock))
    allowed = _admit(second, two_per_second).allowed
    changed = constraint("ankh", "burst", SlidingLogPolicy(5, SECOND))

    with pytest.raises(PolicyConflict):
        _admit(second, changed)
    second.close()
    assert allowed is False


@service
def test_cooldowns_reach_every_handle(pg_store: PostgresStore, two_per_second: Constraint) -> None:
    """
    Given: Two handles on one store.
    When:  One defers the scope for five seconds.
    Then:  The other is denied for at least that long, and a shorter deferral shortens nothing.
    """
    deferring = PostgresBackend(pg_store, namespace="discworld")
    admitting = PostgresBackend(pg_store, namespace="discworld")
    scope = two_per_second.rule.scope
    cooldown = deferring.defer_for(scope, DurationMicros(5 * SECOND), reason="429")
    shorter = deferring.defer_for(scope, DurationMicros(SECOND))
    decision = _admit(admitting, two_per_second)
    deferring.close()
    admitting.close()

    expected = (False, 5 * SECOND, cooldown.until, "429")
    actual = (decision.allowed, decision.retry_after_us, shorter.until, shorter.reason)
    assert actual == expected


@service
def test_a_migration_drains_before_installing_the_new_policy(
    pg_backend: PostgresBackend, timeline: FakeTimeline
) -> None:
    """
    Given: A fixed-window rule with a debit in its current window.
    When:  A migration begins, the window passes, and it completes.
    Then:  Admissions stop while draining, the migration becomes ready once neutral, and
           the new policy then admits while the old one conflicts.
    """
    old = constraint("ankh", "window", FixedWindowPolicy(2, SECOND))
    new = constraint("ankh", "window", FixedWindowPolicy(5, SECOND))
    _admit(pg_backend, old)
    begun = pg_backend.begin_migration(
        old.rule, to_fingerprint=new.fingerprint, to_state_version=new.state_version
    )
    with pytest.raises(PolicyConflict):
        _admit(pg_backend, old)
    timeline.advance(SECOND)
    ready = pg_backend.migration_status(old.rule)
    completed = pg_backend.complete_migration(old.rule)
    allowed = _admit(pg_backend, new).allowed
    with pytest.raises(PolicyConflict):
        _admit(pg_backend, old)
    stored = pg_backend.stored_policy(old.rule)

    expected = (
        MigrationStatus.DRAINING,
        MigrationStatus.READY,
        MigrationStatus.COMPLETE,
        True,
        new.fingerprint,
    )
    actual = (
        begun.status,
        ready.status if ready is not None else None,
        completed.status,
        allowed,
        stored.fingerprint if stored is not None else None,
    )
    assert actual == expected


@service
def test_sweeping_forgets_quota_state_but_never_policy(
    pg_backend: PostgresBackend, timeline: FakeTimeline, two_per_second: Constraint
) -> None:
    """
    Given: A rule used once, then idle past its safe-forget horizon.
    When:  The handle sweeps.
    Then:  One rule's state is forgotten and its policy metadata remains.
    """
    _admit(pg_backend, two_per_second)
    timeline.advance(2 * SECOND)
    forgotten = pg_backend.sweep()
    stored = pg_backend.stored_policy(two_per_second.rule)

    expected = (1, two_per_second.fingerprint)
    actual = (forgotten, stored.fingerprint if stored is not None else None)
    assert actual == expected


@service
def test_inspection_reports_what_this_process_admitted(
    pg_backend: PostgresBackend, two_per_second: Constraint
) -> None:
    """
    Given: A rule admitted once by this handle.
    When:  It is inspected.
    Then:  The snapshot reports one unit remaining.
    """
    _admit(pg_backend, two_per_second)
    snapshot = pg_backend.inspect((two_per_second.rule,))

    expected = 1
    actual = snapshot.rules[0].remaining
    assert actual == expected


#
# Lifecycle and the facade.


@service
def test_a_borrowed_connection_is_never_closed(
    pg_store: PostgresStore, postgres: str, two_per_second: Constraint
) -> None:
    """
    Given: A handle borrowing the caller's autocommit connection.
    When:  It admits and is closed.
    Then:  The connection is still open, and the handle reported it as borrowed.
    """
    with psycopg.connect(postgres, autocommit=True) as connection:
        handle = PostgresBackend(pg_store, connection=connection)
        _admit(handle, two_per_second)
        handle.close()

        expected = (Ownership.BORROWED, False)
        actual = (handle.ownership.client, connection.closed)
        assert actual == expected


@service
def test_a_cancelled_async_admission_leaves_the_handle_usable(
    pg_store: PostgresStore, raw: psycopg.Connection[Any], schema: str, two_per_second: Constraint
) -> None:
    """
    Given: An async admission blocked on a row lock another session holds.
    When:  Its task is cancelled, the lock is released, and the handle admits again.
    Then:  The cancellation propagates unchanged, it consumed nothing, and the handle works.
    """
    handle = AsyncPostgresBackend(pg_store, namespace="discworld")
    request = AdmissionRequest((two_per_second,))

    async def scenario() -> list[bool]:
        await handle.admit(request)
        raw.execute("BEGIN")
        raw.execute(
            statement(
                f'SELECT id FROM {schema}."procrastinators_rules" WHERE name = %s FOR UPDATE'
            ),
            ("burst",),
        )
        blocked = asyncio.create_task(handle.admit(request))
        while not _waiting_for_a_lock(raw):
            await asyncio.sleep(0.02)
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        raw.execute("COMMIT")
        decisions = [(await handle.admit(request)).allowed for _ in range(2)]
        await handle.aclose()
        return decisions

    expected = [True, False]
    actual = asyncio.run(scenario())
    assert actual == expected


@service
def test_limiters_on_one_database_share_its_quota(pg_store: PostgresStore) -> None:
    """
    Given: Two limiters with the same key, each on its own handle to one database.
    When:  Each tries twice against three per hour.
    Then:  Three admissions happen in total.
    """
    handles = [PostgresBackend(pg_store) for _ in range(2)]
    limiters = [
        RateLimiter(key="ankh", limits=[Limit(3, per="1h")], backend=handle, timeout=0)
        for handle in handles
    ]
    admitted = [limiter.try_acquire().allowed for limiter in limiters for _ in range(2)]
    for handle in handles:
        handle.close()

    expected = 3
    actual = sum(admitted)
    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass


@service
def test_a_sweep_during_an_admission_leaves_its_fresh_state(
    pg_store: PostgresStore, timeline: FakeTimeline
) -> None:
    """
    Given: A fixed window of one per second, used once and then idle past its horizon.
    When:  A sweep runs on another connection while the next admission holds the rule's
           lock, and a third attempt follows in the same window.
    Then:  The sweep skips the locked rule rather than deleting what the admission
           writes, so the third attempt is denied: the second admission's debit survived.
    """
    rule = constraint("ankh", "swept", FixedWindowPolicy(1, SECOND))
    sweeper = PostgresBackend(pg_store)
    swept = list()

    def sweep_meanwhile(point: ObservationPoint, request: AdmissionRequest) -> None:
        del request
        if point is ObservationPoint.BEFORE_COMMIT:
            worker = threading.Thread(target=lambda: swept.append(sweeper.sweep()))
            worker.start()
            worker.join(10)
        else:
            pass

    plain = PostgresBackend(pg_store)
    first = _admit(plain, rule)
    timeline.advance(2 * SECOND)
    observed = PostgresBackend(pg_store, observer=sweep_meanwhile)
    second = _admit(observed, rule)
    third = _admit(plain, rule)
    for handle in (plain, observed, sweeper):
        handle.close()

    expected = (True, True, [0], False)
    actual = (first.allowed, second.allowed, swept, third.allowed)
    assert actual == expected


@service
def test_admission_holds_under_a_stricter_default_isolation(postgres: str, schema: str) -> None:
    """
    Given: Sessions whose server default isolation is serializable.
    When:  Eight fresh handles admit on one new rule of capacity three at once.
    Then:  Each transaction runs read committed as the backend requires, so every
           attempt is decided and exactly three are admitted.
    """
    address = f"{postgres}?options=-c%20default_transaction_isolation%3Dserializable"
    rule = constraint("ankh", "isolated", SlidingLogPolicy(3, DurationMicros(3600 * SECOND)))
    handles = [PostgresBackend(PostgresStore(address, schema=schema)) for _ in range(8)]
    decisions: list[bool] = list()
    barrier = threading.Barrier(len(handles))

    def race(handle: PostgresBackend) -> None:
        barrier.wait()
        decisions.append(_admit(handle, rule).allowed)

    threads = [threading.Thread(target=race, args=(handle,)) for handle in handles]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    for handle in handles:
        handle.close()

    assert len(decisions) == len(handles)
    assert sum(decisions) == 3
