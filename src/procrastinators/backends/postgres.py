"""The PostgreSQL backend: one database coordinating workers on any machine.

A :class:`PostgresStore` names the authority — a database, and the table prefix
its quota lives under — and the algorithms it enforces. Handles address it:
:class:`PostgresBackend` with psycopg's synchronous connection,
:class:`AsyncPostgresBackend` with its native asynchronous one. Every handle
owns its own connection, and any number of handles, processes, and machines
reaching the database obey its quotas together.

**One transaction.** Admission creates any missing rule rows (``INSERT ... ON
CONFLICT DO NOTHING``, so racing first users agree on one row), then locks the
scopes it reads cooldowns from and the rules it debits, ``FOR SHARE`` and ``FOR
UPDATE``, in canonical constraint order: every admission takes its locks in the
same order, so admissions cannot deadlock one another. Only then does it sample
authority time, check policy metadata, load every rule's state, evaluate every
rule against that one sample with the reference evaluators, and write every
debit or none (contracts A1, T4, A6). It commits before reporting success.

**Time.** Authority time is the database server's ``clock_timestamp()``, read
after the locks are held — not ``now()``, which is the transaction's start and
would let a queued admission commit an old timestamp. It is clamped against the
latest time stored for each rule, so it never runs backwards for a rule's
state (T7). Fixed windows align to the Unix epoch by that clock (T8).

**Contention and failures.** ``lock_timeout`` and ``statement_timeout`` are set
for each transaction from the request's budget. A lock that stays busy past it
is :exc:`~procrastinators.errors.BackendBusy`, never a denial (O2); a deadlock or
serialization failure, which rolled everything back, is retried within the
budget's contention retries first. Nothing is retried once ``COMMIT`` has been
sent: a commit whose outcome was lost is
:exc:`~procrastinators.errors.IndeterminateAdmission`, never permission and
never a refund (O4).

**Durability.** The guarantee is
:attr:`~procrastinators.models.Durability.SERVICE_DURABLE`: an admission
reported as committed survives as the server's ``synchronous_commit`` and
replication settings promise. ``synchronous_commit = off`` can lose the latest
commits in a crash, forgetting admissions that happened; a failover to an
asynchronous replica can do the same.

**Schema.** Tables carry the store's prefix, ``procrastinators_`` by default,
hold rules by an integer id, and keep scalar state, weighted log events (one
row per admission, whatever its cost — P6), cooldowns, policy metadata, and
migration status apart. The schema is versioned; tables written by another
version are refused rather than reinterpreted. Creating them is serialized by a
transaction-scoped advisory lock, so processes racing to initialize an empty
database agree on one schema.

**Cleanup.** State is forgotten only past the safe-forget horizon its
algorithm reported (L9), by :meth:`PostgresBackend.sweep`, and policy metadata
is never forgotten with it (L10).

**Fork.** A connection is never carried across ``fork``. A handle used in a
forked child opens a fresh connection there; the inherited one is abandoned,
not closed, since closing it in the child would end the parent's session (L8).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import contextlib
import dataclasses
import os
import re
import threading
import urllib.parse
from collections import Counter
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Generic,
    LiteralString,
    TypeAlias,
    TypeVar,
    cast,
)

from procrastinators.algorithms import builtin_specs, reference_algorithms
from procrastinators.backends import bookkeeping
from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.capabilities import Mode
from procrastinators.errors import (
    BackendBusy,
    BackendError,
    BackendUnavailable,
    ConfigurationError,
    IndeterminateAdmission,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.models import (
    MAX_COST,
    MAX_EXACT_INT,
    MAX_TIMESTAMP_US,
    USECS_PER_SECOND,
    Admission,
    BackendIdentity,
    Capabilities,
    Cooldown,
    CoordinationScope,
    Durability,
    DurationMicros,
    EpochMicros,
    Ownership,
    QuotaIdentity,
    ResourceOwnership,
    Snapshot,
)
from procrastinators.protocols import (
    LogEvent,
    MigrationStatus,
    ObservationPoint,
    PolicyMigration,
    RuleState,
    StateRepresentation,
    StoredPolicy,
)
from procrastinators.state import UNUSED, plan_admission

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
    from types import ModuleType

    import psycopg

    from procrastinators.models import (
        AdmissionRequest,
        Constraint,
        Decision,
        PolicyFingerprint,
        RuleId,
    )
    from procrastinators.protocols import AdmissionClock, AdmissionObserver, Algorithm
    from procrastinators.state import AdmissionPlan
else:
    pass

__all__ = [
    "POSTGRES_CAPABILITIES",
    "SCHEMA_VERSION",
    "AsyncPostgresBackend",
    "PostgresBackend",
    "PostgresStore",
    "postgres_backend",
]

FAMILY: Final = "postgresql"
"""The backend family, as it appears in identities."""

SCHEMES: Final = ("postgresql", "postgres")
"""The address schemes naming a PostgreSQL database."""

SCHEMA_VERSION: Final = 1
"""The version of the tables this module reads and writes."""

DEFAULT_PREFIX: Final = "procrastinators"
"""What every table name starts with unless a store is given another prefix."""

DEFAULT_PORT: Final = 5432
"""The port an address without one names."""

DEFAULT_TIMEOUT_US: Final = DurationMicros(5 * USECS_PER_SECOND)
"""Lock and statement budget for inspection, cooldowns, and administration."""

DEFAULT_RETRIES: Final = 3
"""Deadlock and serialization retries for everything but admission."""

_MICROS_PER_MILLI: Final = 1_000
_IDENTIFIER: Final = re.compile(r"[a-z_][a-z0-9_]{0,39}")

# SQLSTATE codes that mean contention, which rolled everything back.
_LOCK_NOT_AVAILABLE: Final = "55P03"
_RETRYABLE: Final = frozenset({"40P01", "40001"})
_QUERY_CANCELED: Final = "57014"

POSTGRES_CAPABILITIES: Final = Capabilities(
    algorithms=frozenset(spec.id for spec in builtin_specs()),
    coordination=CoordinationScope.SHARED_SERVICE,
    durability=Durability.SERVICE_DURABLE,
    supports_sync=True,
    supports_async=True,
    supports_composition=True,
    supports_cooldowns=True,
    supports_policy_administration=True,
    state_representations=frozenset(StateRepresentation),
)
"""What the ``postgresql`` family declares before any store exists.

A store adds the algorithms it hosts.
"""

ResultT = TypeVar("ResultT")

ConnectionT = TypeVar("ConnectionT", bound="psycopg.Connection[Any] | psycopg.AsyncConnection[Any]")


@dataclass(frozen=True, slots=True)
class Query:
    """One statement for a driver to run, and its parameters."""

    sql: str
    """The statement, with ``%s`` placeholders."""

    params: tuple[object, ...] = tuple()
    """Values bound to the placeholders; never interpolated."""


@dataclass(frozen=True, slots=True)
class Result:
    """What a statement returned."""

    rows: list[tuple[Any, ...]]
    """Every row, or none for a statement that returns no rows."""

    rowcount: int
    """Rows the statement affected."""


Operation: TypeAlias = "Generator[Query, Result, Any]"
"""A store operation: yields statements, is sent their results, returns its result.

A driver error raised by a statement is thrown back into the operation, which
translates it; the synchronous and asynchronous handles share every statement
and decision and differ only in how they wait for the database.
"""


def _driver() -> ModuleType:
    try:
        import psycopg
    except ImportError as error:
        raise BackendUnavailable(
            "the postgresql backend needs psycopg: install procrastinators[postgres]",
            cause=error,
        ) from error
    return psycopg


def _driver_error() -> type[Exception]:
    error_type: type[Exception] = _driver().Error
    return error_type


def _sqlstate(error: BaseException) -> str | None:
    state = getattr(error, "sqlstate", None)
    return state


def _translated(error: BaseException, what: str) -> BackendError:
    """The library error a driver error before any commit means; nothing was committed."""
    state = _sqlstate(error)
    if state == _LOCK_NOT_AVAILABLE or state in _RETRYABLE:
        translated: BackendError = BackendBusy(
            f"{what}: rows stayed locked or conflicted past the budget; contention is not a "
            f"denial (O2): {error}",
            cause=error,
        )
    elif state == _QUERY_CANCELED:
        translated = BackendUnavailable(
            f"{what}: the statement exceeded its timeout and was cancelled (B3)", cause=error
        )
    else:
        translated = BackendUnavailable(f"{what}: {error}", cause=error)
    return translated


def _integer(value: object, *, what: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise StateCorruption(f"stored {what} {value!r} is not an integer from {low} to {high}")
    else:
        pass
    return value


def _timestamp(value: object, *, what: str) -> EpochMicros:
    checked = EpochMicros(_integer(value, what=what, low=0, high=MAX_TIMESTAMP_US))
    return checked


def _milliseconds(micros: int) -> int:
    milliseconds = max(1, -(-micros // _MICROS_PER_MILLI))
    return milliseconds


def _fetch(sql: str, *params: object) -> Generator[Query, Result, list[tuple[Any, ...]]]:
    result = yield Query(sql, params)
    return result.rows


def _run(sql: str, *params: object) -> Generator[Query, Result, int]:
    result = yield Query(sql, params)
    return result.rowcount


def _rollback() -> Generator[Query, Result, None]:
    # A failed rollback leaves the connection unusable; the handle notices it
    # is not idle and reconnects, and the server rolls back with the session.
    try:
        yield Query("ROLLBACK")
    except _driver_error():
        pass


class _Tables:
    """The qualified names of one store's tables."""

    def __init__(self, prefix: str, schema: str | None) -> None:
        qualifier = f'"{schema}".' if schema is not None else ""
        self.schema = schema
        self.lock_key = f"{schema or ''}.{prefix}"
        self.meta = f'{qualifier}"{prefix}_meta"'
        self.meta_table = f"{prefix}_meta"
        self.rules = f'{qualifier}"{prefix}_rules"'
        self.policies = f'{qualifier}"{prefix}_policies"'
        self.states = f'{qualifier}"{prefix}_states"'
        self.scalars = f'{qualifier}"{prefix}_scalars"'
        self.events = f'{qualifier}"{prefix}_events"'
        self.scopes = f'{qualifier}"{prefix}_scopes"'
        self.migrations = f'{qualifier}"{prefix}_migrations"'

    def schema_statements(self) -> tuple[str, ...]:
        statements = (
            f"CREATE TABLE {self.meta} (name TEXT PRIMARY KEY, value BIGINT NOT NULL)",
            f"CREATE TABLE {self.rules} (id BIGSERIAL PRIMARY KEY, namespace TEXT NOT NULL, "
            "quota TEXT NOT NULL, name TEXT NOT NULL, last_now BIGINT NOT NULL DEFAULT 0, "
            "UNIQUE (namespace, quota, name))",
            f"CREATE TABLE {self.policies} (rule_id BIGINT PRIMARY KEY REFERENCES {self.rules} "
            "(id), algorithm TEXT NOT NULL, fingerprint TEXT NOT NULL, "
            "state_version INTEGER NOT NULL, updated_at BIGINT NOT NULL)",
            # A row means the rule's state exists; a null horizon means it never becomes neutral.
            f"CREATE TABLE {self.states} (rule_id BIGINT PRIMARY KEY REFERENCES {self.rules} "
            "(id), horizon BIGINT)",
            f"CREATE INDEX ON {self.states} (horizon)",
            f"CREATE TABLE {self.scalars} (rule_id BIGINT NOT NULL REFERENCES {self.rules} "
            "(id), name TEXT NOT NULL, value BIGINT NOT NULL, PRIMARY KEY (rule_id, name))",
            f"CREATE TABLE {self.events} (id BIGSERIAL PRIMARY KEY, rule_id BIGINT NOT NULL "
            f"REFERENCES {self.rules} (id), at BIGINT NOT NULL, cost BIGINT NOT NULL)",
            f"CREATE INDEX ON {self.events} (rule_id, at)",
            # A scope row is created on first use and holds its cooldown, if any.
            f"CREATE TABLE {self.scopes} (namespace TEXT NOT NULL, quota TEXT NOT NULL, "
            "until BIGINT, reason TEXT, PRIMARY KEY (namespace, quota))",
            f"CREATE TABLE {self.migrations} (rule_id BIGINT PRIMARY KEY REFERENCES "
            f"{self.rules} (id), status TEXT NOT NULL, from_fingerprint TEXT, "
            "to_fingerprint TEXT, to_state_version INTEGER, drained_after BIGINT)",
        )
        return statements


class PostgresStore:
    """A PostgreSQL database as an admission authority, and the algorithms it enforces.

    Constructing a store parses its address and touches nothing else: no
    connection exists until a handle first needs one. It hosts the five
    reference algorithms unless given others, and more can be added with
    :meth:`host`; their Python evaluators run inside each transaction.

    :param conninfo: A ``postgresql://`` or ``postgres://`` URL, or a libpq
        ``key=value`` string. Credentials in it reach the driver and never the
        store's identity.
    :param prefix: Starts every table name; lower-case letters, digits, and underscores.
    :param schema: The schema holding the tables; the connection's search path when ``None``.
    :param algorithms: The algorithms to host; the reference algorithms when ``None``.
    :param clock: Authority epoch time, replacing the server's ``clock_timestamp()``;
        for deterministic tests only, since every client would have to share it.
    :param representations: State representations to accept beyond scalars and
        event logs, for third-party algorithms.
    :param connect_timeout: Seconds a handle waits to connect.
    :raises ~procrastinators.errors.ConfigurationError: A setting is invalid, or two hosted
        algorithms share an id.
    """

    def __init__(
        self,
        conninfo: str,
        *,
        prefix: str = DEFAULT_PREFIX,
        schema: str | None = None,
        algorithms: Iterable[Algorithm[Any]] | None = None,
        clock: AdmissionClock | None = None,
        representations: Iterable[str] = tuple(),
        connect_timeout: int = 5,
    ) -> None:
        for name, value in (("prefix", prefix), ("schema", schema)):
            if value is not None and not (isinstance(value, str) and _IDENTIFIER.fullmatch(value)):
                raise ConfigurationError(
                    f"{name} must be lower-case letters, digits, and underscores, got {value!r}"
                )
            else:
                pass
        if isinstance(connect_timeout, bool) or not (
            isinstance(connect_timeout, int) and connect_timeout > 0
        ):
            raise ConfigurationError(
                f"connect_timeout must be a positive integer, got {connect_timeout!r}"
            )
        else:
            pass
        self._conninfo = conninfo
        self._authority = _authority(conninfo)
        self._prefix = prefix
        self._tables = _Tables(prefix, schema)
        self._clock = clock
        self._connect_timeout = connect_timeout
        self._representations = frozenset({*StateRepresentation, *representations})
        self._lock = threading.Lock()
        self._algorithms: dict[str, Algorithm[Any]] = dict()
        self._capabilities: dict[Mode, Capabilities] = dict()
        self._constraints: dict[RuleId, Constraint] = dict()
        self.host(reference_algorithms() if algorithms is None else algorithms)

    @property
    def authority(self) -> str:
        """``host:port/dbname``, with the table prefix and schema; never credentials."""
        qualifier = f"{self._tables.schema}." if self._tables.schema else ""
        authority = f"{self._authority}/{qualifier}{self._prefix}"
        return authority

    @property
    def conninfo(self) -> str:
        """What connections are opened with, credentials included."""
        return self._conninfo

    @property
    def connect_timeout(self) -> int:
        """Seconds a handle waits to connect."""
        return self._connect_timeout

    def identity(self, namespace: str) -> BackendIdentity:
        """The identity of a handle using ``namespace`` on this store.

        :param namespace: The handle's quota namespace.
        """
        identity = BackendIdentity(FAMILY, self.authority, namespace)
        return identity

    def host(self, algorithms: Iterable[Algorithm[Any]]) -> None:
        """Host more algorithms, keeping those already hosted.

        As :meth:`~procrastinators.backends.sqlite.SQLiteStore.host`.

        :param algorithms: The algorithms to add.
        :raises ~procrastinators.errors.ConfigurationError: An id is hosted by an algorithm of
            another type.
        """
        with self._lock:
            for algorithm in algorithms:
                if (existing := self._algorithms.get(algorithm.id)) is None:
                    self._algorithms[algorithm.id] = algorithm
                    self._capabilities.clear()
                elif type(existing) is not type(algorithm):
                    raise ConfigurationError(
                        f"algorithm {algorithm.id!r} is already hosted by "
                        f"{type(existing).__name__}, not {type(algorithm).__name__}"
                    )
                else:
                    pass

    def capabilities(self, mode: Mode) -> Capabilities:
        """What a handle in ``mode`` on this store implements.

        :param mode: The handle's interface.
        """
        with self._lock:
            if (capabilities := self._capabilities.get(mode)) is None:
                capabilities = dataclasses.replace(
                    POSTGRES_CAPABILITIES,
                    algorithms=frozenset(self._algorithms),
                    supports_sync=mode is Mode.SYNC,
                    supports_async=mode is Mode.ASYNC,
                    state_representations=self._representations,
                )
                self._capabilities[mode] = capabilities
            else:
                pass
        return capabilities

    def _resolve(self, constraint: Constraint) -> Algorithm[Any]:
        if (algorithm := self._algorithms.get(constraint.algorithm)) is None:
            raise UnsupportedCapability(
                f"PostgreSQL store {self.authority} does not host algorithm "
                f"{constraint.algorithm!r}"
            )
        else:
            pass
        return algorithm

    #
    # Schema.

    def initialize(self) -> Generator[Query, Result, None]:
        """Create the tables if they are missing, and refuse another schema version.

        An existing schema is only read. Creating one happens under a
        transaction-scoped advisory lock with the check repeated, so racing
        creators agree on a single schema.

        :raises ~procrastinators.errors.UnsupportedCapability: The tables hold another version.
        """
        tables = self._tables
        what = f"initializing {self.authority}"
        try:
            if (version := (yield from self._schema_version())) is None:
                # Read committed, so the check repeated under the lock sees a racing
                # creator's commit whatever the server's default isolation.
                yield Query("BEGIN ISOLATION LEVEL READ COMMITTED")
                yield Query("SELECT pg_advisory_xact_lock(hashtext(%s))", (tables.lock_key,))
                if (version := (yield from self._schema_version())) is None:
                    for statement in tables.schema_statements():
                        yield Query(statement)
                    yield Query(
                        f"INSERT INTO {tables.meta} (name, value) VALUES ('schema_version', %s)",
                        (SCHEMA_VERSION,),
                    )
                    version = SCHEMA_VERSION
                else:
                    pass
                yield Query("COMMIT")
            else:
                pass
        except _driver_error() as error:
            yield from _rollback()
            raise _translated(error, what) from error
        except Exception:
            yield from _rollback()
            raise
        if version != SCHEMA_VERSION:
            raise UnsupportedCapability(
                f"{self.authority} holds procrastinators schema version {version}, and this "
                f"library reads version {SCHEMA_VERSION}; it is refused rather than "
                "reinterpreted"
            )
        else:
            pass

    def _schema_version(self) -> Generator[Query, Result, object | None]:
        # pg_tables is read through the statement's snapshot, which sees a
        # racing creator's commit; to_regclass could consult a stale catalog cache.
        exists = yield from _fetch(
            "SELECT 1 FROM pg_tables WHERE schemaname = coalesce(%s, current_schema()) "
            "AND tablename = %s",
            self._tables.schema,
            self._tables.meta_table,
        )
        if not exists:
            version = None
        else:
            rows = yield from _fetch(
                f"SELECT value FROM {self._tables.meta} WHERE name = 'schema_version'"
            )
            version = rows[0][0] if rows else "missing"
        return version

    #
    # Transactions.

    @staticmethod
    def _begin(
        *, lock_timeout_us: int, storage_timeout_us: int, read_only: bool = False
    ) -> Generator[Query, Result, None]:
        # One round trip: SET LOCAL takes no parameters, the values are
        # validated integers, and a parameterless query may hold several statements.
        yield Query(
            # Read committed, whatever the server's default: re-reading a row a racing
            # first user inserted depends on each statement seeing the latest commits.
            f"BEGIN ISOLATION LEVEL READ COMMITTED{' READ ONLY' if read_only else ''}; "
            f"SET LOCAL lock_timeout = {_milliseconds(lock_timeout_us)}; "
            f"SET LOCAL statement_timeout = {_milliseconds(storage_timeout_us)}"
        )

    def _transaction(
        self,
        what: str,
        body: Callable[[], Generator[Query, Result, ResultT]],
        *,
        read_only: bool = False,
    ) -> Generator[Query, Result, ResultT]:
        """``body`` in one transaction, retried on deadlock or serialization failure."""
        for attempt in range(DEFAULT_RETRIES + 1):
            try:
                yield from self._begin(
                    lock_timeout_us=DEFAULT_TIMEOUT_US,
                    storage_timeout_us=DEFAULT_TIMEOUT_US,
                    read_only=read_only,
                )
                result = yield from body()
                yield Query("COMMIT")
            except _driver_error() as error:
                yield from _rollback()
                if _sqlstate(error) in _RETRYABLE and attempt < DEFAULT_RETRIES:
                    continue
                else:
                    raise _translated(error, what) from error
            except Exception:
                yield from _rollback()
                raise
            else:
                break
        else:
            raise AssertionError("unreachable")
        return result

    def _clock_now(self) -> Generator[Query, Result, EpochMicros]:
        if self._clock is not None:
            sampled = self._clock.now()
        else:
            ((value,),) = yield from _fetch(
                "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint"
            )
            sampled = EpochMicros(value)
        return sampled

    def _rule_ids(
        self, rules: Sequence[RuleId], *, create: bool
    ) -> Generator[Query, Result, dict[RuleId, tuple[int, EpochMicros]]]:
        """Each rule's id and last observed time, locked ``FOR UPDATE`` in the given order."""
        tables = self._tables
        found: dict[RuleId, tuple[int, EpochMicros]] = dict()
        select = (
            f"SELECT id, last_now FROM {tables.rules} "
            "WHERE namespace = %s AND quota = %s AND name = %s FOR UPDATE"
        )
        for rule in rules:
            values = (rule.scope.namespace, rule.scope.key, rule.name)
            # A rule seen before is locked in one statement; only its first use
            # inserts, and racing first users agree on the one row that wins.
            if not (rows := (yield from _fetch(select, *values))) and create:
                yield Query(
                    f"INSERT INTO {tables.rules} (namespace, quota, name) VALUES (%s, %s, %s) "
                    "ON CONFLICT DO NOTHING",
                    values,
                )
                rows = yield from _fetch(select, *values)
            else:
                pass
            if rows:
                found[rule] = (rows[0][0], _timestamp(rows[0][1], what="last observed time"))
            else:
                pass
        return found

    def _record_now(
        self, ids: Mapping[RuleId, tuple[int, EpochMicros]], now: EpochMicros
    ) -> Generator[Query, Result, None]:
        for rule_id, last in ids.values():
            if now > last:
                yield Query(
                    f"UPDATE {self._tables.rules} SET last_now = %s WHERE id = %s", (now, rule_id)
                )
            else:
                pass

    def _lock_scopes(
        self, scopes: Iterable[QuotaIdentity], *, exclusive: bool
    ) -> Generator[Query, Result, dict[QuotaIdentity, tuple[EpochMicros | None, str]]]:
        """Create and lock each scope's row, returning its cooldown, in canonical order."""
        tables = self._tables
        cooldowns: dict[QuotaIdentity, tuple[EpochMicros | None, str]] = dict()
        select = (
            f"SELECT until, reason FROM {tables.scopes} WHERE namespace = %s AND quota = %s "
            + ("FOR UPDATE" if exclusive else "FOR SHARE")
        )
        for scope in sorted(set(scopes)):
            if not (rows := (yield from _fetch(select, scope.namespace, scope.key))):
                yield Query(
                    f"INSERT INTO {tables.scopes} (namespace, quota) VALUES (%s, %s) "
                    "ON CONFLICT DO NOTHING",
                    (scope.namespace, scope.key),
                )
                rows = yield from _fetch(select, scope.namespace, scope.key)
            else:
                pass
            ((until, reason),) = rows
            cooldowns[scope] = (
                None if until is None else _timestamp(until, what="cooldown end"),
                reason or "",
            )
        return cooldowns

    #
    # Admission.

    def admit(
        self,
        request: AdmissionRequest,
        observe: Callable[[ObservationPoint, AdmissionRequest], None],
        identity: BackendIdentity,
    ) -> Generator[Query, Result, Decision]:
        """One admission, as one transaction.

        :param request: The request.
        :param observe: Reports each observation point inside the transaction.
        :param identity: The admitting handle's identity, recorded on the admission.
        :returns: An operation producing the decision; a denial is a value, committed as one.
        """
        what = f"admitting on {self.authority}"
        budget = request.budget
        for attempt in range(budget.max_contention_retries + 1):
            try:
                yield from self._begin(
                    lock_timeout_us=budget.lock_timeout_us,
                    storage_timeout_us=budget.storage_timeout_us,
                )
                plan = yield from self._admit_locked(request, observe)
            except _driver_error() as error:
                yield from _rollback()
                if _sqlstate(error) in _RETRYABLE and attempt < budget.max_contention_retries:
                    continue
                else:
                    raise _translated(error, what) from error
            except Exception:
                yield from _rollback()
                raise
            try:
                yield Query("COMMIT")
            except _driver_error() as error:
                yield from _rollback()
                if plan.admitted and _sqlstate(error) not in _RETRYABLE:
                    raise IndeterminateAdmission(
                        f"{what}: the commit failed ({error}); the admission may have happened",
                        cause=error,
                        cost=request.cost,
                        rules=request.rules,
                    ) from error
                else:
                    raise _translated(error, what) from error
            break
        else:
            raise AssertionError("unreachable")
        with self._lock:
            for constraint in request.constraints:
                self._constraints[constraint.rule] = constraint
        if plan.admitted:
            admission = Admission(request.rules, request.cost, plan.now, identity)
            observe(ObservationPoint.AFTER_COMMIT, request)
            decision = plan.decision(admission)
        else:
            decision = plan.decision()
        return decision

    def _admit_locked(
        self,
        request: AdmissionRequest,
        observe: Callable[[ObservationPoint, AdmissionRequest], None],
    ) -> Generator[Query, Result, AdmissionPlan]:
        cooldowns = yield from self._lock_scopes(
            (rule.scope for rule in request.rules), exclusive=False
        )
        ids = yield from self._rule_ids(request.rules, create=True)
        now = yield from self._clock_now()
        now = EpochMicros(max(now, *(last for _, last in ids.values())))
        loaded: dict[RuleId, bookkeeping.LoadedRule] = dict()
        for rule in request.rules:
            loaded[rule] = yield from self._load_rule(rule, ids[rule][0])
        for constraint in request.constraints:
            entry = loaded[constraint.rule]
            bookkeeping.check_policy(
                constraint,
                stored=entry.stored,
                migration=entry.migration,
                pending=entry.pending,
                resolve=self._resolve,
            )
        observe(ObservationPoint.AFTER_LOAD, request)
        holds = bookkeeping.cooldown_holds(
            request,
            {scope: until for scope, (until, _) in cooldowns.items() if until is not None},
            now,
        )
        plan = plan_admission(
            request,
            {rule: entry.state for rule, entry in loaded.items()},
            self._resolve,
            now,
            holds=holds,
        )
        if plan.admitted:
            observe(ObservationPoint.BEFORE_COMMIT, request)
        else:
            pass
        yield from self._record_now(ids, now)
        yield from self._write(request, plan, loaded)
        return plan

    def _write(
        self,
        request: AdmissionRequest,
        plan: AdmissionPlan,
        loaded: Mapping[RuleId, bookkeeping.LoadedRule],
    ) -> Generator[Query, Result, None]:
        # Policy metadata is recorded on first contact, admitted or not, and
        # outlives quota state, so a later disagreement is always detectable (L10).
        tables = self._tables
        for constraint in request.constraints:
            entry = loaded[constraint.rule]
            if entry.stored is None:
                yield Query(
                    f"INSERT INTO {tables.policies} (rule_id, algorithm, fingerprint, "
                    "state_version, updated_at) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (rule_id) "
                    "DO UPDATE SET algorithm = EXCLUDED.algorithm, fingerprint = "
                    "EXCLUDED.fingerprint, state_version = EXCLUDED.state_version, "
                    "updated_at = EXCLUDED.updated_at",
                    (
                        entry.rule_id,
                        constraint.algorithm,
                        constraint.fingerprint,
                        constraint.state_version,
                        plan.now,
                    ),
                )
            else:
                pass
        horizons = {
            transition.rule: transition.safe_forget_after_us for transition in plan.transitions
        }
        for rule, state in plan.writes.items():
            yield from self._write_state(loaded[rule], state, horizons[rule])

    def _write_state(
        self, entry: bookkeeping.LoadedRule, state: RuleState, horizon: EpochMicros | None
    ) -> Generator[Query, Result, None]:
        """Bring one rule's stored rows from ``entry.state`` to ``state``, touching only changes."""
        tables = self._tables
        rule_id = entry.rule_id
        if not state.exists:
            for table in (tables.scalars, tables.events, tables.states):
                yield Query(f"DELETE FROM {table} WHERE rule_id = %s", (rule_id,))
        else:
            yield Query(
                f"INSERT INTO {tables.states} (rule_id, horizon) VALUES (%s, %s) "
                "ON CONFLICT (rule_id) DO UPDATE SET horizon = EXCLUDED.horizon",
                (rule_id, horizon),
            )
            before = dict(entry.state.scalars)
            after = dict(state.scalars)
            for name in before.keys() - after.keys():
                yield Query(
                    f"DELETE FROM {tables.scalars} WHERE rule_id = %s AND name = %s",
                    (rule_id, name),
                )
            for name, value in after.items():
                if before.get(name) != value:
                    yield Query(
                        f"INSERT INTO {tables.scalars} (rule_id, name, value) VALUES (%s, %s, %s) "
                        "ON CONFLICT (rule_id, name) DO UPDATE SET value = EXCLUDED.value",
                        (rule_id, name, value),
                    )
                else:
                    pass
            old = Counter((event.at, event.cost) for event in entry.state.events)
            new = Counter((event.at, event.cost) for event in state.events)
            if stale := [
                event_id
                for pair, count in (old - new).items()
                for event_id in entry.event_rows[pair][:count]
            ]:
                yield Query(f"DELETE FROM {tables.events} WHERE id = ANY(%s)", (stale,))
            else:
                pass
            for (at, cost), count in (new - old).items():
                for _ in range(count):
                    yield Query(
                        f"INSERT INTO {tables.events} (rule_id, at, cost) VALUES (%s, %s, %s)",
                        (rule_id, at, cost),
                    )

    #
    # Loading.

    def _load(self, rule: RuleId) -> Generator[Query, Result, bookkeeping.LoadedRule]:
        rows = yield from _fetch(
            f"SELECT id FROM {self._tables.rules} "
            "WHERE namespace = %s AND quota = %s AND name = %s",
            rule.scope.namespace,
            rule.scope.key,
            rule.name,
        )
        if not rows:
            loaded = bookkeeping.LoadedRule(None, None, None, None, UNUSED, None)
        else:
            loaded = yield from self._load_rule(rule, rows[0][0])
        return loaded

    def _load_rule(
        self, rule: RuleId, rule_id: int
    ) -> Generator[Query, Result, bookkeeping.LoadedRule]:
        tables = self._tables
        policy = yield from _fetch(
            f"SELECT algorithm, fingerprint, state_version, updated_at FROM {tables.policies} "
            "WHERE rule_id = %s",
            rule_id,
        )
        stored = (
            None
            if not policy
            else StoredPolicy(
                rule,
                str(policy[0][0]),
                policy[0][1],
                _integer(policy[0][2], what="state version", low=1, high=0xFFFF),
                _timestamp(policy[0][3], what="policy update time"),
            )
        )
        migration, target = yield from self._load_migration(rule, rule_id)
        state_rows = yield from _fetch(
            f"SELECT horizon FROM {tables.states} WHERE rule_id = %s", rule_id
        )
        if not state_rows:
            loaded = bookkeeping.LoadedRule(rule_id, stored, migration, target, UNUSED, None)
        else:
            raw = state_rows[0][0]
            horizon = None if raw is None else _timestamp(raw, what="horizon")
            scalars = tuple(
                (
                    str(name),
                    _integer(
                        value, what=f"scalar {name!r}", low=-MAX_EXACT_INT, high=MAX_EXACT_INT
                    ),
                )
                for name, value in (
                    yield from _fetch(
                        f"SELECT name, value FROM {tables.scalars} WHERE rule_id = %s "
                        "ORDER BY name",
                        rule_id,
                    )
                )
            )
            event_rows: dict[tuple[int, int], list[int]] = dict()
            events = list()
            for event_id, at, cost in (
                yield from _fetch(
                    f"SELECT id, at, cost FROM {tables.events} WHERE rule_id = %s "
                    "ORDER BY at, cost, id",
                    rule_id,
                )
            ):
                event = LogEvent(
                    _timestamp(at, what="event time"),
                    _integer(cost, what="event cost", low=1, high=MAX_COST),
                )
                events.append(event)
                event_rows.setdefault((event.at, event.cost), list()).append(event_id)
            loaded = bookkeeping.LoadedRule(
                rule_id,
                stored,
                migration,
                target,
                RuleState(scalars=scalars, events=tuple(events)),
                horizon,
                event_rows,
            )
        return loaded

    def _load_migration(
        self, rule: RuleId, rule_id: int
    ) -> Generator[Query, Result, tuple[PolicyMigration | None, int | None]]:
        rows = yield from _fetch(
            "SELECT status, from_fingerprint, to_fingerprint, to_state_version, drained_after "
            f"FROM {self._tables.migrations} WHERE rule_id = %s",
            rule_id,
        )
        if not rows:
            migration = None
            target = None
        else:
            status, from_fingerprint, to_fingerprint, to_state_version, drained_after = rows[0]
            try:
                parsed = MigrationStatus(status)
            except ValueError as error:
                raise StateCorruption(f"stored migration status {status!r} is unknown") from error
            migration = PolicyMigration(
                rule,
                parsed,
                from_fingerprint=from_fingerprint,
                to_fingerprint=to_fingerprint,
                drained_after=(
                    None
                    if drained_after is None
                    else _timestamp(drained_after, what="migration drain time")
                ),
            )
            target = (
                None
                if to_state_version is None
                else _integer(to_state_version, what="target state version", low=1, high=0xFFFF)
            )
        result = (migration, target)
        return result

    #
    # Everything else.

    def sweep(self) -> Generator[Query, Result, int]:
        """Forget every rule's state past its safe-forget horizon, and expired cooldowns.

        Admission already ignores such state; this reclaims its space. Policy
        metadata is kept (L10).

        :returns: An operation producing how many rules' state was forgotten.
        """
        tables = self._tables

        def body() -> Generator[Query, Result, int]:
            now = yield from self._clock_now()
            yield Query(
                f"UPDATE {tables.scopes} SET until = NULL, reason = NULL WHERE until <= %s",
                (now,),
            )
            # Each forgettable rule is locked as an admission locks it, skipping any
            # an admission holds now, and its horizon is read again under the lock:
            # an admission that committed meanwhile has written a new horizon, and
            # its fresh state must not be deleted with the old.
            rows = yield from _fetch(
                f"SELECT rule.id FROM {tables.rules} AS rule JOIN {tables.states} AS state "
                "ON state.rule_id = rule.id WHERE state.horizon IS NOT NULL "
                "AND state.horizon <= %s ORDER BY rule.id FOR UPDATE OF rule SKIP LOCKED",
                now,
            )
            locked = [row[0] for row in rows]
            forgettable = (
                f"SELECT rule_id FROM {tables.states} WHERE rule_id = ANY(%s) "
                "AND horizon IS NOT NULL AND horizon <= %s"
            )
            for table in (tables.scalars, tables.events):
                yield Query(f"DELETE FROM {table} WHERE rule_id IN ({forgettable})", (locked, now))
            forgotten = yield from _run(
                f"DELETE FROM {tables.states} WHERE rule_id = ANY(%s) "
                "AND horizon IS NOT NULL AND horizon <= %s",
                locked,
                now,
            )
            return forgotten

        forgotten = yield from self._transaction(f"sweeping {self.authority}", body)
        return forgotten

    def inspect(
        self, rules: Sequence[RuleId], identity: BackendIdentity
    ) -> Generator[Query, Result, Snapshot]:
        """Advisory observation of ``rules`` in a read-only transaction. Never a reservation (R7).

        As :meth:`~procrastinators.backends.sqlite.SQLiteStore.inspect`.

        :param rules: The rules to observe.
        :param identity: The observing handle's identity.
        :returns: An operation producing the snapshot.
        """
        tables = self._tables

        def body() -> Generator[Query, Result, Snapshot]:
            sampled = yield from self._clock_now()
            entries: dict[RuleId, bookkeeping.LoadedRule] = dict()
            lasts = [sampled]
            for rule in rules:
                entries[rule] = yield from self._load(rule)
                rows = yield from _fetch(
                    f"SELECT last_now FROM {tables.rules} "
                    "WHERE namespace = %s AND quota = %s AND name = %s",
                    rule.scope.namespace,
                    rule.scope.key,
                    rule.name,
                )
                lasts.extend(_timestamp(row[0], what="last observed time") for row in rows)
            now = EpochMicros(max(lasts))
            snapshots = list()
            for rule in rules:
                entry = entries[rule]
                with self._lock:
                    known = self._constraints.get(rule)
                constraint = (
                    known
                    if known is not None
                    and entry.stored is not None
                    and entry.stored.fingerprint == known.fingerprint
                    else None
                )
                cooldown = yield from _fetch(
                    f"SELECT until FROM {tables.scopes} WHERE namespace = %s AND quota = %s",
                    rule.scope.namespace,
                    rule.scope.key,
                )
                until = (
                    _timestamp(cooldown[0][0], what="cooldown end")
                    if cooldown and cooldown[0][0] is not None
                    else None
                )
                snapshots.append(
                    bookkeeping.rule_snapshot(
                        rule,
                        constraint=constraint,
                        algorithm=entry.stored.algorithm_id if entry.stored is not None else None,
                        state=entry.state,
                        horizon=entry.horizon,
                        cooldown_until=until,
                        resolve=self._resolve,
                        now=now,
                    )
                )
            snapshot = Snapshot(rules=tuple(snapshots), sampled_at=now, backend=identity)
            return snapshot

        snapshot = yield from self._transaction(
            f"inspecting {self.authority}", body, read_only=True
        )
        return snapshot

    def defer(
        self, scope: QuotaIdentity, duration: DurationMicros, reason: str
    ) -> Generator[Query, Result, Cooldown]:
        """Extend ``scope``'s cooldown to at least ``now + duration`` in one transaction.

        The scope's row is locked ``FOR UPDATE``, so the extension is ordered
        against every admission reading it.

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on a cooldown this call creates or lengthens.
        :returns: An operation producing the cooldown in force.
        """

        def body() -> Generator[Query, Result, Cooldown]:
            ((until, stored_reason),) = (
                yield from self._lock_scopes((scope,), exclusive=True)
            ).values()
            existing = None if until is None else Cooldown(scope, until, stored_reason)
            now = yield from self._clock_now()
            cooldown = bookkeeping.extend_cooldown(existing, scope, duration, reason, now)
            if cooldown is not existing:
                yield Query(
                    f"UPDATE {self._tables.scopes} SET until = %s, reason = %s "
                    "WHERE namespace = %s AND quota = %s",
                    (cooldown.until, cooldown.reason, scope.namespace, scope.key),
                )
            else:
                pass
            return cooldown

        cooldown = yield from self._transaction(f"deferring on {self.authority}", body)
        return cooldown

    def stored_policy(self, rule: RuleId) -> Generator[Query, Result, StoredPolicy | None]:
        """The policy metadata the database holds for ``rule``, if any.

        :param rule: The rule to look up.
        """

        def body() -> Generator[Query, Result, StoredPolicy | None]:
            entry = yield from self._load(rule)
            return entry.stored

        stored = yield from self._transaction(
            f"reading policy on {self.authority}", body, read_only=True
        )
        return stored

    def _locked_rule(
        self, rule: RuleId
    ) -> Generator[
        Query,
        Result,
        tuple[EpochMicros, bookkeeping.LoadedRule, dict[RuleId, tuple[int, EpochMicros]]],
    ]:
        ids = yield from self._rule_ids((rule,), create=True)
        sampled = yield from self._clock_now()
        now = EpochMicros(max(sampled, ids[rule][1]))
        yield from self._record_now(ids, now)
        entry = yield from self._load_rule(rule, ids[rule][0])
        result = (now, entry, ids)
        return result

    def _refresh(
        self, entry: bookkeeping.LoadedRule, now: EpochMicros
    ) -> Generator[Query, Result, PolicyMigration | None]:
        migration = bookkeeping.refresh_migration(
            entry.migration, neutral=entry.neutral(now), horizon=entry.horizon, now=now
        )
        if migration is not None:
            yield from self._write_migration(entry, migration)
        else:
            pass
        return migration

    def _write_migration(
        self, entry: bookkeeping.LoadedRule, migration: PolicyMigration
    ) -> Generator[Query, Result, None]:
        yield Query(
            f"INSERT INTO {self._tables.migrations} (rule_id, status, from_fingerprint, "
            "to_fingerprint, to_state_version, drained_after) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (rule_id) DO UPDATE SET status = EXCLUDED.status, "
            "from_fingerprint = EXCLUDED.from_fingerprint, "
            "to_fingerprint = EXCLUDED.to_fingerprint, "
            "to_state_version = EXCLUDED.to_state_version, "
            "drained_after = EXCLUDED.drained_after",
            (
                entry.rule_id,
                migration.status.value,
                migration.from_fingerprint,
                migration.to_fingerprint,
                entry.target_version,
                migration.drained_after,
            ),
        )

    def begin_migration(
        self, rule: RuleId, to_fingerprint: PolicyFingerprint, to_state_version: int
    ) -> Generator[Query, Result, PolicyMigration]:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        As :meth:`~procrastinators.backends.sqlite.SQLiteStore.begin_migration`.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        """

        def body() -> Generator[Query, Result, PolicyMigration | None]:
            now, entry, _ = yield from self._locked_rule(rule)
            migration = bookkeeping.begin_migration(
                rule, entry.migration, current=entry.current, to_fingerprint=to_fingerprint
            )
            if migration is not entry.migration:
                entry.migration = migration
                entry.target_version = to_state_version
            else:
                pass
            refreshed = yield from self._refresh(entry, now)
            return refreshed

        refreshed = yield from self._transaction(f"migrating on {self.authority}", body)
        assert refreshed is not None
        return refreshed

    def migration_status(self, rule: RuleId) -> Generator[Query, Result, PolicyMigration | None]:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """

        def body() -> Generator[Query, Result, PolicyMigration | None]:
            now, entry, _ = yield from self._locked_rule(rule)
            migration = yield from self._refresh(entry, now)
            return migration

        migration = yield from self._transaction(f"reading migration on {self.authority}", body)
        return migration

    def complete_migration(self, rule: RuleId) -> Generator[Query, Result, PolicyMigration]:
        """Install the new policy on a drained rule.

        As :meth:`~procrastinators.backends.sqlite.SQLiteStore.complete_migration`.

        :param rule: The rule whose migration to complete.
        """
        tables = self._tables

        def body() -> Generator[Query, Result, PolicyMigration]:
            now, entry, _ = yield from self._locked_rule(rule)
            migration = bookkeeping.complete_migration(rule, (yield from self._refresh(entry, now)))
            for table in (tables.scalars, tables.events, tables.states, tables.policies):
                yield Query(f"DELETE FROM {table} WHERE rule_id = %s", (entry.rule_id,))
            yield from self._write_migration(entry, migration)
            return migration

        migration = yield from self._transaction(f"completing migration on {self.authority}", body)
        with self._lock:
            self._constraints.pop(rule, None)
        return migration

    def __repr__(self) -> str:
        text = f"PostgresStore({self.authority!r})"
        return text


def _authority(conninfo: str) -> str:
    """``host:port/dbname`` for a connection string, without its credentials."""
    if "://" in conninfo:
        parts = urllib.parse.urlsplit(conninfo)
        if parts.scheme not in SCHEMES:
            raise ConfigurationError(f"not a postgresql address: {conninfo!r}")
        else:
            pass
        options = dict(urllib.parse.parse_qsl(parts.query))
        host = parts.hostname or options.get("host", "localhost")
        try:
            port = parts.port or int(options.get("port", DEFAULT_PORT))
        except ValueError as error:
            raise ConfigurationError(f"{conninfo!r} names an invalid port") from error
        database = parts.path.strip("/") or options.get("dbname", "") or parts.username or ""
    else:
        options = dict(pair.split("=", 1) for pair in conninfo.split() if "=" in pair)
        host = options.get("host", "localhost")
        port = options.get("port", str(DEFAULT_PORT))
        database = options.get("dbname", options.get("user", ""))
    if not database:
        raise ConfigurationError(f"{conninfo!r} names no database")
    else:
        pass
    authority = (
        f"{urllib.parse.quote(host, safe='')}:{port}/{urllib.parse.quote(database, safe='')}"
    )
    return authority


@dataclass(slots=True)
class _Connection(Generic[ConnectionT]):
    """A handle's connection in one process."""

    pid: int = field(default_factory=os.getpid)
    connection: ConnectionT | None = None


_fork_lock = threading.Lock()
_abandoned: Final[list[object]] = list()
"""Connections inherited across a fork: kept referenced so the child never closes them."""


def _renew_fork_lock() -> None:
    # A lock copied mid-acquisition by fork would stay held forever in the child.
    global _fork_lock
    _fork_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_renew_fork_lock)
else:
    pass


def _usable(connection: psycopg.Connection[Any] | psycopg.AsyncConnection[Any] | None) -> bool:
    """Whether ``connection`` is open and idle, outside any transaction."""
    if connection is None or connection.closed:
        usable = False
    else:
        usable = connection.info.transaction_status == _driver().pq.TransactionStatus.IDLE
    return usable


class _PostgresHandle(Generic[ConnectionT]):
    """What the sync and async handles share: the store, namespace, observer, and connection."""

    _mode: ClassVar[Mode]

    def __init__(
        self,
        store: PostgresStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        connection: ConnectionT | None = None,
    ) -> None:
        if connection is not None and not getattr(connection, "autocommit", False):
            raise ConfigurationError(
                "an injected connection must be in autocommit mode: the backend begins and "
                "commits its own transactions"
            )
        else:
            pass
        self._store = store
        self._identity = store.identity(namespace)
        self._observer = observer
        self._borrowed = connection is not None
        self._process: _Connection[ConnectionT] = _Connection(connection=connection)
        self._initialized = False

    @property
    def store(self) -> PostgresStore:
        """The authority this handle addresses."""
        return self._store

    @property
    def capabilities(self) -> Capabilities:
        """Shared-service, service-durable, composing, with cooldowns and policy administration."""
        capabilities = self._store.capabilities(self._mode)
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        """``postgresql://host:port/dbname/prefix#namespace``."""
        return self._identity

    @property
    def ownership(self) -> ResourceOwnership:
        """The connection is owned unless it was injected (L2)."""
        ownership = ResourceOwnership(
            client=Ownership.BORROWED if self._borrowed else Ownership.OWNED
        )
        return ownership

    def _current(self) -> _Connection:
        """This process's connection slot, fresh after a fork (L8)."""
        if self._process.pid != os.getpid():
            with _fork_lock:
                if (inherited := self._process).pid != os.getpid():
                    if inherited.connection is not None:
                        _abandoned.append(inherited.connection)
                    else:
                        pass
                    self._process = _Connection()
                    self._borrowed = False
                    self._initialized = False
                else:
                    pass
        else:
            pass
        return self._process

    def _discard(self, process: _Connection[ConnectionT]) -> ConnectionT | None:
        """Forget a connection that cannot be trusted, returning it to close if it is owned.

        A borrowed connection is never closed or replaced; it stays, and the
        next call refuses it until its owner repairs it.
        """
        connection = process.connection
        self._initialized = False
        if self._borrowed:
            stale = None
        else:
            process.connection = None
            stale = connection
        return stale

    def _replaceable(self, process: _Connection[ConnectionT]) -> ConnectionT | None:
        """The unusable owned connection to close before reconnecting, if any.

        :raises ~procrastinators.errors.BackendUnavailable: The connection is borrowed.
        """
        if self._borrowed:
            raise BackendUnavailable(
                f"the injected connection of {self._identity} is closed or mid-transaction, "
                "and a borrowed connection is never replaced"
            )
        else:
            stale = self._discard(process)
        return stale

    def __repr__(self) -> str:
        text = f"{type(self).__name__}({self._identity})"
        return text


class PostgresBackend(_PostgresHandle["psycopg.Connection[Any]"], BaseSyncBackend):
    """A synchronous handle on a :class:`PostgresStore`, with a connection of its own.

    Satisfies :class:`~procrastinators.protocols.SyncBackend`,
    :class:`~procrastinators.protocols.SupportsCooldown`, and
    :class:`~procrastinators.protocols.SupportsPolicyAdministration`. Threads
    sharing a handle take turns on its connection, waiting at most their lock
    budget; the connection is opened on first use. Closing a handle closes an
    owned connection and never touches quota state (L3).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, for deterministic tests.
    :param connection: A ``psycopg.Connection`` in autocommit mode to borrow; one opened
        from the store's address and owned when ``None``.
    """

    _mode = Mode.SYNC

    def __init__(
        self,
        store: PostgresStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        connection: psycopg.Connection[Any] | None = None,
    ) -> None:
        super().__init__(store, namespace=namespace, observer=observer, connection=connection)
        self._turn = threading.Lock()

    @staticmethod
    def _drive(
        connection: psycopg.Connection[Any], operation: Generator[Query, Result, ResultT]
    ) -> ResultT:
        try:
            query = next(operation)
            while True:
                try:
                    cursor = connection.execute(
                        cast("LiteralString", query.sql), query.params or None
                    )
                    rows = cursor.fetchall() if cursor.description is not None else list()
                    result = Result(rows, cursor.rowcount)
                except BaseException as error:
                    query = operation.throw(error)
                else:
                    query = operation.send(result)
        except StopIteration as stop:
            value = stop.value
        return value

    def _connection(self, process: _Connection[psycopg.Connection[Any]]) -> psycopg.Connection[Any]:
        if (connection := process.connection) is None or not _usable(connection):
            if (stale := self._replaceable(process)) is not None:
                with contextlib.suppress(_driver().Error):
                    stale.close()
            else:
                pass
            driver = _driver()
            try:
                connection = driver.connect(
                    self._store.conninfo,
                    autocommit=True,
                    connect_timeout=self._store.connect_timeout,
                )
            except driver.Error as error:
                raise _translated(error, f"connecting to {self._store.authority}") from error
            process.connection = connection
        else:
            pass
        if not self._initialized:
            self._drive(connection, self._store.initialize())
            self._initialized = True
        else:
            pass
        return connection

    def _execute(
        self,
        operation: Generator[Query, Result, ResultT],
        lock_timeout_us: int = DEFAULT_TIMEOUT_US,
    ) -> ResultT:
        self._ensure_open()
        process = self._current()
        if not self._turn.acquire(timeout=lock_timeout_us / USECS_PER_SECOND):
            raise BackendBusy(
                f"{self._identity} was busy in another thread for {lock_timeout_us} µs; "
                "contention is not a denial (O2)"
            )
        else:
            pass
        try:
            self._ensure_open()
            connection = self._connection(process)
            try:
                result = self._drive(connection, operation)
            except Exception:
                raise
            except BaseException:
                # Interrupted mid-statement: the session cannot be trusted, and
                # closing it makes the server roll back.
                if (stale := self._discard(process)) is not None:
                    stale.close()
                else:
                    pass
                raise
        finally:
            self._turn.release()
        return result

    def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.BackendBusy: Rows stayed locked past the budget.
        :raises ~procrastinators.errors.BackendUnavailable: The database could not be used, or
            the observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit's outcome was lost, or
            the observer failed after it.
        :raises ~procrastinators.errors.StateCorruption: Stored state is malformed.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = self._execute(
            self._store.admit(request, self._observe, self._identity),
            request.budget.lock_timeout_us,
        )
        return decision

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        """
        snapshot = self._execute(self._store.inspect(rules, self._identity))
        return snapshot

    def defer_for(
        self, scope: QuotaIdentity, duration: DurationMicros, *, reason: str = ""
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on the cooldown.
        """
        cooldown = self._execute(self._store.defer(scope, duration, reason))
        return cooldown

    def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the database holds for ``rule``, if any.

        :param rule: The rule to look up.
        """
        stored = self._execute(self._store.stored_policy(rule))
        return stored

    def begin_migration(
        self, rule: RuleId, *, to_fingerprint: PolicyFingerprint, to_state_version: int
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        """
        migration = self._execute(
            self._store.begin_migration(rule, to_fingerprint, to_state_version)
        )
        return migration

    def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """
        migration = self._execute(self._store.migration_status(rule))
        return migration

    def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        """
        migration = self._execute(self._store.complete_migration(rule))
        return migration

    def sweep(self) -> int:
        """Forget state past its safe-forget horizon, and expired cooldowns.

        :returns: How many rules' state was forgotten.
        """
        forgotten = self._execute(self._store.sweep())
        return forgotten

    def close(self) -> None:
        """Close an owned connection, after any call in progress. Idempotent (L1, L2, L4).

        A borrowed connection is left open; quota state is untouched (L3).
        """
        if self._mark_closed():
            process = self._current()
            with self._turn:
                connection, process.connection = process.connection, None
                if connection is not None and not self._borrowed and process.pid == os.getpid():
                    connection.close()
                else:
                    pass
        else:
            pass


class AsyncPostgresBackend(_PostgresHandle["psycopg.AsyncConnection[Any]"], BaseAsyncBackend):
    """An asynchronous handle on a :class:`PostgresStore`, on psycopg's native async connection.

    Satisfies :class:`~procrastinators.protocols.AsyncBackend`,
    :class:`~procrastinators.protocols.SupportsAsyncCooldown`, and
    :class:`~procrastinators.protocols.SupportsAsyncPolicyAdministration`.
    Tasks sharing a handle take turns on its connection. A call cancelled
    mid-transaction closes an owned connection, so the server rolls back; one
    cancelled while ``COMMIT`` is in flight may have committed, which is
    possible consumed capacity, never permission (O5).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, for deterministic tests.
    :param connection: A ``psycopg.AsyncConnection`` in autocommit mode to borrow; one
        opened and owned when ``None``.
    """

    _mode = Mode.ASYNC

    def __init__(
        self,
        store: PostgresStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        connection: psycopg.AsyncConnection[Any] | None = None,
    ) -> None:
        super().__init__(store, namespace=namespace, observer=observer, connection=connection)
        self._turn: asyncio.Lock | None = None

    @staticmethod
    async def _drive(
        connection: psycopg.AsyncConnection[Any], operation: Generator[Query, Result, ResultT]
    ) -> ResultT:
        try:
            query = next(operation)
            while True:
                try:
                    cursor = await connection.execute(
                        cast("LiteralString", query.sql), query.params or None
                    )
                    rows = await cursor.fetchall() if cursor.description is not None else list()
                    result = Result(rows, cursor.rowcount)
                except BaseException as error:
                    query = operation.throw(error)
                else:
                    query = operation.send(result)
        except StopIteration as stop:
            value = stop.value
        return value

    async def _connection(
        self, process: _Connection[psycopg.AsyncConnection[Any]]
    ) -> psycopg.AsyncConnection[Any]:
        if (connection := process.connection) is None or not _usable(connection):
            if (stale := self._replaceable(process)) is not None:
                with contextlib.suppress(_driver().Error):
                    await stale.close()
            else:
                pass
            driver = _driver()
            try:
                connection = await driver.AsyncConnection.connect(
                    self._store.conninfo,
                    autocommit=True,
                    connect_timeout=self._store.connect_timeout,
                )
            except driver.Error as error:
                raise _translated(error, f"connecting to {self._store.authority}") from error
            process.connection = connection
        else:
            pass
        if not self._initialized:
            await self._drive(connection, self._store.initialize())
            self._initialized = True
        else:
            pass
        return connection

    async def _execute(
        self,
        operation: Generator[Query, Result, ResultT],
        lock_timeout_us: int = DEFAULT_TIMEOUT_US,
    ) -> ResultT:
        self._ensure_open()
        process = self._current()
        if self._turn is None:
            self._turn = asyncio.Lock()
        else:
            pass
        turn = self._turn
        try:
            async with asyncio.timeout(lock_timeout_us / USECS_PER_SECOND):
                await turn.acquire()
        except TimeoutError as error:
            raise BackendBusy(
                f"{self._identity} was busy in another task for {lock_timeout_us} µs; "
                "contention is not a denial (O2)"
            ) from error
        try:
            self._ensure_open()
            connection = await self._connection(process)
            try:
                result = await self._drive(connection, operation)
            except Exception:
                raise
            except BaseException:
                # Cancelled mid-statement: the session cannot be trusted, and
                # closing it makes the server roll back.
                if (stale := self._discard(process)) is not None:
                    await stale.close()
                else:
                    pass
                raise
        finally:
            turn.release()
        return result

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        As :meth:`PostgresBackend.admit`, awaited.

        :param request: The request.
        :returns: The decision; a denial is a value.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = await self._execute(
            self._store.admit(request, self._observe, self._identity),
            request.budget.lock_timeout_us,
        )
        return decision

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        """
        snapshot = await self._execute(self._store.inspect(rules, self._identity))
        return snapshot

    async def defer_for(
        self, scope: QuotaIdentity, duration: DurationMicros, *, reason: str = ""
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on the cooldown.
        """
        cooldown = await self._execute(self._store.defer(scope, duration, reason))
        return cooldown

    async def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the database holds for ``rule``, if any.

        :param rule: The rule to look up.
        """
        stored = await self._execute(self._store.stored_policy(rule))
        return stored

    async def begin_migration(
        self, rule: RuleId, *, to_fingerprint: PolicyFingerprint, to_state_version: int
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        """
        migration = await self._execute(
            self._store.begin_migration(rule, to_fingerprint, to_state_version)
        )
        return migration

    async def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """
        migration = await self._execute(self._store.migration_status(rule))
        return migration

    async def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        """
        migration = await self._execute(self._store.complete_migration(rule))
        return migration

    async def sweep(self) -> int:
        """Forget state past its safe-forget horizon, and expired cooldowns.

        :returns: How many rules' state was forgotten.
        """
        forgotten = await self._execute(self._store.sweep())
        return forgotten

    async def aclose(self) -> None:
        """Close an owned connection once calls in progress settle. Idempotent (L1, L2, L4).

        A borrowed connection is left open; quota state is untouched (L3).
        """
        if self._mark_closed():
            process = self._current()
            if (turn := self._turn) is not None:
                await turn.acquire()
            else:
                pass
            try:
                connection, process.connection = process.connection, None
                if connection is not None and not self._borrowed and process.pid == os.getpid():
                    await connection.close()
                else:
                    pass
            finally:
                if turn is not None:
                    turn.release()
                else:
                    pass
        else:
            pass


_STORE_OPTIONS: Final = ("prefix", "schema")


def postgres_backend(
    address: str,
    *,
    mode: Mode,
    namespace: str = "default",
    algorithms: Iterable[Algorithm[Any]] = tuple(),
) -> PostgresBackend | AsyncPostgresBackend:
    """A handle on the PostgreSQL database an address names.

    ``postgresql://user:password@host:5432/dbname``, or ``postgres://``.
    Query parameters ``prefix`` and ``schema`` configure the store; any others
    are passed to libpq. Registered as the factory of the ``postgresql`` and
    ``postgres`` families.

    :param address: The database's address, credentials included if needed; they never
        reach the handle's identity.
    :param mode: Which interface the handle offers.
    :param namespace: The handle's quota namespace.
    :param algorithms: Algorithms the store must host, beyond the reference algorithms.
    :raises ~procrastinators.errors.ConfigurationError: The address is malformed.
    """
    parts = urllib.parse.urlsplit(address)
    if parts.scheme not in SCHEMES:
        raise ConfigurationError(f"not a postgresql address: {address!r}")
    elif parts.fragment:
        raise ConfigurationError(f"a postgresql address carries no fragment: {address!r}")
    else:
        pass
    settings = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    options = {name: settings.pop(name) for name in _STORE_OPTIONS if name in settings}
    # Built by hand: urlunsplit drops the empty host of postgresql:///dbname.
    # Percent-encoded, never "+" for a space, which libpq reads literally.
    query = urllib.parse.urlencode(settings, quote_via=urllib.parse.quote)
    rebuilt = f"postgresql://{parts.netloc}{parts.path}" + (f"?{query}" if query else "")
    store = PostgresStore(
        rebuilt,
        prefix=options.get("prefix", DEFAULT_PREFIX),
        schema=options.get("schema"),
    )
    store.host(algorithms)
    if mode is Mode.SYNC:
        handle: PostgresBackend | AsyncPostgresBackend = PostgresBackend(store, namespace=namespace)
    else:
        handle = AsyncPostgresBackend(store, namespace=namespace)
    return handle


if __name__ == "__main__":
    pass
else:
    pass
