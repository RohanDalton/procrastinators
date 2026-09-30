"""The SQLite backend: one database file coordinating every process on a machine.

A :class:`SQLiteStore` names the authority — a database file — and the
algorithms it enforces. Handles address it: :class:`SQLiteBackend`
synchronously, :class:`AsyncSQLiteBackend` from an event loop through a
:class:`~procrastinators.backends.executor.DedicatedExecutor`. Every handle
owns its own connection, and any number of handles, threads, and independently
started processes sharing the file obey its quotas together.

**One write transaction.** Admission begins with ``BEGIN IMMEDIATE``, which
takes the database's write lock *before* anything is read, so no other
connection can slip a commit between the check and the debit. Inside it the
store samples authority time, checks policy metadata, loads every rule's
state, evaluates every rule against that one sample, and writes every debit or
none (contracts A1, T4, A6). It commits before reporting success. A write lock
that stays busy past the request's lock budget is
:exc:`~procrastinators.errors.BackendBusy`, never a denial (O2).

**Time.** Authority time is the machine's wall clock, which every process on
it shares, clamped against the latest time any process recorded in the file so
it never runs backwards (T7). Fixed windows therefore align to the Unix epoch
for every process alike (T8).

**Durability.** Connections run with ``synchronous=FULL`` and switch the
file to write-ahead logging when they can (any journal mode is correct, since
``BEGIN IMMEDIATE`` serializes writers either way): an admission reported as
committed survives a process crash and, on storage that honors ``fsync``, a
power loss. ``synchronous="normal"`` may be chosen
explicitly; it survives process crashes but can lose the latest commits on
power loss, which would forget admissions that happened.

**Scope.** The guarantee is :attr:`~procrastinators.models.CoordinationScope.LOCAL_MACHINE`:
processes on one machine reaching the same file on a local filesystem. SQLite's
locking is not reliable over network filesystems, so a file there is not
supported, and workers on other machines need a service backend.

**Schema.** Tables carry a ``procrastinators_`` prefix, hold rules by an
indexed integer id, and keep scalar state, weighted log events (one row per
admission, whatever its cost — P6), cooldowns, policy metadata, and migration
status apart. The schema is versioned; a file written by another version is
refused rather than reinterpreted. Initialization runs inside a write
transaction, so processes racing to create a fresh file agree on one schema.

**Cleanup.** State is forgotten only past the safe-forget horizon its
algorithm reported (L9), on a bounded cadence, and policy metadata is never
forgotten with it (L10).

**Fork.** A connection is never carried across ``fork``. A handle used in a
forked child opens a fresh connection there and coordinates with its parent
through the file; the inherited connection is abandoned, not closed, since
closing it in the child could disturb the parent's locks (L8).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import contextlib
import dataclasses
import functools
import os
import sqlite3
import threading
import time
import urllib.parse
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, TypeVar

import platformdirs

from procrastinators.algorithms import builtin_specs, reference_algorithms
from procrastinators.backends import bookkeeping
from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.backends.executor import DEFAULT_MAX_PENDING, DedicatedExecutor
from procrastinators.capabilities import Mode
from procrastinators.clocks import SystemClock
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
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

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
    "DEFAULT_DATABASE_NAME",
    "SCHEMA_VERSION",
    "SQLITE_CAPABILITIES",
    "AsyncSQLiteBackend",
    "SQLiteBackend",
    "SQLiteStore",
    "default_database_path",
    "sqlite_backend",
]

FAMILY: Final = "sqlite"
"""The backend family, as it appears in identities and ``sqlite://`` addresses."""

APPLICATION: Final = "procrastinators"
"""The PlatformDirs application name the default database lives under."""

DEFAULT_DATABASE_NAME: Final = "limits.sqlite3"
"""The default database's file name, inside the user state directory."""

SCHEMA_VERSION: Final = 1
"""The version of the tables this module reads and writes."""

DEFAULT_TIMEOUT_US: Final = DurationMicros(5 * USECS_PER_SECOND)
"""Lock and storage budget for inspection, cooldowns, and administration."""

DEFAULT_SWEEP_EVERY: Final = 1024
"""Commits, per process, between sweeps of state past its safe-forget horizon."""

Synchronous = Literal["full", "normal"]
"""The ``PRAGMA synchronous`` levels a store may run at; see the module notes."""

_PROGRESS_STEPS: Final = 1_000
_NANOS_PER_MICRO: Final = 1_000
_MICROS_PER_MILLI: Final = 1_000

# Primary result codes; the extended code's low byte.
_SQLITE_BUSY: Final = 5
_SQLITE_LOCKED: Final = 6
_SQLITE_INTERRUPT: Final = 9
_SQLITE_CORRUPT: Final = 11
_SQLITE_NOTADB: Final = 26

_META: Final = "procrastinators_meta"
_RULES: Final = "procrastinators_rules"
_POLICIES: Final = "procrastinators_policies"
_STATES: Final = "procrastinators_states"
_SCALARS: Final = "procrastinators_scalars"
_EVENTS: Final = "procrastinators_events"
_COOLDOWNS: Final = "procrastinators_cooldowns"
_MIGRATIONS: Final = "procrastinators_migrations"

_SCHEMA: Final = (
    f"CREATE TABLE {_META} (name TEXT PRIMARY KEY, value INTEGER NOT NULL) WITHOUT ROWID",
    f"CREATE TABLE {_RULES} (id INTEGER PRIMARY KEY, namespace TEXT NOT NULL, "
    "quota TEXT NOT NULL, name TEXT NOT NULL, UNIQUE (namespace, quota, name))",
    f"CREATE TABLE {_POLICIES} (rule_id INTEGER PRIMARY KEY REFERENCES {_RULES} (id), "
    "algorithm TEXT NOT NULL, fingerprint TEXT NOT NULL, state_version INTEGER NOT NULL, "
    "updated_at INTEGER NOT NULL)",
    # A row means the rule's state exists; a null horizon means it never becomes neutral.
    f"CREATE TABLE {_STATES} (rule_id INTEGER PRIMARY KEY REFERENCES {_RULES} (id), "
    "horizon INTEGER)",
    f"CREATE INDEX {_STATES}_by_horizon ON {_STATES} (horizon)",
    f"CREATE TABLE {_SCALARS} (rule_id INTEGER NOT NULL REFERENCES {_RULES} (id), "
    "name TEXT NOT NULL, value INTEGER NOT NULL, PRIMARY KEY (rule_id, name)) WITHOUT ROWID",
    f"CREATE TABLE {_EVENTS} (id INTEGER PRIMARY KEY, "
    f"rule_id INTEGER NOT NULL REFERENCES {_RULES} (id), at INTEGER NOT NULL, "
    "cost INTEGER NOT NULL)",
    f"CREATE INDEX {_EVENTS}_by_rule ON {_EVENTS} (rule_id, at)",
    f"CREATE TABLE {_COOLDOWNS} (namespace TEXT NOT NULL, quota TEXT NOT NULL, "
    "until INTEGER NOT NULL, reason TEXT NOT NULL, PRIMARY KEY (namespace, quota)) WITHOUT ROWID",
    f"CREATE INDEX {_COOLDOWNS}_by_until ON {_COOLDOWNS} (until)",
    f"CREATE TABLE {_MIGRATIONS} (rule_id INTEGER PRIMARY KEY REFERENCES {_RULES} (id), "
    "status TEXT NOT NULL, from_fingerprint TEXT, to_fingerprint TEXT, "
    "to_state_version INTEGER, drained_after INTEGER)",
)

SQLITE_CAPABILITIES: Final = Capabilities(
    algorithms=frozenset(spec.id for spec in builtin_specs()),
    coordination=CoordinationScope.LOCAL_MACHINE,
    durability=Durability.LOCAL_DURABLE,
    supports_sync=True,
    supports_async=True,
    supports_composition=True,
    supports_cooldowns=True,
    supports_policy_administration=True,
    state_representations=frozenset(StateRepresentation),
)
"""What the SQLite family declares before any store exists; a store adds hosted algorithms."""

ResultT = TypeVar("ResultT")

_stores_lock: Final = threading.Lock()
_stores: Final[dict[tuple[int, Path], SQLiteStore]] = dict()
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


def default_database_path() -> Path:
    """The database ``sqlite://`` names: ``limits.sqlite3`` in the user state directory.

    Stable across runs and processes for one OS user, and never a temporary or
    cache location that could be cleared underneath a quota. Computed only;
    nothing is created until a handle first connects.
    """
    path = platformdirs.user_state_path(APPLICATION) / DEFAULT_DATABASE_NAME
    return path


def _resolved(path: str | os.PathLike[str]) -> Path:
    # Resolved once, so a worker that later changes directory still addresses
    # the same file, and two spellings of one file share one identity.
    given = Path(path).expanduser()
    absolute = given if given.is_absolute() else Path.cwd() / given
    resolved = absolute.resolve()
    return resolved


def _error_code(error: sqlite3.Error) -> int | None:
    # The extended result code's low byte is the primary code; Python 3.11
    # and later attach it to every error raised by SQLite itself.
    code = getattr(error, "sqlite_errorcode", None)
    primary = code & 0xFF if isinstance(code, int) else None
    return primary


def _translated(error: sqlite3.Error, what: str) -> BackendError:
    """The library error a driver error before any commit means; nothing was committed."""
    code = _error_code(error)
    if code in (_SQLITE_BUSY, _SQLITE_LOCKED):
        translated: BackendError = BackendBusy(
            f"{what}: the database stayed locked past its budget; contention is not a denial (O2)",
            cause=error,
        )
    elif code == _SQLITE_INTERRUPT:
        translated = BackendUnavailable(
            f"{what}: the storage call exceeded its timeout and was interrupted (B3)",
            cause=error,
        )
    elif code in (_SQLITE_CORRUPT, _SQLITE_NOTADB):
        translated = StateCorruption(f"{what}: the database file is corrupt: {error}", cause=error)
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


class SQLiteStore:
    """A database file as an admission authority, and the algorithms it enforces.

    Constructing a store resolves its path and touches nothing else: no file,
    connection, or thread exists until a handle first connects. It hosts the
    five reference algorithms unless given others, and more can be added with
    :meth:`host`. A process-wide store per path is available from :meth:`at`.

    :param path: The database file; the stable default of
        :func:`default_database_path` when ``None``. A relative path is resolved
        once, against the current directory, now.
    :param algorithms: The algorithms to host; the reference algorithms when ``None``.
    :param clock: Authority epoch time, sampled inside each transaction; the
        system clock when ``None``. Every process sharing the file must read the
        same clock, which the machine's wall clock is.
    :param representations: State representations to accept beyond scalars and
        event logs, for third-party algorithms.
    :param sweep_every: Commits, in this process, between sweeps of forgettable state.
    :param synchronous: ``"full"`` (the default) or ``"normal"``; see the module notes.
    :param create_directory: Whether to create the file's missing parent
        directories on first connection; the default for the default path only.
    :raises ~procrastinators.errors.ConfigurationError: A setting is invalid, or two hosted
        algorithms share an id.
    """

    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        algorithms: Iterable[Algorithm[Any]] | None = None,
        clock: AdmissionClock | None = None,
        representations: Iterable[str] = tuple(),
        sweep_every: int = DEFAULT_SWEEP_EVERY,
        synchronous: Synchronous = "full",
        create_directory: bool | None = None,
    ) -> None:
        if isinstance(sweep_every, bool) or not isinstance(sweep_every, int) or sweep_every < 1:
            raise ConfigurationError(f"sweep_every must be a positive integer, got {sweep_every!r}")
        elif synchronous not in ("full", "normal"):
            raise ConfigurationError(
                f"synchronous must be 'full' or 'normal', got {synchronous!r}; 'off' would "
                "report admissions a crash can forget"
            )
        else:
            pass
        self._path = default_database_path() if path is None else _resolved(path)
        if str(self._path) == ":memory:" or self._path.name == ":memory:":
            raise ConfigurationError(
                "an in-memory SQLite database coordinates nothing; use backend='memory://'"
            )
        else:
            pass
        self._create_directory = path is None if create_directory is None else create_directory
        self._clock: AdmissionClock = clock or SystemClock()
        self._sweep_every = sweep_every
        self._synchronous = synchronous
        self._representations = frozenset({*StateRepresentation, *representations})
        self._lock = threading.Lock()
        self._algorithms: dict[str, Algorithm[Any]] = dict()
        self._capabilities: dict[Mode, Capabilities] = dict()
        self._constraints: dict[RuleId, Constraint] = dict()
        self._commits = 0
        self.host(reference_algorithms() if algorithms is None else algorithms)

    @classmethod
    def at(cls, path: str | os.PathLike[str] | None = None) -> SQLiteStore:
        """The process-wide store for ``path``, created on first use.

        This is what ``sqlite://`` addresses resolve to, so the handles of
        separately constructed limiters naming one file share hosted algorithms.
        The file, not this object, is the authority: stores for one path in
        different processes coordinate through it.

        :param path: The database file; the default database when ``None``.
        """
        resolved = default_database_path() if path is None else _resolved(path)
        key = (os.getpid(), resolved)
        with _stores_lock:
            if (store := _stores.get(key)) is None:
                store = cls(None if path is None else resolved)
                _stores[key] = store
            else:
                pass
        return store

    @property
    def path(self) -> Path:
        """The absolute database path, resolved at construction."""
        return self._path

    @property
    def authority(self) -> str:
        """The store's canonical address: its path, percent-encoded where needed."""
        authority = urllib.parse.quote(self._path.as_posix(), safe="/")
        return authority

    def identity(self, namespace: str) -> BackendIdentity:
        """The identity of a handle using ``namespace`` on this store.

        :param namespace: The handle's quota namespace.
        :raises ~procrastinators.errors.InvalidPolicy: The path or namespace is not a valid name.
        """
        identity = BackendIdentity(FAMILY, self.authority, namespace)
        return identity

    def host(self, algorithms: Iterable[Algorithm[Any]]) -> None:
        """Host more algorithms, keeping those already hosted.

        As :meth:`~procrastinators.backends.memory.MemoryStore.host`: the same
        algorithm again is harmless, another implementation under a hosted id
        is refused.

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
                    SQLITE_CAPABILITIES,
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
                f"SQLite store {self._path} does not host algorithm {constraint.algorithm!r}"
            )
        else:
            pass
        return algorithm

    def connect(self) -> sqlite3.Connection:
        """Open, configure, and if need be initialize a connection to the file.

        Initialization happens inside a write transaction, so processes racing
        to create a fresh file agree on one schema.

        :returns: A connection in autocommit mode, whose transactions this store manages.
        :raises ~procrastinators.errors.BackendUnavailable: The file cannot be opened, or its
            directory does not exist and is not to be created.
        :raises ~procrastinators.errors.BackendBusy: The file stayed locked during setup.
        :raises ~procrastinators.errors.StateCorruption: The file is not a SQLite database.
        :raises ~procrastinators.errors.UnsupportedCapability: The file was written with
            another schema version.
        """
        directory = self._path.parent
        if self._create_directory:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                raise BackendUnavailable(
                    f"cannot create the directory for {self._path}: {error}", cause=error
                ) from error
        elif not directory.is_dir():
            raise BackendUnavailable(
                f"the directory for SQLite database {self._path} does not exist"
            )
        else:
            pass
        try:
            connection = sqlite3.connect(
                self._path, timeout=0, isolation_level=None, check_same_thread=False
            )
        except sqlite3.Error as error:
            raise _translated(error, f"opening {self._path}") from error
        try:
            _busy_timeout(connection, DEFAULT_TIMEOUT_US)
            _prefer_wal(connection)
            connection.execute(f"PRAGMA synchronous = {self._synchronous.upper()}")
            self._initialize(connection)
        except sqlite3.Error as error:
            connection.close()
            raise _translated(error, f"initializing {self._path}") from error
        except BaseException:
            connection.close()
            raise
        return connection

    def _initialize(self, connection: sqlite3.Connection) -> None:
        # An existing schema is only read, so opening a connection never
        # competes with admissions for the write lock. Creating one happens
        # under the write lock with the check repeated, so racing creators
        # agree on a single schema.
        if (version := _schema_version(connection)) is None:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if (version := _schema_version(connection)) is None:
                    for statement in _SCHEMA:
                        connection.execute(statement)
                    connection.executemany(
                        f"INSERT INTO {_META} (name, value) VALUES (?, ?)",
                        (("schema_version", SCHEMA_VERSION), ("last_now", 0)),
                    )
                    version = SCHEMA_VERSION
                else:
                    pass
            except BaseException:
                _rollback(connection)
                raise
            connection.execute("COMMIT")
        else:
            pass
        if version != SCHEMA_VERSION:
            raise UnsupportedCapability(
                f"{self._path} holds procrastinators schema version {version}, and this "
                f"library reads version {SCHEMA_VERSION}; it is refused rather than "
                "reinterpreted"
            )
        else:
            pass

    @contextlib.contextmanager
    def _transaction(
        self,
        connection: sqlite3.Connection,
        what: str,
        *,
        write: bool,
        lock_timeout_us: int = DEFAULT_TIMEOUT_US,
        storage_timeout_us: int = DEFAULT_TIMEOUT_US,
    ) -> Iterator[None]:
        """A transaction for everything but admission, whose commit rules are its own."""
        _begin(connection, what, write=write, lock_timeout_us=lock_timeout_us)
        _deadline(connection, storage_timeout_us)
        try:
            yield
        except sqlite3.Error as error:
            _rollback(connection)
            raise _translated(error, what) from error
        except BaseException:
            _rollback(connection)
            raise
        finally:
            connection.set_progress_handler(None, 0)
        try:
            connection.execute("COMMIT")
        except sqlite3.Error as error:
            _rollback(connection)
            raise _translated(error, what) from error

    def _now(self, connection: sqlite3.Connection, *, record: bool) -> EpochMicros:
        # Clamped against the latest time any process recorded, so authority
        # time never runs backwards, whichever process samples it (T7).
        row = connection.execute(f"SELECT value FROM {_META} WHERE name = 'last_now'").fetchone()
        last = _timestamp(row[0] if row is not None else 0, what="last observed time")
        now = EpochMicros(max(self._clock.now(), last))
        if record and now > last:
            connection.execute(f"UPDATE {_META} SET value = ? WHERE name = 'last_now'", (now,))
        else:
            pass
        return now

    def admit(
        self,
        connection: sqlite3.Connection,
        request: AdmissionRequest,
        observe: Callable[[ObservationPoint, AdmissionRequest], None],
        identity: BackendIdentity,
    ) -> Decision:
        """One admission, as one write transaction on ``connection``.

        :param connection: A connection from :meth:`connect`, used by no one else meanwhile.
        :param request: The request.
        :param observe: Reports each observation point inside the transaction.
        :param identity: The admitting handle's identity, recorded on the admission.
        :returns: The decision; a denial is a value, and was committed as one.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.BackendBusy: The write lock was not free within budget.
        :raises ~procrastinators.errors.BackendUnavailable: Storage failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit of an admission failed
            or the observer failed after it: the debit may be durable.
        :raises ~procrastinators.errors.StateCorruption: Stored state is malformed.
        """
        what = f"admitting on {self._path}"
        budget = request.budget
        _begin(connection, what, write=True, lock_timeout_us=budget.lock_timeout_us)
        _deadline(connection, budget.storage_timeout_us)
        try:
            now = self._now(connection, record=True)
            loaded = {rule: _load(connection, rule) for rule in request.rules}
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
            plan = plan_admission(
                request,
                {rule: entry.state for rule, entry in loaded.items()},
                self._resolve,
                now,
                holds=bookkeeping.cooldown_holds(request, _cooldowns(connection, request), now),
            )
            if plan.admitted:
                observe(ObservationPoint.BEFORE_COMMIT, request)
            else:
                pass
            self._write(connection, request, plan, loaded)
        except sqlite3.Error as error:
            _rollback(connection)
            raise _translated(error, what) from error
        except BaseException:
            _rollback(connection)
            raise
        finally:
            # Cleared before the commit: an interrupted COMMIT would leave its
            # outcome to guesswork.
            connection.set_progress_handler(None, 0)
        try:
            connection.execute("COMMIT")
        except sqlite3.Error as error:
            _rollback(connection)
            if plan.admitted and _error_code(error) not in (_SQLITE_BUSY, _SQLITE_LOCKED):
                raise IndeterminateAdmission(
                    f"{what}: the commit failed ({error}); the admission may have happened",
                    cause=error,
                    cost=request.cost,
                    rules=request.rules,
                ) from error
            else:
                raise _translated(error, what) from error
        for constraint in request.constraints:
            self._remember(constraint)
        if plan.admitted:
            admission = Admission(request.rules, request.cost, now, identity)
            observe(ObservationPoint.AFTER_COMMIT, request)
            decision = plan.decision(admission)
        else:
            decision = plan.decision()
        return decision

    def _remember(self, constraint: Constraint) -> None:
        with self._lock:
            self._constraints[constraint.rule] = constraint

    def _write(
        self,
        connection: sqlite3.Connection,
        request: AdmissionRequest,
        plan: AdmissionPlan,
        loaded: Mapping[RuleId, bookkeeping.LoadedRule],
    ) -> None:
        # Policy metadata is recorded on first contact, admitted or not, and
        # outlives quota state, so a later disagreement is always detectable (L10).
        for constraint in request.constraints:
            entry = loaded[constraint.rule]
            if entry.stored is None:
                entry.rule_id = _rule_id(connection, constraint.rule, entry.rule_id)
                connection.execute(
                    f"INSERT OR REPLACE INTO {_POLICIES} "
                    "(rule_id, algorithm, fingerprint, state_version, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
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
            entry = loaded[rule]
            entry.rule_id = _rule_id(connection, rule, entry.rule_id)
            _write_state(connection, entry, state, horizons[rule])
        with self._lock:
            self._commits += 1
            sweep = self._commits % self._sweep_every == 0
        if sweep:
            _sweep(connection, plan.now)
        else:
            pass

    def sweep(self, connection: sqlite3.Connection) -> int:
        """Forget every rule's state past its safe-forget horizon, and expired cooldowns.

        Admission already ignores such state; this reclaims its space. Policy
        metadata is kept (L10).

        :param connection: A connection from :meth:`connect`.
        :returns: How many rules' state was forgotten.
        """
        with self._transaction(connection, f"sweeping {self._path}", write=True):
            forgotten = _sweep(connection, self._now(connection, record=True))
        return forgotten

    def inspect(
        self, connection: sqlite3.Connection, rules: Sequence[RuleId], identity: BackendIdentity
    ) -> Snapshot:
        """Advisory observation of ``rules`` in a read transaction. Never a reservation (R7).

        A rule this process has admitted is evaluated for a cost of one; one
        known only from the file reports its algorithm without a remainder,
        since its policy is not stored; one never seen reports ``unused``.

        :param connection: A connection from :meth:`connect`.
        :param rules: The rules to observe.
        :param identity: The observing handle's identity.
        """
        with self._transaction(connection, f"inspecting {self._path}", write=False):
            now = self._now(connection, record=False)
            snapshots = list()
            for rule in rules:
                entry = _load(connection, rule)
                with self._lock:
                    known = self._constraints.get(rule)
                constraint = (
                    known
                    if known is not None
                    and entry.stored is not None
                    and entry.stored.fingerprint == known.fingerprint
                    else None
                )
                snapshots.append(
                    bookkeeping.rule_snapshot(
                        rule,
                        constraint=constraint,
                        algorithm=entry.stored.algorithm_id if entry.stored is not None else None,
                        state=entry.state,
                        horizon=entry.horizon,
                        cooldown_until=_cooldown_until(connection, rule.scope),
                        resolve=self._resolve,
                        now=now,
                    )
                )
        snapshot = Snapshot(rules=tuple(snapshots), sampled_at=now, backend=identity)
        return snapshot

    def defer(
        self,
        connection: sqlite3.Connection,
        scope: QuotaIdentity,
        duration: DurationMicros,
        reason: str,
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration`` in one write transaction.

        :param connection: A connection from :meth:`connect`.
        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on a cooldown this call creates or lengthens.
        :raises ~procrastinators.errors.InvalidPolicy: ``duration`` is out of range.
        """
        with self._transaction(connection, f"deferring on {self._path}", write=True):
            row = connection.execute(
                f"SELECT until, reason FROM {_COOLDOWNS} WHERE namespace = ? AND quota = ?",
                (scope.namespace, scope.key),
            ).fetchone()
            existing = (
                None
                if row is None
                else Cooldown(scope, _timestamp(row[0], what="cooldown end"), str(row[1]))
            )
            cooldown = bookkeeping.extend_cooldown(
                existing, scope, duration, reason, self._now(connection, record=True)
            )
            if cooldown is not existing:
                connection.execute(
                    f"INSERT OR REPLACE INTO {_COOLDOWNS} (namespace, quota, until, reason) "
                    "VALUES (?, ?, ?, ?)",
                    (scope.namespace, scope.key, cooldown.until, cooldown.reason),
                )
            else:
                pass
        return cooldown

    def stored_policy(self, connection: sqlite3.Connection, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the file holds for ``rule``, if any.

        :param connection: A connection from :meth:`connect`.
        :param rule: The rule to look up.
        """
        with self._transaction(connection, f"reading policy on {self._path}", write=False):
            stored = _load(connection, rule).stored
        return stored

    def begin_migration(
        self,
        connection: sqlite3.Connection,
        rule: RuleId,
        to_fingerprint: PolicyFingerprint,
        to_state_version: int,
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        As :meth:`~procrastinators.backends.memory.MemoryStore.begin_migration_locked`:
        every migration drains, and active quota history is never deleted early.

        :param connection: A connection from :meth:`connect`.
        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
        """
        with self._transaction(connection, f"migrating on {self._path}", write=True):
            now = self._now(connection, record=True)
            entry = _load(connection, rule)
            migration = bookkeeping.begin_migration(
                rule, entry.migration, current=entry.current, to_fingerprint=to_fingerprint
            )
            if migration is not entry.migration:
                entry.migration = migration
                entry.target_version = to_state_version
            else:
                pass
            refreshed = self._refresh(connection, rule, entry, now)
        assert refreshed is not None
        return refreshed

    def _refresh(
        self,
        connection: sqlite3.Connection,
        rule: RuleId,
        entry: bookkeeping.LoadedRule,
        now: EpochMicros,
    ) -> PolicyMigration | None:
        migration = bookkeeping.refresh_migration(
            entry.migration, neutral=entry.neutral(now), horizon=entry.horizon, now=now
        )
        if migration is not None:
            entry.rule_id = _rule_id(connection, rule, entry.rule_id)
            _write_migration(connection, entry.rule_id, migration, entry.target_version)
        else:
            pass
        return migration

    def migration_status(
        self, connection: sqlite3.Connection, rule: RuleId
    ) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param connection: A connection from :meth:`connect`.
        :param rule: The rule whose migration to report.
        """
        with self._transaction(connection, f"reading migration on {self._path}", write=True):
            now = self._now(connection, record=True)
            migration = self._refresh(connection, rule, _load(connection, rule), now)
        return migration

    def complete_migration(self, connection: sqlite3.Connection, rule: RuleId) -> PolicyMigration:
        """Install the new policy on a drained rule.

        The old state is neutral by then, so forgetting it loses nothing. The
        first admission under the new policy records its metadata.

        :param connection: A connection from :meth:`connect`.
        :param rule: The rule whose migration to complete.
        :raises ~procrastinators.errors.PolicyConflict: No migration is ready for ``rule``.
        """
        with self._transaction(connection, f"completing migration on {self._path}", write=True):
            now = self._now(connection, record=True)
            entry = _load(connection, rule)
            migration = bookkeeping.complete_migration(
                rule, self._refresh(connection, rule, entry, now)
            )
            assert entry.rule_id is not None
            for table in (_SCALARS, _EVENTS, _STATES, _POLICIES):
                connection.execute(f"DELETE FROM {table} WHERE rule_id = ?", (entry.rule_id,))
            _write_migration(connection, entry.rule_id, migration, entry.target_version)
        with self._lock:
            self._constraints.pop(rule, None)
        return migration

    def __repr__(self) -> str:
        text = f"SQLiteStore({str(self._path)!r})"
        return text


def _busy_timeout(connection: sqlite3.Connection, timeout_us: int) -> None:
    milliseconds = max(1, -(-timeout_us // _MICROS_PER_MILLI))
    connection.execute(f"PRAGMA busy_timeout = {int(milliseconds)}")


def _schema_version(connection: sqlite3.Connection) -> object | None:
    """The recorded schema version, ``None`` when no schema exists, or what a stranger wrote."""
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (_META,)
    ).fetchone()
    if exists is None:
        version = None
    else:
        row = connection.execute(
            f"SELECT value FROM {_META} WHERE name = 'schema_version'"
        ).fetchone()
        version = row[0] if row is not None else "missing"
    return version


def _prefer_wal(connection: sqlite3.Connection) -> None:
    # Write-ahead logging lets readers proceed beside the writer, but admission
    # is correct in any journal mode: BEGIN IMMEDIATE serializes writers either
    # way. Switching needs the file to itself and reports busy at once, without
    # waiting, while another connection is setting up; the mode persists in the
    # file, so a later connection finishes the switch.
    (mode,) = connection.execute("PRAGMA journal_mode").fetchone()
    if str(mode).lower() == "wal":
        pass
    else:
        try:
            connection.execute("PRAGMA journal_mode = WAL").fetchone()
        except sqlite3.OperationalError as error:
            if _error_code(error) not in (_SQLITE_BUSY, _SQLITE_LOCKED):
                raise
            else:
                pass


def _begin(connection: sqlite3.Connection, what: str, *, write: bool, lock_timeout_us: int) -> None:
    # BEGIN IMMEDIATE takes the write lock before anything is read: the whole
    # point of the transaction is that no commit can land between check and debit.
    try:
        _busy_timeout(connection, lock_timeout_us)
        connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
    except sqlite3.Error as error:
        raise _translated(error, what) from error


def _deadline(connection: sqlite3.Connection, storage_timeout_us: int) -> None:
    deadline_ns = time.monotonic_ns() + storage_timeout_us * _NANOS_PER_MICRO

    def expired() -> int:
        interrupt = 1 if time.monotonic_ns() > deadline_ns else 0
        return interrupt

    connection.set_progress_handler(expired, _PROGRESS_STEPS)


def _rollback(connection: sqlite3.Connection) -> None:
    if connection.in_transaction:
        # A failed rollback leaves the connection unusable; the handle notices
        # the open transaction and reconnects, and the file stays consistent.
        with contextlib.suppress(sqlite3.Error):
            connection.execute("ROLLBACK")
    else:
        pass


def _rule_id(connection: sqlite3.Connection, rule: RuleId, known: int | None) -> int:
    if known is not None:
        rule_id = known
    else:
        values = (rule.scope.namespace, rule.scope.key, rule.name)
        connection.execute(
            f"INSERT OR IGNORE INTO {_RULES} (namespace, quota, name) VALUES (?, ?, ?)", values
        )
        (rule_id,) = connection.execute(
            f"SELECT id FROM {_RULES} WHERE namespace = ? AND quota = ? AND name = ?", values
        ).fetchone()
    return rule_id


def _load(connection: sqlite3.Connection, rule: RuleId) -> bookkeeping.LoadedRule:
    row = connection.execute(
        f"SELECT id FROM {_RULES} WHERE namespace = ? AND quota = ? AND name = ?",
        (rule.scope.namespace, rule.scope.key, rule.name),
    ).fetchone()
    if row is None:
        loaded = bookkeeping.LoadedRule(None, None, None, None, UNUSED, None)
    else:
        (rule_id,) = row
        loaded = _load_rule(connection, rule, rule_id)
    return loaded


def _load_rule(
    connection: sqlite3.Connection, rule: RuleId, rule_id: int
) -> bookkeeping.LoadedRule:
    policy = connection.execute(
        f"SELECT algorithm, fingerprint, state_version, updated_at FROM {_POLICIES} "
        "WHERE rule_id = ?",
        (rule_id,),
    ).fetchone()
    stored = (
        None
        if policy is None
        else StoredPolicy(
            rule,
            str(policy[0]),
            policy[1],
            _integer(policy[2], what="state version", low=1, high=0xFFFF),
            _timestamp(policy[3], what="policy update time"),
        )
    )
    migration, target = _load_migration(connection, rule, rule_id)
    state_row = connection.execute(
        f"SELECT horizon FROM {_STATES} WHERE rule_id = ?", (rule_id,)
    ).fetchone()
    if state_row is None:
        loaded = bookkeeping.LoadedRule(rule_id, stored, migration, target, UNUSED, None)
    else:
        horizon = None if state_row[0] is None else _timestamp(state_row[0], what="horizon")
        scalars = tuple(
            (
                str(name),
                _integer(value, what=f"scalar {name!r}", low=-MAX_EXACT_INT, high=MAX_EXACT_INT),
            )
            for name, value in connection.execute(
                f"SELECT name, value FROM {_SCALARS} WHERE rule_id = ? ORDER BY name", (rule_id,)
            )
        )
        event_rows: dict[tuple[int, int], list[int]] = dict()
        events = list()
        for event_id, at, cost in connection.execute(
            f"SELECT id, at, cost FROM {_EVENTS} WHERE rule_id = ? ORDER BY at, cost, id",
            (rule_id,),
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
    connection: sqlite3.Connection, rule: RuleId, rule_id: int
) -> tuple[PolicyMigration | None, int | None]:
    row = connection.execute(
        f"SELECT status, from_fingerprint, to_fingerprint, to_state_version, drained_after "
        f"FROM {_MIGRATIONS} WHERE rule_id = ?",
        (rule_id,),
    ).fetchone()
    if row is None:
        migration = None
        target = None
    else:
        status, from_fingerprint, to_fingerprint, to_state_version, drained_after = row
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
    return migration, target


def _write_migration(
    connection: sqlite3.Connection,
    rule_id: int,
    migration: PolicyMigration,
    target_version: int | None,
) -> None:
    connection.execute(
        f"INSERT OR REPLACE INTO {_MIGRATIONS} (rule_id, status, from_fingerprint, "
        "to_fingerprint, to_state_version, drained_after) VALUES (?, ?, ?, ?, ?, ?)",
        (
            rule_id,
            migration.status.value,
            migration.from_fingerprint,
            migration.to_fingerprint,
            target_version,
            migration.drained_after,
        ),
    )


def _write_state(
    connection: sqlite3.Connection,
    entry: bookkeeping.LoadedRule,
    state: RuleState,
    horizon: EpochMicros | None,
) -> None:
    """Bring one rule's stored rows from ``entry.state`` to ``state``, touching only changes."""
    rule_id = entry.rule_id
    if not state.exists:
        for table in (_SCALARS, _EVENTS, _STATES):
            connection.execute(f"DELETE FROM {table} WHERE rule_id = ?", (rule_id,))
    else:
        connection.execute(
            f"INSERT OR REPLACE INTO {_STATES} (rule_id, horizon) VALUES (?, ?)",
            (rule_id, horizon),
        )
        before = dict(entry.state.scalars)
        after = dict(state.scalars)
        connection.executemany(
            f"DELETE FROM {_SCALARS} WHERE rule_id = ? AND name = ?",
            ((rule_id, name) for name in before.keys() - after.keys()),
        )
        connection.executemany(
            f"INSERT OR REPLACE INTO {_SCALARS} (rule_id, name, value) VALUES (?, ?, ?)",
            ((rule_id, name, value) for name, value in after.items() if before.get(name) != value),
        )
        old = Counter((event.at, event.cost) for event in entry.state.events)
        new = Counter((event.at, event.cost) for event in state.events)
        connection.executemany(
            f"DELETE FROM {_EVENTS} WHERE id = ?",
            (
                (event_id,)
                for pair, count in (old - new).items()
                for event_id in entry.event_rows[pair][:count]
            ),
        )
        connection.executemany(
            f"INSERT INTO {_EVENTS} (rule_id, at, cost) VALUES (?, ?, ?)",
            (
                (rule_id, at, cost)
                for (at, cost), count in (new - old).items()
                for _ in range(count)
            ),
        )


def _sweep(connection: sqlite3.Connection, now: EpochMicros) -> int:
    forgettable = f"SELECT rule_id FROM {_STATES} WHERE horizon IS NOT NULL AND horizon <= ?"
    for table in (_SCALARS, _EVENTS):
        connection.execute(f"DELETE FROM {table} WHERE rule_id IN ({forgettable})", (now,))
    forgotten = connection.execute(
        f"DELETE FROM {_STATES} WHERE horizon IS NOT NULL AND horizon <= ?", (now,)
    ).rowcount
    connection.execute(f"DELETE FROM {_COOLDOWNS} WHERE until <= ?", (now,))
    return forgotten


def _cooldown_until(connection: sqlite3.Connection, scope: QuotaIdentity) -> EpochMicros | None:
    row = connection.execute(
        f"SELECT until FROM {_COOLDOWNS} WHERE namespace = ? AND quota = ?",
        (scope.namespace, scope.key),
    ).fetchone()
    until = None if row is None else _timestamp(row[0], what="cooldown end")
    return until


def _cooldowns(
    connection: sqlite3.Connection, request: AdmissionRequest
) -> dict[QuotaIdentity, EpochMicros]:
    cooldowns = {
        scope: until
        for scope in {rule.scope for rule in request.rules}
        if (until := _cooldown_until(connection, scope)) is not None
    }
    return cooldowns


@dataclass(slots=True)
class _Connection:
    """A handle's connection in one process, and the lock serializing its use there."""

    pid: int = field(default_factory=os.getpid)
    lock: threading.Lock = field(default_factory=threading.Lock)
    connection: sqlite3.Connection | None = None


class _SQLiteHandle:
    """What the sync and async handles share: the store, namespace, and observer."""

    _mode: ClassVar[Mode]

    def __init__(
        self,
        store: SQLiteStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
    ) -> None:
        self._store = store
        self._identity = store.identity(namespace)
        self._observer = observer
        self._process = _Connection()

    @property
    def store(self) -> SQLiteStore:
        """The authority this handle addresses."""
        return self._store

    @property
    def capabilities(self) -> Capabilities:
        """Local-machine, locally durable, composing, with cooldowns and policy administration."""
        capabilities = self._store.capabilities(self._mode)
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        """``sqlite://<absolute path>#<namespace>``."""
        return self._identity

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
                    self._forked()
                else:
                    pass
        else:
            pass
        return self._process

    def _forked(self) -> None:
        """Replace anything besides the connection that did not survive a fork."""

    def _use(
        self, process: _Connection, operation: Callable[[sqlite3.Connection], ResultT]
    ) -> ResultT:
        """Run ``operation`` on the process's connection, reconnecting if it was lost."""
        connection = process.connection
        if connection is None or connection.in_transaction:
            # An open transaction here means a rollback failed: that connection
            # cannot be trusted, so it is closed and replaced.
            if connection is not None:
                with contextlib.suppress(sqlite3.Error):
                    connection.close()
            else:
                pass
            connection = process.connection = self._store.connect()
        else:
            pass
        result = operation(connection)
        return result

    def _close_connection(self, process: _Connection) -> None:
        if process.connection is not None and process.pid == os.getpid():
            with contextlib.suppress(sqlite3.Error):
                process.connection.close()
            process.connection = None
        else:
            pass

    def __repr__(self) -> str:
        text = f"{type(self).__name__}({self._identity})"
        return text


class SQLiteBackend(_SQLiteHandle, BaseSyncBackend):
    """A synchronous handle on a :class:`SQLiteStore`, with a connection of its own.

    Satisfies :class:`~procrastinators.protocols.SyncBackend`,
    :class:`~procrastinators.protocols.SupportsCooldown`, and
    :class:`~procrastinators.protocols.SupportsPolicyAdministration`. Threads
    sharing a handle take turns on its connection, waiting at most their lock
    budget; the connection is opened on first use. Closing a handle closes its
    connection and never touches the file's quota state (L3).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, for deterministic tests.
    """

    _mode = Mode.SYNC

    def _run(
        self,
        operation: Callable[[sqlite3.Connection], ResultT],
        lock_timeout_us: int = DEFAULT_TIMEOUT_US,
    ) -> ResultT:
        self._ensure_open()
        process = self._current()
        if not process.lock.acquire(timeout=lock_timeout_us / USECS_PER_SECOND):
            raise BackendBusy(
                f"{self._identity} was busy in another thread for {lock_timeout_us} µs; "
                "contention is not a denial (O2)"
            )
        else:
            pass
        try:
            self._ensure_open()
            result = self._use(process, operation)
        finally:
            process.lock.release()
        return result

    def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.BackendBusy: The connection or the file's write lock was
            not free within budget.
        :raises ~procrastinators.errors.BackendUnavailable: The file could not be used, or the
            observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit failed, or the observer
            failed after it.
        :raises ~procrastinators.errors.StateCorruption: Stored state is malformed.
        :raises ~procrastinators.errors.UnsupportedCapability: An algorithm is not hosted.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = self._run(
            functools.partial(
                self._store.admit,
                request=request,
                observe=self._observe,
                identity=self._identity,
            ),
            request.budget.lock_timeout_us,
        )
        return decision

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        snapshot = self._run(
            functools.partial(self._store.inspect, rules=rules, identity=self._identity)
        )
        return snapshot

    def defer_for(
        self, scope: QuotaIdentity, duration: DurationMicros, *, reason: str = ""
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on the cooldown.
        :returns: The cooldown now in force, which may be longer than requested.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        cooldown = self._run(
            functools.partial(self._store.defer, scope=scope, duration=duration, reason=reason)
        )
        return cooldown

    def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the file holds for ``rule``, if any.

        :param rule: The rule to look up.
        """
        stored = self._run(functools.partial(self._store.stored_policy, rule=rule))
        return stored

    def begin_migration(
        self, rule: RuleId, *, to_fingerprint: PolicyFingerprint, to_state_version: int
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
        """
        migration = self._run(
            functools.partial(
                self._store.begin_migration,
                rule=rule,
                to_fingerprint=to_fingerprint,
                to_state_version=to_state_version,
            )
        )
        return migration

    def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """
        migration = self._run(functools.partial(self._store.migration_status, rule=rule))
        return migration

    def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready.
        """
        migration = self._run(functools.partial(self._store.complete_migration, rule=rule))
        return migration

    def sweep(self) -> int:
        """Forget state past its safe-forget horizon, and expired cooldowns.

        :returns: How many rules' state was forgotten.
        """
        forgotten = self._run(self._store.sweep)
        return forgotten

    def close(self) -> None:
        """Close this handle's connection, after any call in progress. Idempotent (L1, L4).

        The file and its quota state are untouched (L3).
        """
        if self._mark_closed():
            process = self._current()
            with process.lock:
                self._close_connection(process)
        else:
            pass


class AsyncSQLiteBackend(_SQLiteHandle, BaseAsyncBackend):
    """An asynchronous handle on a :class:`SQLiteStore`, never blocking the event loop.

    Satisfies :class:`~procrastinators.protocols.AsyncBackend`,
    :class:`~procrastinators.protocols.SupportsAsyncCooldown`, and
    :class:`~procrastinators.protocols.SupportsAsyncPolicyAdministration`. Each
    storage call runs, one at a time, on a
    :class:`~procrastinators.backends.executor.DedicatedExecutor` whose worker
    thread alone uses this handle's connection; waiting for quota stays on the
    calling task. A cancelled call whose transaction already started may still
    commit: that is possible consumed capacity, never permission (O5).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, on the executor's thread.
    :param executor: An executor to borrow, which this handle never closes; a
        dedicated one it owns when ``None``.
    :param max_pending: Bound on queued calls for an owned executor.
    """

    _mode = Mode.ASYNC

    def __init__(
        self,
        store: SQLiteStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        executor: DedicatedExecutor | None = None,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        super().__init__(store, namespace=namespace, observer=observer)
        self._max_pending = max_pending
        self._borrowed = executor is not None
        self._executor = executor or self._new_executor()
        self._close_task: asyncio.Task[None] | None = None

    def _new_executor(self) -> DedicatedExecutor:
        executor = DedicatedExecutor(name="procrastinators-sqlite", max_pending=self._max_pending)
        return executor

    def _forked(self) -> None:
        if not self._borrowed:
            self._executor = self._new_executor()
        else:
            pass

    @property
    def ownership(self) -> ResourceOwnership:
        """The connection is owned; the executor is owned unless it was injected (L2)."""
        ownership = ResourceOwnership(
            executor=Ownership.BORROWED if self._borrowed else Ownership.OWNED
        )
        return ownership

    @property
    def executor(self) -> DedicatedExecutor:
        """The executor this handle's storage calls run on."""
        return self._executor

    async def _run(
        self,
        operation: Callable[[sqlite3.Connection], ResultT],
        lock_timeout_us: int = DEFAULT_TIMEOUT_US,
    ) -> ResultT:
        self._ensure_open()
        process = self._current()
        result = await self._executor.run(
            functools.partial(self._use, process, operation), start_within_us=lock_timeout_us
        )
        return result

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        Cancelled before its transaction starts, the call consumes nothing;
        after, it may commit without its caller (O5).

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.BackendBusy: The executor was full or could not start the
            call within the lock budget, or the file's write lock was not free.
        :raises ~procrastinators.errors.BackendUnavailable: The file could not be used, or the
            observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit failed, or the observer
            failed after it.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = await self._run(
            functools.partial(
                self._store.admit,
                request=request,
                observe=self._observe,
                identity=self._identity,
            ),
            request.budget.lock_timeout_us,
        )
        return decision

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        snapshot = await self._run(
            functools.partial(self._store.inspect, rules=rules, identity=self._identity)
        )
        return snapshot

    async def defer_for(
        self, scope: QuotaIdentity, duration: DurationMicros, *, reason: str = ""
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on the cooldown.
        :returns: The cooldown now in force, which may be longer than requested.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        cooldown = await self._run(
            functools.partial(self._store.defer, scope=scope, duration=duration, reason=reason)
        )
        return cooldown

    async def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the file holds for ``rule``, if any.

        :param rule: The rule to look up.
        """
        stored = await self._run(functools.partial(self._store.stored_policy, rule=rule))
        return stored

    async def begin_migration(
        self, rule: RuleId, *, to_fingerprint: PolicyFingerprint, to_state_version: int
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
        """
        migration = await self._run(
            functools.partial(
                self._store.begin_migration,
                rule=rule,
                to_fingerprint=to_fingerprint,
                to_state_version=to_state_version,
            )
        )
        return migration

    async def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """
        migration = await self._run(functools.partial(self._store.migration_status, rule=rule))
        return migration

    async def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready.
        """
        migration = await self._run(functools.partial(self._store.complete_migration, rule=rule))
        return migration

    async def aclose(self) -> None:
        """Close this handle once its outstanding calls settle. Idempotent (L1, L4).

        The connection is closed on the worker thread after every call queued
        before it, including one whose caller was cancelled; an owned executor
        is then stopped, a borrowed one left running (L2). The file and its
        quota state are untouched (L3).
        """
        if self._mark_closed():
            self._close_task = asyncio.create_task(self._finish_close())
        else:
            pass
        if self._close_task is not None:
            await asyncio.shield(self._close_task)
        else:
            pass

    async def _finish_close(self) -> None:
        process = self._current()
        if process.connection is None and not self._executor.outstanding:
            pass
        elif self._executor.closed:
            self._close_connection(process)
        else:
            await self._executor.run(
                functools.partial(self._close_connection, process), bounded=False
            )
        if not self._borrowed:
            await self._executor.aclose()
        else:
            pass


def sqlite_backend(
    address: str,
    *,
    mode: Mode,
    namespace: str = "default",
    algorithms: Iterable[Algorithm[Any]] = tuple(),
) -> SQLiteBackend | AsyncSQLiteBackend:
    """A handle on the database file an address names.

    ``sqlite://`` names the default database (:func:`default_database_path`),
    ``sqlite:///quota.sqlite3`` a path relative to the current directory, and
    ``sqlite:////var/lib/etl/quota.sqlite3`` an absolute one; a relative path is
    resolved once, now. Registered as the ``sqlite`` family's factory.

    :param address: A ``sqlite://`` address.
    :param mode: Which interface the handle offers.
    :param namespace: The handle's quota namespace.
    :param algorithms: Algorithms the store must host, beyond those it has.
    :raises ~procrastinators.errors.ConfigurationError: The address is not a ``sqlite``
        address, names a host, carries a query or fragment, or names an in-memory database.
    """
    parts = urllib.parse.urlsplit(address)
    if parts.scheme != FAMILY:
        raise ConfigurationError(f"not a sqlite address: {address!r}")
    elif parts.netloc or parts.query or parts.fragment:
        raise ConfigurationError(
            f"a sqlite address names only a local file, as sqlite:///relative/path or "
            f"sqlite:////absolute/path: {address!r}"
        )
    else:
        pass
    path = urllib.parse.unquote(parts.path)
    if not path:
        store = SQLiteStore.at()
    elif not (relative := path[1:]) or relative == ":memory:":
        raise ConfigurationError(
            f"{address!r} names no database file; an in-memory SQLite database coordinates "
            "nothing, so use backend='memory://' for that"
        )
    else:
        store = SQLiteStore.at(relative)
    store.host(algorithms)
    if mode is Mode.SYNC:
        handle: SQLiteBackend | AsyncSQLiteBackend = SQLiteBackend(store, namespace=namespace)
    else:
        handle = AsyncSQLiteBackend(store, namespace=namespace)
    return handle


if __name__ == "__main__":
    pass
else:
    pass
