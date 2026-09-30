"""The Memcached backend: shared, constant-state quotas with a weaker guarantee.

A :class:`MemcachedStore` names a Memcached server and the algorithms it
enforces. Handles address it: :class:`MemcachedBackend` synchronously,
:class:`AsyncMemcachedBackend` from an event loop through a
:class:`~procrastinators.backends.executor.DedicatedExecutor`. Every handle
owns its own connection.

**Best effort, explicitly.** Memcached may evict any item at any moment and
forgets everything on restart. A rule whose item vanished reads as never used,
so it admits its configured initial allowance again: *a cache miss is not
proof that no quota was consumed* (contract Y4). The family therefore declares
:attr:`~procrastinators.models.Durability.BEST_EFFORT`, and a limiter refuses it
unless the caller passes ``accept_best_effort=True``. Policy metadata lives in
the same item as the state, so it is evicted with it: a disagreement between
workers is detected only while the item survives.

**One item, compare-and-swap.** Each rule is one bounded item holding its
policy metadata, last observed time, safe-forget horizon, and state. Admission
reads the item with its CAS token (``gets``), evaluates the reference
evaluator, and writes back with ``cas`` — or ``add`` when the item was absent
— so a write made in between by another worker fails the swap instead of
being overwritten. A failed swap reads and evaluates again, for at most
``max(MIN_CAS_ROUNDS, max_contention_retries + 1)`` rounds and never past the
request's lock budget; then contention is
:exc:`~procrastinators.errors.BackendBusy`, never a denial (O2). The debit is
committed before success is reported.

**Constant state only.** An item holds scalar state: fixed window, token
bucket, leaky bucket, and sliding counter. A sliding log needs a history no
bounded item should hold and is refused at construction, as is composing
several rules, which would span several items no single swap can cover.
Cooldowns and policy administration are not offered.

**Time.** Memcached has no clock a client can read, so authority time is each
client's wall clock, clamped against the latest time stored in the item so it
never runs backwards for that rule (T7). Every client must therefore keep its
clock synchronized, as with NTP. A client running fast charges its admissions
to later windows and refills buckets early, admitting more than the policy
allows by up to its skew; one running slow is clamped and admits less.

**Expiry.** An item expires at its safe-forget horizon, rounded up to whole
seconds with a second's margin for the server's coarse clock (L9). Memcached
reads an expiry beyond thirty days as an absolute Unix time on the *server's*
clock, so such expiries are computed from this machine's wall clock. An item
whose state never becomes neutral is stored without expiry, though it can
still be evicted.

**Failures.** A failed read commits nothing:
:exc:`~procrastinators.errors.BackendUnavailable`. A failure once an admitting
``cas`` or ``add`` was sent leaves the debit in doubt:
:exc:`~procrastinators.errors.IndeterminateAdmission`, never permission and
never a refund (O4). The client never retries by itself.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import dataclasses
import functools
import hashlib
import os
import struct
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Final, TypeVar

from procrastinators.algorithms import reference_algorithms
from procrastinators.backends import bookkeeping
from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.backends.executor import DEFAULT_MAX_PENDING, DedicatedExecutor
from procrastinators.capabilities import CapabilityRequirement, Mode, require_capabilities
from procrastinators.clocks import SystemClock
from procrastinators.errors import (
    BackendBusy,
    BackendError,
    BackendUnavailable,
    ConfigurationError,
    IndeterminateAdmission,
    PolicyConflict,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.models import (
    MAX_TIMESTAMP_US,
    USECS_PER_SECOND,
    Admission,
    Algorithms,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Durability,
    DurationMicros,
    EpochMicros,
    Ownership,
    PolicyFingerprint,
    ResourceOwnership,
    Snapshot,
)
from procrastinators.protocols import ObservationPoint, StateRepresentation
from procrastinators.state import UNUSED, plan_admission

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from pymemcache.client.base import Client

    from procrastinators.models import AdmissionRequest, Constraint, Decision, RuleId
    from procrastinators.protocols import AdmissionClock, AdmissionObserver, Algorithm, RuleState
else:
    pass

__all__ = [
    "DEFAULT_MAX_ITEM_SIZE",
    "MEMCACHED_CAPABILITIES",
    "MIN_CAS_ROUNDS",
    "AsyncMemcachedBackend",
    "MemcachedBackend",
    "MemcachedStore",
    "expiry_seconds",
    "memcached_backend",
]

FAMILY: Final = "memcached"
"""The backend family, as it appears in identities and ``memcached://`` addresses."""

DEFAULT_PORT: Final = 11211
"""The port an address without one names."""

DEFAULT_PREFIX: Final = "procrastinators"
"""What every key starts with unless a store is given another prefix."""

DEFAULT_MAX_ITEM_SIZE: Final = 1 << 20
"""Memcached's default item size limit, in bytes: one mebibyte."""

MIN_CAS_ROUNDS: Final = 16
"""The fewest read-evaluate-swap rounds an admission gets before contention is reported."""

DEFAULT_TIMEOUT_S: Final = 5.0
"""Seconds a connection waits to connect or for a reply."""

MAX_RELATIVE_EXPIRY_S: Final = 30 * 24 * 60 * 60
"""The longest expiry Memcached reads as relative; anything longer is an absolute Unix time."""

MAX_KEY_BYTES: Final = 250
"""Memcached's key length limit."""

_CONSTANT_STATE: Final = frozenset(
    {
        str(Algorithms.FIXED_WINDOW),
        str(Algorithms.TOKEN_BUCKET),
        str(Algorithms.LEAKY_BUCKET),
        str(Algorithms.SLIDING_COUNTER),
    }
)

MEMCACHED_CAPABILITIES: Final = Capabilities(
    algorithms=_CONSTANT_STATE,
    coordination=CoordinationScope.SHARED_SERVICE,
    durability=Durability.BEST_EFFORT,
    supports_sync=True,
    supports_async=True,
    supports_composition=False,
    supports_cooldowns=False,
    supports_policy_administration=False,
    state_representations=frozenset({StateRepresentation.SCALARS}),
)
"""What the Memcached family declares: constant-state algorithms, no composition, best effort."""

_MAGIC: Final = b"PMC1"
_LAYOUT: Final = 1
_HEADER: Final = struct.Struct(">4sBBHQQ")
_LENGTH8: Final = struct.Struct(">B")
_LENGTH16: Final = struct.Struct(">H")
_FLAG_HORIZON: Final = 0x01
_NANOS_PER_MICRO: Final = 1_000

ResultT = TypeVar("ResultT")


def expiry_seconds(
    horizon: EpochMicros | None, now: EpochMicros, *, wall_seconds: float | None = None
) -> int:
    """The Memcached expiry of an item that may be forgotten at ``horizon``.

    Rounded up to whole seconds, plus one: the server keeps time in whole
    seconds, so an item given ``n`` seconds may expire up to a second early,
    and it must never expire before its horizon. An expiry longer than thirty
    days becomes an absolute Unix time on this machine's wall clock, since
    Memcached reads such values against its own clock.

    :param horizon: The safe-forget horizon, or ``None`` for never.
    :param now: Authority epoch time of the write.
    :param wall_seconds: This machine's wall clock, in seconds; read when ``None``.
    :returns: ``0`` for no expiry, otherwise what to pass as ``expire``.
    """
    if horizon is None:
        expiry = 0
    else:
        seconds = max(0, -(-(horizon - now) // USECS_PER_SECOND)) + 1
        if seconds <= MAX_RELATIVE_EXPIRY_S:
            expiry = seconds
        else:
            wall = time.time() if wall_seconds is None else wall_seconds
            expiry = int(wall) + seconds
    return expiry


@dataclass(frozen=True, slots=True)
class _Item:
    """One rule's item, decoded."""

    algorithm: str
    fingerprint: PolicyFingerprint
    state_version: int
    last: EpochMicros
    horizon: EpochMicros | None
    state: RuleState


def _encode(item: _Item, algorithm: Algorithm[Any]) -> bytes:
    algorithm_bytes = item.algorithm.encode()
    fingerprint_bytes = item.fingerprint.encode()
    if len(algorithm_bytes) > 0xFF or len(fingerprint_bytes) > 0xFFFF:
        raise StateCorruption("an algorithm id or fingerprint is too long to store")
    else:
        pass
    flags = _FLAG_HORIZON if item.horizon is not None else 0
    payload = b"".join(
        (
            _HEADER.pack(
                _MAGIC,
                _LAYOUT,
                flags,
                item.state_version,
                item.last,
                item.horizon or 0,
            ),
            _LENGTH8.pack(len(algorithm_bytes)),
            algorithm_bytes,
            _LENGTH16.pack(len(fingerprint_bytes)),
            fingerprint_bytes,
            algorithm.codec.encode(item.state),
        )
    )
    return payload


def _take(data: bytes, offset: int, size: int) -> tuple[bytes, int]:
    if offset + size > len(data):
        raise StateCorruption("a stored item is truncated")
    else:
        pass
    chunk = (data[offset : offset + size], offset + size)
    return chunk


def _text(raw: bytes, what: str) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise StateCorruption(f"a stored {what} is not valid UTF-8") from error
    return text


def _decode(payload: object, resolve: Callable[[str], Algorithm[Any]]) -> _Item:
    """Parse an item, failing closed on anything malformed."""
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise StateCorruption(f"a stored item must be bytes, got {type(payload).__name__}")
    else:
        pass
    data = bytes(payload)
    header, offset = _take(data, 0, _HEADER.size)
    magic, layout, flags, state_version, last, horizon = _HEADER.unpack(header)
    if magic != _MAGIC:
        raise StateCorruption("a stored item does not carry the PMC1 tag")
    elif layout != _LAYOUT:
        raise StateCorruption(f"a stored item has layout {layout}, not {_LAYOUT}")
    elif flags & ~_FLAG_HORIZON:
        raise StateCorruption(f"a stored item sets unknown flags {flags:#04x}")
    elif last > MAX_TIMESTAMP_US or horizon > MAX_TIMESTAMP_US:
        raise StateCorruption("a stored item holds a timestamp beyond the supported range")
    elif not flags & _FLAG_HORIZON and horizon:
        raise StateCorruption("a stored item without a horizon holds one")
    else:
        pass
    raw, offset = _take(data, offset, _LENGTH8.size)
    algorithm_raw, offset = _take(data, offset, _LENGTH8.unpack(raw)[0])
    raw, offset = _take(data, offset, _LENGTH16.size)
    fingerprint_raw, offset = _take(data, offset, _LENGTH16.unpack(raw)[0])
    algorithm_id = _text(algorithm_raw, "algorithm id")
    algorithm = resolve(algorithm_id)
    if algorithm.state_version != state_version:
        raise StateCorruption(
            f"a stored item holds {algorithm_id} state version {state_version}, but the hosted "
            f"algorithm reads version {algorithm.state_version}"
        )
    else:
        pass
    item = _Item(
        algorithm_id,
        PolicyFingerprint(_text(fingerprint_raw, "fingerprint")),
        state_version,
        EpochMicros(last),
        EpochMicros(horizon) if flags & _FLAG_HORIZON else None,
        algorithm.codec.decode(data[offset:]),
    )
    return item


def _failures() -> tuple[type[BaseException], ...]:
    from pymemcache.exceptions import MemcacheError

    failures = (MemcacheError, OSError)
    return failures


class MemcachedStore:
    """A Memcached server as a best-effort admission authority.

    Constructing a store parses its address and touches nothing else: no
    connection exists until a handle first needs one. It hosts the
    constant-state reference algorithms unless given others, and more can be
    added with :meth:`host`; an algorithm needing anything but scalar state is
    refused when a request uses it.

    :param address: ``memcached://host:port``, or ``host:port``; the port defaults to 11211.
    :param prefix: Starts every key, separating deployments that share a server.
    :param algorithms: The algorithms to host; the constant-state reference algorithms when
        ``None``.
    :param clock: Authority epoch time; this machine's wall clock when ``None``. Every
        client must keep it synchronized.
    :param max_item_size: The server's item size limit, in bytes; nothing larger is written.
    :param timeout: Seconds a connection waits to connect or for a reply.
    :raises ~procrastinators.errors.ConfigurationError: The address or a setting is invalid.
    """

    def __init__(
        self,
        address: str = "memcached://localhost",
        *,
        prefix: str = DEFAULT_PREFIX,
        algorithms: Iterable[Algorithm[Any]] | None = None,
        clock: AdmissionClock | None = None,
        max_item_size: int = DEFAULT_MAX_ITEM_SIZE,
        timeout: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        text = address if "://" in address else f"{FAMILY}://{address}"
        parts = urllib.parse.urlsplit(text)
        if parts.scheme != FAMILY:
            raise ConfigurationError(f"not a memcached address: {address!r}")
        elif parts.username is not None or parts.password is not None:
            raise ConfigurationError(f"a memcached address carries no credentials: {address!r}")
        elif parts.path.strip("/") or parts.query or parts.fragment:
            raise ConfigurationError(f"a memcached address names only a host and port: {address!r}")
        elif (
            not isinstance(prefix, str)
            or not prefix
            or len(prefix) > 64
            or not all(33 <= ord(character) < 127 for character in prefix)
        ):
            raise ConfigurationError(
                f"a key prefix must be 1 to 64 printable ASCII characters without spaces: "
                f"{prefix!r}"
            )
        elif (
            isinstance(max_item_size, bool)
            or not isinstance(max_item_size, int)
            or (max_item_size < _HEADER.size + 64)
        ):
            raise ConfigurationError(f"max_item_size is too small: {max_item_size!r}")
        elif isinstance(timeout, bool) or not (isinstance(timeout, (int, float)) and timeout > 0):
            raise ConfigurationError(f"timeout must be positive, got {timeout!r}")
        else:
            pass
        try:
            port = parts.port or DEFAULT_PORT
        except ValueError as error:
            raise ConfigurationError(f"{address!r} names an invalid port") from error
        self._host = parts.hostname or "localhost"
        self._port = port
        self._prefix = prefix
        self._clock: AdmissionClock = clock or SystemClock()
        self._max_item_size = max_item_size
        self._timeout = float(timeout)
        self._lock = threading.Lock()
        self._algorithms: dict[str, Algorithm[Any]] = dict()
        self.host(
            (algorithm for algorithm in reference_algorithms() if algorithm.id in _CONSTANT_STATE)
            if algorithms is None
            else algorithms
        )

    @property
    def authority(self) -> str:
        """``host:port``; never credentials."""
        authority = f"{self._host}:{self._port}"
        return authority

    @property
    def max_item_size(self) -> int:
        """The largest item, in bytes, this store writes."""
        return self._max_item_size

    def identity(self, namespace: str) -> BackendIdentity:
        """The identity of a handle using ``namespace`` on this store.

        :param namespace: The handle's quota namespace.
        """
        if self._prefix == DEFAULT_PREFIX:
            authority = self.authority
        else:
            prefix_digest = hashlib.sha256(self._prefix.encode()).hexdigest()
            authority = f"{self.authority};prefix-sha256={prefix_digest}"
        identity = BackendIdentity(FAMILY, authority, namespace)
        return identity

    def host(self, algorithms: Iterable[Algorithm[Any]]) -> None:
        """Host more algorithms, keeping those already hosted.

        As :meth:`~procrastinators.backends.sqlite.SQLiteStore.host`, except that a
        sliding log is skipped: its history does not fit a bounded item.

        :param algorithms: The algorithms to add.
        :raises ~procrastinators.errors.ConfigurationError: An id is hosted by an algorithm of
            another type.
        """
        with self._lock:
            for algorithm in algorithms:
                if algorithm.id == Algorithms.SLIDING_LOG:
                    pass
                elif (existing := self._algorithms.get(algorithm.id)) is None:
                    self._algorithms[algorithm.id] = algorithm
                elif type(existing) is not type(algorithm):
                    raise ConfigurationError(
                        f"algorithm {algorithm.id!r} is already hosted by "
                        f"{type(existing).__name__}, not {type(algorithm).__name__}"
                    )
                else:
                    pass

    def capabilities(self, mode: Mode) -> Capabilities:
        """What a handle in ``mode`` implements.

        :param mode: The handle's interface.
        """
        with self._lock:
            hosted = frozenset(self._algorithms)
        capabilities = dataclasses.replace(
            MEMCACHED_CAPABILITIES,
            algorithms=hosted,
            supports_sync=mode is Mode.SYNC,
            supports_async=mode is Mode.ASYNC,
        )
        return capabilities

    def key(self, rule: RuleId) -> str:
        """The key of ``rule``'s item: the prefix and a digest of the rule's identity.

        :param rule: The rule.
        """
        identity = "\0".join((rule.scope.namespace, rule.scope.key, rule.name))
        digest = hashlib.sha256(identity.encode()).hexdigest()
        key = f"{self._prefix}:r1:{digest}"
        return key

    def connect(self) -> Client:
        """A new client for this server, which never retries by itself.

        :raises ~procrastinators.errors.BackendUnavailable: The driver is not installed.
        """
        try:
            from pymemcache.client.base import Client as Driver
        except ImportError as error:
            raise BackendUnavailable(
                "the memcached backend needs pymemcache: install procrastinators[memcached]",
                cause=error,
            ) from error
        client = Driver(
            (self._host, self._port),
            connect_timeout=self._timeout,
            timeout=self._timeout,
            no_delay=True,
            default_noreply=False,
        )
        return client

    def _resolve_id(self, algorithm_id: str) -> Algorithm[Any]:
        with self._lock:
            algorithm = self._algorithms.get(algorithm_id)
        if algorithm is None:
            raise UnsupportedCapability(
                f"memcached store {self.authority} does not host algorithm {algorithm_id!r}"
            )
        else:
            pass
        return algorithm

    def _resolve(self, constraint: Constraint) -> Algorithm[Any]:
        algorithm = self._resolve_id(constraint.algorithm)
        return algorithm

    def _stored_item(self, payload: object) -> _Item | None:
        item = None if payload is None else _decode(payload, self._resolve_id)
        return item

    def validate(self, request: AdmissionRequest) -> None:
        """Refuse a request no single item can serve.

        :param request: The request about to be admitted.
        :raises ~procrastinators.errors.UnsupportedCapability: Several rules, an algorithm not
            hosted, or one needing anything but scalar state.
        :raises ~procrastinators.errors.InvalidPolicy: A policy is invalid for its algorithm.
        """
        if len(request.constraints) != 1:
            raise UnsupportedCapability(
                "memcached admits one rule per request: composed rules would span several "
                "items, which no single compare-and-swap covers"
            )
        else:
            pass
        (constraint,) = request.constraints
        algorithm = self._resolve(constraint)
        algorithm.validate(constraint.policy)
        if (representation := algorithm.requirements(constraint.policy).representation) != (
            StateRepresentation.SCALARS
        ):
            raise UnsupportedCapability(
                f"{constraint} needs {representation} state; a memcached item holds only scalars"
            )
        else:
            pass

    def admit(
        self,
        client: Client,
        request: AdmissionRequest,
        observe: Callable[[ObservationPoint, AdmissionRequest], None],
        identity: BackendIdentity,
    ) -> Decision:
        """One admission: read, evaluate, and swap, until a swap lands or the budget ends.

        :param client: A client from :meth:`connect`, used by no one else meanwhile.
        :param request: A validated request of one constraint.
        :param observe: Reports each observation point.
        :param identity: The admitting handle's identity, recorded on the admission.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: The item holds another policy.
        :raises ~procrastinators.errors.BackendBusy: Every round lost its swap.
        :raises ~procrastinators.errors.BackendUnavailable: The server failed before a write.
        :raises ~procrastinators.errors.IndeterminateAdmission: An admitting write was sent and
            its reply lost, or the observer failed after the commit.
        :raises ~procrastinators.errors.StateCorruption: The item is malformed, or would exceed
            the item size limit.
        """
        (constraint,) = request.constraints
        rule = constraint.rule
        key = self.key(rule)
        algorithm = self._resolve(constraint)
        budget = request.budget
        rounds = max(MIN_CAS_ROUNDS, budget.max_contention_retries + 1)
        give_up_ns = time.monotonic_ns() + budget.lock_timeout_us * _NANOS_PER_MICRO
        for _ in range(rounds):
            try:
                payload, token = client.gets(key)
            except _failures() as error:
                raise BackendUnavailable(
                    f"reading {rule} from {self.authority}: {error}", cause=error
                ) from error
            item = self._stored_item(payload)
            if item is not None and item.fingerprint != constraint.fingerprint:
                raise PolicyConflict(
                    f"{rule} is stored under another policy; change it deliberately, never by "
                    "starting with a different configuration (I4)",
                    rule=rule,
                    expected=constraint.fingerprint,
                    found=item.fingerprint,
                )
            else:
                pass
            now = EpochMicros(max(self._clock.now(), item.last if item is not None else 0))
            loaded = item.state if item is not None else UNUSED
            observe(ObservationPoint.AFTER_LOAD, request)
            plan = plan_admission(request, {rule: loaded}, self._resolve, now)
            if item is not None and rule not in plan.writes:
                committed = True
            else:
                (transition,) = plan.transitions
                written = _Item(
                    constraint.algorithm,
                    constraint.fingerprint,
                    constraint.state_version,
                    now,
                    transition.safe_forget_after_us,
                    plan.writes.get(rule, loaded),
                )
                value = _encode(written, algorithm)
                if len(value) + len(key) > self._max_item_size:
                    raise StateCorruption(
                        f"the item of {rule} would be {len(value)} bytes, over the "
                        f"{self._max_item_size}-byte item size limit; nothing was written"
                    )
                else:
                    pass
                if plan.admitted:
                    observe(ObservationPoint.BEFORE_COMMIT, request)
                else:
                    pass
                committed = self._swap(
                    client,
                    key,
                    value,
                    token if item is not None else None,
                    expiry_seconds(written.horizon, now),
                    request if plan.admitted else None,
                )
            if committed:
                break
            elif time.monotonic_ns() > give_up_ns:
                raise _contention(rule, "its lock budget")
            else:
                pass
        else:
            raise _contention(rule, f"{rounds} rounds")
        if plan.admitted:
            admission = Admission(request.rules, request.cost, now, identity)
            observe(ObservationPoint.AFTER_COMMIT, request)
            decision = plan.decision(admission)
        else:
            decision = plan.decision()
        return decision

    def _swap(
        self,
        client: Client,
        key: str,
        value: bytes,
        token: object | None,
        expire: int,
        admitting: AdmissionRequest | None,
    ) -> bool:
        """Write ``value`` if the item is unchanged; whether the write landed."""
        try:
            if token is None:
                landed = bool(client.add(key, value, expire=expire, noreply=False))
            else:
                # None means the item vanished since it was read: another round.
                landed = client.cas(key, value, token, expire=expire, noreply=False) is True
        except _failures() as error:
            raise _lost(error, key, admitting) from error
        return landed

    def inspect(
        self, client: Client, rules: Sequence[RuleId], identity: BackendIdentity
    ) -> Snapshot:
        """Advisory observation of ``rules``, committing nothing (R7).

        A missing item reports ``unused``, which on this backend may mean evicted.

        :param client: A client from :meth:`connect`.
        :param rules: The rules to observe.
        :param identity: The observing handle's identity.
        """
        snapshots = list()
        now = self._clock.now()
        for rule in rules:
            try:
                payload = client.get(self.key(rule))
            except _failures() as error:
                raise BackendUnavailable(
                    f"reading {rule} from {self.authority}: {error}", cause=error
                ) from error
            item = self._stored_item(payload)
            now = EpochMicros(max(now, item.last if item is not None else 0))
            snapshots.append(
                bookkeeping.rule_snapshot(
                    rule,
                    constraint=None,
                    algorithm=item.algorithm if item is not None else None,
                    state=item.state if item is not None else UNUSED,
                    horizon=item.horizon if item is not None else None,
                    cooldown_until=None,
                    resolve=self._resolve,
                    now=now,
                )
            )
        snapshot = Snapshot(rules=tuple(snapshots), sampled_at=now, backend=identity)
        return snapshot

    def __repr__(self) -> str:
        text = f"MemcachedStore({self.authority!r})"
        return text


def _lost(error: BaseException, key: str, admitting: AdmissionRequest | None) -> BackendError:
    if admitting is not None:
        lost: BackendError = IndeterminateAdmission(
            f"writing {key}: the reply was lost ({error}); the admission may have happened",
            cause=error,
            cost=admitting.cost,
            rules=admitting.rules,
        )
    else:
        # A denial writes only initial state, which reading it again reproduces.
        lost = BackendUnavailable(f"writing {key}: {error}", cause=error)
    return lost


def _contention(rule: RuleId, bound: str) -> BackendBusy:
    error = BackendBusy(
        f"{rule} kept changing under every compare-and-swap within {bound}; contention is "
        "not a denial (O2)"
    )
    return error


@dataclass(slots=True)
class _Connection:
    """A handle's client in one process, and the lock serializing its use there."""

    pid: int = field(default_factory=os.getpid)
    lock: threading.Lock = field(default_factory=threading.Lock)
    client: Client | None = None


class _MemcachedHandle:
    """What the sync and async handles share: the store, namespace, observer, and client."""

    _mode: ClassVar[Mode]

    def __init__(
        self,
        store: MemcachedStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
    ) -> None:
        self._store = store
        self._identity = store.identity(namespace)
        self._observer = observer
        self._process = _Connection()

    @property
    def store(self) -> MemcachedStore:
        """The authority this handle addresses."""
        return self._store

    @property
    def capabilities(self) -> Capabilities:
        """Shared-service and best-effort, constant state only, one rule per request."""
        capabilities = self._store.capabilities(self._mode)
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        """``memcached://host:port#<namespace>``."""
        return self._identity

    def validate_request(self, request: AdmissionRequest) -> None:
        """Reject a request this handle cannot serve.

        Constructing a Memcached handle is itself the explicit acceptance of
        best-effort state, so that is not demanded again here; a limiter still
        demands ``accept_best_effort=True`` of its caller (Y4).

        :param request: The admission request about to be served.
        :raises ~procrastinators.errors.UnsupportedCapability: The request needs something this
            handle does not implement.
        """
        requirement = dataclasses.replace(
            CapabilityRequirement.for_request(request, mode=self._mode), accept_best_effort=True
        )
        require_capabilities(self.capabilities, requirement, backend=self._identity)
        self._store.validate(request)

    def _current(self) -> _Connection:
        """This process's client slot, fresh after a fork (L8)."""
        if self._process.pid != os.getpid():
            # The inherited socket belongs to the parent; it is dropped, not closed.
            self._process = _Connection()
            self._forked()
        else:
            pass
        return self._process

    def _forked(self) -> None:
        """Replace anything besides the client that did not survive a fork."""

    def _use(self, process: _Connection, operation: Callable[[Client], ResultT]) -> ResultT:
        if process.client is None:
            process.client = self._store.connect()
        else:
            pass
        result = operation(process.client)
        return result

    def _close_client(self, process: _Connection) -> None:
        if process.client is not None and process.pid == os.getpid():
            process.client.close()
            process.client = None
        else:
            pass

    def __repr__(self) -> str:
        text = f"{type(self).__name__}({self._identity})"
        return text


class MemcachedBackend(_MemcachedHandle, BaseSyncBackend):
    """A synchronous handle on a :class:`MemcachedStore`, with a connection of its own.

    Satisfies :class:`~procrastinators.protocols.SyncBackend`. Threads sharing a
    handle take turns on its connection, waiting at most their lock budget.

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, for deterministic tests.
    """

    _mode = Mode.SYNC

    def _run(self, operation: Callable[[Client], ResultT], lock_timeout_us: int) -> ResultT:
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
        """Admit ``request``'s one constraint atomically, by compare-and-swap.

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: The item holds another policy.
        :raises ~procrastinators.errors.UnsupportedCapability: Several rules, or state an item
            cannot hold.
        :raises ~procrastinators.errors.BackendBusy: Contention outlasted the budget.
        :raises ~procrastinators.errors.BackendUnavailable: Nothing was committed.
        :raises ~procrastinators.errors.IndeterminateAdmission: An admitting write's reply was
            lost, or the observer failed after the commit.
        :raises ~procrastinators.errors.StateCorruption: The item is malformed or too large.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = self._run(
            functools.partial(
                self._store.admit, request=request, observe=self._observe, identity=self._identity
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
            functools.partial(self._store.inspect, rules=rules, identity=self._identity),
            DurationMicros(int(DEFAULT_TIMEOUT_S * USECS_PER_SECOND)),
        )
        return snapshot

    def close(self) -> None:
        """Close this handle's connection, after any call in progress. Idempotent (L1, L4).

        Items on the server are untouched (L3).
        """
        if self._mark_closed():
            process = self._current()
            with process.lock:
                self._close_client(process)
        else:
            pass


class AsyncMemcachedBackend(_MemcachedHandle, BaseAsyncBackend):
    """An asynchronous handle on a :class:`MemcachedStore`, never blocking the event loop.

    Satisfies :class:`~procrastinators.protocols.AsyncBackend`. Each call runs,
    one at a time, on a :class:`~procrastinators.backends.executor.DedicatedExecutor`
    whose worker thread alone uses this handle's connection. A cancelled call
    whose swap already started may still commit: possible consumed capacity,
    never permission (O5).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, on the executor's thread.
    :param executor: An executor to borrow, which this handle never closes; a dedicated
        one it owns when ``None``.
    :param max_pending: Bound on queued calls for an owned executor.
    """

    _mode = Mode.ASYNC

    def __init__(
        self,
        store: MemcachedStore,
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
        executor = DedicatedExecutor(
            name="procrastinators-memcached", max_pending=self._max_pending
        )
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
        """The executor this handle's calls run on."""
        return self._executor

    async def _run(self, operation: Callable[[Client], ResultT], lock_timeout_us: int) -> ResultT:
        self._ensure_open()
        process = self._current()
        result = await self._executor.run(
            functools.partial(self._use, process, operation), start_within_us=lock_timeout_us
        )
        return result

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Admit ``request``'s one constraint atomically, by compare-and-swap.

        As :meth:`MemcachedBackend.admit`, awaited. Cancelled before its call
        starts, it consumes nothing; after, it may commit without its caller (O5).

        :param request: The request.
        :returns: The decision; a denial is a value.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = await self._run(
            functools.partial(
                self._store.admit, request=request, observe=self._observe, identity=self._identity
            ),
            request.budget.lock_timeout_us,
        )
        return decision

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        """
        snapshot = await self._run(
            functools.partial(self._store.inspect, rules=rules, identity=self._identity),
            DurationMicros(int(DEFAULT_TIMEOUT_S * USECS_PER_SECOND)),
        )
        return snapshot

    async def aclose(self) -> None:
        """Close this handle once its outstanding calls settle. Idempotent (L1, L4).

        As :meth:`~procrastinators.backends.sqlite.AsyncSQLiteBackend.aclose`.
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
        if process.client is None and not self._executor.outstanding:
            pass
        elif self._executor.closed:
            self._close_client(process)
        else:
            await self._executor.run(functools.partial(self._close_client, process), bounded=False)
        if not self._borrowed:
            await self._executor.aclose()
        else:
            pass


def memcached_backend(
    address: str,
    *,
    mode: Mode,
    namespace: str = "default",
    algorithms: Iterable[Algorithm[Any]] = tuple(),
) -> MemcachedBackend | AsyncMemcachedBackend:
    """A handle on the Memcached server an address names.

    ``memcached://host:11211``, with an optional ``?prefix=`` for the key
    prefix. Registered as the ``memcached`` family's factory. A limiter built on
    it must be given ``accept_best_effort=True``.

    :param address: A ``memcached://`` address.
    :param mode: Which interface the handle offers.
    :param namespace: The handle's quota namespace.
    :param algorithms: Algorithms the store must host, beyond the built-ins; a sliding log is
        skipped.
    :raises ~procrastinators.errors.ConfigurationError: The address is malformed.
    """
    parts = urllib.parse.urlsplit(address)
    settings = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    prefix = settings.pop("prefix", DEFAULT_PREFIX)
    if parts.scheme != FAMILY:
        raise ConfigurationError(f"not a memcached address: {address!r}")
    elif settings:
        raise ConfigurationError(
            f"a memcached address accepts only a prefix setting, got {sorted(settings)}"
        )
    else:
        pass
    store = MemcachedStore(urllib.parse.urlunsplit(parts._replace(query="")), prefix=prefix)
    store.host(algorithms)
    if mode is Mode.SYNC:
        handle: MemcachedBackend | AsyncMemcachedBackend = MemcachedBackend(
            store, namespace=namespace
        )
    else:
        handle = AsyncMemcachedBackend(store, namespace=namespace)
    return handle


if __name__ == "__main__":
    pass
else:
    pass
