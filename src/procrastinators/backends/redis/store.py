"""A Redis or Valkey deployment as an admission authority, without the I/O.

:class:`RedisStore` holds what a deployment needs to be addressed — where it
is, how keys are laid out, which algorithms run natively — and expresses every
operation as a generator of :class:`Call` values. A transport sends each call
and sends the reply back in; the generator decodes it, decides, and finally
returns a result. The synchronous and asynchronous handles therefore share
every decision and differ only in how they wait for the network.

Decoding fails closed: a malformed field is
:exc:`~procrastinators.errors.StateCorruption`, never an empty bucket.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import dataclasses
import hashlib
import re
import threading
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, TypeAlias, TypeVar

from procrastinators.algorithms import reference_algorithms
from procrastinators.backends import bookkeeping
from procrastinators.backends.redis.layout import (
    DEFAULT_PREFIX,
    LAYOUT_VERSION,
    KeyLayout,
    RuleKeys,
    key_slot,
)
from procrastinators.backends.redis.scripts import packaged
from procrastinators.capabilities import Mode
from procrastinators.errors import (
    BackendBusy,
    ConfigurationError,
    InvalidPolicy,
    PolicyConflict,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.models import (
    MAX_AMOUNT,
    MAX_COST,
    MAX_DURATION_US,
    MAX_EXACT_INT,
    MAX_PERIOD_US,
    MAX_TIMESTAMP_US,
    Admission,
    Algorithms,
    BackendIdentity,
    Capabilities,
    Cooldown,
    CoordinationScope,
    Decision,
    Durability,
    DurationMicros,
    EpochMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    PolicyFingerprint,
    RemainingEstimate,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    Snapshot,
    TokenBucketPolicy,
)
from procrastinators.protocols import (
    LogEvent,
    MigrationStatus,
    NativeExecutorSpec,
    PolicyMigration,
    RuleState,
    StateRepresentation,
    StoredPolicy,
)
from procrastinators.state import UNUSED

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping, Sequence

    from procrastinators.backends.redis.scripts import Script
    from procrastinators.models import (
        AdmissionRequest,
        Constraint,
        QuotaIdentity,
        RuleId,
    )
    from procrastinators.protocols import AdmissionClock, Algorithm
else:
    pass

__all__ = [
    "DEFAULT_PORT",
    "FAMILIES",
    "REDIS_CAPABILITIES",
    "Call",
    "RedisStore",
    "ScriptOperation",
    "native_executor_specs",
]

_INTEGER: Final = re.compile(r"-?[0-9]+")

FAMILIES: Final = ("redis", "valkey")
"""The backend families this store serves; one implementation speaks to both servers."""

DEFAULT_PORT: Final = 6379
"""The port an address without one names."""

POLICY_VERSION: Final = 1
"""The version of the policy parameters the native executors read."""

MAX_ADMIN_ROUNDS: Final = 16
"""Compare-and-swap rounds an administrative change gets before it is contention."""

_NATIVE: Final = frozenset(Algorithms)
_REPRESENTATIONS: Final = frozenset({StateRepresentation.SCALARS, StateRepresentation.EVENT_LOG})

REDIS_CAPABILITIES: Final = Capabilities(
    algorithms=frozenset(str(algorithm) for algorithm in _NATIVE),
    coordination=CoordinationScope.SHARED_SERVICE,
    durability=Durability.SERVICE_DURABLE,
    supports_sync=True,
    supports_async=True,
    supports_composition=True,
    supports_cooldowns=True,
    supports_policy_administration=True,
    native_executors=frozenset(str(algorithm) for algorithm in _NATIVE),
    state_representations=_REPRESENTATIONS,
)
"""What the ``redis`` and ``valkey`` families declare: native executors for all five algorithms."""


def native_executor_specs(family: str) -> tuple[NativeExecutorSpec, ...]:
    """The native executors a family runs, one per built-in algorithm.

    Each names the shared traces it is verified against — every trace of its
    algorithm — and the full numeric range of contract N1, within which the
    policy models keep every intermediate value exact (N3).

    :param family: ``redis`` or ``valkey``.
    """
    from procrastinators.testing.catalog import TRACES

    specs = tuple(
        NativeExecutorSpec(
            backend_family=family,
            algorithm_id=str(algorithm),
            state_version=1,
            policy_version=POLICY_VERSION,
            max_amount=MAX_AMOUNT,
            max_cost=MAX_COST,
            max_period_us=DurationMicros(MAX_PERIOD_US),
            conformance_traces=tuple(
                trace.name for trace in TRACES if str(algorithm) in trace.algorithms
            ),
        )
        for algorithm in sorted(_NATIVE)
    )
    return specs


@dataclass(frozen=True, slots=True)
class Call:
    """One command for a transport to send, and what a lost reply would mean.

    A script is sent by ``EVALSHA`` with ``keys`` and ``args``; anything else
    is the raw ``command``. ``slot`` routes it within a cluster.
    """

    slot: int
    """The hash slot every key of the call lies in."""

    script: Script | None = None
    """The script to run, or ``None`` for a raw command."""

    keys: tuple[str, ...] = tuple()
    """The script's keys."""

    args: tuple[str, ...] = tuple()
    """The script's arguments."""

    command: tuple[str, ...] = tuple()
    """A raw command, when ``script`` is ``None``."""

    writes: bool = False
    """Whether the call may change data."""

    admission: AdmissionRequest | None = None
    """The request a call admits, when a lost reply leaves an admission in doubt."""


Reply: TypeAlias = Any
"""A decoded server reply: bytes or text, integers, and nested lists."""

ScriptResultT = TypeVar("ScriptResultT")
"""What an operation finally returns."""

ScriptOperation: TypeAlias = "Generator[Call, Reply, ScriptResultT]"
"""A store operation: yields calls, is sent their replies, returns its result."""


def _text(value: object) -> str:
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="strict")
    elif isinstance(value, str):
        text = value
    else:
        raise StateCorruption(f"expected text from the server, got {value!r}")
    return text


def _pairs(flat: object) -> dict[str, str]:
    if not isinstance(flat, list) or len(flat) % 2:
        raise StateCorruption(f"expected field/value pairs from the server, got {flat!r}")
    else:
        pass
    pairs = {_text(flat[index]): _text(flat[index + 1]) for index in range(0, len(flat), 2)}
    return pairs


def _integer(text: str | None, *, what: str, low: int, high: int) -> int:
    if text is None or _INTEGER.fullmatch(text) is None:
        raise StateCorruption(f"stored {what} {text!r} is not an integer")
    else:
        pass
    value = int(text)
    if not low <= value <= high:
        raise StateCorruption(f"stored {what} {value} is outside {low} to {high}")
    else:
        pass
    return value


def _fingerprint(text: str | None) -> PolicyFingerprint | None:
    fingerprint = None if text is None else PolicyFingerprint(text)
    return fingerprint


def _timestamp(text: str | None, *, what: str) -> EpochMicros:
    stamp = EpochMicros(_integer(text, what=what, low=0, high=MAX_TIMESTAMP_US))
    return stamp


def _parameters(constraint: Constraint) -> tuple[int, int, int, int]:
    """The four numbers the native executor for ``constraint`` reads, in its order."""
    policy = constraint.policy
    if isinstance(policy, FixedWindowPolicy):
        parameters = (policy.amount, policy.period_us, policy.epoch_offset_us, 0)
    elif isinstance(policy, (SlidingLogPolicy, SlidingCounterPolicy)):
        parameters = (policy.amount, policy.period_us, 0, 0)
    elif isinstance(policy, TokenBucketPolicy):
        parameters = (
            policy.capacity,
            policy.refill_amount,
            policy.refill_period_us,
            policy.starting_tokens,
        )
    elif isinstance(policy, LeakyBucketPolicy):
        parameters = (policy.amount, policy.period_us, policy.burst_tolerance, 0)
    else:
        raise UnsupportedCapability(
            f"{constraint.rule} uses a {type(policy).__name__}, which no native executor reads"
        )
    return parameters


@dataclass(slots=True)
class _Rule(bookkeeping.LoadedRule):
    """One rule as a read script returned it, decoded and checked."""

    revision: str = ""
    """The rule's metadata revision, which an administrative write must still find."""


def _decode_rule(rule: RuleId, meta_flat: object, state_flat: object, members: object) -> _Rule:
    meta = _pairs(meta_flat)
    state = _pairs(state_flat)
    if meta and meta.get("lay") != LAYOUT_VERSION:
        raise UnsupportedCapability(
            f"{rule} is stored in key layout {meta.get('lay')!r}, and this library reads layout "
            f"{LAYOUT_VERSION}; it is refused rather than reinterpreted"
        )
    else:
        pass
    if "fp" in meta:
        stored: StoredPolicy | None = StoredPolicy(
            rule,
            meta.get("alg", ""),
            PolicyFingerprint(meta["fp"]),
            _integer(meta.get("sv"), what="state version", low=1, high=0xFFFF),
            _timestamp(meta.get("at"), what="policy update time"),
        )
    else:
        stored = None
    if (status := meta.get("ms")) is None:
        migration = None
        target = None
    else:
        try:
            parsed = MigrationStatus(status)
        except ValueError as error:
            raise StateCorruption(f"stored migration status {status!r} is unknown") from error
        drained = meta.get("md")
        migration = PolicyMigration(
            rule,
            parsed,
            from_fingerprint=_fingerprint(meta.get("mf")),
            to_fingerprint=_fingerprint(meta.get("mt")),
            drained_after=(
                None if drained is None else _timestamp(drained, what="migration drain time")
            ),
        )
        version = meta.get("mv")
        target = (
            None
            if version is None
            else _integer(version, what="target state version", low=1, high=0xFFFF)
        )
    if not state:
        decoded = UNUSED
        horizon = None
    elif state.get("x") != "1":
        raise StateCorruption(f"the state of {rule} carries no existence marker")
    else:
        horizon_text = state.get("h", "-")
        horizon = None if horizon_text == "-" else _timestamp(horizon_text, what="horizon")
        scalars = tuple(
            sorted(
                (
                    name[2:],
                    _integer(
                        value, what=f"scalar {name!r}", low=-MAX_EXACT_INT, high=MAX_EXACT_INT
                    ),
                )
                for name, value in state.items()
                if name.startswith("v:")
            )
        )
        if not isinstance(members, list):
            raise StateCorruption(f"the events of {rule} are not a list")
        else:
            pass
        events = list()
        for member in members:
            parts = _text(member).split(":")
            if len(parts) != 3:
                raise StateCorruption(f"event {member!r} of {rule} is malformed")
            else:
                pass
            events.append(
                LogEvent(
                    _timestamp(parts[0], what="event time"),
                    _integer(parts[1], what="event cost", low=1, high=MAX_COST),
                )
            )
        decoded = RuleState(scalars=scalars, events=tuple(sorted(events)))
    entry = _Rule(None, stored, migration, target, decoded, horizon, revision=meta.get("rev", ""))
    return entry


class RedisStore:
    """A Redis or Valkey deployment as an admission authority.

    Constructing a store parses its address and touches nothing else: no
    client, connection, or script exists until a handle first needs one. The
    store runs the five built-in algorithms as native executors — Lua scripts
    verified against the reference traces — and refuses any other algorithm,
    since a Python evaluator cannot run inside the server (contract Y5).

    Authority time is the server's ``TIME``, sampled inside each admission
    script and clamped against the latest time stored for each rule, so it
    never runs backwards for a rule's state (T7). A ``clock`` replaces it, for
    deterministic tests only: every client would then have to share it, and
    since the server's key expiry cannot follow an injected clock, state then
    never expires.

    :param url: ``redis://``, ``rediss://``, ``valkey://``, or ``valkeys://``, with an
        optional ``/db``; the host defaults to ``localhost`` and the port to 6379.
    :param family: ``redis`` or ``valkey``, as the store's identity shows it; taken from
        ``url`` when given.
    :param cluster: Whether the address names a Redis Cluster, whose keys a
        request must keep in one hash slot.
    :param prefix: Starts every key, separating deployments that share a server.
    :param clock: Authority time for tests; the server's clock when ``None``.
    :param require_noeviction: Whether to refuse a server whose ``maxmemory-policy``
        could evict quota state; see the module notes of
        :mod:`procrastinators.backends.redis`.
    :param socket_timeout: Seconds a connection waits to connect, and for a reply
        outside admission, on clients the store creates.
    :param client_options: More keyword arguments for clients the store creates.
    :raises ~procrastinators.errors.ConfigurationError: The address or a setting is invalid.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        family: str | None = None,
        cluster: bool = False,
        prefix: str = DEFAULT_PREFIX,
        clock: AdmissionClock | None = None,
        require_noeviction: bool = True,
        socket_timeout: float = 5.0,
        client_options: Mapping[str, object] | None = None,
    ) -> None:
        parts = urllib.parse.urlsplit(url or "redis://localhost")
        scheme = parts.scheme
        chosen = family or ("valkey" if scheme.startswith("valkey") else "redis")
        if scheme not in ("redis", "rediss", "valkey", "valkeys"):
            raise ConfigurationError(f"not a redis or valkey address: {url!r}")
        elif chosen not in FAMILIES:
            raise ConfigurationError(f"family must be one of {FAMILIES}, got {family!r}")
        elif parts.fragment:
            raise ConfigurationError(f"a redis address carries no fragment: {url!r}")
        elif isinstance(socket_timeout, bool) or not (
            isinstance(socket_timeout, (int, float)) and socket_timeout > 0
        ):
            raise ConfigurationError(f"socket_timeout must be positive, got {socket_timeout!r}")
        else:
            pass
        path = parts.path.strip("/")
        if cluster and path not in ("", "0"):
            raise ConfigurationError("a Redis Cluster has only database 0")
        elif path and not path.isdigit():
            raise ConfigurationError(f"a redis database is a number, got {path!r} in {url!r}")
        else:
            pass
        try:
            port = parts.port or DEFAULT_PORT
        except ValueError as error:
            raise ConfigurationError(f"{url!r} names an invalid port") from error
        self._family = chosen
        self._host = parts.hostname or "localhost"
        self._port = port
        self._database = int(path or 0)
        self._tls = scheme in ("rediss", "valkeys")
        # The driver reads only redis:// and rediss://; credentials stay in the
        # URL handed to it and never reach the identity.
        self._url = urllib.parse.urlunsplit(
            ("rediss" if self._tls else "redis", parts.netloc or self._host, parts.path, "", "")
        )
        self._query = parts.query
        self._cluster = cluster
        self._layout = KeyLayout(prefix)
        self._clock = clock
        self._require_noeviction = require_noeviction
        self._socket_timeout = float(socket_timeout)
        self._client_options = dict(client_options or dict())
        self._lock = threading.Lock()
        self._algorithms: dict[str, Algorithm[Any]] = {
            algorithm.id: algorithm for algorithm in reference_algorithms()
        }
        self._executors = {spec.algorithm_id: spec for spec in native_executor_specs(chosen)}
        self._constraints: dict[RuleId, Constraint] = dict()

    @property
    def family(self) -> str:
        """``redis`` or ``valkey``."""
        return self._family

    @property
    def authority(self) -> str:
        """``host:port/db``, or ``cluster:host:port`` for a cluster; never credentials."""
        if self._cluster:
            authority = f"cluster:{self._host}:{self._port}"
        else:
            authority = f"{self._host}:{self._port}/{self._database}"
        return authority

    @property
    def cluster(self) -> bool:
        """Whether the store is a Redis Cluster."""
        return self._cluster

    @property
    def layout(self) -> KeyLayout:
        """Where rules and cooldowns live in the keyspace."""
        return self._layout

    @property
    def require_noeviction(self) -> bool:
        """Whether a server that could evict quota state is refused."""
        return self._require_noeviction

    def identity(self, namespace: str) -> BackendIdentity:
        """The identity of a handle using ``namespace`` on this store.

        :param namespace: The handle's quota namespace.
        """
        if self._layout.prefix == DEFAULT_PREFIX:
            authority = self.authority
        else:
            prefix_digest = hashlib.sha256(self._layout.prefix.encode()).hexdigest()
            authority = f"{self.authority};prefix-sha256={prefix_digest}"
        identity = BackendIdentity(self._family, authority, namespace)
        return identity

    def capabilities(self, mode: Mode) -> Capabilities:
        """What a handle in ``mode`` implements.

        :param mode: The handle's interface.
        """
        capabilities = dataclasses.replace(
            REDIS_CAPABILITIES,
            supports_sync=mode is Mode.SYNC,
            supports_async=mode is Mode.ASYNC,
        )
        return capabilities

    def client_arguments(self) -> tuple[str, dict[str, Any]]:
        """The URL and keyword arguments a created client is built with."""
        options: dict[str, Any] = {
            "socket_timeout": self._socket_timeout,
            "socket_connect_timeout": self._socket_timeout,
            **dict(urllib.parse.parse_qsl(self._query)),
            **self._client_options,
        }
        arguments = (self._url, options)
        return arguments

    #
    # Requests.

    def validate(self, request: AdmissionRequest) -> None:
        """Refuse what no native executor verified, or what a cluster cannot run atomically.

        :param request: The request about to be admitted.
        :raises ~procrastinators.errors.UnsupportedCapability: A constraint is outside every
            native executor's verified range, or composed rules fall in several cluster slots.
        :raises ~procrastinators.errors.InvalidPolicy: A policy is invalid for its algorithm.
        """
        for constraint in request.constraints:
            spec = self._executors.get(constraint.algorithm)
            if spec is None or not spec.accepts(constraint) or request.cost > spec.max_cost:
                raise UnsupportedCapability(
                    f"no {self._family} native executor is verified for {constraint}; a Python "
                    "algorithm does not run inside the server (Y5)"
                )
            else:
                self._algorithms[constraint.algorithm].validate(constraint.policy)
        if (
            self._cluster
            and len({constraint.rule.scope for constraint in request.constraints}) > 1
            and request.coordination_domain is None
        ):
            raise UnsupportedCapability(
                "composed rules of several scopes on Redis Cluster require one "
                "coordination domain (C4, I6)"
            )
        else:
            pass
        if self._cluster and len(self._slots(request)) > 1:
            raise UnsupportedCapability("composed rules must share one Redis Cluster hash slot")
        else:
            pass

    def _slots(self, request: AdmissionRequest) -> set[int]:
        slots = {key_slot(self.rule_keys(constraint).meta) for constraint in request.constraints}
        return slots

    def rule_keys(self, constraint: Constraint) -> RuleKeys:
        """Where ``constraint``'s rule lives.

        A domain never changes a rule's identity or placement. A cluster uses
        one shared tag for rules and cooldowns so one script can check both.

        :param constraint: The constraint whose rule to place.
        """
        tag = self._layout.cluster_tag if self._cluster else None
        keys = self._layout.rule_keys(
            constraint.rule, tag or KeyLayout.scope_tag(constraint.rule.scope)
        )
        return keys

    def _keys_for(self, rule: RuleId, domain: str | None) -> RuleKeys:
        """The keys of an administered or inspected rule, placed as its admissions place it."""
        tag = self._layout.cluster_tag if self._cluster else None
        keys = self._layout.rule_keys(rule, tag or KeyLayout.scope_tag(rule.scope))
        return keys

    def _now_argument(self) -> str:
        now = "" if self._clock is None else str(self._clock.now())
        return now

    def admit(
        self, request: AdmissionRequest, identity: BackendIdentity
    ) -> ScriptOperation[Decision]:
        """One admission: every rule admits and is debited, or none is.

        :param request: A validated request.
        :param identity: The admitting handle's identity, recorded on the admission.
        :returns: An operation producing the decision.
        """
        constraints = request.constraints
        keys: list[str] = list()
        for constraint in constraints:
            keys.extend(self.rule_keys(constraint).all)
        slot = key_slot(keys[0])
        references: dict[QuotaIdentity, str] = dict()
        for scope in dict.fromkeys(constraint.rule.scope for constraint in constraints):
            cooldown_key = self._layout.cooldown_key(scope, cluster=self._cluster)
            keys.append(cooldown_key)
            references[scope] = f"k{len(keys)}"
        args = [LAYOUT_VERSION, self._now_argument(), str(request.cost), str(len(constraints))]
        for constraint in constraints:
            args.extend(
                (
                    constraint.algorithm,
                    constraint.fingerprint,
                    str(constraint.state_version),
                    *(str(parameter) for parameter in _parameters(constraint)),
                    references[constraint.rule.scope],
                )
            )
        reply = yield Call(
            slot,
            packaged("admit"),
            tuple(keys),
            tuple(args),
            writes=True,
            admission=request,
        )
        decision = self._decision(request, reply, identity)
        with self._lock:
            for constraint in constraints:
                self._constraints[constraint.rule] = constraint
        return decision

    def _decision(
        self, request: AdmissionRequest, reply: Reply, identity: BackendIdentity
    ) -> Decision:
        if not isinstance(reply, list) or not reply:
            raise StateCorruption(f"the admission script replied {reply!r}")
        else:
            pass
        kind = _text(reply[0])
        constraints = request.constraints
        if kind == "ok":
            now = EpochMicros(int(reply[2]))
            admitted = bool(int(reply[1]))
            retry = DurationMicros(int(reply[3]))
            verdicts = reply[4:]
            blocking = tuple(
                constraint.rule
                for index, constraint in enumerate(constraints)
                if not int(verdicts[2 * index])
            )
            remaining = tuple(
                RemainingEstimate(constraint.rule, int(text))
                for index, constraint in enumerate(constraints)
                if (text := _text(verdicts[2 * index + 1]))
            )
            if admitted:
                admission = Admission(request.rules, request.cost, now, identity)
                decision = Decision.allow(admission, remaining=remaining, observed_at=now)
            else:
                decision = Decision.deny(blocking, retry, remaining=remaining, observed_at=now)
        else:
            constraint = constraints[int(reply[1]) - 1]
            detail = _text(reply[2])
            raise _refusal(kind, constraint, detail, reply)
        return decision

    def inspect(
        self, rules: Sequence[RuleId], identity: BackendIdentity
    ) -> ScriptOperation[Snapshot]:
        """Advisory observation of ``rules``, committing nothing (R7).

        One consistent read per hash slot; a rule this process has admitted is
        evaluated for a cost of one by its reference evaluator.

        :param rules: The rules to observe.
        :param identity: The observing handle's identity.
        """
        placed = {rule: self._keys_for(rule, None) for rule in rules}
        cooldowns = {
            rule.scope: self._layout.cooldown_key(rule.scope, cluster=self._cluster)
            for rule in rules
        }
        by_slot: dict[int, tuple[list[RuleId], list[QuotaIdentity]]] = dict()
        for rule, keys in placed.items():
            by_slot.setdefault(key_slot(keys.meta), (list(), list()))[0].append(rule)
        for scope, key in cooldowns.items():
            by_slot.setdefault(key_slot(key), (list(), list()))[1].append(scope)
        entries: dict[RuleId, _Rule] = dict()
        until: dict[QuotaIdentity, EpochMicros | None] = dict()
        now = EpochMicros(0)
        for slot, (slot_rules, slot_scopes) in by_slot.items():
            keys = [key for rule in slot_rules for key in placed[rule].all]
            keys.extend(cooldowns[scope] for scope in slot_scopes)
            reply = yield Call(
                slot,
                packaged("read"),
                tuple(keys),
                (self._now_argument(), str(len(slot_rules))),
            )
            sampled, decoded, ends = _read_reply(reply, slot_rules)
            now = EpochMicros(max(now, sampled))
            entries.update(decoded)
            until.update(zip(slot_scopes, ends, strict=True))
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
            snapshots.append(
                bookkeeping.rule_snapshot(
                    rule,
                    constraint=constraint,
                    algorithm=entry.stored.algorithm_id if entry.stored is not None else None,
                    state=entry.state,
                    horizon=entry.horizon,
                    cooldown_until=until[rule.scope],
                    resolve=self._resolve,
                    now=now,
                )
            )
        snapshot = Snapshot(rules=tuple(snapshots), sampled_at=now, backend=identity)
        return snapshot

    def _resolve(self, constraint: Constraint) -> Algorithm[Any]:
        if (algorithm := self._algorithms.get(constraint.algorithm)) is None:
            raise UnsupportedCapability(f"no evaluator for algorithm {constraint.algorithm!r}")
        else:
            pass
        return algorithm

    def defer(
        self, scope: QuotaIdentity, duration: DurationMicros, reason: str
    ) -> ScriptOperation[Cooldown]:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on a cooldown this call creates or lengthens.
        :raises ~procrastinators.errors.InvalidPolicy: ``duration`` is out of range.
        """
        if isinstance(duration, bool) or not isinstance(duration, int):
            raise InvalidPolicy(f"cooldown duration must be an integer, got {duration!r}")
        elif not 0 <= duration <= MAX_DURATION_US:
            raise InvalidPolicy(f"cooldown duration must be between 0 and {MAX_DURATION_US} µs")
        else:
            pass
        key = self._layout.cooldown_key(scope, cluster=self._cluster)
        reply = yield Call(
            key_slot(key),
            packaged("defer"),
            (key,),
            (self._now_argument(), str(duration), reason, str(MAX_TIMESTAMP_US)),
            writes=True,
        )
        if not isinstance(reply, list) or not reply:
            raise StateCorruption(f"the cooldown script replied {reply!r}")
        elif _text(reply[0]) == "range":
            raise InvalidPolicy(f"a cooldown of {duration} µs ends beyond the supported range")
        else:
            pass
        cooldown = Cooldown(
            scope, _timestamp(_text(reply[0]), what="cooldown end"), _text(reply[1])
        )
        return cooldown

    #
    # Policy administration: read, decide with the shared bookkeeping, write
    # only if the rule's revision is unchanged.

    def _read_rule(
        self, rule: RuleId, keys: RuleKeys
    ) -> ScriptOperation[tuple[EpochMicros, _Rule]]:
        reply = yield Call(
            key_slot(keys.meta), packaged("read"), keys.all, (self._now_argument(), "1")
        )
        now, entries, _ = _read_reply(reply, [rule])
        result = (now, entries[rule])
        return result

    def _apply(
        self,
        keys: RuleKeys,
        entry: _Rule,
        migration: PolicyMigration,
        target_version: int | None,
        *,
        forget: bool = False,
    ) -> ScriptOperation[bool]:
        fields = {
            "ms": migration.status.value,
            "mf": migration.from_fingerprint or "",
            "mt": migration.to_fingerprint or "",
            "mv": "" if target_version is None else str(target_version),
            "md": "" if migration.drained_after is None else str(migration.drained_after),
        }
        reply = yield Call(
            key_slot(keys.meta),
            packaged("apply"),
            keys.all,
            (
                LAYOUT_VERSION,
                entry.revision,
                "forget" if forget else "keep",
                *(part for pair in fields.items() for part in pair),
            ),
            writes=True,
        )
        applied = isinstance(reply, list) and bool(reply) and _text(reply[0]) == "ok"
        return applied

    def stored_policy(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> ScriptOperation[StoredPolicy | None]:
        """The policy metadata stored for ``rule``, if any.

        :param rule: The rule to look up.
        :param coordination_domain: The domain its constraints declare, if any and not
            already learned from an admission in this process.
        """
        _, entry = yield from self._read_rule(rule, self._keys_for(rule, coordination_domain))
        return entry.stored

    def begin_migration(
        self,
        rule: RuleId,
        to_fingerprint: PolicyFingerprint,
        to_state_version: int,
        *,
        coordination_domain: str | None = None,
    ) -> ScriptOperation[PolicyMigration]:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        As :meth:`~procrastinators.backends.sqlite.SQLiteStore.begin_migration`.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :param coordination_domain: As for :meth:`stored_policy`.
        :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
        """
        keys = self._keys_for(rule, coordination_domain)
        for _ in range(MAX_ADMIN_ROUNDS):
            now, entry = yield from self._read_rule(rule, keys)
            target = entry.target_version
            migration = bookkeeping.begin_migration(
                rule, entry.migration, current=entry.current, to_fingerprint=to_fingerprint
            )
            if migration is not entry.migration:
                target = to_state_version
            else:
                pass
            refreshed = bookkeeping.refresh_migration(
                migration, neutral=entry.neutral(now), horizon=entry.horizon, now=now
            )
            assert refreshed is not None
            if (yield from self._apply(keys, entry, refreshed, target)):
                break
            else:
                pass
        else:
            raise _contention(rule)
        return refreshed

    def migration_status(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> ScriptOperation[PolicyMigration | None]:
        """Progress of ``rule``'s migration, if any, recorded as it stands now.

        :param rule: The rule whose migration to report.
        :param coordination_domain: As for :meth:`stored_policy`.
        """
        keys = self._keys_for(rule, coordination_domain)
        for _ in range(MAX_ADMIN_ROUNDS):
            now, entry = yield from self._read_rule(rule, keys)
            refreshed = bookkeeping.refresh_migration(
                entry.migration, neutral=entry.neutral(now), horizon=entry.horizon, now=now
            )
            if (
                refreshed is None
                or refreshed == entry.migration
                or (yield from self._apply(keys, entry, refreshed, entry.target_version))
            ):
                break
            else:
                pass
        else:
            raise _contention(rule)
        return refreshed

    def complete_migration(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> ScriptOperation[PolicyMigration]:
        """Install the new policy on a drained rule, forgetting its neutral old state.

        :param rule: The rule whose migration to complete.
        :param coordination_domain: As for :meth:`stored_policy`.
        :raises ~procrastinators.errors.PolicyConflict: No migration is ready for ``rule``.
        """
        keys = self._keys_for(rule, coordination_domain)
        for _ in range(MAX_ADMIN_ROUNDS):
            now, entry = yield from self._read_rule(rule, keys)
            refreshed = bookkeeping.refresh_migration(
                entry.migration, neutral=entry.neutral(now), horizon=entry.horizon, now=now
            )
            completed = bookkeeping.complete_migration(rule, refreshed)
            if (yield from self._apply(keys, entry, completed, entry.target_version, forget=True)):
                break
            else:
                pass
        else:
            raise _contention(rule)
        with self._lock:
            self._constraints.pop(rule, None)
        return completed

    def check_eviction(self, slot: int = 0) -> ScriptOperation[None]:
        """Refuse a server whose memory policy could evict quota state.

        Any policy but ``noeviction`` may delete keys before their safe-forget
        horizon, and a vanished key reads as unused quota. A server that will
        not say — ``CONFIG`` renamed or denied, as on some managed services — is
        trusted, since the deployment's documentation must then vouch for it.

        :param slot: A slot on the node to ask, in a cluster.
        :raises ~procrastinators.errors.UnsupportedCapability: The policy can evict.
        """
        reply = yield Call(slot, command=("CONFIG", "GET", "maxmemory-policy"))
        if isinstance(reply, list) and len(reply) == 2:
            policy = _text(reply[1])
        elif isinstance(reply, dict) and reply:
            policy = _text(next(iter(reply.values())))
        else:
            policy = "noeviction"
        if policy != "noeviction":
            raise UnsupportedCapability(
                f"{self._family} at {self.authority} runs maxmemory-policy {policy!r}, which can "
                "evict quota state before it is safe to forget; configure 'noeviction', or pass "
                "require_noeviction=False to accept the weaker guarantee knowingly"
            )
        else:
            pass

    def __repr__(self) -> str:
        text = f"RedisStore({self._family}://{self.authority})"
        return text


def _read_reply(
    reply: Reply, rules: Sequence[RuleId]
) -> tuple[EpochMicros, dict[RuleId, _Rule], list[EpochMicros | None]]:
    if not isinstance(reply, list) or not reply:
        raise StateCorruption(f"the read script replied {reply!r}")
    elif isinstance(reply[0], (bytes, str)) and _text(reply[0]) == "corrupt":
        raise StateCorruption(f"stored data has the wrong type: {_text(reply[2])}")
    else:
        pass
    now = EpochMicros(int(reply[0]))
    entries = {
        rule: _decode_rule(rule, *reply[1 + 3 * index : 4 + 3 * index])
        for index, rule in enumerate(rules)
    }
    ends: list[EpochMicros | None] = list()
    for pair in reply[1 + 3 * len(rules) :]:
        text = _text(pair[0])
        ends.append(_timestamp(text, what="cooldown end") if text else None)
    result = (now, entries, ends)
    return result


def _refusal(kind: str, constraint: Constraint, detail: str, reply: Reply) -> Exception:
    """The error an admission script's refusal means; nothing was written."""
    rule = constraint.rule
    if kind == "conflict":
        found = PolicyFingerprint(_text(reply[3]))
        if detail == "migrating":
            error: Exception = PolicyConflict(
                f"{rule} is being migrated; admissions stay stopped until the migration "
                "completes (L12)",
                rule=rule,
                expected=constraint.fingerprint,
                found=found or None,
            )
        elif detail == "migrated":
            error = PolicyConflict(
                f"{rule} was migrated to another policy",
                rule=rule,
                expected=constraint.fingerprint,
                found=found,
            )
        else:
            error = PolicyConflict(
                f"{rule} is stored under another policy; change it with an explicit migration, "
                "never by starting with a different configuration (I4)",
                rule=rule,
                expected=constraint.fingerprint,
                found=found,
            )
    elif kind == "layout":
        error = UnsupportedCapability(
            f"{rule} is stored in key layout {detail!r}, and this library reads layout "
            f"{LAYOUT_VERSION}; it is refused rather than reinterpreted"
        )
    elif kind == "invalid":
        error = InvalidPolicy(
            f"admitting {rule} would schedule it beyond the supported timestamp range ending at "
            f"{MAX_TIMESTAMP_US} (N2)"
        )
    elif kind == "unsupported":
        error = UnsupportedCapability(f"the admission script has no executor for {detail}")
    elif kind == "corrupt":
        error = StateCorruption(f"the stored state of {rule} is malformed: {detail}")
    else:
        error = StateCorruption(f"the admission script replied {kind!r} for {rule}")
    return error


def _contention(rule: RuleId) -> BackendBusy:
    error = BackendBusy(
        f"{rule} kept changing during {MAX_ADMIN_ROUNDS} administrative rounds; contention "
        "is not a denial (O2)"
    )
    return error


if __name__ == "__main__":
    pass
else:
    pass
