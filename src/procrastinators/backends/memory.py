"""The atomic in-process memory backend.

A :class:`MemoryStore` is the authority: quota state, policy metadata,
cooldowns, and migrations for one process, behind one lock. Handles address it
— :class:`MemoryBackend` synchronously, :class:`AsyncMemoryBackend` from an
event loop — and any number of handles, limiters, threads, and tasks sharing a
store obey its quotas together.

**One critical section.** Admission samples authority time, checks policy
metadata, loads state, evaluates every rule against that one sample, and
commits every debit or none, all under the store's lock (contracts A1, T4,
A6). The work inside is bounded by what the algorithms declared; no I/O
happens there and nothing waits for quota there (W1).

**Async without blocking the loop.** An asynchronous handle never waits on the
thread lock. It tries to take it without blocking and, while another thread
holds it, yields to the event loop between tries until the request's lock
budget runs out, then raises :exc:`~procrastinators.errors.BackendBusy` (O2).
Nothing inside the critical section awaits, so a cancellation lands either
before the lock was taken, having consumed nothing, or not at all (O5).

**Cleanup.** State is forgotten only past the safe-forget horizon its
algorithm reported (L9), lazily and on a bounded cadence, and policy metadata
is never forgotten with it (L10). A store given ``max_rules`` refuses new state
when full of active rules rather than evicting one (L11).

**Fork.** A store belongs to the process that created it. After ``fork`` the
child's copy refuses to serve, because a copy coordinates with nobody and its
lock may have been copied mid-admission (L8, Y3). The process-wide stores
behind ``memory://`` addresses are per process, so a child resolving the same
address gets a fresh, empty store of its own.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import dataclasses
import functools
import os
import threading
import time
import urllib.parse
import uuid
from collections.abc import Hashable
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, TypeVar

from procrastinators.algorithms import builtin_specs, reference_algorithms
from procrastinators.backends import bookkeeping
from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.capabilities import Mode
from procrastinators.clocks import LocalEpochClock
from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ConfigurationError,
    UnsupportedCapability,
)
from procrastinators.models import (
    USECS_PER_SECOND,
    Admission,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Durability,
    DurationMicros,
    EpochMicros,
    Snapshot,
)
from procrastinators.protocols import (
    ObservationPoint,
    StateRepresentation,
    StoredPolicy,
)
from procrastinators.state import UNUSED, plan_admission

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from procrastinators.models import (
        AdmissionRequest,
        Constraint,
        Cooldown,
        Decision,
        PolicyFingerprint,
        QuotaIdentity,
        RuleId,
    )
    from procrastinators.protocols import (
        AdmissionClock,
        AdmissionObserver,
        Algorithm,
        PolicyMigration,
        RuleState,
    )
    from procrastinators.state import AdmissionPlan
else:
    pass

__all__ = [
    "DEFAULT_LOCK_TIMEOUT_US",
    "DEFAULT_STORE_NAME",
    "MEMORY_CAPABILITIES",
    "AsyncMemoryBackend",
    "MemoryBackend",
    "MemoryStore",
    "memory_backend",
]

FAMILY: Final = "memory"
"""The backend family, as it appears in identities and ``memory://`` addresses."""

DEFAULT_STORE_NAME: Final = "default"
"""The process-wide store ``memory://`` addresses without a name resolve to."""

DEFAULT_LOCK_TIMEOUT_US: Final = DurationMicros(5 * USECS_PER_SECOND)
"""Lock budget for inspection, cooldowns, and administration, which carry no budget of their own."""

DEFAULT_SWEEP_EVERY: Final = 4096
"""Commits between sweeps of state past its safe-forget horizon."""

_POLL_START_S: Final = 50e-6
_POLL_MAX_S: Final = 5e-3

MEMORY_CAPABILITIES: Final = Capabilities(
    algorithms=frozenset(spec.id for spec in builtin_specs()),
    coordination=CoordinationScope.IN_PROCESS,
    durability=Durability.EPHEMERAL,
    supports_sync=True,
    supports_async=True,
    supports_composition=True,
    supports_cooldowns=True,
    supports_policy_administration=True,
    state_representations=frozenset(StateRepresentation),
)
"""What the memory family declares before any store exists; a store adds hosted algorithms."""

T = TypeVar("T")
ExpiryKeyT = TypeVar("ExpiryKeyT", bound=Hashable)

_named_lock: Final = threading.Lock()
_named_stores: Final[dict[tuple[int, str], MemoryStore]] = dict()


class _ExpiryIndex(Generic[ExpiryKeyT]):
    """An indexed min-heap with one current expiry per key."""

    def __init__(self) -> None:
        self._items: list[tuple[int, int, ExpiryKeyT]] = list()
        self._positions: dict[ExpiryKeyT, int] = dict()
        self._sequence = 0

    def _swap(self, first: int, second: int) -> None:
        self._items[first], self._items[second] = self._items[second], self._items[first]
        self._positions[self._items[first][2]] = first
        self._positions[self._items[second][2]] = second

    def _repair(self, position: int) -> None:
        while position > 0:
            parent = (position - 1) // 2
            if self._items[parent][:2] <= self._items[position][:2]:
                break
            else:
                self._swap(parent, position)
                position = parent
        else:
            pass
        size = len(self._items)
        while (left := 2 * position + 1) < size:
            right = left + 1
            child = (
                right if right < size and self._items[right][:2] < self._items[left][:2] else left
            )
            if self._items[position][:2] <= self._items[child][:2]:
                break
            else:
                self._swap(position, child)
                position = child
        else:
            pass

    def set(self, key: ExpiryKeyT, horizon: EpochMicros | None) -> None:
        if horizon is None:
            self.discard(key)
        else:
            self._sequence += 1
            item = (int(horizon), self._sequence, key)
            if (position := self._positions.get(key)) is None:
                position = len(self._items)
                self._items.append(item)
                self._positions[key] = position
            else:
                self._items[position] = item
            self._repair(position)

    def discard(self, key: ExpiryKeyT) -> None:
        if (position := self._positions.pop(key, None)) is not None:
            last = self._items.pop()
            if position < len(self._items):
                self._items[position] = last
                self._positions[last[2]] = position
                self._repair(position)
            else:
                pass
        else:
            pass

    def first_due(self, now: EpochMicros) -> ExpiryKeyT | None:
        key = self._items[0][2] if self._items and self._items[0][0] <= now else None
        return key


def _micros_to_seconds(micros: int) -> float:
    seconds = micros / USECS_PER_SECOND
    return seconds


class MemoryStore:
    """The in-process admission authority that memory handles share.

    Constructing a store takes no lock and starts nothing. It hosts the five
    reference algorithms unless given others, and more can be added with
    :meth:`host`.

    :param name: Shown in the store's identity; a random one when ``None``.
    :param algorithms: The algorithms to host; the reference algorithms when ``None``.
    :param clock: Authority epoch time, sampled inside the lock; a
        :class:`~procrastinators.clocks.LocalEpochClock` when ``None``.
    :param max_rules: The most rules holding state at once, or ``None`` for no bound.
    :param representations: State representations to accept beyond scalars and
        event logs, for third-party algorithms.
    :param sweep_every: Commits between sweeps of forgettable state.
    :raises ~procrastinators.errors.ConfigurationError: Two hosted algorithms share an id, or a
        bound is not a positive integer.
    """

    def __init__(
        self,
        *,
        name: str | None = None,
        algorithms: Iterable[Algorithm[Any]] | None = None,
        clock: AdmissionClock | None = None,
        max_rules: int | None = None,
        representations: Iterable[str] = tuple(),
        sweep_every: int = DEFAULT_SWEEP_EVERY,
    ) -> None:
        for what, bound in (("max_rules", max_rules), ("sweep_every", sweep_every)):
            if bound is not None and (
                isinstance(bound, bool) or not isinstance(bound, int) or bound < 1
            ):
                raise ConfigurationError(f"{what} must be a positive integer, got {bound!r}")
            else:
                pass
        self._name = name or uuid.uuid4().hex
        self._instance_id = uuid.uuid4().hex
        self._pid = os.getpid()
        self._clock: AdmissionClock = clock or LocalEpochClock()
        self._max_rules = max_rules
        self._sweep_every = sweep_every
        self._representations = frozenset({*StateRepresentation, *representations})
        self._lock = threading.Lock()
        self._algorithms: dict[str, Algorithm[Any]] = dict()
        self._capabilities: dict[Mode, Capabilities] = dict()
        self._states: dict[RuleId, RuleState] = dict()
        self._horizons: dict[RuleId, EpochMicros | None] = dict()
        self._state_expiry: _ExpiryIndex[RuleId] = _ExpiryIndex()
        self._policies: dict[RuleId, StoredPolicy] = dict()
        self._constraints: dict[RuleId, Constraint] = dict()
        self._pending: dict[RuleId, tuple[PolicyFingerprint, int]] = dict()
        self._migrations: dict[RuleId, PolicyMigration] = dict()
        self._targets: dict[RuleId, int] = dict()
        self._cooldowns: dict[QuotaIdentity, Cooldown] = dict()
        self._cooldown_expiry: _ExpiryIndex[QuotaIdentity] = _ExpiryIndex()
        self._last_now = EpochMicros(0)
        self._commits = 0
        self.host(reference_algorithms() if algorithms is None else algorithms)

    @classmethod
    def named(cls, name: str = DEFAULT_STORE_NAME) -> MemoryStore:
        """The process-wide store called ``name``, created on first use.

        This is what ``memory://`` and ``memory://<name>`` addresses resolve to,
        so separately constructed limiters naming the same store share its
        quotas. Each process has its own; a forked child gets a fresh one.

        :param name: The store's name.
        :raises ~procrastinators.errors.InvalidPolicy: ``name`` is not a valid name.
        """
        BackendIdentity(FAMILY, name, DEFAULT_STORE_NAME)
        key = (os.getpid(), name)
        with _named_lock:
            if (store := _named_stores.get(key)) is None:
                store = cls(name=name)
                _named_stores[key] = store
            else:
                pass
        return store

    @property
    def authority(self) -> str:
        """The store's process, display name, and unique instance token."""
        authority = f"pid-{self._pid}/{self._name}/{self._instance_id}"
        return authority

    @property
    def lock_held(self) -> bool:
        """Whether the store's lock is held right now; for sleeper guards (W1)."""
        held = self._lock.locked()
        return held

    def identity(self, namespace: str) -> BackendIdentity:
        """The identity of a handle using ``namespace`` on this store.

        :param namespace: The handle's quota namespace.
        """
        identity = BackendIdentity(FAMILY, self.authority, namespace)
        return identity

    def host(self, algorithms: Iterable[Algorithm[Any]]) -> None:
        """Host more algorithms, keeping those already hosted.

        Hosting the same algorithm again is harmless. A different
        implementation under a hosted id is refused, because two workers that
        disagreed about what an id means would be a conflict no fingerprint
        could detect.

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
        if (capabilities := self._capabilities.get(mode)) is None:
            capabilities = dataclasses.replace(
                MEMORY_CAPABILITIES,
                algorithms=frozenset(self._algorithms),
                supports_sync=mode is Mode.SYNC,
                supports_async=mode is Mode.ASYNC,
                state_representations=self._representations,
            )
            self._capabilities[mode] = capabilities
        else:
            pass
        return capabilities

    def _check_process(self) -> None:
        if (pid := os.getpid()) != self._pid:
            raise BackendUnavailable(
                f"memory store {self._name!r} was created in process {self._pid} and cannot "
                f"serve process {pid}: memory coordinates one process only, and a copy made "
                "by fork shares nothing with the original (L8)"
            )
        else:
            pass

    def acquire(self, timeout_us: int) -> None:
        """Take the lock, waiting at most ``timeout_us``.

        :param timeout_us: The contention budget, in microseconds.
        :raises ~procrastinators.errors.BackendBusy: The lock was not free in time.
        :raises ~procrastinators.errors.BackendUnavailable: This is a forked copy.
        """
        self._check_process()
        if not self._lock.acquire(timeout=_micros_to_seconds(timeout_us)):
            raise BackendBusy(
                f"memory store {self._name!r} stayed locked for {timeout_us} µs; "
                "contention is not a denial (O2)"
            )
        else:
            pass

    async def acquire_async(self, timeout_us: int) -> None:
        """Take the lock without blocking the event loop, waiting at most ``timeout_us``.

        Tries without blocking and yields between tries with a growing pause,
        so the loop keeps running while another thread holds the lock (W4).

        :param timeout_us: The contention budget, in microseconds.
        :raises ~procrastinators.errors.BackendBusy: The lock was not free in time.
        :raises ~procrastinators.errors.BackendUnavailable: This is a forked copy.
        :raises asyncio.CancelledError: Cancelled while waiting; nothing was taken.
        """
        self._check_process()
        give_up = time.monotonic() + _micros_to_seconds(timeout_us)
        pause = _POLL_START_S
        while not self._lock.acquire(blocking=False):
            if time.monotonic() >= give_up:
                raise BackendBusy(
                    f"memory store {self._name!r} stayed locked for {timeout_us} µs; "
                    "contention is not a denial (O2)"
                )
            else:
                pass
            await asyncio.sleep(pause)
            pause = min(pause * 2, _POLL_MAX_S)

    def release(self) -> None:
        """Release the lock taken by :meth:`acquire` or :meth:`acquire_async`."""
        self._lock.release()

    def _now_locked(self) -> EpochMicros:
        # Clamped against the last sample: authority time never runs backwards
        # here, whatever the injected clock does (T7).
        now = EpochMicros(max(self._clock.now(), self._last_now))
        self._last_now = now
        return now

    def _resolve(self, constraint: Constraint) -> Algorithm[Any]:
        if (algorithm := self._algorithms.get(constraint.algorithm)) is None:
            raise UnsupportedCapability(
                f"memory store {self._name!r} does not host algorithm {constraint.algorithm!r}"
            )
        else:
            pass
        return algorithm

    def admit_locked(
        self,
        request: AdmissionRequest,
        observe: Callable[[ObservationPoint, AdmissionRequest], None],
        identity: BackendIdentity,
    ) -> Decision:
        """The critical section of one admission; the caller holds the lock.

        :param request: The request.
        :param observe: Reports each observation point inside the critical section.
        :param identity: The admitting handle's identity, recorded on the admission.
        :returns: The decision; a denial is a value.
        """
        now = self._now_locked()
        self._check_policies_locked(request)
        states = {rule: self._states.get(rule, UNUSED) for rule in request.rules}
        observe(ObservationPoint.AFTER_LOAD, request)
        plan = plan_admission(
            request, states, self._resolve, now, holds=self._holds_locked(request, now)
        )
        self._reserve_locked(plan, now)
        if plan.admitted:
            observe(ObservationPoint.BEFORE_COMMIT, request)
            self._commit_locked(request, plan)
            admission = Admission(request.rules, request.cost, now, identity)
            observe(ObservationPoint.AFTER_COMMIT, request)
            decision = plan.decision(admission)
        else:
            self._commit_locked(request, plan)
            decision = plan.decision()
        return decision

    def _check_policies_locked(self, request: AdmissionRequest) -> None:
        for constraint in request.constraints:
            rule = constraint.rule
            bookkeeping.check_policy(
                constraint,
                stored=self._policies.get(rule),
                migration=self._migrations.get(rule),
                pending=self._pending.get(rule),
                resolve=self._resolve,
            )

    def _holds_locked(
        self, request: AdmissionRequest, now: EpochMicros
    ) -> dict[RuleId, DurationMicros]:
        until = {
            scope: cooldown.until
            for scope in dict.fromkeys(constraint.rule.scope for constraint in request.constraints)
            if (cooldown := self._cooldowns.get(scope)) is not None
        }
        holds = bookkeeping.cooldown_holds(request, until, now)
        return holds

    def _reserve_locked(self, plan: AdmissionPlan, now: EpochMicros) -> None:
        """Refuse new state beyond ``max_rules``, forgetting only neutral state to make room."""
        if self._max_rules is not None and (needed := self._needed_slots(plan)) > 0:
            self._sweep_locked(now, limit=max(128, needed))
            if self._needed_slots(plan) > 0:
                raise BackendUnavailable(
                    f"memory store {self._name!r} holds its limit of {self._max_rules} active "
                    "rules; it refuses new state rather than evict a rule still in use (L11)"
                )
            else:
                pass
        else:
            pass

    def _needed_slots(self, plan: AdmissionPlan) -> int:
        assert self._max_rules is not None
        new = sum(
            1 for rule, state in plan.writes.items() if state.exists and rule not in self._states
        )
        needed = len(self._states) + new - self._max_rules
        return needed

    def _commit_locked(self, request: AdmissionRequest, plan: AdmissionPlan) -> None:
        # Policy metadata is recorded on first contact, admitted or not, and
        # outlives quota state, so a later disagreement is always detectable (L10).
        for constraint in request.constraints:
            rule = constraint.rule
            if rule not in self._policies:
                self._policies[rule] = StoredPolicy(
                    rule,
                    constraint.algorithm,
                    constraint.fingerprint,
                    constraint.state_version,
                    plan.now,
                )
                self._constraints[rule] = constraint
                self._pending.pop(rule, None)
            else:
                pass
        horizons = {
            transition.rule: transition.safe_forget_after_us for transition in plan.transitions
        }
        for rule, state in plan.writes.items():
            if state.exists:
                self._states[rule] = state
                self._horizons[rule] = horizons[rule]
                self._state_expiry.set(rule, horizons[rule])
            else:
                self._states.pop(rule, None)
                self._horizons.pop(rule, None)
                self._state_expiry.discard(rule)
        self._commits += 1
        if self._commits % self._sweep_every == 0:
            self._sweep_locked(plan.now, limit=128)
        else:
            pass

    def _neutral_locked(self, rule: RuleId, now: EpochMicros) -> bool:
        horizon = self._horizons.get(rule)
        neutral = rule not in self._states or (horizon is not None and horizon <= now)
        return neutral

    def _sweep_locked(self, now: EpochMicros, *, limit: int | None = None) -> int:
        budget = len(self._states) + len(self._cooldowns) if limit is None else limit
        forgotten = 0
        while forgotten < budget and (rule := self._state_expiry.first_due(now)) is not None:
            self._state_expiry.discard(rule)
            self._states.pop(rule, None)
            self._horizons.pop(rule, None)
            forgotten += 1
        swept_cooldowns = 0
        while (
            forgotten + swept_cooldowns < budget
            and (scope := self._cooldown_expiry.first_due(now)) is not None
        ):
            self._cooldown_expiry.discard(scope)
            self._cooldowns.pop(scope, None)
            swept_cooldowns += 1
        return forgotten

    def sweep(self, *, timeout_us: int = DEFAULT_LOCK_TIMEOUT_US) -> int:
        """Forget every rule's state that is past its safe-forget horizon, and expired cooldowns.

        Admission already ignores such state; this reclaims its memory. Policy
        metadata is kept (L10).

        :param timeout_us: The contention budget, in microseconds.
        :returns: How many rules' state was forgotten.
        :raises ~procrastinators.errors.BackendBusy: The lock was not free in time.
        """
        self.acquire(timeout_us)
        try:
            forgotten = self._sweep_locked(self._now_locked())
        finally:
            self.release()
        return forgotten

    @property
    def active_rules(self) -> int:
        """How many rules hold quota state right now, forgettable or not."""
        with self._lock:
            count = len(self._states)
        return count

    def state_of(self, rule: RuleId) -> RuleState:
        """The stored state of ``rule``; the unused state if there is none. For tests.

        :param rule: The rule to look up.
        """
        with self._lock:
            state = self._states.get(rule, UNUSED)
        return state

    def inspect_locked(self, rules: Sequence[RuleId], identity: BackendIdentity) -> Snapshot:
        """Advisory observation of ``rules``; the caller holds the lock.

        Each known rule is evaluated for a cost of one without committing
        anything; a rule this store has never seen reports ``unused``.

        :param rules: The rules to observe.
        :param identity: The observing handle's identity.
        """
        now = self._now_locked()
        snapshots = list()
        for rule in rules:
            cooldown = self._cooldowns.get(rule.scope)
            constraint = self._constraints.get(rule)
            snapshots.append(
                bookkeeping.rule_snapshot(
                    rule,
                    constraint=constraint,
                    algorithm=None,
                    state=self._states.get(rule, UNUSED),
                    horizon=self._horizons.get(rule),
                    cooldown_until=cooldown.until if cooldown is not None else None,
                    resolve=self._resolve,
                    now=now,
                )
            )
        snapshot = Snapshot(rules=tuple(snapshots), sampled_at=now, backend=identity)
        return snapshot

    def defer_locked(self, scope: QuotaIdentity, duration: DurationMicros, reason: str) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``; the caller holds the lock.

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on a cooldown this call creates or lengthens.
        :raises ~procrastinators.errors.InvalidPolicy: ``duration`` is not an integer between 0
            and :data:`~procrastinators.models.MAX_DURATION_US`, or would end beyond the
            supported timestamp range.
        """
        cooldown = bookkeeping.extend_cooldown(
            self._cooldowns.get(scope), scope, duration, reason, self._now_locked()
        )
        self._cooldowns[scope] = cooldown
        self._cooldown_expiry.set(scope, cooldown.until)
        return cooldown

    def stored_policy_locked(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata held for ``rule``; the caller holds the lock.

        :param rule: The rule to look up.
        """
        stored = self._policies.get(rule)
        return stored

    def begin_migration_locked(
        self, rule: RuleId, to_fingerprint: PolicyFingerprint, to_state_version: int
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy; the caller holds the lock.

        No live conversion between policies is supported, so every migration
        drains: the rule denies with a conflict until its state is neutral,
        then may be completed. Active quota history is never deleted early.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
        """
        now = self._now_locked()
        existing = self._migrations.get(rule)
        stored = self._policies.get(rule)
        pending = self._pending.get(rule)
        migration = bookkeeping.begin_migration(
            rule,
            existing,
            current=(
                stored.fingerprint
                if stored is not None
                else (pending[0] if pending is not None else None)
            ),
            to_fingerprint=to_fingerprint,
        )
        if migration is not existing:
            self._migrations[rule] = migration
            self._targets[rule] = to_state_version
        else:
            pass
        refreshed = self._refresh_locked(rule, now)
        assert refreshed is not None
        return refreshed

    def _refresh_locked(self, rule: RuleId, now: EpochMicros) -> PolicyMigration | None:
        migration = bookkeeping.refresh_migration(
            self._migrations.get(rule),
            neutral=self._neutral_locked(rule, now),
            horizon=self._horizons.get(rule),
            now=now,
        )
        if migration is not None:
            self._migrations[rule] = migration
        else:
            pass
        return migration

    def migration_status_locked(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any; the caller holds the lock.

        :param rule: The rule whose migration to report.
        """
        migration = self._refresh_locked(rule, self._now_locked())
        return migration

    def complete_migration_locked(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy on a drained rule; the caller holds the lock.

        The old state is neutral by then, so forgetting it loses nothing. The
        first admission under the new policy records its metadata.

        :param rule: The rule whose migration to complete.
        :raises ~procrastinators.errors.PolicyConflict: No migration is ready for ``rule``.
        """
        migration = bookkeeping.complete_migration(
            rule, self._refresh_locked(rule, self._now_locked())
        )
        assert migration.to_fingerprint is not None
        for table in (self._states, self._horizons, self._policies, self._constraints):
            table.pop(rule, None)
        self._state_expiry.discard(rule)
        self._pending[rule] = (migration.to_fingerprint, self._targets.pop(rule))
        self._migrations[rule] = migration
        return migration


class _MemoryHandle:
    """What the sync and async handles share: the store, namespace, and observer."""

    _mode: ClassVar[Mode]

    def __init__(
        self,
        store: MemoryStore,
        *,
        namespace: str = DEFAULT_STORE_NAME,
        observer: AdmissionObserver | None = None,
    ) -> None:
        self._store = store
        self._identity = store.identity(namespace)
        self._observer = observer

    @property
    def store(self) -> MemoryStore:
        """The authority this handle addresses."""
        return self._store

    @property
    def capabilities(self) -> Capabilities:
        """In-process, ephemeral, composing, with cooldowns and policy administration."""
        capabilities = self._store.capabilities(self._mode)
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        """``memory://pid-<pid>/<name>#<namespace>``."""
        return self._identity

    def __repr__(self) -> str:
        text = f"{type(self).__name__}({self._identity})"
        return text


class MemoryBackend(_MemoryHandle, BaseSyncBackend):
    """A synchronous handle on a :class:`MemoryStore`.

    Satisfies :class:`~procrastinators.protocols.SyncBackend`,
    :class:`~procrastinators.protocols.SupportsCooldown`, and
    :class:`~procrastinators.protocols.SupportsPolicyAdministration`. Closing a
    handle closes only the handle: the store and its quota state live on for
    every other handle (L3).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called at each observation point, for deterministic tests.
    """

    _mode = Mode.SYNC

    def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        Waits for the store's lock at most ``request.budget.lock_timeout_us``.

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.BackendBusy: The lock was not free within budget.
        :raises ~procrastinators.errors.BackendUnavailable: The store is full of active rules,
            is a forked copy, or the observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The observer failed after the
            commit.
        :raises ~procrastinators.errors.UnsupportedCapability: An algorithm is not hosted.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        self._store.acquire(request.budget.lock_timeout_us)
        try:
            decision = self._store.admit_locked(request, self._observe, self._identity)
        finally:
            self._store.release()
        return decision

    def _locked(self, operation: Callable[[], T]) -> T:
        self._ensure_open()
        self._store.acquire(DEFAULT_LOCK_TIMEOUT_US)
        try:
            result = operation()
        finally:
            self._store.release()
        return result

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        snapshot = self._locked(
            functools.partial(self._store.inspect_locked, rules, self._identity)
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
        cooldown = self._locked(
            functools.partial(self._store.defer_locked, scope, duration, reason)
        )
        return cooldown

    def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the store holds for ``rule``, if any.

        :param rule: The rule to look up.
        """
        stored = self._locked(functools.partial(self._store.stored_policy_locked, rule))
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
        migration = self._locked(
            functools.partial(
                self._store.begin_migration_locked, rule, to_fingerprint, to_state_version
            )
        )
        return migration

    def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """
        migration = self._locked(functools.partial(self._store.migration_status_locked, rule))
        return migration

    def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready.
        """
        migration = self._locked(functools.partial(self._store.complete_migration_locked, rule))
        return migration

    def close(self) -> None:
        """Close this handle. Idempotent; the store and its state are untouched (L1, L3)."""
        self._mark_closed()


class AsyncMemoryBackend(_MemoryHandle, BaseAsyncBackend):
    """An asynchronous handle on a :class:`MemoryStore`.

    Satisfies :class:`~procrastinators.protocols.AsyncBackend`,
    :class:`~procrastinators.protocols.SupportsAsyncCooldown`, and
    :class:`~procrastinators.protocols.SupportsAsyncPolicyAdministration`. It
    never blocks the event loop on the store's lock: see the module notes.

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called synchronously at each observation point.
    """

    _mode = Mode.ASYNC

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        Cancellation can land only while waiting for the lock, before anything
        was read, so it consumes nothing (O5).

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.BackendBusy: The lock was not free within budget.
        :raises ~procrastinators.errors.BackendUnavailable: The store is full of active rules,
            is a forked copy, or the observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The observer failed after the
            commit.
        :raises ~procrastinators.errors.UnsupportedCapability: An algorithm is not hosted.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        await self._store.acquire_async(request.budget.lock_timeout_us)
        try:
            decision = self._store.admit_locked(request, self._observe, self._identity)
        finally:
            self._store.release()
        return decision

    async def _locked(self, operation: Callable[[], T]) -> T:
        self._ensure_open()
        await self._store.acquire_async(DEFAULT_LOCK_TIMEOUT_US)
        try:
            result = operation()
        finally:
            self._store.release()
        return result

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        snapshot = await self._locked(
            functools.partial(self._store.inspect_locked, rules, self._identity)
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
        cooldown = await self._locked(
            functools.partial(self._store.defer_locked, scope, duration, reason)
        )
        return cooldown

    async def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The policy metadata the store holds for ``rule``, if any.

        :param rule: The rule to look up.
        """
        stored = await self._locked(functools.partial(self._store.stored_policy_locked, rule))
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
        migration = await self._locked(
            functools.partial(
                self._store.begin_migration_locked, rule, to_fingerprint, to_state_version
            )
        )
        return migration

    async def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        """
        migration = await self._locked(functools.partial(self._store.migration_status_locked, rule))
        return migration

    async def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready.
        """
        migration = await self._locked(
            functools.partial(self._store.complete_migration_locked, rule)
        )
        return migration

    async def aclose(self) -> None:
        """Close this handle. Idempotent; the store and its state are untouched (L1, L3)."""
        self._mark_closed()


def memory_backend(
    address: str,
    *,
    mode: Mode,
    namespace: str = DEFAULT_STORE_NAME,
    algorithms: Iterable[Algorithm[Any]] = tuple(),
) -> MemoryBackend | AsyncMemoryBackend:
    """A handle on the process-wide store an address names.

    ``memory://`` names the ``default`` store and ``memory://ankh`` the store
    called ``ankh``. Registered as the ``memory`` family's factory.

    :param address: A ``memory://`` address.
    :param mode: Which interface the handle offers.
    :param namespace: The handle's quota namespace.
    :param algorithms: Algorithms the store must host, beyond those it has.
    :raises ~procrastinators.errors.ConfigurationError: The address is not a ``memory`` address,
        carries a query, credentials, or a port, or names an invalid store.
    """
    parts = urllib.parse.urlsplit(address)
    if parts.scheme != FAMILY:
        raise ConfigurationError(f"not a memory address: {address!r}")
    elif parts.query or parts.fragment or "@" in parts.netloc or ":" in parts.netloc:
        raise ConfigurationError(f"a memory address names only a store: {address!r}")
    else:
        pass
    name = (parts.netloc + parts.path).strip("/") or DEFAULT_STORE_NAME
    store = MemoryStore.named(name)
    store.host(algorithms)
    if mode is Mode.SYNC:
        handle: MemoryBackend | AsyncMemoryBackend = MemoryBackend(store, namespace=namespace)
    else:
        handle = AsyncMemoryBackend(store, namespace=namespace)
    return handle


if __name__ == "__main__":
    pass
else:
    pass
