"""Extension boundaries: what a plugin implements, and what it may assume.

Structural :class:`~typing.Protocol` types describe boundaries that callers and
plugins implement. Abstract base classes live in :mod:`procrastinators.algorithms.base`
and :mod:`procrastinators.backends.base`, and only where they enforce required
methods or supply real shared behavior.

Three rules shape this module.

*Sync and async are separate types.* No method returns ``T | Awaitable[T]``.
A wrapper cannot make a blocking database call non-blocking, so pretending one
interface serves both only moves the problem into every caller.

*Optional functionality lives in its own protocol.* A backend that cannot hold
a cooldown does not implement a ``defer_for`` that raises; it simply does not
satisfy :class:`SupportsCooldown`, and capability validation says so at
construction.

*Evaluation is pure.* An algorithm receives observations that have already been
collected and returns a proposal. It never reads a clock, opens a connection,
takes a lock, or sleeps.

.. rubric:: The state access boundary

This is the part most easily got wrong, so it is stated once, here.

An algorithm declares what it needs through :class:`StateRequirements`. The
backend fulfils those requirements *inside its own transaction*, awaiting I/O
where it must, and then calls the synchronous, pure evaluator with an immutable
:class:`StateView`. The evaluator never hides a database round trip behind a
property or a callback, because a lazy read inside evaluation would escape the
transaction that was supposed to make admission atomic.

The first implementations materialize a bounded event window. Indexed aggregate
queries may replace that later only where they preserve the same observations
and measurably reduce cost. A view that could not supply everything requested
says so through :attr:`StateView.truncated`, and an exact algorithm must then
fail closed rather than decide on a partial history.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from procrastinators.errors import InvalidPolicy
from procrastinators.models import (
    MAX_COST,
    MAX_PERIOD_US,
    Algorithms,
    DurationMicros,
    EpochMicros,
    StateChange,
    Transition,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from procrastinators.config_models import ConfigLayer
    from procrastinators.models import (
        AdmissionRequest,
        BackendIdentity,
        Capabilities,
        Constraint,
        Cooldown,
        Decision,
        DiagnosticEvent,
        MonotonicMicros,
        PolicyFingerprint,
        QuotaIdentity,
        RuleId,
        Snapshot,
    )
else:
    pass

__all__ = [
    "AdmissionClock",
    "AdmissionObserver",
    "Algorithm",
    "AlgorithmSpec",
    "AsyncAdmissionClock",
    "AsyncBackend",
    "AsyncConfigStore",
    "AsyncSleeper",
    "BackendSpec",
    "ConfigLoader",
    "ConfigStore",
    "DeadlineClock",
    "DiagnosticsCallback",
    "EventWindow",
    "LogEvent",
    "MigrationStatus",
    "NativeExecutorSpec",
    "ObservationPoint",
    "PolicyMigration",
    "RuleState",
    "Sleeper",
    "StateCodec",
    "StateRepresentation",
    "StateRequirements",
    "StateView",
    "StoredPolicy",
    "SupportsAsyncCooldown",
    "SupportsAsyncPolicyAdministration",
    "SupportsCooldown",
    "SupportsPolicyAdministration",
    "SyncBackend",
    "is_builtin_algorithm",
]


class StateRepresentation(StrEnum):
    """The shape of state an algorithm needs and a backend must be able to hold.

    This is a negotiation, not a fixed pair of options: a third-party algorithm
    may declare its own representation string, and only backends advertising it
    will accept that algorithm. It is how Memcached ends up supporting constant
    state and refusing sliding logs at construction rather than at runtime.
    """

    SCALARS = "scalars"
    """A bounded set of named integers. Fixed window, token and leaky bucket."""

    EVENT_LOG = "event_log"
    """Timestamp/cost entries. Sliding log; the only exact rolling guarantee."""


@dataclass(frozen=True, slots=True, order=True)
class LogEvent:
    """One recorded admission: when it happened and how much it weighed.

    A weighted admission is a single entry with ``cost`` on it, never ``cost``
    separate entries (contract P6).
    """

    at: EpochMicros
    """Authority epoch time, in microseconds, at which the admission was recorded.

    Declared first so that events order chronologically.
    """

    cost: int
    """Weight of the admission: the whole cost of one acquisition, in one entry."""


@dataclass(frozen=True, slots=True)
class EventWindow:
    """How much log history an evaluator needs, and how much it will tolerate.

    ``horizon_us`` is measured back from the admission timestamp. ``max_events``
    bounds what a backend must materialize; exceeding it marks the view
    truncated rather than silently narrowing the history.

    :raises ~procrastinators.errors.InvalidPolicy: ``horizon_us`` is not within ``(0,
        MAX_PERIOD_US]`` or ``max_events`` is not within ``(0, MAX_COST]``.
    """

    horizon_us: DurationMicros
    """How far back from the admission timestamp to load events, in microseconds.

    Must be positive and at most :data:`~procrastinators.models.MAX_PERIOD_US`,
    or construction raises :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    max_events: int
    """Most log entries a backend must materialize for one evaluation.

    Must be positive and at most :data:`~procrastinators.models.MAX_COST`, or
    construction raises :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    def __post_init__(self) -> None:
        if not 0 < self.horizon_us <= MAX_PERIOD_US:
            raise InvalidPolicy(f"event horizon must be within a supported period: {self}")
        elif not 0 < self.max_events <= MAX_COST:
            raise InvalidPolicy(f"max_events must be a positive bounded count: {self}")
        else:
            pass


@dataclass(frozen=True, slots=True)
class StateRequirements:
    """What a backend must load before it may call an evaluator.

    Declared per policy, because the same algorithm may need different history
    for different parameters. A backend reads exactly this much inside its
    transaction; anything an evaluator wants that is not declared here is
    simply not available to it.

    :raises ~procrastinators.errors.InvalidPolicy: ``representation`` is empty, or an event-log
        representation declares no ``events`` window.
    """

    representation: str = StateRepresentation.SCALARS
    """Shape of state the evaluator needs, matched against what a backend can hold.

    A :class:`StateRepresentation` value or a third party's own string. Must not
    be empty, or construction raises :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    scalars: frozenset[str] = frozenset()
    """Names of the scalars the evaluator will read through :meth:`StateView.scalar`."""

    events: EventWindow | None = None
    """History the evaluator will read through :meth:`StateView.events`.

    Required when :attr:`representation` is
    :attr:`StateRepresentation.EVENT_LOG`; leaving it ``None`` then raises
    :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    def __post_init__(self) -> None:
        if not self.representation:
            raise InvalidPolicy("a state representation must be named")
        elif self.representation == StateRepresentation.EVENT_LOG and self.events is None:
            raise InvalidPolicy("an event-log algorithm must declare the window it needs")
        else:
            pass


@dataclass(frozen=True, slots=True)
class RuleState:
    """A rule's persisted state, in the bounded form codecs move around.

    ``exists`` separates "never used" from "used and currently zero", which
    contract P5 depends on: a token bucket configured to start empty must not
    become a full one by being forgotten.
    """

    scalars: tuple[tuple[str, int], ...] = tuple()
    """Named integers, as ``(name, value)`` pairs."""

    events: tuple[LogEvent, ...] = tuple()
    """Retained log entries, for log-structured representations."""

    exists: bool = True
    """Whether any state was stored for the rule.

    ``False`` means the rule is at its configured initial state (contract S7).
    """


@runtime_checkable
class StateView(Protocol):
    """Immutable observations collected inside the backend's transaction.

    Every accessor is a plain read of already-loaded data. None of them may
    perform I/O, block, or return an awaitable: by the time an evaluator holds
    a view, the reading is over.

    Time domain: every timestamp is authority epoch time.
    """

    @property
    def rule(self) -> RuleId:
        """Which rule these observations belong to."""
        ...

    @property
    def exists(self) -> bool:
        """Whether any state was stored, as opposed to zeroed state.

        ``False`` means the rule is at its configured initial state.
        """
        ...

    @property
    def truncated(self) -> bool:
        """Whether the backend could not supply everything that was requested.

        An exact algorithm must deny and report rather than decide on a partial
        history (contract O3, fail closed).
        """
        ...

    def scalar(self, name: str, default: int = 0) -> int:
        """Return a named integer, or ``default`` when it was never written.

        :param name: A scalar named in :attr:`StateRequirements.scalars`.
        :param default: Value to return when the scalar has never been written.
        """
        ...

    def events(self) -> Sequence[LogEvent]:
        """Return the materialized window, oldest first.

        Empty when the algorithm's requirements did not ask for events.
        """
        ...


StateT = TypeVar("StateT")
"""The in-memory state type a :class:`StateCodec` encodes and decodes."""


@runtime_checkable
class StateCodec(Protocol[StateT]):
    """Versioned, deterministic serialization of a rule's state.

    Requirements, all of which have bitten someone before:

    * **Deterministic.** The same state encodes to the same bytes, so a
      compare-and-swap backend can tell a real change from a re-serialization.
    * **Versioned.** ``version`` is part of the policy fingerprint. Decoding an
      unknown version raises :class:`~procrastinators.errors.StateCorruption`
      rather than guessing.
    * **Bounded.** ``max_encoded_size`` lets a backend reject oversized items
      before a store does it for them, which matters where an item limit is
      hard (Memcached) and where a hot key is shared (Redis).
    * **Never pickle.** Stored state crosses trust and version boundaries;
      decoding it must not be able to execute anything.

    Fails closed: a truncated, malformed, or out-of-range payload raises
    :class:`~procrastinators.errors.StateCorruption`. It never decodes to an
    empty bucket, because that would hand out capacity.
    """

    @property
    def version(self) -> int:
        """State version this codec reads and writes."""
        ...

    @property
    def max_encoded_size(self) -> int:
        """Largest payload, in bytes, this codec will produce or accept."""
        ...

    def encode(self, state: StateT) -> bytes:
        """Serialize ``state``. Deterministic; raises on unrepresentable values.

        :param state: The rule state to encode.
        :returns: The encoded payload, at most :attr:`max_encoded_size` bytes.
        """
        ...

    def decode(self, payload: bytes) -> StateT:
        """Parse ``payload``.

        :param payload: Bytes previously produced by :meth:`encode`, as read from
            storage.
        :raises ~procrastinators.errors.StateCorruption: unknown version, malformed
            input, oversized input, or a value outside the supported numeric bounds.
        """
        ...


PolicyT_contra = TypeVar("PolicyT_contra", contravariant=True)
"""The policy type an :class:`Algorithm` evaluates.

Contravariant, because an algorithm that accepts a general policy type can stand
in wherever one accepting a narrower type is expected.
"""


@runtime_checkable
class Algorithm(Protocol[PolicyT_contra]):
    """A deterministic, side-effect-free policy evaluator.

    An algorithm answers one question: given this policy, these observations,
    this instant, and this cost, what should change and what should the caller
    be told? It does not decide *when* to ask, does not record anything, and
    does not know which store it is running against.

    Implementations must be stateless and safe to share across threads and
    tasks: every input arrives as an argument.
    """

    @property
    def id(self) -> str:
        """Stable identifier, persisted and compared across processes.

        Built-ins use :class:`~procrastinators.models.Algorithms` values. A
        third-party algorithm picks its own and must not change it afterwards.
        """
        ...

    @property
    def state_version(self) -> int:
        """Version of this algorithm's stored representation.

        Bumped whenever the codec or the meaning of the state changes. Part of
        the policy fingerprint, so a mixed fleet detects the disagreement.
        """
        ...

    def validate(self, policy: PolicyT_contra) -> None:
        """Check that ``policy`` is usable by this algorithm.

        :param policy: The policy to check.
        :raises ~procrastinators.errors.InvalidPolicy: contradictory, out-of-range, or
            unsupported policy.
        """
        ...

    def requirements(self, policy: PolicyT_contra) -> StateRequirements:
        """Declare the state a backend must load before calling :meth:`evaluate`.

        :param policy: Validated policy for the rule; the same algorithm may need
            different history for different parameters.
        """
        ...

    def initial_changes(self, policy: PolicyT_contra, now: EpochMicros) -> tuple[StateChange, ...]:
        """State to write when a rule is first used.

        Empty for algorithms whose unused state is already their initial state.
        A token bucket needs this, both for its starting balance and for the
        moment its refill clock starts; forgetting it is how an initially-empty
        bucket turns into a full one (contract P5).

        The backend applies these changes before calling :meth:`evaluate`, and
        records them even when that first attempt is denied: establishing the
        initial state consumes no quota, and a denied first attempt that left
        no trace would restart the refill clock on every retry. The changes must
        not include an :class:`~procrastinators.models.AppendEvent`.

        :param policy: Validated policy for the rule.
        :param now: Authority epoch time of the first attempt, the same instant
            :meth:`evaluate` then receives.
        """
        ...

    def evaluate(
        self,
        policy: PolicyT_contra,
        state: StateView,
        now: EpochMicros,
        cost: int,
    ) -> Transition:
        """Decide, purely, what should happen.

        Must not read a clock, sleep, lock, perform I/O, mutate ``state``, or
        raise for ordinary denial. Denial is a returned value, not an exception.

        :param policy: Validated policy for this rule.
        :param state: Observations already collected inside the backend's
            transaction. Reading it performs no I/O.
        :param now: Authority epoch time, sampled inside that transaction after
            locks were acquired (contract T4).
        :param cost: Positive integer, already known to be within capacity.
        :returns: A transition proposing changes. It commits nothing: the
            backend applies every rule's changes together or none of them.
        """
        ...

    @property
    def codec(self) -> StateCodec[RuleState]:
        """Codec binding for backends that store state as bytes."""
        ...


class ObservationPoint(StrEnum):
    """Named instants inside one admission, for deterministic race and failure tests.

    A backend that accepts an :class:`AdmissionObserver` calls it at each of
    these, in this order. Tests use them to pause a thread at a precise moment,
    to inject a storage failure, or to raise :class:`asyncio.CancelledError`
    exactly where a real cancellation could land — without sleeping and hoping.

    ``BEFORE_COMMIT`` and ``AFTER_COMMIT`` are observed only on the path that
    admits; a denial ends after ``AFTER_LOAD``.
    """

    BEFORE_LOCK = "before_lock"
    """Before any lock or transaction is acquired. Nothing has been read."""

    AFTER_LOAD = "after_load"
    """Inside the critical section, after authority time was sampled and state loaded."""

    BEFORE_COMMIT = "before_commit"
    """Every rule admitted; nothing has been written. A failure here commits nothing."""

    AFTER_COMMIT = "after_commit"
    """The debit is durable; the caller has not been told. A failure here is indeterminate."""


@runtime_checkable
class AdmissionObserver(Protocol):
    """A hook a backend calls at each :class:`ObservationPoint`.

    An observer may block (to hold a thread at a point) or raise; it must not
    call back into the same backend from inside the critical section. What a
    raised exception means is fixed, so every backend reports an injected
    failure the same way:

    * Before the commit, an exception is a storage failure that committed
      nothing. A :exc:`~procrastinators.errors.ProcrastinatorsError` propagates
      as raised; anything else becomes
      :exc:`~procrastinators.errors.BackendUnavailable` with its cause kept.
    * At ``AFTER_COMMIT`` the debit may already be durable, so anything but an
      :exc:`~procrastinators.errors.IndeterminateAdmission` becomes one. The
      caller is never told it was admitted and never refunded (contract O4).
    * :class:`asyncio.CancelledError` and other non-:class:`Exception` errors
      propagate unchanged at every point (contract O5).

    Asynchronous backends call the observer synchronously, so an observer that
    blocks is suitable only for thread-based tests.
    """

    def __call__(self, point: ObservationPoint, request: AdmissionRequest) -> None:
        """Observe ``point`` during the admission of ``request``.

        :param point: Where in the admission sequence the backend is.
        :param request: The request being admitted.
        """
        ...


@runtime_checkable
class SyncBackend(Protocol):
    """A synchronous admission authority.

    The backend is the extension boundary that matters: everything about
    atomicity lives behind :meth:`admit`.
    """

    @property
    def capabilities(self) -> Capabilities:
        """What this backend implements. Validated before first use, not at runtime."""
        ...

    @property
    def identity(self) -> BackendIdentity:
        """Which authority and namespace this handle addresses.

        Two handles coordinate only if this says they do. Composition across
        different authorities is rejected rather than emulated.
        """
        ...

    def admit(self, request: AdmissionRequest) -> Decision:
        """Atomically check every constraint and commit every debit, or none.

        Under one transaction or critical section: validate policy metadata,
        sample authority time *after* acquiring locks, prune obsolete state,
        evaluate every rule, and commit only if all of them admitted.

        Time domain: timestamps are authority epoch time. ``request.budget``
        carries the caller's local monotonic deadline, which must never be sent
        to a remote authority (contract T2).

        Side effects: on admission, exactly the committed debits. On denial, at
        most pruning of state that can no longer affect a decision.

        Never sleeps waiting for quota: waiting happens above this call, with
        no lock or transaction held.

        :param request: The constraints to admit together, the cost, and the
            caller's operation budget.
        :returns: The decision. A denial is a returned value; only failures raise.
        :raises ~procrastinators.errors.PolicyConflict: The stored fingerprint
            disagrees with the request's.
        :raises ~procrastinators.errors.BackendBusy: Contention exceeded the budget.
            Never a denial.
        :raises ~procrastinators.errors.BackendUnavailable: The authority could not
            be reached or refused.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit may or may
            not have happened.
        :raises ~procrastinators.errors.StateCorruption: Stored state could not be
            decoded.
        :raises ~procrastinators.errors.ClosedResource: This backend was closed.
        """
        ...

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory, non-committing observation. Never a reservation.

        Contract R7: acting on this without acquiring is a misuse, because
        another worker may consume everything it reports.

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This backend was closed (L5).
        """
        ...

    def close(self) -> None:
        """Release owned resources. Idempotent; never deletes quota state.

        Borrowed clients are left open (contract L2). Waits for owned
        outstanding work to settle (L4). Calls after this raise
        :class:`~procrastinators.errors.ClosedResource` (L5).
        """
        ...


@runtime_checkable
class AsyncBackend(Protocol):
    """An asynchronous admission authority.

    Deliberately a separate protocol rather than a mode of :class:`SyncBackend`.
    A wrapper that calls a blocking method and checks whether the result is
    awaitable does not make storage I/O non-blocking; async storage needs a
    native driver or an explicit executor adapter.
    """

    @property
    def capabilities(self) -> Capabilities:
        """What this backend implements. Validated before first use, not at runtime.

        As :attr:`SyncBackend.capabilities`.
        """
        ...

    @property
    def identity(self) -> BackendIdentity:
        """Which authority and namespace this handle addresses.

        As :attr:`SyncBackend.identity`: two handles coordinate only if this says
        they do.
        """
        ...

    async def admit(self, request: AdmissionRequest) -> Decision:
        """As :meth:`SyncBackend.admit`, awaiting I/O.

        Cancellation: cancelling before the commit consumes nothing. Cancelling
        after the commit may have consumed capacity and raises
        :class:`~procrastinators.errors.IndeterminateAdmission` or propagates
        the cancellation — never a refund, and never permission to run the body
        (contract O5). :class:`asyncio.CancelledError` is never translated into
        a quota error.

        Must not block the event loop for the duration of a storage call.

        :param request: The constraints to admit together, the cost, and the
            caller's operation budget.
        :returns: The decision. A denial is a returned value; only failures raise.
        :raises ~procrastinators.errors.PolicyConflict: The stored fingerprint
            disagrees with the request's.
        :raises ~procrastinators.errors.BackendBusy: Contention exceeded the budget.
            Never a denial.
        :raises ~procrastinators.errors.BackendUnavailable: The authority could not
            be reached or refused.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit may or may
            not have happened, including cancellation after the commit.
        :raises ~procrastinators.errors.StateCorruption: Stored state could not be
            decoded.
        :raises ~procrastinators.errors.ClosedResource: This backend was closed.
        """
        ...

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory, non-committing observation. Never a reservation.

        The asynchronous form of :meth:`SyncBackend.inspect`.

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This backend was closed (L5).
        """
        ...

    async def aclose(self) -> None:
        """Release owned resources. Idempotent.

        Waits for owned outstanding work to settle, including executor work
        that may still commit after its caller was cancelled.
        """
        ...


@runtime_checkable
class SupportsCooldown(Protocol):
    """Atomic extension of a shared pause on a scope."""

    def defer_for(
        self,
        scope: QuotaIdentity,
        duration: DurationMicros,
        *,
        reason: str = "",
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically.

        Uses ``max(existing, new)``, so a later, shorter cooldown cannot shorten
        one already in force (contract K2). The resulting cooldown participates
        in ordinary admission, not a separate check callers must remember (K3).

        Fabricates no quota events and replays no application work (K4).

        :param scope: The quota identity to pause.
        :param duration: Minimum length of the pause, in microseconds, measured
            from the authority's current epoch time.
        :param reason: Free-text explanation recorded on the cooldown, such as
            the vendor response that prompted it.
        :returns: The cooldown now in force, which may be longer than requested.
        """
        ...


@runtime_checkable
class SupportsAsyncCooldown(Protocol):
    """Asynchronous counterpart of :class:`SupportsCooldown`."""

    async def defer_for(
        self,
        scope: QuotaIdentity,
        duration: DurationMicros,
        *,
        reason: str = "",
    ) -> Cooldown:
        """The asynchronous form of :meth:`SupportsCooldown.defer_for`.

        :param scope: The quota identity to pause.
        :param duration: Minimum length of the pause, in microseconds, measured
            from the authority's current epoch time.
        :param reason: Free-text explanation recorded on the cooldown, such as
            the vendor response that prompted it.
        :returns: The cooldown now in force, which may be longer than requested.
        """
        ...


class MigrationStatus(StrEnum):
    """Where a rule stands in an explicit policy migration."""

    NOT_STARTED = "not_started"
    """No migration has begun; the rule runs under its stored policy."""

    DRAINING = "draining"
    """Admissions are stopped while the old policy's state becomes neutral."""

    READY = "ready"
    """Old state is neutral; the new policy may be installed."""

    COMPLETE = "complete"
    """The new policy is installed."""


@dataclass(frozen=True, slots=True)
class StoredPolicy:
    """The policy metadata an authority currently holds for a rule.

    Retained independently of quota state, so expiry cannot hide a
    configuration disagreement between workers (contract L10).
    """

    rule: RuleId
    """The rule this metadata governs."""

    algorithm_id: str
    """:attr:`Algorithm.id` of the algorithm the stored policy uses."""

    fingerprint: PolicyFingerprint
    """Fingerprint of the stored policy, checked against every admission request."""

    state_version: int
    """:attr:`Algorithm.state_version` the rule's stored state was written with."""

    updated_at: EpochMicros
    """Authority epoch time, in microseconds, at which this metadata was last written."""


@dataclass(frozen=True, slots=True)
class PolicyMigration:
    """Progress of an explicit, administrative policy change."""

    rule: RuleId
    """The rule being migrated."""

    status: MigrationStatus
    """Where the migration stands."""

    from_fingerprint: PolicyFingerprint | None = None
    """Fingerprint of the policy being replaced, or ``None`` when not recorded."""

    to_fingerprint: PolicyFingerprint | None = None
    """Fingerprint of the policy being installed, or ``None`` when not recorded."""

    drained_after: EpochMicros | None = None
    """Authority epoch time, in microseconds, after which the old state is neutral.

    ``None`` until it is known.
    """


@runtime_checkable
class SupportsPolicyAdministration(Protocol):
    """Deliberate policy changes, under the same authority that admits.

    Separate from admission because changing a policy is an operator action, not
    something a worker does by starting up with a different configuration file.
    Updating configuration never silently resets counters (contract L12).
    """

    def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """Return the authority's current metadata for ``rule``, if any.

        :param rule: The rule to look up.
        :returns: The stored metadata, or ``None`` when the authority holds none.
        """
        ...

    def begin_migration(
        self,
        rule: RuleId,
        *,
        to_fingerprint: PolicyFingerprint,
        to_state_version: int,
    ) -> PolicyMigration:
        """Start draining ``rule`` towards a new policy.

        Preserves relevant history where the backend supports conversion;
        otherwise stops admissions and drains the old state until neutral. Does
        not delete active quota history.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :returns: The migration's progress after starting it.
        """
        ...

    def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """Report progress without changing anything.

        :param rule: The rule whose migration to report.
        :returns: The migration's progress, or ``None`` when none is recorded.
        """
        ...

    def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """Install the new policy once draining has finished.

        :param rule: The rule whose migration to complete.
        :returns: The migration's final progress.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready, or
            moved underneath this call.
        """
        ...


@runtime_checkable
class SupportsAsyncPolicyAdministration(Protocol):
    """Asynchronous counterpart of :class:`SupportsPolicyAdministration`."""

    async def stored_policy(self, rule: RuleId) -> StoredPolicy | None:
        """The asynchronous form of :meth:`SupportsPolicyAdministration.stored_policy`.

        :param rule: The rule to look up.
        :returns: The stored metadata, or ``None`` when the authority holds none.
        """
        ...

    async def begin_migration(
        self,
        rule: RuleId,
        *,
        to_fingerprint: PolicyFingerprint,
        to_state_version: int,
    ) -> PolicyMigration:
        """The asynchronous form of :meth:`SupportsPolicyAdministration.begin_migration`.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :returns: The migration's progress after starting it.
        """
        ...

    async def migration_status(self, rule: RuleId) -> PolicyMigration | None:
        """The asynchronous form of :meth:`SupportsPolicyAdministration.migration_status`.

        :param rule: The rule whose migration to report.
        :returns: The migration's progress, or ``None`` when none is recorded.
        """
        ...

    async def complete_migration(self, rule: RuleId) -> PolicyMigration:
        """The asynchronous form of :meth:`SupportsPolicyAdministration.complete_migration`.

        :param rule: The rule whose migration to complete.
        :returns: The migration's final progress.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready, or
            moved underneath this call.
        """
        ...


@runtime_checkable
class DeadlineClock(Protocol):
    """Local, monotonic time, for deadlines and elapsed measurement only.

    Never persisted, never transmitted, never compared across processes. A
    monotonic reading has no meaning anywhere but here, which is exactly why
    deadlines use it: a paused or re-clocked machine cannot corrupt them.
    """

    def now(self) -> MonotonicMicros:
        """Return the current local monotonic time, in microseconds."""
        ...


@runtime_checkable
class AdmissionClock(Protocol):
    """Authority epoch time, sampled where admission is decided.

    For a memory backend this is the local wall clock; for a remote store it is
    the server's own time. Clients do not supply their wall clock to a shared
    authority, because atomic writes do not reconcile disagreeing clocks.

    Sampled inside the critical section, after locks are acquired, so a queued
    operation cannot commit a stale timestamp (contract T4).
    """

    def now(self) -> EpochMicros:
        """Return the authority's current epoch time, in microseconds."""
        ...


@runtime_checkable
class AsyncAdmissionClock(Protocol):
    """Authority epoch time where obtaining it is itself I/O."""

    async def now(self) -> EpochMicros:
        """The asynchronous form of :meth:`AdmissionClock.now`.

        :returns: The authority's current epoch time, in microseconds.
        """
        ...


@runtime_checkable
class Sleeper(Protocol):
    """Waits for a duration, holding nothing.

    A sleeper is given a duration, never a deadline or a timestamp, so it
    cannot be handed a value from the wrong clock domain. Waiting happens
    outside every storage lock and transaction (contract W1); an implementation
    that acquired one would serialize unrelated keys behind a waiting caller.
    """

    def sleep(self, duration: DurationMicros) -> None:
        """Block for ``duration``, holding no lock or transaction.

        :param duration: How long to wait, in microseconds.
        """
        ...


@runtime_checkable
class AsyncSleeper(Protocol):
    """Asynchronous counterpart of :class:`Sleeper`.

    Must be cancellable, and must not block the event loop: an unrelated
    heartbeat keeps running while a key waits for quota (contract W4).
    """

    async def sleep(self, duration: DurationMicros) -> None:
        """The asynchronous form of :meth:`Sleeper.sleep`, yielding to the event loop.

        :param duration: How long to wait, in microseconds.
        :raises asyncio.CancelledError: The wait was cancelled. It propagates
            unchanged (contract O5).
        """
        ...


@runtime_checkable
class ConfigLoader(Protocol):
    """Supplies one configuration layer.

    Loading is explicit. There is no working-directory discovery and no upward
    directory scan: an ETL worker's behavior must not depend on where it was
    started (contract G3).
    """

    def load(self) -> ConfigLayer | None:
        """Return this source's layer, or ``None`` when it does not exist.

        The returned layer keeps its origin and revision for provenance, and
        distinguishes an absent field from an explicit null (contract G2).

        :raises ~procrastinators.errors.ConfigurationError: The layer exists but is
            malformed, or contains unknown fields.
        """
        ...


@runtime_checkable
class ConfigStore(Protocol):
    """A configuration layer that can also be written back."""

    def load(self) -> ConfigLayer | None:
        """Return this source's layer, or ``None`` when it does not exist.

        As :meth:`ConfigLoader.load`.

        :raises ~procrastinators.errors.ConfigurationError: The layer exists but is
            malformed, or contains unknown fields.
        """
        ...

    def save(self, layer: ConfigLayer, *, expected_revision: str | None = None) -> ConfigLayer:
        """Atomically replace this source's contents.

        Writes and validates a temporary file, then replaces the destination.
        ``expected_revision`` detects a lost update; ``None`` means the caller
        expects the source not to exist yet.

        :param layer: The configuration to write.
        :param expected_revision: Revision the caller last read, or ``None`` if it
            expects the source not to exist yet.
        :returns: The saved layer, carrying its new revision.
        :raises ~procrastinators.errors.ConfigurationError: The revision did not
            match, the values are invalid, or the destination is not writable.
        """
        ...


@runtime_checkable
class AsyncConfigStore(Protocol):
    """Asynchronous counterpart of :class:`ConfigStore`."""

    async def load(self) -> ConfigLayer | None:
        """The asynchronous form of :meth:`ConfigStore.load`.

        :returns: This source's layer, or ``None`` when it does not exist.
        :raises ~procrastinators.errors.ConfigurationError: The layer exists but is
            malformed, or contains unknown fields.
        """
        ...

    async def save(
        self, layer: ConfigLayer, *, expected_revision: str | None = None
    ) -> ConfigLayer:
        """The asynchronous form of :meth:`ConfigStore.save`.

        :param layer: The configuration to write.
        :param expected_revision: Revision the caller last read, or ``None`` if it
            expects the source not to exist yet.
        :returns: The saved layer, carrying its new revision.
        :raises ~procrastinators.errors.ConfigurationError: The revision did not
            match, the values are invalid, or the destination is not writable.
        """
        ...


@runtime_checkable
class DiagnosticsCallback(Protocol):
    """Consumes immutable events after a critical section has ended.

    A callback cannot change, delay, or invalidate an admission: it is invoked
    outside locks, with a result already committed. An exception raised here is
    isolated, because turning a committed admission into a reported failure
    would make the caller believe it holds quota it does not (contract D2).
    """

    def __call__(self, event: DiagnosticEvent) -> None:
        """Receive one event, outside every lock and after the outcome is committed.

        :param event: The immutable event describing what happened.
        """
        ...


#
# Registration is explicit and typed. Nothing here discovers or executes a
# plugin named in configuration text: a rate-limit config file must not be a
# way to import arbitrary code. procrastinators.registry consumes these specs.


@dataclass(frozen=True, slots=True)
class AlgorithmSpec:
    """Registration for one algorithm implementation.

    :raises ~procrastinators.errors.InvalidPolicy: ``id`` is empty or ``state_version`` is less than
        1.
    """

    id: str
    """Stable algorithm identifier, as :attr:`Algorithm.id` reports it.

    Must not be empty, or construction raises
    :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    state_version: int
    """Stored-state version, as :attr:`Algorithm.state_version` reports it.

    Must be at least 1, or construction raises
    :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    representation: str
    """The :class:`StateRepresentation` value, or third-party string, the algorithm needs.

    Only backends advertising it will accept the algorithm.
    """

    factory: Callable[[], Algorithm[Any]]
    """Zero-argument callable that builds the algorithm implementation.

    Its policy type is left open: each algorithm evaluates its own policy type,
    and the registry pairs it with constraints by algorithm id.
    """

    def __post_init__(self) -> None:
        if not self.id:
            raise InvalidPolicy("an algorithm registration needs a stable id")
        elif self.state_version < 1:
            raise InvalidPolicy(f"state_version must be positive, got {self.state_version}")
        else:
            pass


@dataclass(frozen=True, slots=True)
class BackendSpec:
    """Registration for one backend family.

    ``family`` is what appears in a :class:`~procrastinators.models.BackendIdentity`
    and in an address like ``sqlite:///./quota.sqlite3``.

    :raises ~procrastinators.errors.InvalidPolicy: ``family`` is empty.
    """

    family: str
    """Name of the backend family, such as ``sqlite``.

    Must not be empty, or construction raises
    :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    factory: Callable[..., SyncBackend | AsyncBackend]
    """Callable that constructs a synchronous or asynchronous backend of this family."""

    capabilities: Capabilities
    """What backends of this family implement, declared before any is constructed."""

    def __post_init__(self) -> None:
        if not self.family:
            raise InvalidPolicy("a backend registration needs a family name")
        else:
            pass


@dataclass(frozen=True, slots=True)
class NativeExecutorSpec:
    """Registration for an algorithm implemented inside a store.

    A custom Python algorithm does not automatically run inside Redis: a Lua
    script, a stored procedure, or a CAS routine is a separate implementation
    of the same contract, and must be matched to the exact policy and state
    versions it was written against.

    ``numeric_limits`` restate the bounds within which this executor is known to
    agree with the reference evaluator — for a double-precision interpreter,
    :data:`~procrastinators.models.MAX_EXACT_INT` is the ceiling.
    ``conformance_traces`` names the shared traces it passes; an executor that
    has not been run against them is not registered.

    :raises ~procrastinators.errors.InvalidPolicy: ``conformance_traces`` is empty, or
        ``max_amount``, ``max_cost``, or ``max_period_us`` is outside its supported range.
    """

    backend_family: str
    """The :attr:`BackendSpec.family` whose store runs this executor."""

    algorithm_id: str
    """:attr:`Algorithm.id` of the algorithm this executor implements."""

    state_version: int
    """State version the executor was written against; matched exactly."""

    policy_version: int
    """Policy version the executor was written against."""

    max_amount: int
    """Largest policy capacity the executor is verified for.

    Must be positive and at most :data:`~procrastinators.models.MAX_COST`, or
    construction raises :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    max_cost: int
    """Largest single-acquisition cost the executor is verified for.

    Must be positive and at most :data:`~procrastinators.models.MAX_COST`, or
    construction raises :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    max_period_us: DurationMicros
    """Longest policy period, in microseconds, the executor is verified for.

    Must be positive and at most :data:`~procrastinators.models.MAX_PERIOD_US`, or
    construction raises :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    conformance_traces: tuple[str, ...] = tuple()
    """Names of the shared conformance traces the executor passes.

    Must not be empty, or construction raises
    :exc:`~procrastinators.errors.InvalidPolicy`.
    """

    def __post_init__(self) -> None:
        if not self.conformance_traces:
            raise InvalidPolicy(
                f"native executor {self.backend_family}/{self.algorithm_id} must name the "
                "conformance traces it was verified against"
            )
        else:
            pass
        for name, value, ceiling in (
            ("max_amount", self.max_amount, MAX_COST),
            ("max_cost", self.max_cost, MAX_COST),
            ("max_period_us", self.max_period_us, MAX_PERIOD_US),
        ):
            if not 0 < value <= ceiling:
                raise InvalidPolicy(f"{name} must be within the supported range, got {value}")
            else:
                pass

    def accepts(self, constraint: Constraint) -> bool:
        """Whether this executor may serve ``constraint``.

        Matching is exact on identity and version, and conservative on bounds:
        a policy outside the verified numeric range falls back to the reference
        evaluator rather than being run by an executor that was never checked
        against it.

        :param constraint: The constraint a backend proposes to run natively.
        :returns: ``True`` when the algorithm id and state version match and the
            constraint's capacity is within :attr:`max_amount`.
        """
        if (
            constraint.algorithm != self.algorithm_id
            or constraint.state_version != self.state_version
        ):
            accepted = False
        else:
            accepted = constraint.capacity <= self.max_amount
        return accepted


def is_builtin_algorithm(algorithm_id: str) -> bool:
    """Whether ``algorithm_id`` names one of the five reference algorithms.

    :param algorithm_id: An algorithm identifier, compared against the
        :class:`~procrastinators.models.Algorithms` values.
    """
    is_builtin = algorithm_id in set(Algorithms)
    return is_builtin


if __name__ == "__main__":
    pass
else:
    pass
