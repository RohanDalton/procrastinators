"""Rule state: versioned codecs, immutable views, and all-or-nothing planning.

The pieces every backend that runs reference evaluators needs, and none of
the parts that make a backend atomic:

* :class:`RuleStateCodec` turns a :class:`~procrastinators.protocols.RuleState`
  into deterministic bytes and back, failing closed on anything malformed.
* :func:`materialize` builds the immutable
  :class:`~procrastinators.protocols.StateView` an evaluator reads, holding
  exactly what the algorithm declared (contracts S1 to S4, S6, S7).
* :func:`apply_changes` applies proposed changes to a state.
* :func:`plan_admission` evaluates every constraint of a request against one
  time sample and says what to write: every change if every rule admitted,
  otherwise only initial state and pruning (contracts A5, A6, C7).

A backend supplies the critical section around :func:`plan_admission` — a
lock, a transaction, a script — and commits the plan's writes inside it.
Nothing here locks, sleeps, reads a clock, or performs I/O.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import bisect
import struct
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from procrastinators.errors import InvalidPolicy, StateCorruption
from procrastinators.models import (
    MAX_COST,
    MAX_EXACT_INT,
    MAX_TIMESTAMP_US,
    AppendEvent,
    ClearState,
    Decision,
    DropEventsBefore,
    DurationMicros,
    RemainingEstimate,
    SetScalar,
    Transition,
)
from procrastinators.protocols import LogEvent, RuleState

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from procrastinators.models import (
        Admission,
        AdmissionRequest,
        Constraint,
        EpochMicros,
        RuleId,
        StateChange,
    )
    from procrastinators.protocols import Algorithm, StateRequirements
else:
    pass

__all__ = [
    "DEFAULT_MAX_ENCODED_SIZE",
    "UNUSED",
    "AdmissionPlan",
    "MaterializedView",
    "RuleStateCodec",
    "apply_changes",
    "materialize",
    "plan_admission",
]

UNUSED: Final = RuleState(exists=False)
"""The state of a rule that has never been used: nothing stored."""

DEFAULT_MAX_ENCODED_SIZE: Final = 1 << 20
"""Default bound on one encoded rule state, in bytes: one mebibyte."""

_MAGIC: Final = b"PRS1"
_HEADER: Final = struct.Struct(">4sHB")
_COUNT16: Final = struct.Struct(">H")
_COUNT32: Final = struct.Struct(">I")
_NAME_LENGTH: Final = struct.Struct(">B")
_SCALAR_VALUE: Final = struct.Struct(">q")
_EVENT: Final = struct.Struct(">QI")
_FLAG_EXISTS: Final = 0x01
_MAX_NAME_BYTES: Final = 255
_MAX_SCALARS: Final = 0xFFFF


@dataclass(frozen=True, slots=True)
class RuleStateCodec:
    """Deterministic, versioned binary encoding of a :class:`~procrastinators.protocols.RuleState`.

    Satisfies :class:`~procrastinators.protocols.StateCodec`. The layout, all
    big-endian, is a ``PRS1`` tag, the 16-bit state version, a flag byte (bit 0:
    ``exists``), a 16-bit scalar count and each scalar as a length-prefixed
    UTF-8 name and a signed 64-bit value, then a 32-bit event count and each
    event as a 64-bit timestamp and a 32-bit cost.

    Encoding is canonical: scalars sorted by name, events by time then cost.
    Decoding accepts only canonical input, so ``encode(decode(payload)) ==
    payload`` for every payload it accepts, and a compare-and-swap backend can
    trust byte equality. Nothing is ever unpickled.

    :raises ~procrastinators.errors.InvalidPolicy: ``version`` is not between 1 and 65535, or
        ``max_encoded_size`` is smaller than an empty state's encoding.
    """

    version: int
    """The algorithm state version this codec reads and writes, 1 to 65535."""

    max_encoded_size: int = DEFAULT_MAX_ENCODED_SIZE
    """Largest payload, in bytes, produced or accepted."""

    def __post_init__(self) -> None:
        minimum = _HEADER.size + _COUNT16.size + _COUNT32.size
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise InvalidPolicy(f"codec version must be an int, got {self.version!r}")
        elif not 1 <= self.version <= 0xFFFF:
            raise InvalidPolicy(f"codec version must be between 1 and 65535, got {self.version}")
        elif not isinstance(self.max_encoded_size, int) or self.max_encoded_size < minimum:
            raise InvalidPolicy(f"max_encoded_size must be at least {minimum} bytes")
        else:
            pass

    def encode(self, state: RuleState) -> bytes:
        """Serialize ``state`` canonically.

        :param state: The state to encode.
        :returns: At most :attr:`max_encoded_size` bytes.
        :raises ~procrastinators.errors.StateCorruption: The state cannot be represented: a
            duplicate, empty, or overlong scalar name, a value outside the exact-integer range, an
            event outside the timestamp or cost range, content on a state that does not exist, or
            an encoding over the size bound. Such a state must never be written.
        """
        scalars = sorted(
            ((self._name_bytes(name), value) for name, value in state.scalars),
            key=_first,
        )
        events = sorted(state.events)
        if not state.exists and (scalars or events):
            raise StateCorruption("a state that does not exist cannot hold scalars or events")
        elif len(scalars) > _MAX_SCALARS:
            raise StateCorruption(f"a state holds at most {_MAX_SCALARS} scalars")
        else:
            pass
        parts = [
            _HEADER.pack(_MAGIC, self.version, _FLAG_EXISTS if state.exists else 0),
            _COUNT16.pack(len(scalars)),
        ]
        previous: bytes | None = None
        for name, value in scalars:
            if name == previous:
                raise StateCorruption(f"scalar {name.decode()!r} appears more than once")
            else:
                previous = name
            parts += [_NAME_LENGTH.pack(len(name)), name, _SCALAR_VALUE.pack(_scalar(value))]
        parts.append(_COUNT32.pack(len(events)))
        for event in events:
            checked = _checked_event(event)
            parts.append(_EVENT.pack(checked.at, checked.cost))
        payload = b"".join(parts)
        if len(payload) > self.max_encoded_size:
            raise StateCorruption(
                f"encoded state is {len(payload)} bytes, "
                f"over the {self.max_encoded_size}-byte bound"
            )
        else:
            pass
        return payload

    def decode(self, payload: bytes) -> RuleState:
        """Parse ``payload``, failing closed.

        :param payload: Bytes read from storage.
        :returns: The decoded state.
        :raises ~procrastinators.errors.StateCorruption: Not bytes, oversized, truncated, trailing
            data, a wrong tag or version, unknown flags, a non-canonical order, a duplicate or
            undecodable name, or a value outside the supported bounds. Never an empty bucket.
        """
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise StateCorruption(f"state payload must be bytes, got {type(payload).__name__}")
        else:
            pass
        data = bytes(payload)
        if len(data) > self.max_encoded_size:
            raise StateCorruption(
                f"state payload is {len(data)} bytes, over the {self.max_encoded_size}-byte bound"
            )
        else:
            pass
        reader = _Reader(data)
        magic, version, flags = reader.unpack(_HEADER)
        if magic != _MAGIC:
            raise StateCorruption("state payload does not carry the PRS1 tag")
        elif version != self.version:
            raise StateCorruption(
                f"state version {version} cannot be read by a version {self.version} codec"
            )
        elif flags & ~_FLAG_EXISTS:
            raise StateCorruption(f"state payload sets unknown flags {flags:#04x}")
        else:
            pass
        (scalar_count,) = reader.unpack(_COUNT16)
        scalars = list()
        previous: bytes | None = None
        for _ in range(scalar_count):
            (length,) = reader.unpack(_NAME_LENGTH)
            name = reader.take(length)
            (value,) = reader.unpack(_SCALAR_VALUE)
            if not name:
                raise StateCorruption("scalar names must not be empty")
            elif previous is not None and name <= previous:
                raise StateCorruption("scalars are not in strictly increasing name order")
            else:
                previous = name
            scalars.append((_decoded_name(name), _scalar(value)))
        (event_count,) = reader.unpack(_COUNT32)
        if event_count * _EVENT.size > reader.left:
            raise StateCorruption("state payload is truncated in its event log")
        else:
            pass
        events = list()
        for _ in range(event_count):
            event = _checked_event(LogEvent(*reader.unpack(_EVENT)))
            if events and event < events[-1]:
                raise StateCorruption("events are not in chronological order")
            else:
                pass
            events.append(event)
        if reader.left:
            raise StateCorruption(f"state payload has {reader.left} trailing bytes")
        elif not flags & _FLAG_EXISTS and (scalars or events):
            raise StateCorruption("a state that does not exist cannot hold scalars or events")
        else:
            pass
        state = RuleState(
            scalars=tuple(scalars), events=tuple(events), exists=bool(flags & _FLAG_EXISTS)
        )
        return state

    @staticmethod
    def _name_bytes(name: str) -> bytes:
        if not isinstance(name, str) or not name:
            raise StateCorruption(f"scalar names must be non-empty str, got {name!r}")
        else:
            pass
        try:
            encoded = name.encode("utf-8")
        except UnicodeEncodeError as error:
            raise StateCorruption(f"scalar name is not valid Unicode: {name!r}") from error
        if len(encoded) > _MAX_NAME_BYTES:
            raise StateCorruption(f"scalar name {name!r} exceeds {_MAX_NAME_BYTES} bytes")
        else:
            pass
        return encoded


def _first(pair: tuple[bytes, int]) -> bytes:
    return pair[0]


def _scalar(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StateCorruption(f"scalar values must be int, got {value!r}")
    elif not -MAX_EXACT_INT <= value <= MAX_EXACT_INT:
        raise StateCorruption(f"scalar value {value} is outside the exact-integer range")
    else:
        pass
    return value


def _checked_event(event: LogEvent) -> LogEvent:
    if not 0 <= event.at <= MAX_TIMESTAMP_US:
        raise StateCorruption(f"event timestamp {event.at} is outside the supported range")
    elif not 1 <= event.cost <= MAX_COST:
        raise StateCorruption(f"event cost {event.cost} is outside the supported range")
    else:
        pass
    return event


def _decoded_name(name: bytes) -> str:
    try:
        decoded = name.decode("utf-8")
    except UnicodeDecodeError as error:
        raise StateCorruption("scalar name is not valid UTF-8") from error
    return decoded


class _Reader:
    __slots__ = ("_data", "_offset")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._offset = 0

    @property
    def left(self) -> int:
        return len(self._data) - self._offset

    def take(self, size: int) -> bytes:
        if size > self.left:
            raise StateCorruption("state payload is truncated")
        else:
            pass
        chunk = self._data[self._offset : self._offset + size]
        self._offset += size
        return chunk

    def unpack(self, layout: struct.Struct) -> tuple[Any, ...]:
        values = layout.unpack(self.take(layout.size))
        return values


class MaterializedView:
    """Immutable observations of one rule, already loaded.

    Satisfies :class:`~procrastinators.protocols.StateView`. Every accessor
    reads data held in memory; there is nothing left to fetch (contract S3).
    Reading a scalar the algorithm did not declare raises :exc:`KeyError`, so an
    evaluator that depends on undeclared state fails in tests rather than
    silently reading a default in production (S4).
    """

    __slots__ = ("_declared", "_events", "_exists", "_rule", "_scalars", "_truncated")

    def __init__(
        self,
        rule: RuleId,
        *,
        scalars: Mapping[str, int],
        events: Sequence[LogEvent],
        declared: frozenset[str],
        exists: bool,
        truncated: bool,
    ) -> None:
        self._rule = rule
        self._scalars = MappingProxyType(dict(scalars))
        self._events = tuple(events)
        self._declared = declared
        self._exists = exists
        self._truncated = truncated

    @property
    def rule(self) -> RuleId:
        """The rule these observations belong to."""
        return self._rule

    @property
    def exists(self) -> bool:
        """Whether any state was stored, including initial state written this attempt."""
        return self._exists

    @property
    def truncated(self) -> bool:
        """Whether more events were live than the requirements allow materializing."""
        return self._truncated

    def scalar(self, name: str, default: int = 0) -> int:
        """Return a declared scalar, or ``default`` when it was never written.

        :param name: A scalar named in the algorithm's requirements.
        :param default: Returned when the scalar has never been written.
        :raises KeyError: ``name`` was not declared.
        """
        if name not in self._declared:
            raise KeyError(f"scalar {name!r} was not declared in the algorithm's requirements")
        else:
            pass
        value = self._scalars.get(name, default)
        return value

    def events(self) -> Sequence[LogEvent]:
        """Return the materialized window, oldest first; empty if none was declared."""
        return self._events

    def __repr__(self) -> str:
        text = (
            f"MaterializedView({self._rule}, exists={self._exists}, "
            f"truncated={self._truncated}, scalars={dict(self._scalars)}, "
            f"events={len(self._events)})"
        )
        return text


def materialize(
    rule: RuleId, state: RuleState, requirements: StateRequirements, now: EpochMicros
) -> MaterializedView:
    """Build the view an evaluator reads, holding only what it declared.

    Events are those strictly newer than ``now - horizon``. When more are live
    than ``max_events``, the newest ``max_events`` are kept and the view is
    marked truncated (contract S6): an exact algorithm must then fail closed.

    :param rule: The rule being evaluated.
    :param state: Its loaded state.
    :param requirements: What the algorithm declared for this policy.
    :param now: Authority epoch time of the attempt.
    :returns: The immutable view.
    """
    scalars = {name: value for name, value in state.scalars if name in requirements.scalars}
    if (window := requirements.events) is None:
        events: tuple[LogEvent, ...] = tuple()
        truncated = False
    else:
        cutoff = now - window.horizon_us
        live = [event for event in state.events if event.at > cutoff]
        truncated = len(live) > window.max_events
        events = tuple(live[-window.max_events :])
    view = MaterializedView(
        rule,
        scalars=scalars,
        events=events,
        declared=requirements.scalars,
        exists=state.exists,
        truncated=truncated,
    )
    return view


def apply_changes(state: RuleState, changes: Iterable[StateChange]) -> RuleState:
    """Apply ``changes`` to ``state`` in order, returning the new state.

    Scalars stay sorted by name and events chronological, so equal states
    compare equal and encode identically. Any change but
    :class:`~procrastinators.models.ClearState` leaves the rule existing.

    :param state: The state before the changes.
    :param changes: The changes to apply.
    :returns: The state after them; ``state`` itself when there are none.
    :raises TypeError: A change is not a known state change.
    """
    scalars = dict(state.scalars)
    events = list(state.events)
    exists = state.exists
    changed = False
    for change in changes:
        changed = True
        if isinstance(change, SetScalar):
            scalars[change.name] = change.value
            exists = True
        elif isinstance(change, AppendEvent):
            bisect.insort(events, LogEvent(change.at, change.cost))
            exists = True
        elif isinstance(change, DropEventsBefore):
            events = [event for event in events if event.at > change.at]
        elif isinstance(change, ClearState):
            scalars.clear()
            events.clear()
            exists = False
        else:
            raise TypeError(f"unknown state change: {change!r}")
    if changed:
        result = RuleState(
            scalars=tuple(sorted(scalars.items())), events=tuple(events), exists=exists
        )
    else:
        result = state
    return result


@dataclass(frozen=True, slots=True)
class AdmissionPlan:
    """What one admission attempt decided and what must be written for it.

    Produced by :func:`plan_admission`; committed by a backend inside the same
    critical section that loaded the states it was planned from.
    """

    now: EpochMicros
    """The single authority time sample every constraint was evaluated at."""

    transitions: tuple[Transition, ...]
    """One proposal per constraint, in canonical order."""

    writes: Mapping[RuleId, RuleState] = field(default_factory=dict)
    """The new state of every rule whose state changed.

    On admission, every proposed change. On denial, only initial state and
    changes marked ``prunes_only``: a denial consumes nothing (contract A5).
    """

    def __post_init__(self) -> None:
        object.__setattr__(self, "writes", MappingProxyType(dict(self.writes)))

    @property
    def admitted(self) -> bool:
        """Whether every constraint admitted, and the debits may be committed."""
        admitted = all(transition.admitted for transition in self.transitions)
        return admitted

    @property
    def blocking(self) -> tuple[RuleId, ...]:
        """The rules that denied, in canonical order."""
        blocking = tuple(
            transition.rule for transition in self.transitions if not transition.admitted
        )
        return blocking

    @property
    def retry_after_us(self) -> DurationMicros:
        """The largest delay any denying rule reported, or zero (contract C7)."""
        delays = [transition.retry_after_us for transition in self.transitions]
        retry = DurationMicros(0 if self.admitted else max(delays))
        return retry

    @property
    def remaining(self) -> tuple[RemainingEstimate, ...]:
        """Advisory per-rule remainders the evaluators reported."""
        remaining = tuple(
            RemainingEstimate(transition.rule, transition.remaining)
            for transition in self.transitions
            if transition.remaining is not None
        )
        return remaining

    def decision(self, admission: Admission | None = None) -> Decision:
        """The decision to report, once the plan's writes are committed.

        :param admission: Proof of the commit; required exactly when the plan admitted.
        :raises ~procrastinators.errors.InvalidPolicy: ``admission`` is missing for an admitting
            plan or present for a denying one.
        """
        if self.admitted:
            if admission is None:
                raise InvalidPolicy("an admitting plan is reported only with its Admission")
            else:
                pass
            decision = Decision.allow(admission, remaining=self.remaining, observed_at=self.now)
        elif admission is not None:
            raise InvalidPolicy("a denying plan committed nothing and has no Admission")
        else:
            decision = Decision.deny(
                self.blocking,
                self.retry_after_us,
                remaining=self.remaining,
                observed_at=self.now,
            )
        return decision


def plan_admission(
    request: AdmissionRequest,
    states: Mapping[RuleId, RuleState],
    resolve: Callable[[Constraint], Algorithm[Any]],
    now: EpochMicros,
    *,
    holds: Mapping[RuleId, DurationMicros] | None = None,
) -> AdmissionPlan:
    """Evaluate every constraint against one time sample and plan the writes.

    For each constraint in canonical order: apply the algorithm's initial
    changes if the rule was never used, materialize a view, and evaluate. Every
    rule is evaluated even after one denies, so the reported delay is the
    maximum over all blocking rules (contract C7).

    A rule in ``holds`` — one whose scope is under a cooldown — denies whatever
    its algorithm said, waiting at least the hold. Its evaluation still runs,
    so its own delay counts if longer and its initial state is still recorded;
    only its debit is withheld (contracts K3, K4).

    :param request: The request being admitted.
    :param states: The loaded state of each rule; a missing rule is unused.
    :param resolve: Returns the algorithm for a constraint.
    :param now: Authority epoch time, sampled after locks were acquired (T4).
    :param holds: Rules that must deny, with the least delay each must report.
    :returns: The plan. Nothing has been written.
    :raises ~procrastinators.errors.InvalidPolicy: An algorithm proposed an initial debit or
        answered for a different rule.
    """
    transitions = list()
    admitted_writes = dict()
    denied_writes = dict()
    for constraint in request.constraints:
        rule = constraint.rule
        policy = constraint.policy
        algorithm = resolve(constraint)
        loaded = states.get(rule, UNUSED)
        if loaded.exists:
            base = loaded
        else:
            initial = algorithm.initial_changes(policy, now)
            if any(isinstance(change, AppendEvent) for change in initial):
                raise InvalidPolicy(f"{algorithm.id} proposed a debit as initial state for {rule}")
            else:
                pass
            base = apply_changes(loaded, initial)
        view = materialize(rule, base, algorithm.requirements(policy), now)
        transition = algorithm.evaluate(policy, view, now, request.cost)
        if transition.rule != rule:
            raise InvalidPolicy(f"{algorithm.id} answered for {transition.rule}, not {rule}")
        elif holds and (hold := holds.get(rule)) is not None:
            transition = _held(transition, hold)
        else:
            pass
        transitions.append(transition)
        admitted_writes[rule] = apply_changes(base, transition.changes)
        denied_writes[rule] = apply_changes(
            base, (change for change in transition.changes if change.prunes_only)
        )
    admitted = all(transition.admitted for transition in transitions)
    candidates = admitted_writes if admitted else denied_writes
    writes = {
        rule: state for rule, state in candidates.items() if state != states.get(rule, UNUSED)
    }
    plan = AdmissionPlan(now=now, transitions=tuple(transitions), writes=writes)
    return plan


def _held(transition: Transition, hold: DurationMicros) -> Transition:
    """``transition`` turned into a denial waiting at least ``hold``, keeping only its pruning."""
    held = Transition(
        rule=transition.rule,
        admitted=False,
        changes=tuple(change for change in transition.changes if change.prunes_only),
        retry_after_us=DurationMicros(max(hold, transition.retry_after_us)),
        safe_forget_after_us=transition.safe_forget_after_us,
        remaining=transition.remaining,
    )
    return held


if __name__ == "__main__":
    pass
else:
    pass
