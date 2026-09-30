"""Versioned codecs fail closed; views hold only what was declared; plans are all-or-nothing.

The Librarian shelves every book in exactly one place and bites anyone who
reshelves one. The codec is similarly strict about canonical order.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import struct
from typing import TYPE_CHECKING, Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from procrastinators.errors import InvalidPolicy, StateCorruption
from procrastinators.models import (
    MAX_COST,
    MAX_EXACT_INT,
    MAX_TIMESTAMP_US,
    AdmissionRequest,
    AppendEvent,
    ClearState,
    DropEventsBefore,
    DurationMicros,
    EpochMicros,
    QuotaIdentity,
    RuleId,
    SetScalar,
    SlidingLogPolicy,
    TokenBucketPolicy,
    Transition,
)
from procrastinators.protocols import (
    EventWindow,
    LogEvent,
    RuleState,
    StateCodec,
    StateRepresentation,
    StateRequirements,
    StateView,
)
from procrastinators.state import (
    UNUSED,
    MaterializedView,
    RuleStateCodec,
    apply_changes,
    materialize,
    plan_admission,
)
from procrastinators.testing import TraceRule
from procrastinators.testing.harness import DEFAULT_NAMESPACE
from tests.oracles import SlidingLogOracle, TokenBucketOracle

if TYPE_CHECKING:
    from collections.abc import Callable

    from procrastinators.models import Constraint, StateChange
    from procrastinators.protocols import Algorithm
else:
    pass

ONE_SECOND = DurationMicros(1_000_000)
RULE = RuleId(QuotaIdentity("discworld", "library"), "books")
CODEC = RuleStateCodec(3)


def _fits_a_name(name: str) -> bool:
    fits = len(name.encode()) <= 255
    return fits


def _pairs(mapping: dict[str, int]) -> tuple[tuple[str, int], ...]:
    pairs = tuple(mapping.items())
    return pairs


def _name_bytes(pair: tuple[str, int]) -> bytes:
    encoded = pair[0].encode()
    return encoded


scalar_names = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), min_size=1, max_size=20
).filter(_fits_a_name)
states = st.builds(
    RuleState,
    scalars=st.dictionaries(
        scalar_names, st.integers(min_value=-MAX_EXACT_INT, max_value=MAX_EXACT_INT), max_size=6
    ).map(_pairs),
    events=st.lists(
        st.builds(
            LogEvent,
            at=st.integers(min_value=0, max_value=MAX_TIMESTAMP_US),
            cost=st.integers(min_value=1, max_value=MAX_COST),
        ),
        max_size=10,
    ).map(tuple),
    exists=st.just(True),
)


@given(state=states)
def test_encoding_round_trips_canonically(state: RuleState) -> None:
    """
    Given: Any representable state, in any order.
    When:  It is encoded, decoded, and encoded again.
    Then:  The decoded state is the canonical form, and both encodings are identical.
    """
    payload = CODEC.encode(state)
    expected = RuleState(
        scalars=tuple(sorted(state.scalars, key=_name_bytes)),
        events=tuple(sorted(state.events)),
        exists=True,
    )

    actual = CODEC.decode(payload)

    assert actual == expected
    assert CODEC.encode(actual) == payload


def test_the_codec_is_a_state_codec_and_encodes_absence() -> None:
    """
    Given: The codec and the unused state.
    When:  The unused state round-trips.
    Then:  It stays unused, and the codec satisfies the StateCodec protocol.
    """
    expected = UNUSED

    actual = CODEC.decode(CODEC.encode(UNUSED))

    assert actual == expected
    assert isinstance(CODEC, StateCodec)


def _valid_payload() -> bytes:
    payload = CODEC.encode(
        RuleState(scalars=(("tokens", 3),), events=(LogEvent(EpochMicros(10), 2),))
    )
    return payload


def _replace(payload: bytes, offset: int, value: bytes) -> bytes:
    replaced = payload[:offset] + value + payload[offset + len(value) :]
    return replaced


CORRUPT_PAYLOADS = {
    "not bytes": "PRS1",
    "empty": b"",
    "wrong tag": _replace(_valid_payload(), 0, b"PICK"),
    "other version": _replace(_valid_payload(), 4, struct.pack(">H", 4)),
    "unknown flag": _replace(_valid_payload(), 6, b"\x03"),
    "truncated": _valid_payload()[:-1],
    "trailing bytes": _valid_payload() + b"\x00",
    "absent with content": _replace(_valid_payload(), 6, b"\x00"),
    "event count beyond payload": _replace(
        _valid_payload(), len(_valid_payload()) - 16, struct.pack(">I", 2**31)
    ),
    "zero-cost event": _valid_payload()[:-4] + struct.pack(">I", 0),
    "oversized": b"\x00" * (RuleStateCodec(3, max_encoded_size=64).max_encoded_size + 1),
}


@pytest.fixture(params=list(CORRUPT_PAYLOADS))
def corrupt(request: pytest.FixtureRequest) -> object:
    payload = CORRUPT_PAYLOADS[request.param]
    return payload


def test_the_librarian_rejects_every_corrupt_payload(corrupt: object) -> None:
    """
    Given: A payload that is malformed, truncated, padded, versioned wrongly, or out
           of range.
    When:  It is decoded.
    Then:  StateCorruption is raised: never an empty bucket.
    """
    codec = RuleStateCodec(3, max_encoded_size=64)

    with pytest.raises(StateCorruption):
        codec.decode(corrupt)  # ty: ignore[invalid-argument-type]


def test_decoding_rejects_non_canonical_order() -> None:
    """
    Given: A hand-built payload whose scalars are out of name order.
    When:  It is decoded.
    Then:  StateCorruption, so byte equality always means state equality.
    """
    parts = [
        struct.pack(">4sHB", b"PRS1", 3, 1),
        struct.pack(">H", 2),
        struct.pack(">B", 1) + b"b" + struct.pack(">q", 1),
        struct.pack(">B", 1) + b"a" + struct.pack(">q", 1),
        struct.pack(">I", 0),
    ]

    with pytest.raises(StateCorruption, match="order"):
        CODEC.decode(b"".join(parts))


UNREPRESENTABLE_STATES = {
    "duplicate scalar": RuleState(scalars=(("a", 1), ("a", 2))),
    "empty name": RuleState(scalars=(("", 1),)),
    "overlong name": RuleState(scalars=(("n" * 256, 1),)),
    "inexact value": RuleState(scalars=(("a", MAX_EXACT_INT + 1),)),
    "bool value": RuleState(scalars=(("a", True),)),
    "negative time": RuleState(events=(LogEvent(EpochMicros(-1), 1),)),
    "zero cost": RuleState(events=(LogEvent(EpochMicros(1), 0),)),
    "absent with content": RuleState(scalars=(("a", 1),), exists=False),
}


@pytest.mark.parametrize(
    "state", list(UNREPRESENTABLE_STATES.values()), ids=list(UNREPRESENTABLE_STATES)
)
def test_an_unrepresentable_state_is_never_written(state: RuleState) -> None:
    """
    Given: A state the format cannot represent faithfully.
    When:  It is encoded.
    Then:  StateCorruption is raised before anything reaches storage.
    """
    with pytest.raises(StateCorruption):
        CODEC.encode(state)


def test_an_oversized_state_is_refused_on_the_way_in() -> None:
    """
    Given: A codec bounded at 64 bytes and a state of many events.
    When:  It is encoded.
    Then:  StateCorruption names the bound.
    """
    codec = RuleStateCodec(1, max_encoded_size=64)
    state = RuleState(events=tuple(LogEvent(EpochMicros(at), 1) for at in range(10)))

    with pytest.raises(StateCorruption, match="bound"):
        codec.encode(state)


@pytest.mark.parametrize(
    ("version", "size"), [(0, 1024), (70_000, 1024), (1, 4)], ids=["zero", "too big", "tiny bound"]
)
def test_a_codec_needs_a_real_version_and_bound(version: int, size: int) -> None:
    """
    Given: A version outside 1 to 65535, or a bound smaller than an empty state.
    When:  A codec is built.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        RuleStateCodec(version, max_encoded_size=size)


def test_a_view_holds_only_what_was_declared() -> None:
    """
    Given: A state with two scalars, and requirements declaring one.
    When:  It is materialized and both scalars are read.
    Then:  The declared one is available, the other raises KeyError (S4); the view is
           a StateView.
    """
    state = RuleState(scalars=(("declared", 5), ("secret", 9)))
    view = materialize(
        RULE, state, StateRequirements(scalars=frozenset({"declared"})), EpochMicros(0)
    )

    assert view.scalar("declared") == 5
    with pytest.raises(KeyError, match="not declared"):
        view.scalar("secret")
    assert isinstance(view, StateView)


def test_a_declared_but_unwritten_scalar_reads_its_default() -> None:
    """
    Given: Requirements declaring a scalar the state never wrote.
    When:  It is read with a default.
    Then:  The default is returned.
    """
    view = materialize(
        RULE, UNUSED, StateRequirements(scalars=frozenset({"tokens"})), EpochMicros(0)
    )
    expected = (7, False)

    actual = (view.scalar("tokens", 7), view.exists)

    assert actual == expected


def test_a_view_windows_events_and_says_when_it_truncated() -> None:
    """
    Given: Five events, a horizon that excludes the oldest, and room for three.
    When:  The state is materialized at time 100.
    Then:  The newest three live events are kept and the view is marked truncated (S6);
           an event exactly one horizon old has left (P1).
    """
    events = tuple(LogEvent(EpochMicros(at), 1) for at in (50, 60, 70, 80, 90))
    requirements = StateRequirements(
        representation=StateRepresentation.EVENT_LOG,
        events=EventWindow(DurationMicros(50), max_events=3),
    )
    expected = ((70, 80, 90), True)

    view = materialize(RULE, RuleState(events=events), requirements, EpochMicros(100))
    actual = (tuple(event.at for event in view.events()), view.truncated)

    assert actual == expected


def test_changes_apply_in_order_and_keep_canonical_form() -> None:
    """
    Given: An unused state.
    When:  Scalars are set, events appended out of order, old events dropped, and
           finally the state cleared.
    Then:  Before the clear, scalars are sorted and events chronological; after it the
           rule is unused again.
    """
    changes: list[StateChange] = [
        SetScalar("zeta", 1),
        SetScalar("alpha", 2),
        AppendEvent(EpochMicros(30), 1),
        AppendEvent(EpochMicros(10), 2),
        AppendEvent(EpochMicros(20), 3),
        DropEventsBefore(EpochMicros(10)),
    ]
    expected = RuleState(
        scalars=(("alpha", 2), ("zeta", 1)),
        events=(LogEvent(EpochMicros(20), 3), LogEvent(EpochMicros(30), 1)),
    )

    actual = apply_changes(UNUSED, changes)

    assert actual == expected
    assert apply_changes(actual, [ClearState()]) == UNUSED
    assert apply_changes(actual, []) is actual


def test_an_unknown_change_is_refused() -> None:
    """
    Given: Something that is not a state change.
    When:  It is applied.
    Then:  TypeError is raised.
    """
    with pytest.raises(TypeError, match="unknown state change"):
        apply_changes(UNUSED, ["refund everything"])  # ty: ignore[invalid-argument-type]


def _constraint(label: str, amount: int, *, scope: str = "ankh") -> Constraint:
    constraint = TraceRule(label, SlidingLogPolicy(amount, ONE_SECOND), scope=scope).constraint(
        DEFAULT_NAMESPACE
    )
    return constraint


def _resolve_with(algorithm: Algorithm[Any]) -> Callable[[Constraint], Algorithm[Any]]:
    def resolve(constraint: Constraint) -> Algorithm[Any]:
        del constraint
        return algorithm

    return resolve


def test_a_plan_writes_everything_or_only_pruning() -> None:
    """
    Given: A roomy rule and a full one, composed, with an expired event on the roomy one.
    When:  An attempt is planned.
    Then:  It is denied by the full rule, the roomy rule's only write is pruning its
           expired event, and the full rule is not written at all (A5, A6).
    """
    roomy, full = _constraint("roomy", 3), _constraint("full", 1, scope="ankh.orders")
    now = EpochMicros(5_000_000)
    states = {
        roomy.rule: RuleState(events=(LogEvent(EpochMicros(1_000_000), 1),)),
        full.rule: RuleState(events=(LogEvent(EpochMicros(4_500_000), 1),)),
    }
    expected = {roomy.rule: RuleState(events=())}

    plan = plan_admission(
        AdmissionRequest((roomy, full)), states, _resolve_with(SlidingLogOracle()), now
    )
    actual = dict(plan.writes)

    assert not plan.admitted
    assert plan.blocking == (full.rule,)
    assert plan.retry_after_us == 500_000
    assert actual == expected


def test_an_admitting_plan_writes_every_rule() -> None:
    """
    Given: Two roomy rules composed.
    When:  An attempt is planned and its decision built.
    Then:  Both rules gain the event, and the decision carries the admission and time.
    """
    first, second = _constraint("first", 2), _constraint("second", 2, scope="ankh.orders")
    now = EpochMicros(7)
    request = AdmissionRequest((first, second))
    expected = {rule: RuleState(events=(LogEvent(now, 1),)) for rule in request.rules}

    plan = plan_admission(request, dict(), _resolve_with(SlidingLogOracle()), now)

    assert plan.admitted
    assert dict(plan.writes) == expected
    with pytest.raises(InvalidPolicy, match="only with its Admission"):
        plan.decision()


def test_initial_state_is_written_even_when_the_first_attempt_is_denied() -> None:
    """
    Given: An initially empty token bucket never used.
    When:  Its first attempt is planned.
    Then:  The plan denies and writes the initial state anyway (P5).
    """
    policy = TokenBucketPolicy(2, 1, ONE_SECOND, initial_tokens=0)
    constraint = TraceRule("bucket", policy).constraint(DEFAULT_NAMESPACE)
    now = EpochMicros(42)
    expected = {constraint.rule: RuleState(scalars=(("anchor", 42), ("tokens", 0)))}

    plan = plan_admission(
        AdmissionRequest((constraint,)), dict(), _resolve_with(TokenBucketOracle()), now
    )
    actual = dict(plan.writes)

    assert not plan.admitted
    assert actual == expected


class PreCharging(SlidingLogOracle):
    """Tries to charge through initial state."""

    def initial_changes(self, policy: object, now: EpochMicros) -> tuple[StateChange, ...]:
        del policy
        return (AppendEvent(now, 1),)


class Misdirected(SlidingLogOracle):
    """Answers for a different rule."""

    def evaluate(self, policy: object, state: StateView, now: EpochMicros, cost: int) -> Transition:
        del policy, state, now, cost
        transition = Transition(RuleId(QuotaIdentity("discworld", "elsewhere"), "x"), True)
        return transition


@pytest.mark.parametrize(
    ("algorithm", "message"),
    [(PreCharging(), "initial state"), (Misdirected(), "answered for")],
    ids=["debit as initial state", "wrong rule"],
)
def test_a_misbehaving_algorithm_is_stopped_by_the_planner(
    algorithm: Algorithm[Any], message: str
) -> None:
    """
    Given: An algorithm that charges through initial state, or answers for another rule.
    When:  An attempt is planned.
    Then:  InvalidPolicy is raised before anything could be written.
    """
    request = AdmissionRequest((_constraint("victim", 1),))

    with pytest.raises(InvalidPolicy, match=message):
        plan_admission(request, dict(), _resolve_with(algorithm), EpochMicros(1))


def test_a_view_says_what_it_is() -> None:
    """
    Given: A materialized view.
    When:  It is shown.
    Then:  Its repr names the rule, flags, and counts.
    """
    view = MaterializedView(
        RULE, scalars={"a": 1}, events=(), declared=frozenset({"a"}), exists=True, truncated=False
    )

    assert "books" in repr(view)
    assert "truncated=False" in repr(view)


if __name__ == "__main__":
    pass
else:
    pass
