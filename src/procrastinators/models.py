"""Immutable models: rates, policies, identity, requests, and results.

Everything here is a frozen dataclass that validates itself on construction.
The intent is that an invalid or self-contradictory value cannot be built at
all, so later phases never have to ask whether a ``Decision`` that claims to be
allowed also carries blocking rules.

``docs/source/contracts.md`` is the normative specification; this module is its typed
expression. Nothing here performs I/O, sleeps, takes a lock, or reads a clock.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import datetime as dt
import math
import operator
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from fractions import Fraction
from typing import ClassVar, Final, NewType, Protocol, TypeAlias, runtime_checkable

from procrastinators.errors import InvalidCost, InvalidPolicy, PolicyConflict

__all__ = [
    "MAX_AMOUNT",
    "MAX_COST",
    "MAX_DURATION_US",
    "MAX_EXACT_INT",
    "MAX_PERIOD_US",
    "MAX_TIMESTAMP_US",
    "MIN_PERIOD_US",
    "USECS_PER_SECOND",
    "Admission",
    "AdmissionRequest",
    "AdmittedEvent",
    "Algorithms",
    "AppendEvent",
    "BackendFailureEvent",
    "BackendIdentity",
    "Capabilities",
    "ClearState",
    "Constraint",
    "Cooldown",
    "CooldownEvent",
    "CoordinationScope",
    "Decision",
    "DeniedEvent",
    "DiagnosticEvent",
    "DropEventsBefore",
    "Durability",
    "DurationMicros",
    "EpochMicros",
    "FixedWindowPolicy",
    "LeakyBucketPolicy",
    "Limit",
    "MonotonicMicros",
    "OperationBudget",
    "Ownership",
    "Policy",
    "PolicyConflictEvent",
    "PolicyFingerprint",
    "PolicySpec",
    "QuotaIdentity",
    "RemainingEstimate",
    "ResourceOwnership",
    "RuleId",
    "RuleSnapshot",
    "SetScalar",
    "SlidingCounterPolicy",
    "SlidingLogPolicy",
    "Snapshot",
    "StateChange",
    "TokenBucketPolicy",
    "Transition",
    "WaitEvent",
    "canonical_constraints",
    "duration_to_micros",
    "policy_capacity",
]


#
# Three distinct integer-microsecond domains that must never be interchanged.
# They are ``NewType`` aliases so the type checker rejects the mistake the
# design calls out explicitly: sending a client's local deadline to a remote
# server as if it were a comparable timestamp.

EpochMicros = NewType("EpochMicros", int)
"""Microseconds since the Unix epoch, *as read by the admission authority*.

The only domain in which stored state and admission timestamps are recorded. A
client never substitutes its own wall clock for a backend's.
"""

MonotonicMicros = NewType("MonotonicMicros", int)
"""Microseconds from an arbitrary local origin, comparable only within one process.

Deadlines and elapsed-time measurements live here. Meaningless to any other
process and never persisted or transmitted.
"""

DurationMicros = NewType("DurationMicros", int)
"""A length of time in microseconds, with no origin.

Retry delays, timeouts, and periods are durations. A retry delay is a duration
precisely so that a client and a server need not agree on what time it is.
"""

USECS_PER_SECOND: Final = 1_000_000
"""Microseconds in one second: the scale between public seconds and internal time."""


#
# Bounds exist so that a native executor produces the same answers as the
# reference Python evaluator. The binding constraint is the Lua interpreter
# embedded in Redis, whose numbers are IEEE doubles: integers above 2**53 stop
# being exactly representable. Every intermediate value an executor must
# compute exactly is therefore kept at or below MAX_EXACT_INT.

MAX_EXACT_INT: Final = 2**53 - 1
"""Largest integer every supported executor represents exactly."""

MAX_AMOUNT: Final = 2**31 - 1
"""Largest admissible ``amount``/``capacity`` in a policy."""

MAX_COST: Final = 2**31 - 1
"""Largest admissible ``cost`` for one acquisition."""

MIN_PERIOD_US: Final = 1_000
"""Shortest supported period: one millisecond.

Below this, sleep-based waiting cannot pace admissions on any supported
platform, so a shorter period would promise precision the waiters cannot keep.
"""

MAX_PERIOD_US: Final = 100 * 365 * 24 * 60 * 60 * USECS_PER_SECOND
"""Longest supported period: one hundred years."""

MAX_TIMESTAMP_US: Final = MAX_EXACT_INT
"""Largest representable timestamp, about the year 2255.

Equal to :data:`MAX_EXACT_INT`; epoch timestamps range from zero up to it.
"""

MAX_DURATION_US: Final = MAX_EXACT_INT
"""Largest representable duration, equal to :data:`MAX_EXACT_INT` microseconds.

Bounds timeouts and retry delays; periods have the much tighter
:data:`MAX_PERIOD_US`.
"""

_MAX_NAME_LENGTH: Final = 512
_CONTROL_CHARACTERS: Final = re.compile(r"[\x00-\x1f\x7f]")


def _require_int(value: object, *, what: str, error: type[Exception] = InvalidPolicy) -> int:
    """Return ``value`` as an ``int``, rejecting bools and non-integers.

    ``bool`` is a subclass of ``int``, so ``Limit(True, per="1s")`` would
    otherwise be a rate of one per second. That is never what anyone meant.
    """
    if isinstance(value, bool):
        raise error(f"{what} must be an integer, not a bool: {value!r}")
    elif not isinstance(value, int):
        raise error(f"{what} must be an integer, got {type(value).__name__}: {value!r}")
    else:
        pass
    return value


def _require_range(value: int, *, what: str, low: int, high: int, error: type[Exception]) -> int:
    if not low <= value <= high:
        raise error(f"{what} must be between {low} and {high}, got {value}")
    else:
        pass
    return value


def _require_name(value: object, *, what: str) -> str:
    if not isinstance(value, str):
        raise InvalidPolicy(f"{what} must be a string, got {type(value).__name__}: {value!r}")
    elif not value:
        raise InvalidPolicy(f"{what} must not be empty")
    elif len(value) > _MAX_NAME_LENGTH:
        raise InvalidPolicy(f"{what} must be at most {_MAX_NAME_LENGTH} characters")
    elif _CONTROL_CHARACTERS.search(value):
        raise InvalidPolicy(f"{what} must not contain control characters: {value!r}")
    else:
        pass
    return value


_DURATION_UNITS: Final[dict[str, int]] = {
    "us": 1,
    "µs": 1,
    "ms": 1_000,
    "s": USECS_PER_SECOND,
    "m": 60 * USECS_PER_SECOND,
    "h": 60 * 60 * USECS_PER_SECOND,
    "d": 24 * 60 * 60 * USECS_PER_SECOND,
}
_DURATION_PATTERN: Final = re.compile(
    r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>us|µs|ms|s|m|h|d)?\s*$",
    re.IGNORECASE,
)


def duration_to_micros(value: str | int | float | dt.timedelta) -> DurationMicros:
    """Convert a public duration into exact integer microseconds.

    Accepts ``"1s"``, ``"500ms"``, ``"2h"``, a bare number of seconds, or a
    :class:`~datetime.timedelta`. Conversion is exact: floats are read through
    their decimal spelling, so ``0.1`` means one tenth of a second rather than
    the binary value nearest to it.

    A remainder rounds *up*, lengthening the period. For a rate of ``N`` per
    ``T`` a longer ``T`` is the stricter reading, so rounding never invents
    capacity the caller did not ask for.

    No upper bound is applied here; callers such as :class:`Limit` check the
    result against the range they support.

    :param value: A string of a non-negative number with an optional unit
        (``us``, ``µs``, ``ms``, ``s``, ``m``, ``h``, ``d``; case-insensitive,
        seconds when omitted), an ``int`` or ``float`` number of seconds, or a
        :class:`~datetime.timedelta`.
    :returns: The duration in microseconds, rounded up to a whole microsecond.
    :raises ~procrastinators.errors.InvalidPolicy: If ``value`` is a ``bool``, a
        non-finite ``float``, a string that does not parse, an unsupported type,
        or negative.
    """
    if isinstance(value, bool):
        raise InvalidPolicy(f"duration must not be a bool: {value!r}")
    elif isinstance(value, dt.timedelta):
        exact = Fraction(value // dt.timedelta(microseconds=1))
    elif isinstance(value, int):
        exact = Fraction(value) * USECS_PER_SECOND
    elif isinstance(value, float):
        # str() gives the shortest decimal that round-trips, which is what the
        # caller wrote. Fraction(0.1) would give 3602879701896397/2**55.
        if not math.isfinite(value):
            raise InvalidPolicy(f"duration must be finite, got {value!r}")
        else:
            pass
        exact = Fraction(str(value)) * USECS_PER_SECOND
    elif isinstance(value, str):
        if (match := _DURATION_PATTERN.match(value)) is None:
            raise InvalidPolicy(
                f"duration {value!r} is not understood; "
                "use a number of seconds or a string like '250ms', '1s', '5m', '2h', '1d'"
            )
        else:
            pass
        unit = _DURATION_UNITS[(match["unit"] or "s").lower()]
        exact = Fraction(match["value"]) * unit
    else:
        raise InvalidPolicy(f"duration must be a str, number, or timedelta, got {value!r}")

    if exact < 0:
        raise InvalidPolicy(f"duration must not be negative, got {value!r}")
    else:
        pass
    micros = DurationMicros(-((-exact.numerator) // exact.denominator))
    return micros


class Algorithms(StrEnum):
    """Stable algorithm identifiers.

    The value is what gets persisted and compared across processes, so these
    strings are part of the on-disk contract and do not change with renames.
    """

    FIXED_WINDOW = "fixed_window"
    """Epoch-aligned fixed windows; see :class:`FixedWindowPolicy`."""

    SLIDING_LOG = "sliding_log"
    """Exact rolling window over a log of admissions; see :class:`SlidingLogPolicy`."""

    SLIDING_COUNTER = "sliding_counter"
    """Approximate rolling window from two counters; see :class:`SlidingCounterPolicy`."""

    TOKEN_BUCKET = "token_bucket"
    """Burst-plus-refill bucket; see :class:`TokenBucketPolicy`."""

    LEAKY_BUCKET = "leaky_bucket"
    """Pacing with optional burst tolerance; see :class:`LeakyBucketPolicy`."""


@dataclass(frozen=True, slots=True)
class Limit:
    """A public rate specification: ``amount`` operations per ``per``.

    This is the ergonomic surface (``Limit(10, per="1s")``). It says what the
    vendor's documentation says and deliberately carries no algorithm: the same
    "10 per second" becomes a different policy under a sliding log than under a
    token bucket, and that choice belongs to the caller.

    :raises ~procrastinators.errors.InvalidPolicy: ``amount`` is not an integer between 1 and
        :data:`MAX_AMOUNT`, or ``per`` is not an understood duration between :data:`MIN_PERIOD_US`
        and :data:`MAX_PERIOD_US`.
    """

    amount: int
    """Operations permitted per period, an ``int`` from 1 to :data:`MAX_AMOUNT`."""

    per: str | int | float | dt.timedelta = "1s"
    """The period as the caller wrote it, in any form :func:`duration_to_micros` accepts.

    Kept verbatim for display; :attr:`period_us` is the normalized value.
    """

    period_us: DurationMicros = field(init=False)
    """``per`` in microseconds, computed on construction rather than passed in.

    Lies between :data:`MIN_PERIOD_US` and :data:`MAX_PERIOD_US`.
    """

    def __post_init__(self) -> None:
        amount = _require_int(self.amount, what="Limit amount")
        _require_range(amount, what="Limit amount", low=1, high=MAX_AMOUNT, error=InvalidPolicy)
        period = duration_to_micros(self.per)
        _require_range(
            period,
            what=f"Limit period ({self.per!r})",
            low=MIN_PERIOD_US,
            high=MAX_PERIOD_US,
            error=InvalidPolicy,
        )
        object.__setattr__(self, "period_us", period)

    def __str__(self) -> str:
        text = f"{self.amount} per {self.per}"
        return text


@dataclass(frozen=True, slots=True)
class FixedWindowPolicy:
    """At most ``amount`` cost units per aligned interval ``[start, start + period)``.

    Windows are aligned to the Unix epoch plus ``epoch_offset_us``, never to
    process startup: two workers that began at different times must agree on
    where the boundary is. This policy admits up to ``2 * amount`` across a
    boundary, which is correct behavior and not a rolling guarantee.

    :raises ~procrastinators.errors.InvalidPolicy: ``amount`` or ``period_us`` is not an integer or
        is out of range, or ``epoch_offset_us`` is not an integer in ``[0, period_us)``.
    """

    amount: int
    """Cost units admitted per window, 1 to :data:`MAX_AMOUNT`."""

    period_us: DurationMicros
    """Window length in microseconds, :data:`MIN_PERIOD_US` to :data:`MAX_PERIOD_US`."""

    epoch_offset_us: DurationMicros = DurationMicros(0)
    """Shift of every window boundary from the Unix epoch, in microseconds.

    Must lie in ``[0, period_us)``; zero aligns windows to the epoch itself.
    """

    algorithm: ClassVar[Algorithms] = Algorithms.FIXED_WINDOW
    """Stable algorithm id, persisted and covered by the policy fingerprint."""

    state_version: ClassVar[int] = 1
    """Version of this algorithm's stored state, covered by the policy fingerprint."""

    def __post_init__(self) -> None:
        _validate_rate(self.amount, self.period_us)
        offset = _require_int(self.epoch_offset_us, what="epoch_offset_us")
        _require_range(
            offset,
            what="epoch_offset_us",
            low=0,
            high=self.period_us - 1,
            error=InvalidPolicy,
        )

    @property
    def capacity(self) -> int:
        """The largest cost this rule can ever admit."""
        return self.amount


@dataclass(frozen=True, slots=True)
class SlidingLogPolicy:
    """At most ``amount`` cost units in every rolling ``(now - period, now]``.

    Exact, at the price of retaining one timestamp/cost entry per admission
    still inside a window. The default policy: it is the only one whose
    guarantee matches how an undocumented "N per T" quota is usually read.

    :raises ~procrastinators.errors.InvalidPolicy: ``amount`` or ``period_us`` is not an integer or
        is out of range.
    """

    amount: int
    """Cost units admitted in any rolling window, 1 to :data:`MAX_AMOUNT`."""

    period_us: DurationMicros
    """Rolling window length in microseconds, :data:`MIN_PERIOD_US` to :data:`MAX_PERIOD_US`."""

    algorithm: ClassVar[Algorithms] = Algorithms.SLIDING_LOG
    """Stable algorithm id, persisted and covered by the policy fingerprint."""

    state_version: ClassVar[int] = 1
    """Version of this algorithm's stored state, covered by the policy fingerprint."""

    def __post_init__(self) -> None:
        _validate_rate(self.amount, self.period_us)

    @property
    def capacity(self) -> int:
        """The largest cost this rule can ever admit: ``amount``."""
        return self.amount


@dataclass(frozen=True, slots=True)
class SlidingCounterPolicy:
    """Approximate rolling limit weighting the previous window into the current one.

    Admission requires ``floor(previous * remaining / period) + current + cost <= amount``,
    written identically in every executor. This does *not* promise a strict
    rolling bound, which is why it must be chosen explicitly rather than
    substituted for a sliding log.

    :raises ~procrastinators.errors.InvalidPolicy: ``amount`` or ``period_us`` is not an integer or
        is out of range, or ``amount * period_us`` exceeds :data:`MAX_EXACT_INT`.
    """

    amount: int
    """Cost units admitted per window, 1 to :data:`MAX_AMOUNT`.

    ``amount * period_us`` must also not exceed :data:`MAX_EXACT_INT`.
    """

    period_us: DurationMicros
    """Window length in microseconds, :data:`MIN_PERIOD_US` to :data:`MAX_PERIOD_US`.

    Further bounded by the ``amount * period_us`` limit.
    """

    algorithm: ClassVar[Algorithms] = Algorithms.SLIDING_COUNTER
    """Stable algorithm id, persisted and covered by the policy fingerprint."""

    state_version: ClassVar[int] = 1
    """Version of this algorithm's stored state, covered by the policy fingerprint."""

    def __post_init__(self) -> None:
        _validate_rate(self.amount, self.period_us)
        # The weighting multiplies a count by a remaining-time value before
        # dividing. Keeping that product exact is what lets a Lua executor
        # agree with Python, so the pair is rejected rather than silently
        # rounded differently in two places.
        if self.amount * self.period_us > MAX_EXACT_INT:
            raise InvalidPolicy(
                f"sliding counter requires amount * period_us <= {MAX_EXACT_INT} so every "
                f"executor computes the weighting exactly; got {self.amount} * "
                f"{self.period_us} = {self.amount * self.period_us}"
            )
        else:
            pass

    @property
    def capacity(self) -> int:
        """The largest cost this rule can ever admit: ``amount``."""
        return self.amount


@dataclass(frozen=True, slots=True)
class TokenBucketPolicy:
    """Refill ``refill_amount`` tokens every ``refill_period_us``, capped at ``capacity``.

    Permitted traffic follows a burst-plus-refill envelope. ``capacity`` and
    ``initial_tokens`` are explicit because the difference is observable: a
    bucket that starts full admits a burst on the very first call, and one that
    starts empty does not.

    :raises ~procrastinators.errors.InvalidPolicy: ``capacity``, ``refill_amount``, or
        ``refill_period_us`` is not an integer or is out of range, ``initial_tokens`` is not an
        integer between 0 and ``capacity``, or refilling an empty bucket would take longer than
        :data:`MAX_DURATION_US`.
    """

    capacity: int
    """Most tokens the bucket holds, and so the largest admissible cost; 1 to :data:`MAX_AMOUNT`."""

    refill_amount: int
    """Tokens added each refill period, 1 to :data:`MAX_AMOUNT`."""

    refill_period_us: DurationMicros
    """Interval between refills in microseconds, :data:`MIN_PERIOD_US` to :data:`MAX_PERIOD_US`."""

    initial_tokens: int | None = None
    """Tokens at first use, 0 to ``capacity``; ``None`` means a full bucket.

    See :attr:`starting_tokens` for the resolved value.
    """

    algorithm: ClassVar[Algorithms] = Algorithms.TOKEN_BUCKET
    """Stable algorithm id, persisted and covered by the policy fingerprint."""

    state_version: ClassVar[int] = 1
    """Version of this algorithm's stored state, covered by the policy fingerprint."""

    def __post_init__(self) -> None:
        _validate_rate(self.capacity, self.refill_period_us, amount_name="capacity")
        refill = _require_int(self.refill_amount, what="refill_amount")
        _require_range(refill, what="refill_amount", low=1, high=MAX_AMOUNT, error=InvalidPolicy)
        if self.initial_tokens is not None:
            initial = _require_int(self.initial_tokens, what="initial_tokens")
            _require_range(
                initial, what="initial_tokens", low=0, high=self.capacity, error=InvalidPolicy
            )
        else:
            pass
        # The longest retry delay is the time to refill an empty bucket; it
        # must be a representable duration for every executor (N3).
        if (fill_us := -(-self.capacity // refill) * self.refill_period_us) > MAX_DURATION_US:
            raise InvalidPolicy(
                f"refilling {self.capacity} tokens at {refill} per {self.refill_period_us} µs "
                f"takes {fill_us} µs, beyond the supported {MAX_DURATION_US} µs"
            )
        else:
            pass

    @property
    def starting_tokens(self) -> int:
        """Tokens present at first use; a full bucket unless stated otherwise."""
        tokens = self.capacity if self.initial_tokens is None else self.initial_tokens
        return tokens

    def fingerprint_parameters(self) -> dict[str, int]:
        """Parameters the policy fingerprint covers, with ``initial_tokens`` resolved.

        ``initial_tokens=None`` and ``initial_tokens=capacity`` behave identically,
        so they must fingerprint identically; otherwise two workers that agree
        on the policy would be told they conflict.
        """
        parameters = {
            "capacity": self.capacity,
            "refill_amount": self.refill_amount,
            "refill_period_us": self.refill_period_us,
            "initial_tokens": self.starting_tokens,
        }
        return parameters


@dataclass(frozen=True, slots=True)
class LeakyBucketPolicy:
    """Pace admissions at ``amount`` per ``period_us`` with no idle credit by default.

    An admitted cost ``c`` advances the next eligible time by
    ``ceil(c * period_us / amount)`` microseconds. ``burst_tolerance`` is the
    number of cost units that may arrive ahead of that schedule; it defaults to
    zero, which is the difference between this and a token bucket. Cost
    describes one atomic operation and does not space out ``c`` separate
    requests made inside the caller's body.

    Weighting and tolerance are independent: a strictly paced bucket still
    admits one operation of any cost up to ``amount``, and pays for it by
    pushing the next eligible time further out (contract P4).

    :raises ~procrastinators.errors.InvalidPolicy: ``amount`` or ``period_us`` is not an integer or
        is out of range, ``burst_tolerance`` is not an integer between 0 and :data:`MAX_AMOUNT`, or
        the tolerance plus one period is longer than :data:`MAX_DURATION_US`.
    """

    amount: int
    """Cost units admitted per period, 1 to :data:`MAX_AMOUNT`; with ``period_us`` sets the pace."""

    period_us: DurationMicros
    """Pacing period in microseconds, :data:`MIN_PERIOD_US` to :data:`MAX_PERIOD_US`."""

    burst_tolerance: int = 0
    """Cost units that may arrive ahead of the pace, 0 to :data:`MAX_AMOUNT`.

    Zero, the default, means strict pacing with no idle credit.
    """

    algorithm: ClassVar[Algorithms] = Algorithms.LEAKY_BUCKET
    """Stable algorithm id, persisted and covered by the policy fingerprint."""

    state_version: ClassVar[int] = 1
    """Version of this algorithm's stored state, covered by the policy fingerprint."""

    def __post_init__(self) -> None:
        _validate_rate(self.amount, self.period_us)
        tolerance = _require_int(self.burst_tolerance, what="burst_tolerance")
        _require_range(
            tolerance, what="burst_tolerance", low=0, high=MAX_AMOUNT, error=InvalidPolicy
        )
        # The schedule runs at most the tolerance plus one full-capacity
        # interval ahead of now; that span must be a representable duration (N3).
        if (ahead_us := tolerance * self.period_us // self.amount + self.period_us) > (
            MAX_DURATION_US
        ):
            raise InvalidPolicy(
                f"a burst tolerance of {tolerance} lets the schedule run {ahead_us} µs ahead, "
                f"beyond the supported {MAX_DURATION_US} µs"
            )
        else:
            pass

    @property
    def capacity(self) -> int:
        """The largest cost one operation may have: one period's worth, ``amount``.

        Independent of ``burst_tolerance``, which governs how early an
        operation may start, not how large it may be.
        """
        return self.amount


Policy: TypeAlias = (
    FixedWindowPolicy
    | SlidingLogPolicy
    | SlidingCounterPolicy
    | TokenBucketPolicy
    | LeakyBucketPolicy
)
"""Any of the five built-in normalized policies.

Third-party policies are not members; they satisfy :class:`PolicySpec` instead.
"""


@runtime_checkable
class PolicySpec(Protocol):
    """What a constraint needs from a policy, whoever wrote it.

    Structural rather than a closed union, because a third-party algorithm
    brings its own policy type. Requiring it to subclass one of the built-ins
    would make "users can implement their own algorithms" true only for
    algorithms shaped like the ones already shipped.

    A policy's fingerprint covers its parameters. A dataclass policy's fields
    are used as they are; any other policy, or one whose fields admit two
    spellings of the same behavior, supplies an optional
    ``fingerprint_parameters()`` method returning a mapping of canonical values.
    See :func:`procrastinators.keys.policy_fingerprint`.
    """

    @property
    def algorithm(self) -> str:
        """Stable algorithm id. Built-ins use :class:`Algorithms` values."""
        ...

    @property
    def state_version(self) -> int:
        """Version of the stored representation, part of the fingerprint."""
        ...

    @property
    def capacity(self) -> int:
        """Largest single cost this policy could ever admit."""
        ...


def _validate_rate(amount: object, period_us: object, *, amount_name: str = "amount") -> None:
    value = _require_int(amount, what=amount_name)
    _require_range(value, what=amount_name, low=1, high=MAX_AMOUNT, error=InvalidPolicy)
    period = _require_int(period_us, what="period_us")
    _require_range(
        period, what="period_us", low=MIN_PERIOD_US, high=MAX_PERIOD_US, error=InvalidPolicy
    )


def policy_capacity(policy: PolicySpec) -> int:
    """The largest single cost ``policy`` could ever admit.

    A cost above this can never succeed, so it is an
    :exc:`~procrastinators.errors.InvalidCost` rather than a denial with an
    infinite retry delay.

    :param policy: Any built-in or third-party policy.
    """
    return policy.capacity


PolicyFingerprint = NewType("PolicyFingerprint", str)
"""Digest over algorithm, parameters, state version, and schema.

Deliberately *not* part of quota identity. Raising a rate must keep addressing
the same stored quota — otherwise every rate change would hand out a fresh,
empty bucket — while still being detectable as a disagreement between workers.
Computed in Phase 4; this module only carries it.
"""


@dataclass(frozen=True, slots=True, order=True)
class QuotaIdentity:
    """What is being limited: a namespace plus a stable key.

    The key comes from ``idempotent_key()`` over the caller's scope mapping
    (vendor, dataset, endpoint, credential reference). It never contains rate
    values, so changing a rate addresses the same quota.

    The namespace separates environments and deployments that must not share
    counters even when their keys coincide.

    Instances order by ``(namespace, key)``, which is the prefix of the
    canonical constraint order.

    :raises ~procrastinators.errors.InvalidPolicy: ``namespace`` or ``key`` is not a non-empty
        string of bounded length free of control characters.
    """

    namespace: str
    """Environment or deployment the quota belongs to.

    A non-empty string of at most 512 characters with no control characters.
    """

    key: str
    """Stable digest of the caller's scope mapping, under the same constraints as ``namespace``."""

    def __post_init__(self) -> None:
        _require_name(self.namespace, what="namespace")
        _require_name(self.key, what="quota key")

    def __str__(self) -> str:
        text = f"{self.namespace}/{self.key}"
        return text


@dataclass(frozen=True, slots=True, order=True)
class RuleId:
    """Which rule inside a quota: a scope plus a name.

    Names are either explicit (``"burst"``, ``"daily"``) or positional. The
    simple list API assigns positional names ``#0``, ``#1``, ... in the order
    given, so reordering a list is a policy conflict rather than a silent
    request for fresh quota. Explicit names survive reordering, which is why
    managed configuration should use them.

    :raises ~procrastinators.errors.InvalidPolicy: ``name`` is not a valid name, or uses the
        positional prefix without being a positional rule name.
    """

    scope: QuotaIdentity
    """Quota this rule belongs to."""

    name: str
    """Explicit name, or ``#`` followed by digits for a positional rule.

    Subject to the same length and character limits as a namespace; any other
    name starting with ``#`` is rejected as reserved.
    """

    _POSITIONAL_PREFIX: ClassVar[str] = "#"

    def __post_init__(self) -> None:
        _require_name(self.name, what="rule name")
        if self.name.startswith(self._POSITIONAL_PREFIX) and not self.name[1:].isdigit():
            raise InvalidPolicy(
                f"rule names starting with {self._POSITIONAL_PREFIX!r} are reserved for "
                f"positional rules, got {self.name!r}"
            )
        else:
            pass

    @classmethod
    def positional(cls, scope: QuotaIdentity, index: int) -> RuleId:
        """The rule name assigned to position ``index`` of a limits list.

        :param scope: Quota the list of limits belongs to.
        :param index: Zero-based position in the list, 0 to :data:`MAX_AMOUNT`.
        :returns: The rule named ``#<index>`` within ``scope``.
        :raises ~procrastinators.errors.InvalidPolicy: If ``index`` is not an
            ``int``, is a ``bool``, or is out of range.
        """
        position = _require_int(index, what="rule index")
        _require_range(position, what="rule index", low=0, high=MAX_AMOUNT, error=InvalidPolicy)
        rule = cls(scope, f"{cls._POSITIONAL_PREFIX}{position}")
        return rule

    @property
    def is_positional(self) -> bool:
        """Whether this is a positional ``#N`` rule rather than an explicitly named one."""
        positional = self.name.startswith(self._POSITIONAL_PREFIX)
        return positional

    def __str__(self) -> str:
        text = f"{self.scope}:{self.name}"
        return text


@dataclass(frozen=True, slots=True)
class Constraint:
    """One rule to satisfy: an identity, its policy, and its fingerprint.

    ``coordination_domain`` groups rules that a caller intends to compose.
    Composed constraints either all leave it unset or all declare the same
    one. Redis Cluster keeps every rule and cooldown under one prefix in one
    slot, so the domain is a logical grouping rather than a placement key.

    :raises ~procrastinators.errors.InvalidPolicy: ``fingerprint`` or a given
        ``coordination_domain`` is not a valid name.
    """

    rule: RuleId
    """Rule whose state this constraint reads and debits."""

    policy: PolicySpec
    """Policy to enforce: a built-in :data:`Policy` or any :class:`PolicySpec`."""

    fingerprint: PolicyFingerprint
    """Digest of ``policy``, stored with the rule's state and checked at admission.

    Must be a non-empty string of at most 512 characters with no control characters.
    """

    coordination_domain: str | None = None
    """Logical composition group, or ``None`` for no explicit group."""

    def __post_init__(self) -> None:
        _require_name(self.fingerprint, what="policy fingerprint")
        if self.coordination_domain is not None:
            _require_name(self.coordination_domain, what="coordination domain")
        else:
            pass

    @property
    def algorithm(self) -> str:
        """The policy's stable algorithm id."""
        return self.policy.algorithm

    @property
    def state_version(self) -> int:
        """The policy's stored-state version."""
        return self.policy.state_version

    @property
    def capacity(self) -> int:
        """The largest single cost the policy could ever admit."""
        capacity = policy_capacity(self.policy)
        return capacity

    def __str__(self) -> str:
        text = f"{self.rule} ({self.algorithm})"
        return text


def canonical_constraints(constraints: Sequence[Constraint]) -> tuple[Constraint, ...]:
    """Order, deduplicate, and check a set of constraints for composition.

    Identical constraints (same rule, same policy, same fingerprint) collapse to
    one: naming the same quota twice is a caller convenience, not a request to
    be charged twice. Constraints that share a rule but disagree on policy are a
    :exc:`~procrastinators.errors.PolicyConflict`, as are incompatible
    coordination domains.

    Ordering is by ``(namespace, key, rule name)``. It is stable and independent
    of the order the caller happened to pass, which is what lets backends take
    row locks in a consistent order and avoid deadlocking against each other.

    :param constraints: The constraints of one request, in any order.
    :returns: The distinct constraints in canonical order.
    :raises ~procrastinators.errors.InvalidPolicy: If ``constraints`` is empty.
    :raises ~procrastinators.errors.PolicyConflict: If two constraints name the
        same rule but differ in policy, fingerprint, or coordination domain, or
        if the constraints do not all share one coordination domain.
    """
    if not constraints:
        raise InvalidPolicy("an admission request needs at least one constraint")
    else:
        pass

    by_rule: dict[RuleId, Constraint] = dict()
    for constraint in constraints:
        if (existing := by_rule.get(constraint.rule)) is None:
            by_rule[constraint.rule] = constraint
        elif existing != constraint:
            raise PolicyConflict(
                f"rule {constraint.rule} appears twice with different policies",
                rule=constraint.rule,
                expected=existing.fingerprint,
                found=constraint.fingerprint,
            )
        else:
            pass

    ordered = tuple(sorted(by_rule.values(), key=operator.attrgetter("rule.scope", "rule.name")))

    domains = {constraint.coordination_domain for constraint in ordered}
    if len(domains) > 1:
        shown = sorted(str(domain) for domain in domains)
        raise PolicyConflict(
            "composed constraints must share one coordination domain to be admitted "
            f"atomically, got {shown}"
        )
    else:
        pass
    return ordered


@dataclass(frozen=True, slots=True)
class OperationBudget:
    """The bounded time and effort one admission attempt may spend.

    Four separate budgets, because conflating them is how a caller ends up
    unable to tell a slow database from an exhausted quota:

    ``deadline_us``
        When the caller gives up, in **local monotonic** time. ``None`` permits
        indefinite waiting for quota. This value is local and must never be
        sent to a remote authority, which the type system enforces.
    ``storage_timeout_us``
        Cap on a single storage call. Never ``None``: unbounded quota waiting
        still does not license an unbounded individual round trip.
    ``lock_timeout_us``
        Cap on waiting for local or backend contention.
    ``max_contention_retries``
        How many times a *definitely uncommitted* transient failure may be
        retried inside this budget.

    The public ``timeout`` maps here: ``None`` gives no deadline and waiting;
    ``0`` gives ``wait_for_quota=False`` and one attempt; a positive value gives
    a deadline covering contention, storage work, and quota waiting together.

    :raises ~procrastinators.errors.InvalidPolicy: ``deadline_us`` is given but not an integer,
        ``storage_timeout_us`` or ``lock_timeout_us`` is not an integer between 1 and
        :data:`MAX_DURATION_US`, or ``max_contention_retries`` is not an integer between 0 and 100.
    """

    deadline_us: MonotonicMicros | None = None
    """Local monotonic give-up time, or ``None`` to wait for quota indefinitely."""

    storage_timeout_us: DurationMicros = DurationMicros(5 * USECS_PER_SECOND)
    """Cap on one storage call in microseconds, 1 to :data:`MAX_DURATION_US`; default 5 s."""

    lock_timeout_us: DurationMicros = DurationMicros(5 * USECS_PER_SECOND)
    """Cap on contention waits in microseconds, 1 to :data:`MAX_DURATION_US`; default 5 s."""

    max_contention_retries: int = 3
    """Retries allowed for definitely uncommitted transient failures, 0 to 100."""

    wait_for_quota: bool = True
    """Whether a denial may be waited out; ``False`` means exactly one admission attempt."""

    def __post_init__(self) -> None:
        if self.deadline_us is not None:
            _require_int(self.deadline_us, what="deadline_us", error=InvalidPolicy)
        else:
            pass
        for name in ("storage_timeout_us", "lock_timeout_us"):
            value = _require_int(getattr(self, name), what=name)
            _require_range(value, what=name, low=1, high=MAX_DURATION_US, error=InvalidPolicy)
        retries = _require_int(self.max_contention_retries, what="max_contention_retries")
        _require_range(retries, what="max_contention_retries", low=0, high=100, error=InvalidPolicy)


@dataclass(frozen=True, slots=True)
class AdmissionRequest:
    """One atomic check-and-commit attempt for every constraint at once.

    Constraints are canonicalized on construction, so a request cannot exist in
    a form a backend would have to repair. All rules admit or nothing is
    consumed; there is no partial debit to undo.

    :raises ~procrastinators.errors.InvalidCost: ``cost`` is not an integer between 1 and
        :data:`MAX_COST`, or exceeds the capacity of any constraint.
    :raises ~procrastinators.errors.InvalidPolicy: ``constraints`` is empty.
    :raises ~procrastinators.errors.PolicyConflict: Two constraints address the same rule with
        different policies, or the constraints do not share one coordination domain.
    """

    constraints: tuple[Constraint, ...]
    """Every rule to admit together, put through :func:`canonical_constraints` on construction."""

    cost: int = 1
    """Units charged to every constraint, 1 to :data:`MAX_COST`.

    Must not exceed any constraint's capacity; either violation raises
    :exc:`~procrastinators.errors.InvalidCost`.
    """

    budget: OperationBudget = field(default_factory=OperationBudget)
    """Time and retry budget for this attempt; the :class:`OperationBudget` defaults if omitted."""

    def __post_init__(self) -> None:
        cost = _require_int(self.cost, what="cost", error=InvalidCost)
        _require_range(cost, what="cost", low=1, high=MAX_COST, error=InvalidCost)
        canonical = canonical_constraints(self.constraints)
        object.__setattr__(self, "constraints", canonical)

        # A cost above a rule's capacity can never be admitted. Denying it would
        # mean an unbounded retry delay, so it is rejected up front instead.
        for constraint in canonical:
            if cost > constraint.capacity:
                raise InvalidCost(
                    f"cost {cost} can never be admitted by {constraint.rule}: its capacity "
                    f"is {constraint.capacity}"
                )
            else:
                pass

    @property
    def rules(self) -> tuple[RuleId, ...]:
        """The rule of each constraint, in canonical order."""
        rules = tuple(constraint.rule for constraint in self.constraints)
        return rules

    @property
    def coordination_domain(self) -> str | None:
        """The coordination domain every constraint shares, or ``None`` if none declares one."""
        return self.constraints[0].coordination_domain


@dataclass(frozen=True, slots=True)
class BackendIdentity:
    """Which storage authority a backend addresses.

    Two handles coordinate only if they address the same ``family`` and
    ``authority``; the ``namespace`` then decides whether they share counters.
    Composition across different authorities is impossible, and this is what
    detects the attempt.

    ``authority`` is canonical and credential-free: an absolute path, a
    ``host:port/db``, never a URL with a password in it.

    :raises ~procrastinators.errors.InvalidPolicy: ``family``, ``authority``, or ``namespace`` is
        not a valid name, or ``authority`` carries credentials.
    """

    family: str
    """Kind of backend, such as ``memory``, ``sqlite``, or ``redis``."""

    authority: str
    """Canonical, credential-free address of the store; must not contain ``@``."""

    namespace: str
    """Quota namespace this handle uses within the authority."""

    def __post_init__(self) -> None:
        _require_name(self.family, what="backend family")
        _require_name(self.authority, what="backend authority")
        _require_name(self.namespace, what="namespace")
        if "@" in self.authority:
            raise InvalidPolicy(
                "backend authority must not contain credentials; strip the userinfo "
                f"component before constructing BackendIdentity: {self.authority!r}"
            )
        else:
            pass

    def addresses_same_authority(self, other: BackendIdentity) -> bool:
        """Whether two backends talk to the same store, namespace aside.

        :param other: Identity of the backend to compare with.
        :returns: ``True`` when ``family`` and ``authority`` both match.
        """
        same = self.family == other.family and self.authority == other.authority
        return same

    def __str__(self) -> str:
        text = f"{self.family}://{self.authority}#{self.namespace}"
        return text


class Ownership(StrEnum):
    """Who is responsible for closing a resource."""

    OWNED = "owned"
    """Created by this library; closed when the limiter closes."""

    BORROWED = "borrowed"
    """Injected by the caller; never closed by this library."""


@dataclass(frozen=True, slots=True)
class ResourceOwnership:
    """Ownership of the resources a backend may hold."""

    client: Ownership = Ownership.OWNED
    """Who closes the driver client or connection; owned unless injected."""

    executor: Ownership = Ownership.OWNED
    """Who closes the executor the backend runs work on; owned unless injected."""


def _new_admission_id() -> str:
    admission_id = uuid.uuid4().hex
    return admission_id


@dataclass(frozen=True, slots=True)
class Admission:
    """Proof that one acquisition was committed.

    Only a backend creates this, and only *after* its commit succeeded. It is
    the difference between "the policy would allow this" and "the policy has
    recorded this": an advisory :class:`Snapshot` never becomes one.

    It carries no release or refund operation, by design. Quota is consumed at
    admission and exiting a context does not give it back.

    :raises ~procrastinators.errors.InvalidPolicy: ``charged`` is empty, or ``admitted_at`` is not
        an integer between 0 and :data:`MAX_TIMESTAMP_US`.
    :raises ~procrastinators.errors.InvalidCost: ``cost`` is not an integer between 1 and
        :data:`MAX_COST`.
    """

    charged: tuple[RuleId, ...]
    """Every rule debited by this acquisition; never empty."""

    cost: int
    """Units charged to each rule in ``charged``, 1 to :data:`MAX_COST`."""

    admitted_at: EpochMicros
    """Authority epoch time recorded for the commit, 0 to :data:`MAX_TIMESTAMP_US`."""

    backend: BackendIdentity
    """Authority that committed the acquisition."""

    admission_id: str = field(default_factory=_new_admission_id)
    """Identifier for this acquisition; a fresh random UUID4 hex string unless given."""

    def __post_init__(self) -> None:
        if not self.charged:
            raise InvalidPolicy("an admission must record at least one charged rule")
        else:
            pass
        _require_range(
            _require_int(self.cost, what="cost", error=InvalidCost),
            what="cost",
            low=1,
            high=MAX_COST,
            error=InvalidCost,
        )
        _require_range(
            _require_int(self.admitted_at, what="admitted_at"),
            what="admitted_at",
            low=0,
            high=MAX_TIMESTAMP_US,
            error=InvalidPolicy,
        )


@dataclass(frozen=True, slots=True)
class RemainingEstimate:
    """An advisory count of units left on one rule.

    Advisory in the strict sense: another worker may consume the remainder
    before the caller acts on it. Never a reservation.
    """

    rule: RuleId
    """Rule the estimate describes."""

    units: int
    """Cost units the rule could still admit when it was evaluated."""


@dataclass(frozen=True, slots=True)
class Decision:
    """The atomic result of one admission attempt.

    Construct through :meth:`allow` and :meth:`deny`; the invariants are checked
    either way, so a decision that claims to be allowed while naming a blocking
    rule cannot be built.

    ``retry_after_us`` is a duration derived from the policy, not a wall-clock
    instant, so the caller and the authority need not agree on what time it is.
    It is advisory: another worker may take the capacity first, which is why
    waiters recheck rather than trusting it.

    :raises ~procrastinators.errors.InvalidPolicy: ``retry_after_us`` is not an integer between 0
        and :data:`MAX_DURATION_US`; an allowed decision lacks its admission, names blocking rules,
        or asks for a retry; or a denied decision carries an admission or names no blocking rule.
    """

    allowed: bool
    """Whether the attempt was admitted, and therefore already charged."""

    admission: Admission | None = None
    """Proof of the commit; present exactly when ``allowed`` is true."""

    blocking: tuple[RuleId, ...] = tuple()
    """Rules that denied the attempt; non-empty exactly when ``allowed`` is false."""

    retry_after_us: DurationMicros = DurationMicros(0)
    """Advisory delay before retrying, 0 to :data:`MAX_DURATION_US`; zero when allowed."""

    remaining: tuple[RemainingEstimate, ...] = tuple()
    """Advisory per-rule counts of units left; empty when the backend reports none."""

    observed_at: EpochMicros | None = None
    """Authority epoch time of the evaluation, or ``None`` if the backend did not report it."""

    def __post_init__(self) -> None:
        retry = _require_int(self.retry_after_us, what="retry_after_us")
        _require_range(
            retry, what="retry_after_us", low=0, high=MAX_DURATION_US, error=InvalidPolicy
        )
        if self.allowed:
            if self.admission is None:
                raise InvalidPolicy("an allowed decision must carry its Admission")
            elif self.blocking:
                raise InvalidPolicy(
                    f"an allowed decision cannot name blocking rules: {self.blocking}"
                )
            elif retry:
                raise InvalidPolicy("an allowed decision must not ask the caller to retry")
            else:
                pass
        elif self.admission is not None:
            raise InvalidPolicy("a denied decision must not carry an Admission")
        elif not self.blocking:
            raise InvalidPolicy("a denied decision must name the rules that blocked it")
        else:
            pass

    @classmethod
    def allow(
        cls,
        admission: Admission,
        *,
        remaining: Sequence[RemainingEstimate] = tuple(),
        observed_at: EpochMicros | None = None,
    ) -> Decision:
        """Build the decision for a committed acquisition.

        :param admission: Proof of the commit the decision reports.
        :param remaining: Advisory per-rule counts of units left.
        :param observed_at: Authority epoch time of the evaluation, if known.
        :raises ~procrastinators.errors.InvalidPolicy: If ``admission`` is ``None``.
        """
        decision = cls(
            allowed=True,
            admission=admission,
            remaining=tuple(remaining),
            observed_at=observed_at,
        )
        return decision

    @classmethod
    def deny(
        cls,
        blocking: Sequence[RuleId],
        retry_after_us: DurationMicros,
        *,
        remaining: Sequence[RemainingEstimate] = tuple(),
        observed_at: EpochMicros | None = None,
    ) -> Decision:
        """Build the decision for a denied attempt, which consumed nothing.

        :param blocking: Rules that denied the attempt; at least one.
        :param retry_after_us: Advisory delay before retrying, in microseconds.
        :param remaining: Advisory per-rule counts of units left.
        :param observed_at: Authority epoch time of the evaluation, if known.
        :raises ~procrastinators.errors.InvalidPolicy: If ``blocking`` is empty,
            or ``retry_after_us`` is not an ``int`` from 0 to
            :data:`MAX_DURATION_US`.
        """
        decision = cls(
            allowed=False,
            blocking=tuple(blocking),
            retry_after_us=retry_after_us,
            remaining=tuple(remaining),
            observed_at=observed_at,
        )
        return decision

    def __bool__(self) -> bool:
        return self.allowed


@dataclass(frozen=True, slots=True)
class RuleSnapshot:
    """Advisory state of one rule at a moment that has already passed."""

    rule: RuleId
    """Rule the snapshot describes."""

    algorithm: str
    """Algorithm id of the rule's policy."""

    remaining: int | None = None
    """Advisory cost units left, or ``None`` if the backend cannot say."""

    reset_after_us: DurationMicros | None = None
    """Advisory duration until the rule's state resets, or ``None`` if not reported."""

    cooldown_until: EpochMicros | None = None
    """Authority epoch time at which a cooldown in force ends, or ``None`` if there is none."""


@dataclass(frozen=True, slots=True)
class Snapshot:
    """An advisory inspection result. Explicitly not a reservation.

    Acting on a snapshot without acquiring is the mistake this type is named to
    prevent: by the time the caller reads it, another worker may have consumed
    everything it reported.
    """

    rules: tuple[RuleSnapshot, ...]
    """State of each inspected rule."""

    sampled_at: EpochMicros
    """Authority epoch time at which the state was read."""

    backend: BackendIdentity
    """Authority the state was read from."""

    advisory: ClassVar[bool] = True
    """Always ``True``: a snapshot is never a reservation."""


@dataclass(frozen=True, slots=True)
class Cooldown:
    """A shared pause applied to a scope, typically from a vendor's ``Retry-After``.

    Extended with ``max(existing, new)`` so a late, shorter cooldown cannot
    shorten a longer one already in force.
    """

    scope: QuotaIdentity
    """Quota the pause applies to."""

    until: EpochMicros
    """Authority epoch time at which the pause ends."""

    reason: str = ""
    """Free-text explanation for the pause; empty by default."""


@dataclass(frozen=True, slots=True)
class SetScalar:
    """Set a named scalar in a rule's state.

    ``prunes_only`` marks a write that reclaims space without consuming quota —
    rewriting a compacted counter, say. Only such writes may accompany a denial.
    A never-used rule's initial state is written separately, from
    ``Algorithm.initial_changes``, and is recorded even when the first attempt
    is denied (contract P5).
    """

    name: str
    """Scalar to set, one of those the algorithm declared in its state requirements."""

    value: int
    """New value of the scalar."""

    prunes_only: bool = False
    """Whether the write only reclaims space; only such a write may accompany a denial."""


@dataclass(frozen=True, slots=True)
class AppendEvent:
    """Record one weighted admission in a log-structured rule.

    :raises ~procrastinators.errors.InvalidCost: ``cost`` is not an integer between 1 and
        :data:`MAX_COST`.
    """

    at: EpochMicros
    """Authority epoch time of the admission."""

    cost: int
    """Weight of the single entry, 1 to :data:`MAX_COST`, however large the cost."""

    prunes_only: ClassVar[bool] = False
    """Always ``False``: an append consumes quota, so it never accompanies a denial."""

    def __post_init__(self) -> None:
        _require_range(
            _require_int(self.cost, what="event cost", error=InvalidCost),
            what="event cost",
            low=1,
            high=MAX_COST,
            error=InvalidCost,
        )


@dataclass(frozen=True, slots=True)
class DropEventsBefore:
    """Discard log entries at or before ``at``; they can no longer block anyone."""

    at: EpochMicros
    """Authority epoch cutoff; entries at or before it are discarded."""

    prunes_only: ClassVar[bool] = True
    """Always ``True``: dropping expired entries is permitted alongside a denial."""


@dataclass(frozen=True, slots=True)
class ClearState:
    """Forget a rule's state entirely, having become indistinguishable from unused.

    Policy metadata is *not* cleared with it: expiry must never hide a
    configuration disagreement between workers.
    """

    prunes_only: ClassVar[bool] = True
    """Always ``True``: forgetting indistinguishable state is permitted alongside a denial."""


StateChange: TypeAlias = SetScalar | AppendEvent | DropEventsBefore | ClearState
"""Any one proposed change to a rule's state, as carried by :attr:`Transition.changes`."""


@dataclass(frozen=True, slots=True)
class Transition:
    """An evaluator's proposal for one rule. It commits nothing.

    A backend collects one transition per rule inside its critical section and
    commits every change only if every rule admitted. This is where the
    all-or-nothing composition guarantee is actually enforced, and why an
    evaluator that could commit on its own would break it.

    ``safe_forget_after_us`` is the earliest epoch time at which this rule's
    state stops affecting any future decision, which is what cleanup, TTLs, and
    expiry follow.

    :raises ~procrastinators.errors.InvalidPolicy: ``retry_after_us`` is not an integer between 0
        and :data:`MAX_DURATION_US`, an admitting transition asks for a retry, or a denying
        transition proposes a change that consumes quota.
    """

    rule: RuleId
    """Rule this proposal is for."""

    admitted: bool
    """Whether this rule would admit the request."""

    changes: tuple[StateChange, ...] = tuple()
    """Proposed state changes; when not ``admitted``, only ``prunes_only`` changes are allowed."""

    retry_after_us: DurationMicros = DurationMicros(0)
    """Delay before this rule could admit, 0 to :data:`MAX_DURATION_US`; zero when ``admitted``."""

    safe_forget_after_us: EpochMicros | None = None
    """Authority epoch time after which the rule's state may be forgotten; ``None`` if not given."""

    remaining: int | None = None
    """Advisory cost units left on the rule after this proposal, or ``None`` if not reported."""

    def __post_init__(self) -> None:
        retry = _require_int(self.retry_after_us, what="retry_after_us")
        _require_range(
            retry, what="retry_after_us", low=0, high=MAX_DURATION_US, error=InvalidPolicy
        )
        if self.admitted:
            if retry:
                raise InvalidPolicy("an admitting transition must not ask the caller to retry")
            else:
                pass
        else:
            # Denial may reclaim obsolete state but must not consume quota or
            # extend a penalty: a rejected attempt costs the caller nothing.
            if charging := [change for change in self.changes if not change.prunes_only]:
                raise InvalidPolicy(
                    f"a denied transition for {self.rule} must not consume quota, "
                    f"but proposes {charging}"
                )
            else:
                pass


class CoordinationScope(StrEnum):
    """How far a backend's guarantee reaches."""

    IN_PROCESS = "in_process"
    """Threads and coroutines sharing one backend instance. Not across a fork."""

    LOCAL_MACHINE = "local_machine"
    """Processes on one machine reaching the same file."""

    SHARED_SERVICE = "shared_service"
    """Any worker that can reach the same service and namespace."""


class Durability(StrEnum):
    """What survives a restart, and what a caller may therefore assume."""

    EPHEMERAL = "ephemeral"
    """Lost on process exit. Memory."""

    LOCAL_DURABLE = "local_durable"
    """Survives process restart on one machine. SQLite."""

    SERVICE_DURABLE = "service_durable"
    """Survives under the service's stated persistence and failover policy."""

    BEST_EFFORT = "best_effort"
    """State may vanish at any time through eviction. Memcached.

    A cache miss is not proof that no quota was consumed, so this must be opted
    into explicitly rather than accepted as a default.
    """


@dataclass(frozen=True, slots=True)
class Capabilities:
    """What a backend actually implements.

    Checked at construction so unsupported combinations fail there rather than
    degrading quietly at the first acquisition.

    :raises ~procrastinators.errors.InvalidPolicy: No algorithm is supported, neither sync nor async
        is supported, ``native_executors`` names an unsupported algorithm, or ``max_composed_rules``
        is given but not an integer between 1 and :data:`MAX_AMOUNT`.
    """

    algorithms: frozenset[str]
    """Algorithm ids this backend can admit against; must not be empty."""

    coordination: CoordinationScope
    """How far the backend's admission guarantee reaches."""

    durability: Durability
    """What of the backend's state survives a restart."""

    supports_sync: bool = True
    """Whether a synchronous interface is offered; this or ``supports_async`` must be true."""

    supports_async: bool = False
    """Whether an awaitable interface is offered."""

    supports_composition: bool = False
    """Whether one request may admit several constraints atomically."""

    supports_cooldowns: bool = False
    """Whether shared cooldowns on a scope are supported."""

    supports_policy_administration: bool = False
    """Whether the backend offers administration of stored policy metadata."""

    requires_shared_coordination_domain: bool = False
    """Whether every request's constraints must declare a coordination domain."""

    max_composed_rules: int | None = None
    """Most constraints one request may compose, 1 to :data:`MAX_AMOUNT`; ``None`` for no limit."""

    native_executors: frozenset[str] = frozenset()
    """Algorithm ids evaluated natively inside the store; must be a subset of ``algorithms``."""

    state_representations: frozenset[str] = frozenset({"scalars"})
    """State shapes this backend can hold.

    A store that keeps one bounded item cannot hold an event log, so it
    advertises only ``scalars`` and refuses a sliding log at construction
    rather than at the first acquisition.
    """

    def __post_init__(self) -> None:
        if not self.algorithms:
            raise InvalidPolicy("a backend must support at least one algorithm")
        elif not (self.supports_sync or self.supports_async):
            raise InvalidPolicy("a backend must support at least one of sync or async")
        elif unknown := self.native_executors - self.algorithms:
            raise InvalidPolicy(
                f"native executors claim algorithms the backend does not support: {sorted(unknown)}"
            )
        elif self.max_composed_rules is not None:
            _require_range(
                _require_int(self.max_composed_rules, what="max_composed_rules"),
                what="max_composed_rules",
                low=1,
                high=MAX_AMOUNT,
                error=InvalidPolicy,
            )
        else:
            pass


@dataclass(frozen=True, slots=True)
class DiagnosticEvent:
    """Base for observations delivered after a critical section has ended.

    Events are immutable and callbacks run outside locks, so a diagnostic
    consumer cannot change, delay, or invalidate an admission. A callback that
    raises must not turn a committed admission into a reported failure.

    Events carry rule identities, which hold digested keys, and never raw
    credentials or connection strings.
    """

    at: MonotonicMicros
    """Local monotonic time of the observation; comparable only within this process."""


@dataclass(frozen=True, slots=True)
class AdmittedEvent(DiagnosticEvent):
    """An acquisition was committed."""

    rules: tuple[RuleId, ...]
    """Rules charged by the acquisition."""

    cost: int
    """Units charged to each rule."""

    waited_us: DurationMicros
    """Time the acquisition spent waiting before it was admitted, in microseconds."""

    backend: BackendIdentity
    """Authority that committed the acquisition."""


@dataclass(frozen=True, slots=True)
class DeniedEvent(DiagnosticEvent):
    """An admission attempt was denied and consumed nothing."""

    blocking: tuple[RuleId, ...]
    """Rules that denied the attempt."""

    cost: int
    """Units the attempt asked for."""

    retry_after_us: DurationMicros
    """Advisory delay before retrying, in microseconds."""


@dataclass(frozen=True, slots=True)
class WaitEvent(DiagnosticEvent):
    """A waiter slept outside every critical section before trying again."""

    rules: tuple[RuleId, ...]
    """Rules being waited on."""

    slept_us: DurationMicros
    """Length of this sleep in microseconds."""

    attempt: int
    """Which admission attempt of the acquisition the sleep followed."""


@dataclass(frozen=True, slots=True)
class BackendFailureEvent(DiagnosticEvent):
    """A storage operation failed."""

    backend: BackendIdentity
    """Backend whose operation failed."""

    operation: str
    """Name of the operation that failed."""

    error_type: str
    """Name of the exception type raised, carried instead of the exception itself."""

    indeterminate: bool = False
    """True when the commit may have happened. Never treated as a refund."""


@dataclass(frozen=True, slots=True)
class PolicyConflictEvent(DiagnosticEvent):
    """A rule's policy fingerprint disagreed with another for the same rule."""

    rule: RuleId
    """Rule whose policies disagree."""

    expected: PolicyFingerprint
    """Fingerprint of the policy this caller presented."""

    found: PolicyFingerprint
    """Fingerprint of the conflicting policy, usually the one already stored."""


@dataclass(frozen=True, slots=True)
class CooldownEvent(DiagnosticEvent):
    """A cooldown was applied to a scope."""

    scope: QuotaIdentity
    """Quota the pause applies to."""

    until: EpochMicros
    """Authority epoch time at which the cooldown in force ends."""

    reason: str = ""
    """Free-text explanation for the pause; empty by default."""


if __name__ == "__main__":
    pass
else:
    pass
