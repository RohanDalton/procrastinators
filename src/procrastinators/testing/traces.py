"""Traces: small, hand-checked admission histories with expected outcomes.

A :class:`Trace` is data. It names some rules and their policies, then lists
steps in time order: an :class:`Attempt` at an authority epoch instant with a
cost and the expected verdict (and, for a denial, the exact retry delay), or a
:class:`Finish` marking when an admitted caller's body ended. The expected
values were worked out by hand from ``docs/source/contracts.md`` §17, not produced by
running an implementation, so they check implementations rather than describe
them.

A :class:`Scenario` is coarser: bursts of identical attempts, with the number
each policy should admit per burst. It records failures demonstrated against
existing rate limiters as finite cases and states which policy allows or
rejects each, rather than judging every policy by a sliding log's rules.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, TypeAlias

from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    MAX_TIMESTAMP_US,
    Constraint,
    QuotaIdentity,
    RuleId,
)

if TYPE_CHECKING:
    from procrastinators.models import PolicySpec
else:
    pass

__all__ = [
    "Attempt",
    "Burst",
    "Covers",
    "Expectation",
    "Finish",
    "Scenario",
    "Trace",
    "TraceRule",
    "TraceStep",
]


class Covers(StrEnum):
    """What a trace exercises, so coverage of each algorithm can be checked."""

    FIRST_ADMISSION = "first_admission"
    """The first attempt on a never-used rule."""
    FULL_CAPACITY = "full_capacity"
    """Admissions up to exactly the policy's capacity."""
    EXACT_BOUNDARY = "exact_boundary"
    """Attempts one microsecond either side of a boundary."""
    IDLE_REFILL = "idle_refill"
    """Behavior after a long idle period."""
    WEIGHTED = "weighted"
    """Costs above one."""
    DENIAL = "denial"
    """Denials, and that they consume nothing."""
    RETRY_TIMING = "retry_timing"
    """Exact retry delays."""
    LONG_RUNNING = "long_running"
    """Bodies that outlast the period; exit neither refunds nor delays."""
    INITIAL_STATE = "initial_state"
    """A configured initial state, and that forgetting cannot reset it."""
    COMPOSITION = "composition"
    """Several rules admitted atomically together."""
    ROUNDING = "rounding"
    """Conservative rounding of non-integral intervals."""


@dataclass(frozen=True, slots=True)
class TraceRule:
    """One rule a trace admits against.

    :raises ValueError: ``label`` is empty.
    """

    label: str
    """How steps refer to this rule; unique within the trace."""

    policy: PolicySpec
    """The policy the rule enforces."""

    scope: str = "trace"
    """The quota key of the rule's scope; rules sharing a scope share a quota."""

    name: str | None = None
    """The rule name within the scope; the label when ``None``."""

    def __post_init__(self) -> None:
        if not self.label:
            raise ValueError("a trace rule needs a label")
        else:
            pass

    def rule(self, namespace: str) -> RuleId:
        """The rule identity in ``namespace``.

        :param namespace: The quota namespace the trace runs in.
        """
        rule = RuleId(QuotaIdentity(namespace, self.scope), self.name or self.label)
        return rule

    def constraint(self, namespace: str) -> Constraint:
        """The constraint, fingerprinted, in ``namespace``.

        :param namespace: The quota namespace the trace runs in.
        """
        constraint = Constraint(self.rule(namespace), self.policy, policy_fingerprint(self.policy))
        return constraint


@dataclass(frozen=True, slots=True)
class Attempt:
    """One admission attempt and its expected outcome.

    :raises ValueError: An allowed attempt names a retry delay or blocking rules.
    """

    at: int
    """Authority epoch time of the attempt, in microseconds."""

    allowed: bool
    """Whether it must be admitted."""

    cost: int = 1
    """The cost requested."""

    retry_after_us: int | None = None
    """For a denial, the exact expected retry delay; ``None`` leaves it unchecked."""

    rules: tuple[str, ...] | None = None
    """Labels of the rules requested together; every rule of the trace when ``None``."""

    blocking: tuple[str, ...] | None = None
    """For a denial, labels of the rules expected to block; ``None`` leaves it unchecked."""

    note: str = ""
    """Why this step is here, shown when it fails."""

    def __post_init__(self) -> None:
        if self.allowed and (self.retry_after_us is not None or self.blocking is not None):
            raise ValueError(f"an allowed attempt names no retry delay or blocking rules: {self}")
        else:
            pass


@dataclass(frozen=True, slots=True)
class Finish:
    """The body of an earlier admitted attempt ends here.

    A correct subject ignores it: exit neither refunds quota nor records a
    charge (contracts A2, A3, W5). A subject that charges on exit is caught by
    the attempts made before it.
    """

    attempt: int
    """Index, within the trace's steps, of the attempt whose body ends."""

    at: int
    """Authority epoch time at which the body ends."""

    note: str = ""
    """Why this step is here, shown when it fails."""


TraceStep: TypeAlias = Attempt | Finish
"""One step of a trace."""


@dataclass(frozen=True, slots=True)
class Trace:
    """A named, hand-checked admission history.

    :raises ValueError: The trace is malformed: no rules or steps, duplicate labels, steps out of
        time order, an attempt naming an unknown rule, or a finish that does not follow an allowed
        attempt.
    """

    name: str
    """Stable, dotted name, such as ``sliding_log.basics``."""

    description: str
    """What the trace demonstrates."""

    rules: tuple[TraceRule, ...]
    """The rules the steps refer to."""

    steps: tuple[TraceStep, ...]
    """The history, in non-decreasing time order."""

    contracts: tuple[str, ...] = tuple()
    """Rule numbers from ``docs/source/contracts.md`` the trace checks, such as ``P1``."""

    covers: frozenset[Covers] = field(default_factory=frozenset)
    """What the trace exercises."""

    def __post_init__(self) -> None:
        labels = [rule.label for rule in self.rules]
        if not self.rules or not self.steps:
            raise ValueError(f"trace {self.name} needs rules and steps")
        elif len(set(labels)) != len(labels):
            raise ValueError(f"trace {self.name} repeats a rule label")
        else:
            pass
        previous = 0
        for index, step in enumerate(self.steps):
            if not 0 <= step.at <= MAX_TIMESTAMP_US or step.at < previous:
                raise ValueError(f"trace {self.name} step {index} is out of time order")
            elif isinstance(step, Attempt) and not set(step.rules or ()) <= set(labels):
                raise ValueError(f"trace {self.name} step {index} names an unknown rule")
            elif isinstance(step, Attempt) and not set(step.blocking or ()) <= set(labels):
                raise ValueError(f"trace {self.name} step {index} blocks on an unknown rule")
            elif isinstance(step, Finish) and not (
                0 <= step.attempt < index
                and isinstance(earlier := self.steps[step.attempt], Attempt)
                and earlier.allowed
            ):
                raise ValueError(f"trace {self.name} step {index} finishes no admitted attempt")
            else:
                previous = step.at

    @property
    def algorithms(self) -> frozenset[str]:
        """Every algorithm id the trace's rules use."""
        algorithms = frozenset(rule.policy.algorithm for rule in self.rules)
        return algorithms

    @property
    def max_rules(self) -> int:
        """The most distinct rules any one attempt requests together."""
        counts = [
            len(set(step.rules or [rule.label for rule in self.rules]))
            for step in self.steps
            if isinstance(step, Attempt)
        ]
        most = max(counts)
        return most

    def labels_of(self, namespace: str) -> dict[RuleId, str]:
        """Map each rule identity in ``namespace`` back to its label.

        :param namespace: The quota namespace the trace runs in.
        """
        labels = {rule.rule(namespace): rule.label for rule in self.rules}
        return labels


@dataclass(frozen=True, slots=True)
class Burst:
    """Identical attempts made back to back at one instant.

    :raises ValueError: ``attempts`` is less than one or ``body_us`` is not positive.
    """

    at: int
    """Authority epoch time of every attempt in the burst."""

    attempts: int
    """How many attempts."""

    cost: int = 1
    """The cost of each."""

    body_us: int | None = None
    """How long each admitted caller's body runs, or ``None`` if it never ends in the scenario."""

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError(f"a burst makes at least one attempt, got {self.attempts}")
        elif self.body_us is not None and self.body_us <= 0:
            raise ValueError(f"a body runs for a positive time, got {self.body_us}")
        else:
            pass


@dataclass(frozen=True, slots=True)
class Expectation:
    """How many attempts of each burst one policy admits, and why."""

    label: str
    """Short name for the policy, such as ``sliding_log``."""

    policy: PolicySpec
    """The policy."""

    allowed: tuple[int, ...]
    """Admitted attempts per burst, in burst order."""

    rationale: str
    """Why this is the correct behavior for this policy."""


@dataclass(frozen=True, slots=True)
class Scenario:
    """A finite demonstration, with each policy's correct outcome recorded.

    :raises ValueError: The bursts are not in time order, or an expectation's counts do not
        match them.
    """

    name: str
    """Stable, dotted name."""

    source: str
    """Where the demonstration comes from."""

    description: str
    """What it shows."""

    bursts: tuple[Burst, ...]
    """The attempts, in time order."""

    expectations: tuple[Expectation, ...]
    """What each policy admits."""

    contracts: tuple[str, ...] = tuple()
    """Rule numbers from ``docs/source/contracts.md`` the scenario checks."""

    def __post_init__(self) -> None:
        times = [burst.at for burst in self.bursts]
        if times != sorted(times):
            raise ValueError(f"scenario {self.name}: bursts are not in time order")
        else:
            pass
        for expectation in self.expectations:
            if len(expectation.allowed) != len(self.bursts) or any(
                not 0 <= allowed <= burst.attempts
                for allowed, burst in zip(expectation.allowed, self.bursts, strict=True)
            ):
                raise ValueError(f"scenario {self.name}: {expectation.label} counts do not fit")
            else:
                pass

    def trace_for(self, expectation: Expectation) -> Trace:
        """Compile the scenario, under one policy, into a trace.

        Within a burst the first ``allowed`` attempts are admitted and the rest
        denied: at one instant nothing but admissions changes state, so once an
        attempt is denied every later one in the burst is too. Retry delays are
        left unchecked. Bodies that end are finished at their end time, before
        any attempt at that same instant.

        :param expectation: One of this scenario's expectations.
        :returns: A trace named ``<scenario>.<label>``.
        """
        pending: list[tuple[int, int, Attempt | tuple[int, int]]] = list()
        for burst_index, (burst, admitted) in enumerate(
            zip(self.bursts, expectation.allowed, strict=True)
        ):
            for position in range(burst.attempts):
                allowed = position < admitted
                pending.append((burst.at, 1, Attempt(burst.at, allowed, burst.cost)))
                if allowed and burst.body_us is not None:
                    pending.append((burst.at + burst.body_us, 0, (burst_index, position)))
                else:
                    pass
        pending.sort(key=_pending_order)
        steps: list[TraceStep] = list()
        index_of: dict[tuple[int, int], int] = dict()
        attempt_keys = iter(
            (burst_index, position)
            for burst_index, burst in enumerate(self.bursts)
            for position in range(burst.attempts)
        )
        for at, _, item in pending:
            if isinstance(item, Attempt):
                index_of[next(attempt_keys)] = len(steps)
                steps.append(item)
            else:
                steps.append(Finish(index_of[item], at))
        trace = Trace(
            name=f"{self.name}.{expectation.label}",
            description=f"{self.description} Under {expectation.label}: {expectation.rationale}",
            rules=(TraceRule(expectation.label, expectation.policy, scope=self.name),),
            steps=tuple(steps),
            contracts=self.contracts,
        )
        return trace


def _pending_order(entry: tuple[int, int, object]) -> tuple[int, int]:
    # Stable sort on (time, kind): finishes (0) before attempts (1) at one instant.
    at, kind, _ = entry
    return (at, kind)


if __name__ == "__main__":
    pass
else:
    pass
