"""Scripted backends: replay a fixed sequence of outcomes.

For testing what sits *above* a backend — waiters, the facade, diagnostics —
without an algorithm. Each admission consumes the next step of the script: an
allow, a deny with a chosen retry delay, a prepared decision, or an exception
to raise. The requests are recorded, so a test can assert what was asked for as
well as how the caller reacted.

A scripted backend still behaves like a backend where it can: it validates
requests against its declared capabilities, calls its observer at every
observation point, raises :exc:`~procrastinators.errors.ClosedResource` after
closing, and stamps admissions with its clock. It makes no promise about
quota, which is the point.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.models import (
    Admission,
    AdmissionRequest,
    Algorithms,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Decision,
    Durability,
    DurationMicros,
    RuleSnapshot,
    Snapshot,
)
from procrastinators.protocols import ObservationPoint, StateRepresentation
from procrastinators.testing.violations import ContractViolation

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from procrastinators.models import RuleId
    from procrastinators.protocols import AdmissionClock, AdmissionObserver
else:
    pass

__all__ = [
    "AsyncScriptedBackend",
    "ScriptStep",
    "ScriptedAllow",
    "ScriptedBackend",
    "ScriptedDeny",
    "allow",
    "deny",
]


@dataclass(frozen=True, slots=True)
class ScriptedAllow:
    """Admit whatever is requested, stamped with the backend's clock."""


@dataclass(frozen=True, slots=True)
class ScriptedDeny:
    """Deny with this retry delay, blocked by these rules (every requested rule if ``None``)."""

    retry_after_us: DurationMicros
    """The advisory retry delay to report."""

    blocking: tuple[RuleId, ...] | None = None
    """The rules to name as blocking, or ``None`` for every requested rule."""


ScriptStep: TypeAlias = (
    ScriptedAllow | ScriptedDeny | Decision | BaseException | Callable[[AdmissionRequest], Decision]
)
"""One scripted outcome: allow, deny, a prepared decision, an exception, or a callable."""


def allow() -> ScriptedAllow:
    """A step that admits the request."""
    step = ScriptedAllow()
    return step


def deny(retry_after_us: int, *blocking: RuleId) -> ScriptedDeny:
    """A step that denies the request.

    :param retry_after_us: The advisory retry delay to report.
    :param blocking: The rules to name; every requested rule when omitted.
    """
    step = ScriptedDeny(DurationMicros(retry_after_us), tuple(blocking) or None)
    return step


_DEFAULT_CAPABILITIES = Capabilities(
    algorithms=frozenset(Algorithms),
    coordination=CoordinationScope.IN_PROCESS,
    durability=Durability.EPHEMERAL,
    supports_sync=True,
    supports_async=True,
    supports_composition=True,
    state_representations=frozenset(StateRepresentation),
)


class _Script:
    """The replay logic shared by the sync and async scripted backends."""

    def __init__(self, steps: Iterable[ScriptStep], clock: AdmissionClock) -> None:
        self.steps: deque[ScriptStep] = deque(steps)
        self.clock = clock
        self.requests: list[AdmissionRequest] = list()
        self.inspected: list[tuple[RuleId, ...]] = list()

    def play(
        self,
        request: AdmissionRequest,
        identity: BackendIdentity,
        observe: Callable[[ObservationPoint, AdmissionRequest], None],
    ) -> Decision:
        self.requests.append(request)
        if not self.steps:
            raise ContractViolation(
                f"the script is exhausted; admission attempt {len(self.requests)} was not expected"
            )
        else:
            pass
        step = self.steps.popleft()
        observe(ObservationPoint.BEFORE_LOCK, request)
        now = self.clock.now()
        observe(ObservationPoint.AFTER_LOAD, request)
        if isinstance(step, BaseException):
            raise step
        elif isinstance(step, ScriptedAllow):
            decision = Decision.allow(
                Admission(request.rules, request.cost, now, identity), observed_at=now
            )
        elif isinstance(step, ScriptedDeny):
            blocking = request.rules if step.blocking is None else step.blocking
            decision = Decision.deny(blocking, step.retry_after_us, observed_at=now)
        elif isinstance(step, Decision):
            decision = step
        else:
            decision = step(request)
        if decision.allowed:
            observe(ObservationPoint.BEFORE_COMMIT, request)
            observe(ObservationPoint.AFTER_COMMIT, request)
        else:
            pass
        return decision

    def snapshot(self, rules: Sequence[RuleId], identity: BackendIdentity) -> Snapshot:
        self.inspected.append(tuple(rules))
        snapshot = Snapshot(
            rules=tuple(RuleSnapshot(rule, "scripted") for rule in rules),
            sampled_at=self.clock.now(),
            backend=identity,
        )
        return snapshot


class _ScriptedCommon:
    """Configuration and inspection shared by both scripted backends."""

    _script: _Script
    _capabilities: Capabilities
    _identity: BackendIdentity

    def _configure(
        self,
        steps: Iterable[ScriptStep],
        clock: AdmissionClock,
        observer: AdmissionObserver | None,
        identity: BackendIdentity | None,
        capabilities: Capabilities | None,
    ) -> None:
        self._script = _Script(steps, clock)
        self._observer = observer
        self._identity = identity or BackendIdentity("scripted", "script", "conformance")
        self._capabilities = capabilities or _DEFAULT_CAPABILITIES

    @property
    def capabilities(self) -> Capabilities:
        """The declared capabilities; by default everything, both modes, composition."""
        return self._capabilities

    @property
    def identity(self) -> BackendIdentity:
        """The declared identity; ``scripted://script#conformance`` by default."""
        return self._identity

    @property
    def requests(self) -> tuple[AdmissionRequest, ...]:
        """Every admission request received, in order."""
        requests = tuple(self._script.requests)
        return requests

    @property
    def inspected(self) -> tuple[tuple[RuleId, ...], ...]:
        """The rules of every inspection, in order."""
        inspected = tuple(self._script.inspected)
        return inspected

    @property
    def remaining_steps(self) -> int:
        """How many scripted outcomes have not been played."""
        remaining = len(self._script.steps)
        return remaining

    def assert_exhausted(self) -> None:
        """Raise unless every scripted outcome was played.

        :raises ~procrastinators.testing.violations.ContractViolation: Steps remain.
        """
        if remaining := len(self._script.steps):
            raise ContractViolation(f"{remaining} scripted outcomes were never requested")
        else:
            pass


class ScriptedBackend(_ScriptedCommon, BaseSyncBackend):
    """A synchronous backend that replays ``steps``.

    :param steps: The outcomes to replay, in order.
    :param clock: Stamps admissions and snapshots.
    :param observer: Called at each observation point.
    :param identity: The identity to declare.
    :param capabilities: The capabilities to declare.
    """

    def __init__(
        self,
        steps: Iterable[ScriptStep],
        *,
        clock: AdmissionClock,
        observer: AdmissionObserver | None = None,
        identity: BackendIdentity | None = None,
        capabilities: Capabilities | None = None,
    ) -> None:
        self._configure(steps, clock, observer, identity, capabilities)

    def admit(self, request: AdmissionRequest) -> Decision:
        """Play the next step for ``request``.

        :param request: The request.
        :returns: The scripted decision.
        :raises ~procrastinators.testing.violations.ContractViolation: The script is exhausted.
        """
        self._ensure_open()
        self.validate_request(request)
        decision = self._script.play(request, self._identity, self._observe)
        return decision

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Record the inspection and report nothing about quota.

        :param rules: The rules to observe.
        """
        self._ensure_open()
        snapshot = self._script.snapshot(rules, self._identity)
        return snapshot

    def close(self) -> None:
        """Mark the backend closed. Idempotent."""
        self._mark_closed()


class AsyncScriptedBackend(_ScriptedCommon, BaseAsyncBackend):
    """An asynchronous backend that replays ``steps``.

    Yields to the event loop once before playing each step, so an admission can
    be cancelled before it is decided.

    :param steps: The outcomes to replay, in order.
    :param clock: Stamps admissions and snapshots.
    :param observer: Called at each observation point.
    :param identity: The identity to declare.
    :param capabilities: The capabilities to declare.
    """

    def __init__(
        self,
        steps: Iterable[ScriptStep],
        *,
        clock: AdmissionClock,
        observer: AdmissionObserver | None = None,
        identity: BackendIdentity | None = None,
        capabilities: Capabilities | None = None,
    ) -> None:
        self._configure(steps, clock, observer, identity, capabilities)

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Yield once, then play the next step for ``request``.

        :param request: The request.
        :returns: The scripted decision.
        :raises ~procrastinators.testing.violations.ContractViolation: The script is exhausted.
        """
        self._ensure_open()
        self.validate_request(request)
        await asyncio.sleep(0)
        decision = self._script.play(request, self._identity, self._observe)
        return decision

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Record the inspection and report nothing about quota.

        :param rules: The rules to observe.
        """
        self._ensure_open()
        snapshot = self._script.snapshot(rules, self._identity)
        return snapshot

    async def aclose(self) -> None:
        """Mark the backend closed. Idempotent."""
        self._mark_closed()


if __name__ == "__main__":
    pass
else:
    pass
