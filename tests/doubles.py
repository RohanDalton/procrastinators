"""Deliberately broken backends, each reproducing one mistake real libraries shipped.

Every class here is an :class:`~procrastinators.testing.EvaluatorHost` with one
flaw. The Phase 3 contract checkpoint requires the conformance suite to catch
each of them; ``test_checkpoint.py`` checks that it does, and that the
unmodified host passes.

Named for the Discworld's less reliable institutions.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from procrastinators.backends.base import BaseAsyncBackend
from procrastinators.errors import BackendUnavailable, IndeterminateAdmission
from procrastinators.models import (
    Admission,
    AdmissionRequest,
    Decision,
    DurationMicros,
    EpochMicros,
)
from procrastinators.protocols import ObservationPoint
from procrastinators.state import UNUSED, plan_admission
from procrastinators.testing import EvaluatorHost
from tests.oracles import SlidingLogOracle

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from procrastinators.models import BackendIdentity, Capabilities, RuleId, Snapshot
    from procrastinators.protocols import AdmissionClock, AdmissionObserver, Algorithm
else:
    pass


class ChargesOnExit(EvaluatorHost):
    """Decides at entry but records the admission only when the body exits.

    RazerM/ratelimiter's flaw: every concurrent entrant passes the check before
    any of them has recorded anything.
    """

    def __init__(
        self,
        algorithms: Iterable[Algorithm[Any]],
        *,
        clock: AdmissionClock,
        observer: AdmissionObserver | None = None,
    ) -> None:
        super().__init__(algorithms, clock=clock, observer=observer)
        self._owed: dict[str, AdmissionRequest] = dict()

    def admit(self, request: AdmissionRequest) -> Decision:
        self._ensure_open()
        self.validate_request(request)
        with self._lock:
            now = self._clock.now()
            states = {rule: self._states.get(rule, UNUSED) for rule in request.rules}
            plan = plan_admission(request, states, self._resolve, now)
        if plan.admitted:
            admission = Admission(request.rules, request.cost, now, self.identity)
            self._owed[admission.admission_id] = request
            decision = plan.decision(admission)
        else:
            decision = plan.decision()
        return decision

    def finish(self, admission: Admission) -> None:
        if (request := self._owed.pop(admission.admission_id, None)) is not None:
            super().admit(request)
        else:
            pass


@dataclass(frozen=True, slots=True)
class ExitAwareSubject:
    """Drives a :class:`ChargesOnExit`, telling it when each body ends."""

    backend: ChargesOnExit

    def admit(self, request: AdmissionRequest) -> Decision:
        decision = self.backend.admit(request)
        return decision

    def finish(self, admission: Admission) -> None:
        self.backend.finish(admission)

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        snapshot = self.backend.inspect(rules)
        return snapshot

    def close(self) -> None:
        self.backend.close()


class DebitsAsItGoes(EvaluatorHost):
    """Charges each composed rule in turn, stopping at the first denial.

    Increment-and-hope composition: an earlier rule is debited before a later
    one denies, and nothing is ever given back.
    """

    def admit(self, request: AdmissionRequest) -> Decision:
        admitted_at = None
        for constraint in request.constraints:
            decision = super().admit(AdmissionRequest((constraint,), request.cost, request.budget))
            if not decision.allowed:
                break
            else:
                assert decision.admission is not None
                admitted_at = decision.admission.admitted_at
        else:
            assert admitted_at is not None
            decision = Decision.allow(
                Admission(request.rules, request.cost, admitted_at, self.identity)
            )
        return decision


class ClaimsSuccessAfterUnknownCommit(EvaluatorHost):
    """Treats a lost response after the commit as success.

    The caller's body then runs on permission nobody can vouch for.
    """

    def admit(self, request: AdmissionRequest) -> Decision:
        try:
            decision = super().admit(request)
        except IndeterminateAdmission:
            now = self._clock.now()
            decision = Decision.allow(Admission(request.rules, request.cost, now, self.identity))
        return decision


class RefundsUnknownCommit(EvaluatorHost):
    """Rolls back and reports a denial when the commit's outcome is unknown.

    A refund of a commit that did happen hands out capacity the policy never
    granted.
    """

    def admit(self, request: AdmissionRequest) -> Decision:
        before = copy.copy(self._states)
        try:
            decision = super().admit(request)
        except IndeterminateAdmission:
            self._states = before
            decision = Decision.deny(request.rules, DurationMicros(0))
        return decision


class SamplesTimeBeforeLock(EvaluatorHost):
    """Reads the clock before queueing for the lock, then commits that stale time."""

    def admit(self, request: AdmissionRequest) -> Decision:
        self._ensure_open()
        self.validate_request(request)
        now = self._clock.now()
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        with self._lock:
            decision = self._admit_locked(request, now)
        return decision


class TranslatesCancellation(EvaluatorHost):
    """Reports a cancelled caller as an unavailable backend."""

    def admit(self, request: AdmissionRequest) -> Decision:
        try:
            decision = super().admit(request)
        except asyncio.CancelledError as error:
            raise BackendUnavailable("the caller went away", cause=error) from error
        return decision


class IgnoresPolicyConflicts(EvaluatorHost):
    """Never compares a rule's stored fingerprint with the request's."""

    def _check_policies(self, request: AdmissionRequest) -> None:
        del request


class ReopensAfterClose(EvaluatorHost):
    """Closing does nothing, so a closed handle keeps coordinating with nobody in particular."""

    def close(self) -> None:
        pass


class ForgetsIdleState(EvaluatorHost):
    """Evicts any rule untouched for a second, however much of its quota is in use.

    A drained token bucket then comes back full: exactly the reset contract P5
    forbids.
    """

    _IDLE_US: ClassVar[int] = 1_000_000

    def __init__(
        self,
        algorithms: Iterable[Algorithm[Any]],
        *,
        clock: AdmissionClock,
        observer: AdmissionObserver | None = None,
    ) -> None:
        super().__init__(algorithms, clock=clock, observer=observer)
        self._touched: dict[RuleId, EpochMicros] = dict()

    def _admit_locked(self, request: AdmissionRequest, now: EpochMicros) -> Decision:
        for rule in request.rules:
            if now - self._touched.get(rule, now) > self._IDLE_US:
                self._states.pop(rule, None)
            else:
                pass
            self._touched[rule] = now
        decision = super()._admit_locked(request, now)
        return decision


class AsyncEvaluatorHost(BaseAsyncBackend):
    """An :class:`~procrastinators.testing.EvaluatorHost` behind the async protocol.

    Yields to the event loop before each call. Adequate here because the host
    never blocks; a real async backend needs native I/O or an executor.
    """

    def __init__(self, host: EvaluatorHost) -> None:
        self._host = host

    @property
    def capabilities(self) -> Capabilities:
        base = self._host.capabilities
        capabilities = type(base)(
            algorithms=base.algorithms,
            coordination=base.coordination,
            durability=base.durability,
            supports_sync=False,
            supports_async=True,
            supports_composition=base.supports_composition,
            state_representations=base.state_representations,
        )
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        return self._host.identity

    async def admit(self, request: AdmissionRequest) -> Decision:
        self._ensure_open()
        await asyncio.sleep(0)
        decision = self._host.admit(request)
        return decision

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        self._ensure_open()
        snapshot = self._host.inspect(rules)
        return snapshot

    async def aclose(self) -> None:
        if self._mark_closed():
            self._host.close()
        else:
            pass


@dataclass(frozen=True, slots=True)
class ClacksPolicy:
    """A third-party policy: at most ``amount`` semaphore messages per ``period_us``."""

    amount: int
    period_us: DurationMicros

    algorithm: ClassVar[str] = "discworld.clacks"
    state_version: ClassVar[int] = 1

    @property
    def capacity(self) -> int:
        return self.amount


class ClacksAlgorithm(SlidingLogOracle):
    """A third-party algorithm; the sliding-log oracle under its own id and policy."""

    id = "discworld.clacks"
    policy_type = ClacksPolicy


if __name__ == "__main__":
    pass
else:
    pass
