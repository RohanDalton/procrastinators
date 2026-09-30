"""A lock-protected, in-process host for evaluators, for running traces.

:class:`EvaluatorHost` runs any set of
:class:`~procrastinators.protocols.Algorithm` implementations behind the full
admission sequence of contract A1: check policy metadata, sample authority
time after taking its lock, load state, evaluate every rule against that one
sample, and commit every debit only if every rule admitted — calling its
observer at each observation point on the way.

It exists so that algorithms can be checked against the shared traces before
any production backend exists, and so the conformance tools have a correct
reference to compare faulty doubles against. It is test support: not exported
from the package root, not registered as a backend family, ephemeral, and it
makes no claim about forks or processes.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import threading
from typing import TYPE_CHECKING, Any

from procrastinators.backends.base import BaseSyncBackend
from procrastinators.errors import ConfigurationError, PolicyConflict
from procrastinators.models import (
    Admission,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Durability,
    RuleSnapshot,
    Snapshot,
)
from procrastinators.protocols import ObservationPoint, StateRepresentation
from procrastinators.state import UNUSED, plan_admission

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from procrastinators.models import (
        AdmissionRequest,
        Constraint,
        Decision,
        EpochMicros,
        PolicyFingerprint,
        RuleId,
    )
    from procrastinators.protocols import (
        AdmissionClock,
        AdmissionObserver,
        Algorithm,
        RuleState,
    )
else:
    pass

__all__ = ["EvaluatorHost"]


class EvaluatorHost(BaseSyncBackend):
    """Runs evaluators atomically under one lock, in memory.

    :param algorithms: The algorithms to host, each under its own id.
    :param clock: Authority epoch time, sampled after the lock is taken.
    :param observer: Called at each observation point.
    :param identity: The identity to declare; ``evaluator-host://memory#conformance``
        by default.
    :param representations: Extra state representations to accept, for
        third-party algorithms; scalars and event logs are always accepted.
    :raises ~procrastinators.errors.ConfigurationError: No algorithms, or two share an id.
    """

    def __init__(
        self,
        algorithms: Iterable[Algorithm[Any]],
        *,
        clock: AdmissionClock,
        observer: AdmissionObserver | None = None,
        identity: BackendIdentity | None = None,
        representations: Iterable[str] = tuple(),
    ) -> None:
        self._algorithms: dict[str, Algorithm[Any]] = dict()
        for algorithm in algorithms:
            if algorithm.id in self._algorithms:
                raise ConfigurationError(f"two hosted algorithms share the id {algorithm.id!r}")
            else:
                self._algorithms[algorithm.id] = algorithm
        if not self._algorithms:
            raise ConfigurationError("an evaluator host needs at least one algorithm")
        else:
            pass
        self._clock = clock
        self._observer = observer
        self._identity = identity or BackendIdentity("evaluator-host", "memory", "conformance")
        self._capabilities = Capabilities(
            algorithms=frozenset(self._algorithms),
            coordination=CoordinationScope.IN_PROCESS,
            durability=Durability.EPHEMERAL,
            supports_sync=True,
            supports_composition=True,
            state_representations=frozenset({*StateRepresentation, *representations}),
        )
        self._lock = threading.Lock()
        self._states: dict[RuleId, RuleState] = dict()
        self._fingerprints: dict[RuleId, PolicyFingerprint] = dict()
        self._algorithm_of: dict[RuleId, str] = dict()

    @property
    def capabilities(self) -> Capabilities:
        """The hosted algorithms, in-process and ephemeral, with composition."""
        return self._capabilities

    @property
    def identity(self) -> BackendIdentity:
        """The declared identity."""
        return self._identity

    @property
    def lock_held(self) -> bool:
        """Whether the admission lock is held right now; for sleeper guards (W1)."""
        held = self._lock.locked()
        return held

    def state_of(self, rule: RuleId) -> RuleState:
        """The stored state of ``rule``; the unused state if there is none.

        :param rule: The rule to look up.
        """
        with self._lock:
            state = self._states.get(rule, UNUSED)
        return state

    def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule's stored fingerprint differs.
        :raises ~procrastinators.errors.UnsupportedCapability: An algorithm is not hosted.
        :raises ~procrastinators.errors.ClosedResource: The host was closed.
        :raises ~procrastinators.errors.BackendUnavailable: The observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The observer failed after it.
        """
        self._ensure_open()
        self.validate_request(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        with self._lock:
            decision = self._admit_locked(request, self._clock.now())
        return decision

    def _admit_locked(self, request: AdmissionRequest, now: EpochMicros) -> Decision:
        """The critical section: everything after the lock is taken and time sampled."""
        self._check_policies(request)
        states = {rule: self._states.get(rule, UNUSED) for rule in request.rules}
        self._observe(ObservationPoint.AFTER_LOAD, request)
        plan = plan_admission(request, states, self._resolve, now)
        self._remember_policies(request)
        if plan.admitted:
            self._observe(ObservationPoint.BEFORE_COMMIT, request)
            self._states.update(plan.writes)
            admission = Admission(request.rules, request.cost, now, self._identity)
            self._observe(ObservationPoint.AFTER_COMMIT, request)
            decision = plan.decision(admission)
        else:
            self._states.update(plan.writes)
            decision = plan.decision()
        return decision

    def _check_policies(self, request: AdmissionRequest) -> None:
        for constraint in request.constraints:
            stored = self._fingerprints.get(constraint.rule)
            if stored is not None and stored != constraint.fingerprint:
                raise PolicyConflict(
                    f"{constraint.rule} is stored under another policy",
                    rule=constraint.rule,
                    expected=constraint.fingerprint,
                    found=stored,
                )
            else:
                pass

    def _remember_policies(self, request: AdmissionRequest) -> None:
        # Recorded on first contact, admitted or not: policy metadata outlives
        # quota state, so a later disagreement is always detectable (L10).
        for constraint in request.constraints:
            self._fingerprints.setdefault(constraint.rule, constraint.fingerprint)
            self._algorithm_of.setdefault(constraint.rule, constraint.algorithm)

    def _resolve(self, constraint: Constraint) -> Algorithm[Any]:
        algorithm = self._algorithms[constraint.algorithm]
        return algorithm

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation: which algorithm each rule uses, and nothing more.

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: The host was closed.
        """
        self._ensure_open()
        with self._lock:
            now = self._clock.now()
            snapshots = tuple(
                RuleSnapshot(rule, self._algorithm_of.get(rule, "unused")) for rule in rules
            )
        snapshot = Snapshot(rules=snapshots, sampled_at=now, backend=self._identity)
        return snapshot

    def close(self) -> None:
        """Mark the host closed. Idempotent; the state is kept but unreachable."""
        self._mark_closed()


if __name__ == "__main__":
    pass
else:
    pass
