"""Explicit registration of algorithms, backend families, and native executors.

Nothing is discovered. A registry holds exactly what was registered with it,
by value, and resolves names only against that. Configuration text names an
algorithm id or a backend family; it never names a module to import or a
callable to run, because a rate-limit configuration file must not be a way to
execute arbitrary code.

Registrations are immutable once made. Registering a second implementation
under an existing name is an error rather than a replacement: two workers that
silently disagreed about what ``"sliding_log"`` means would be the worst kind of
policy conflict, one no fingerprint could detect.

Creating a :class:`Registry` has no side effects; it is safe to share between
threads.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import threading
from typing import TYPE_CHECKING, Any

from procrastinators.capabilities import CapabilityRequirement, Mode, require_capabilities
from procrastinators.errors import ConfigurationError, UnsupportedCapability
from procrastinators.models import canonical_constraints

if TYPE_CHECKING:
    from collections.abc import Sequence

    from procrastinators.models import BackendIdentity, Capabilities, Constraint
    from procrastinators.protocols import (
        Algorithm,
        AlgorithmSpec,
        BackendSpec,
        NativeExecutorSpec,
    )
else:
    pass

__all__ = ["Registry"]


class Registry:
    """Algorithms, backend families, and native executors, registered explicitly."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._algorithm_specs: dict[str, AlgorithmSpec] = dict()
        self._algorithms: dict[str, Algorithm[Any]] = dict()
        self._backend_specs: dict[str, BackendSpec] = dict()
        self._executors: dict[tuple[str, str, int, int], NativeExecutorSpec] = dict()

    @property
    def algorithm_ids(self) -> frozenset[str]:
        """Every registered algorithm id."""
        with self._lock:
            ids = frozenset(self._algorithm_specs)
        return ids

    @property
    def backend_families(self) -> frozenset[str]:
        """Every registered backend family."""
        with self._lock:
            families = frozenset(self._backend_specs)
        return families

    def register_algorithm(self, spec: AlgorithmSpec) -> None:
        """Register one algorithm implementation under its stable id.

        :param spec: The registration.
        :raises ~procrastinators.errors.ConfigurationError: The id is already registered.
        """
        with self._lock:
            if spec.id in self._algorithm_specs:
                raise ConfigurationError(f"algorithm '{spec.id}' is already registered")
            else:
                self._algorithm_specs[spec.id] = spec

    def register_backend(self, spec: BackendSpec) -> None:
        """Register one backend family.

        :param spec: The registration.
        :raises ~procrastinators.errors.ConfigurationError: The family is already registered.
        """
        with self._lock:
            if spec.family in self._backend_specs:
                raise ConfigurationError(f"backend family '{spec.family}' is already registered")
            else:
                self._backend_specs[spec.family] = spec

    def register_native_executor(self, spec: NativeExecutorSpec) -> None:
        """Register an in-store implementation of one algorithm for one backend family.

        The family and the algorithm must already be registered, the family
        must declare the algorithm among its native executors, and the state
        versions must match exactly: an executor written against another state
        version would read someone else's bytes.

        :param spec: The registration.
        :raises ~procrastinators.errors.ConfigurationError: An executor for the same family,
            algorithm, state version, and policy version is already registered.
        :raises ~procrastinators.errors.UnsupportedCapability: The family or algorithm is not
            registered, the family does not declare this native executor, or the state versions
            differ.
        """
        key = (spec.backend_family, spec.algorithm_id, spec.state_version, spec.policy_version)
        with self._lock:
            backend = self._backend_specs.get(spec.backend_family)
            algorithm = self._algorithm_specs.get(spec.algorithm_id)
            if backend is None:
                raise UnsupportedCapability(
                    f"backend family '{spec.backend_family}' is not registered"
                )
            elif algorithm is None:
                raise UnsupportedCapability(f"algorithm '{spec.algorithm_id}' is not registered")
            elif spec.algorithm_id not in backend.capabilities.native_executors:
                raise UnsupportedCapability(
                    f"backend family '{spec.backend_family}' does not declare a native executor "
                    f"for '{spec.algorithm_id}'"
                )
            elif spec.state_version != algorithm.state_version:
                raise UnsupportedCapability(
                    f"executor for '{spec.algorithm_id}' targets state version "
                    f"{spec.state_version}, but the algorithm uses {algorithm.state_version}"
                )
            elif key in self._executors:
                raise ConfigurationError(
                    f"a native executor for {spec.backend_family}/{spec.algorithm_id} "
                    f"(state {spec.state_version}, policy {spec.policy_version}) is already "
                    "registered"
                )
            else:
                self._executors[key] = spec

    def algorithm_spec(self, algorithm_id: str) -> AlgorithmSpec:
        """Return the registration for ``algorithm_id``.

        :param algorithm_id: A registered algorithm id.
        :raises ~procrastinators.errors.UnsupportedCapability: Nothing is registered under it.
        """
        with self._lock:
            spec = self._algorithm_specs.get(algorithm_id)
        if spec is None:
            raise UnsupportedCapability(
                f"algorithm '{algorithm_id}' is not registered; register it explicitly, "
                "configuration cannot import one"
            )
        else:
            pass
        return spec

    def algorithm(self, algorithm_id: str) -> Algorithm[Any]:
        """Return the implementation of ``algorithm_id``, built once and shared.

        Algorithms are stateless (a protocol requirement), so one instance
        serves every caller.

        :param algorithm_id: A registered algorithm id.
        :raises ~procrastinators.errors.UnsupportedCapability: Nothing is registered under it.
        :raises ~procrastinators.errors.ConfigurationError: The factory built an algorithm whose id
            or state version differs from its registration.
        """
        spec = self.algorithm_spec(algorithm_id)
        with self._lock:
            if (built := self._algorithms.get(algorithm_id)) is None:
                built = spec.factory()
                if built.id != spec.id or built.state_version != spec.state_version:
                    raise ConfigurationError(
                        f"algorithm registered as '{spec.id}' (state {spec.state_version}) built "
                        f"'{built.id}' (state {built.state_version})"
                    )
                else:
                    self._algorithms[algorithm_id] = built
            else:
                pass
        return built

    def backend_spec(self, family: str) -> BackendSpec:
        """Return the registration for backend ``family``.

        :param family: A registered backend family.
        :raises ~procrastinators.errors.UnsupportedCapability: Nothing is registered under it.
        """
        with self._lock:
            spec = self._backend_specs.get(family)
        if spec is None:
            raise UnsupportedCapability(f"backend family '{family}' is not registered")
        else:
            pass
        return spec

    def native_executor(
        self, family: str, constraint: Constraint, *, cost: int | None = None
    ) -> NativeExecutorSpec | None:
        """The native executor that may serve ``constraint`` on ``family``, if any.

        ``None`` means the backend must use the reference evaluator, or refuse
        at construction if it has none: a policy outside an executor's verified
        range is never run on it (contract Y5).

        :param family: The backend family.
        :param constraint: The constraint to be served.
        :param cost: The request's cost, checked against the executor's bound when given.
        :returns: The matching executor, or ``None``.
        """
        with self._lock:
            candidates = [
                spec
                for (spec_family, algorithm_id, _, _), spec in self._executors.items()
                if spec_family == family and algorithm_id == constraint.algorithm
            ]
        for spec in candidates:
            if spec.accepts(constraint) and (cost is None or cost <= spec.max_cost):
                executor: NativeExecutorSpec | None = spec
                break
            else:
                pass
        else:
            executor = None
        return executor

    def requirement_for(
        self,
        constraints: Sequence[Constraint],
        *,
        mode: Mode,
        cooldowns: bool = False,
        accept_best_effort: bool = False,
    ) -> CapabilityRequirement:
        """The capability requirement a set of constraints imposes.

        State representations come from the algorithms' registrations, so an
        unregistered algorithm fails here, before any backend is consulted.

        :param constraints: Every constraint a limiter will admit together.
        :param mode: Which interface the limiter will use.
        :param cooldowns: Whether cooldowns will be applied.
        :param accept_best_effort: Whether best-effort durability is acceptable.
        :raises ~procrastinators.errors.UnsupportedCapability: An algorithm is not registered.
        :raises ~procrastinators.errors.InvalidPolicy: ``constraints`` is empty.
        :raises ~procrastinators.errors.PolicyConflict: The constraints conflict or declare
            different coordination domains.
        """
        canonical = canonical_constraints(constraints)
        algorithms = frozenset(constraint.algorithm for constraint in canonical)
        representations = frozenset(
            self.algorithm_spec(algorithm_id).representation for algorithm_id in algorithms
        )
        requirement = CapabilityRequirement(
            mode=mode,
            algorithms=algorithms,
            representations=representations,
            rules=len(canonical),
            coordination_domain=canonical[0].coordination_domain,
            cooldowns=cooldowns,
            accept_best_effort=accept_best_effort,
        )
        return requirement

    def validate(
        self,
        capabilities: Capabilities,
        constraints: Sequence[Constraint],
        *,
        mode: Mode,
        backend: BackendIdentity | None = None,
        cooldowns: bool = False,
        accept_best_effort: bool = False,
    ) -> None:
        """Check, without any connection, that a backend can serve these constraints.

        :param capabilities: What the backend declares.
        :param constraints: Every constraint the limiter will admit together.
        :param mode: Which interface the limiter will use.
        :param backend: The backend's identity, named in the error when given.
        :param cooldowns: Whether cooldowns will be applied.
        :param accept_best_effort: Whether best-effort durability is acceptable.
        :raises ~procrastinators.errors.UnsupportedCapability: Listing every unmet requirement.
        """
        requirement = self.requirement_for(
            constraints, mode=mode, cooldowns=cooldowns, accept_best_effort=accept_best_effort
        )
        require_capabilities(capabilities, requirement, backend=backend)


if __name__ == "__main__":
    pass
else:
    pass
