"""Capability validation and backend identity comparison, without a connection.

Everything here compares declarations: a backend's
:class:`~procrastinators.models.Capabilities` against what a limiter needs, and
one :class:`~procrastinators.models.BackendIdentity` against another. None of
it opens a connection, because an unsupported combination must fail when the
limiter is built (contract Y2), not at the first acquisition, and certainly
not only when the network happens to be up.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from procrastinators.errors import InvalidPolicy, UnsupportedCapability
from procrastinators.models import CoordinationScope, Durability

if TYPE_CHECKING:
    from collections.abc import Iterable

    from procrastinators.models import AdmissionRequest, BackendIdentity, Capabilities
else:
    pass

__all__ = [
    "CapabilityRequirement",
    "Mode",
    "require_capabilities",
    "require_same_authority",
    "unmet_requirements",
]


class Mode(StrEnum):
    """Which interface a caller will use."""

    SYNC = "sync"
    """Blocking calls through :class:`~procrastinators.protocols.SyncBackend`."""

    ASYNC = "async"
    """Awaitable calls through :class:`~procrastinators.protocols.AsyncBackend`."""


_COORDINATION_REACH: Final[dict[CoordinationScope, int]] = {
    CoordinationScope.IN_PROCESS: 0,
    CoordinationScope.LOCAL_MACHINE: 1,
    CoordinationScope.SHARED_SERVICE: 2,
}


@dataclass(frozen=True, slots=True)
class CapabilityRequirement:
    """What a limiter needs from its backend, stated before any backend is used.

    Every field defaults to the weakest requirement, so a requirement names
    only what matters to the caller.

    :raises ~procrastinators.errors.InvalidPolicy: ``rules`` is not a positive integer.
    """

    mode: Mode = Mode.SYNC
    """Which interface the caller will use."""

    algorithms: frozenset[str] = frozenset()
    """Algorithm ids every request will evaluate."""

    representations: frozenset[str] = frozenset()
    """State shapes those algorithms need (contract Y6)."""

    rules: int = 1
    """Most constraints one request composes; more than one needs atomic composition."""

    coordination_domain: str | None = None
    """The coordination domain the constraints declare, if any."""

    cooldowns: bool = False
    """Whether shared cooldowns will be applied."""

    policy_administration: bool = False
    """Whether explicit policy migrations will be run."""

    coordination: CoordinationScope = CoordinationScope.IN_PROCESS
    """The least reach the caller will accept (contract Y3)."""

    accept_best_effort: bool = False
    """Whether state that may vanish through eviction is acceptable.

    Never implied: a cache miss is not proof that no quota was consumed, so the
    weaker guarantee must be accepted explicitly (contract Y4).
    """

    def __post_init__(self) -> None:
        if isinstance(self.rules, bool) or not isinstance(self.rules, int) or self.rules < 1:
            raise InvalidPolicy(f"a requirement composes at least one rule, got {self.rules!r}")
        else:
            pass

    @classmethod
    def for_request(cls, request: AdmissionRequest, *, mode: Mode) -> CapabilityRequirement:
        """The requirement one admission request imposes on the backend serving it.

        :param request: The request about to be served.
        :param mode: Which interface is serving it.
        :returns: A requirement naming the request's algorithms, rule count, and
            coordination domain.
        """
        requirement = cls(
            mode=mode,
            algorithms=frozenset(constraint.algorithm for constraint in request.constraints),
            rules=len(request.constraints),
            coordination_domain=request.coordination_domain,
        )
        return requirement


def unmet_requirements(
    capabilities: Capabilities, requirement: CapabilityRequirement
) -> tuple[str, ...]:
    """Every way ``capabilities`` falls short of ``requirement``, in a stable order.

    Reports all shortfalls rather than the first, so an operator fixes a
    configuration in one pass.

    :param capabilities: What the backend declares.
    :param requirement: What the caller needs.
    :returns: Human-readable reasons; empty when every requirement is met.
    """
    reasons = list()
    if requirement.mode is Mode.SYNC and not capabilities.supports_sync:
        reasons.append("it offers no synchronous interface")
    elif requirement.mode is Mode.ASYNC and not capabilities.supports_async:
        reasons.append(
            "it offers no asynchronous interface; wrapping blocking calls would "
            "block the event loop, so use an explicit executor adapter"
        )
    else:
        pass
    if missing := requirement.algorithms - capabilities.algorithms:
        reasons.append(
            f"it does not implement {sorted(missing)}; "
            f"it supports {sorted(capabilities.algorithms)}"
        )
    else:
        pass
    if missing := requirement.representations - capabilities.state_representations:
        reasons.append(
            f"it cannot hold {sorted(missing)} state; it holds "
            f"{sorted(capabilities.state_representations)}"
        )
    else:
        pass
    if requirement.rules > 1 and not capabilities.supports_composition:
        reasons.append(
            f"it cannot admit {requirement.rules} rules atomically; compose only on a backend "
            "that supports it, never by charging each in turn"
        )
    elif (limit := capabilities.max_composed_rules) is not None and requirement.rules > limit:
        reasons.append(f"it composes at most {limit} rules, got {requirement.rules}")
    else:
        pass
    if capabilities.requires_shared_coordination_domain and requirement.coordination_domain is None:
        reasons.append(
            "it requires every composed constraint to declare a coordination domain so they "
            "share one partition"
        )
    else:
        pass
    if requirement.cooldowns and not capabilities.supports_cooldowns:
        reasons.append("it does not support shared cooldowns")
    else:
        pass
    if requirement.policy_administration and not capabilities.supports_policy_administration:
        reasons.append("it does not support policy administration")
    else:
        pass
    if (
        _COORDINATION_REACH[capabilities.coordination]
        < _COORDINATION_REACH[requirement.coordination]
    ):
        reasons.append(
            f"its coordination reaches only {capabilities.coordination.value}, "
            f"not {requirement.coordination.value}"
        )
    else:
        pass
    if capabilities.durability is Durability.BEST_EFFORT and not requirement.accept_best_effort:
        reasons.append(
            "its state is best-effort and may vanish through eviction; accept that "
            "explicitly if it is acceptable"
        )
    else:
        pass
    unmet = tuple(reasons)
    return unmet


def require_capabilities(
    capabilities: Capabilities,
    requirement: CapabilityRequirement,
    *,
    backend: BackendIdentity | None = None,
) -> None:
    """Raise unless ``capabilities`` meet ``requirement``.

    :param capabilities: What the backend declares.
    :param requirement: What the caller needs.
    :param backend: The backend's identity, named in the error when given.
    :raises ~procrastinators.errors.UnsupportedCapability: Listing every unmet requirement.
    """
    if reasons := unmet_requirements(capabilities, requirement):
        subject = str(backend) if backend is not None else "the backend"
        raise UnsupportedCapability(f"{subject} cannot serve this limiter: " + "; ".join(reasons))
    else:
        pass


def require_same_authority(identities: Iterable[BackendIdentity]) -> BackendIdentity:
    """Check that every identity addresses one store, and return the first.

    Composition across authorities cannot be atomic and is not emulated with
    increment-and-refund (contract C6). Namespaces may differ: each constraint
    carries its own namespace in its quota identity, and one store can commit
    constraints from several namespaces together.

    :param identities: The identities of every backend taking part.
    :returns: The first identity.
    :raises ~procrastinators.errors.UnsupportedCapability: The identities are empty, or name
        more than one family or authority.
    """
    ordered = tuple(identities)
    if not ordered:
        raise UnsupportedCapability("composition needs at least one backend")
    else:
        pass
    first = ordered[0]
    if strangers := [
        identity for identity in ordered if not first.addresses_same_authority(identity)
    ]:
        shown = sorted({str(first), *(str(identity) for identity in strangers)})
        raise UnsupportedCapability(
            "separate backends cannot admit atomically together; every composed limiter "
            f"must address one store, got {shown}"
        )
    else:
        pass
    return first


if __name__ == "__main__":
    pass
else:
    pass
