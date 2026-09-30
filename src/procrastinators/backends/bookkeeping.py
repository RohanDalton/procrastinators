"""Backend-neutral decisions every storage authority makes the same way.

Policy checks, cooldown holds and extensions, advisory snapshots, and the
steps of a draining policy migration are decisions about values a backend has
already loaded inside its critical section. Keeping them here, as pure
functions, means the memory store and the SQLite file cannot drift apart on
what a conflict is or when a migration may complete; each backend supplies
only the loading, locking, and writing around them.

Nothing here locks, reads a clock, or performs I/O.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import dataclasses
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final, TypeAlias

from procrastinators.errors import InvalidPolicy, PolicyConflict
from procrastinators.models import (
    MAX_DURATION_US,
    MAX_TIMESTAMP_US,
    AdmissionRequest,
    Cooldown,
    DurationMicros,
    EpochMicros,
    RuleSnapshot,
)
from procrastinators.protocols import MigrationStatus, PolicyMigration
from procrastinators.state import plan_admission

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from procrastinators.models import (
        Constraint,
        PolicyFingerprint,
        QuotaIdentity,
        RuleId,
    )
    from procrastinators.protocols import Algorithm, RuleState, StoredPolicy
else:
    pass

__all__ = [
    "DRAINING",
    "LoadedRule",
    "PendingPolicy",
    "begin_migration",
    "check_policy",
    "complete_migration",
    "cooldown_holds",
    "extend_cooldown",
    "refresh_migration",
    "rule_snapshot",
]

DRAINING: Final = frozenset({MigrationStatus.DRAINING, MigrationStatus.READY})
"""Migration states in which a rule refuses admissions (L12)."""

PendingPolicy: TypeAlias = "tuple[PolicyFingerprint, int]"
"""The fingerprint and state version a completed migration installs, before first use."""


@dataclass(slots=True)
class LoadedRule:
    """One rule as a backend read it inside its critical section.

    What the policy checks and migration steps below decide on. A SQL backend
    also keeps the row ids it read, so it can write back only what changed.
    """

    rule_id: int | None
    stored: StoredPolicy | None
    migration: PolicyMigration | None
    target_version: int | None
    state: RuleState
    horizon: EpochMicros | None
    event_rows: dict[tuple[int, int], list[int]] = field(default_factory=dict)

    @property
    def pending(self) -> PendingPolicy | None:
        """What a completed migration installs, while no metadata is stored."""
        migration = self.migration
        if (
            self.stored is None
            and migration is not None
            and migration.status is MigrationStatus.COMPLETE
            and migration.to_fingerprint is not None
            and self.target_version is not None
        ):
            pending: PendingPolicy | None = (
                migration.to_fingerprint,
                self.target_version,
            )
        else:
            pending = None
        return pending

    @property
    def current(self) -> PolicyFingerprint | None:
        """The fingerprint the rule runs under now, stored or pending."""
        if self.stored is not None:
            current: PolicyFingerprint | None = self.stored.fingerprint
        elif (pending := self.pending) is not None:
            current = pending[0]
        else:
            current = None
        return current

    def neutral(self, now: EpochMicros) -> bool:
        """Whether the rule's state is gone or past its safe-forget horizon."""
        neutral = not self.state.exists or (self.horizon is not None and self.horizon <= now)
        return neutral


def check_policy(
    constraint: Constraint,
    *,
    stored: StoredPolicy | None,
    migration: PolicyMigration | None,
    pending: PendingPolicy | None,
    resolve: Callable[[Constraint], Algorithm[Any]],
) -> None:
    """Refuse ``constraint`` unless it agrees with what the authority holds for its rule.

    A rule being migrated refuses everything; a stored fingerprint must match
    (I4); a migration completed but not yet used admits only its target
    policy. A rule the authority has never seen has its policy validated by the
    algorithm the authority hosts, since that is where it will be enforced.

    :param constraint: The constraint about to be admitted.
    :param stored: The policy metadata held for the rule, if any.
    :param migration: The rule's migration record, if any.
    :param pending: What a completed migration installs, while no metadata is stored.
    :param resolve: The authority's algorithm for a constraint.
    :raises ~procrastinators.errors.PolicyConflict: The rule is migrating, or held under
        another policy.
    :raises ~procrastinators.errors.InvalidPolicy: A first-contact policy is invalid.
    :raises ~procrastinators.errors.UnsupportedCapability: The algorithm is not hosted.
    """
    rule = constraint.rule
    if migration is not None and migration.status in DRAINING:
        raise PolicyConflict(
            f"{rule} is being migrated; admissions stay stopped until the migration "
            "completes (L12)",
            rule=rule,
            expected=constraint.fingerprint,
            found=migration.from_fingerprint,
        )
    elif stored is not None:
        if stored.fingerprint != constraint.fingerprint:
            raise PolicyConflict(
                f"{rule} is stored under another policy; change it with an explicit "
                "migration, never by starting with a different configuration (I4)",
                rule=rule,
                expected=constraint.fingerprint,
                found=stored.fingerprint,
            )
        else:
            pass
    elif pending is not None:
        if pending != (constraint.fingerprint, constraint.state_version):
            raise PolicyConflict(
                f"{rule} was migrated to another policy",
                rule=rule,
                expected=constraint.fingerprint,
                found=pending[0],
            )
        else:
            pass
    else:
        resolve(constraint).validate(constraint.policy)


def cooldown_holds(
    request: AdmissionRequest,
    cooldowns: Mapping[QuotaIdentity, EpochMicros],
    now: EpochMicros,
) -> dict[RuleId, DurationMicros]:
    """How long each of ``request``'s rules must still wait for a cooldown on its scope (K3).

    :param request: The request being admitted.
    :param cooldowns: When each cooled scope's pause ends; expired entries are ignored.
    :param now: Authority epoch time of the attempt.
    :returns: The remaining hold of every rule whose scope is paused.
    """
    holds = {
        rule: DurationMicros(until - now)
        for rule in request.rules
        if (until := cooldowns.get(rule.scope)) is not None and until > now
    }
    return holds


def extend_cooldown(
    existing: Cooldown | None,
    scope: QuotaIdentity,
    duration: DurationMicros,
    reason: str,
    now: EpochMicros,
) -> Cooldown:
    """The cooldown in force after pausing ``scope`` for at least ``duration`` (K2).

    :param existing: The cooldown already recorded for ``scope``, if any.
    :param scope: The quota to pause.
    :param duration: The least length of the pause, in microseconds.
    :param reason: Recorded on a cooldown this call creates or lengthens.
    :param now: Authority epoch time.
    :returns: ``existing`` when it already lasts as long, otherwise a longer cooldown to record.
    :raises ~procrastinators.errors.InvalidPolicy: ``duration`` is not an integer between 0
        and :data:`~procrastinators.models.MAX_DURATION_US`, or would end beyond the
        supported timestamp range.
    """
    if isinstance(duration, bool) or not isinstance(duration, int):
        raise InvalidPolicy(f"cooldown duration must be an integer, got {duration!r}")
    elif not 0 <= duration <= MAX_DURATION_US:
        raise InvalidPolicy(f"cooldown duration must be between 0 and {MAX_DURATION_US} µs")
    elif (until := now + duration) > MAX_TIMESTAMP_US:
        raise InvalidPolicy(f"a cooldown of {duration} µs ends beyond the supported range")
    elif existing is not None and existing.until >= until:
        cooldown = existing
    else:
        cooldown = Cooldown(scope, EpochMicros(until), reason)
    return cooldown


def rule_snapshot(
    rule: RuleId,
    *,
    constraint: Constraint | None,
    algorithm: str | None,
    state: RuleState,
    horizon: EpochMicros | None,
    cooldown_until: EpochMicros | None,
    resolve: Callable[[Constraint], Algorithm[Any]],
    now: EpochMicros,
) -> RuleSnapshot:
    """Advisory observation of one rule, committing nothing (R7).

    With the rule's constraint at hand, it is evaluated for a cost of one and
    the remainder reported; with only the stored algorithm id, the snapshot
    says what it can and leaves ``remaining`` unknown. A rule the authority has
    never seen reports ``unused``.

    :param rule: The rule to observe.
    :param constraint: The rule's constraint, when this process knows it.
    :param algorithm: The stored algorithm id, or ``None`` when no metadata is held.
    :param state: The rule's loaded state.
    :param horizon: When the rule's state stops mattering, or ``None`` for never.
    :param cooldown_until: When a cooldown on the rule's scope ends, if one is in force.
    :param resolve: The authority's algorithm for a constraint.
    :param now: Authority epoch time.
    """
    until = cooldown_until if cooldown_until is not None and cooldown_until > now else None
    reset_after = DurationMicros(max(0, horizon - now)) if horizon is not None else None
    if constraint is not None:
        probe = AdmissionRequest((constraint,))
        (transition,) = plan_admission(probe, {rule: state}, resolve, now).transitions
        remaining = transition.remaining
        if transition.admitted and remaining is not None:
            remaining += probe.cost
        else:
            pass
        snapshot = RuleSnapshot(
            rule,
            constraint.algorithm,
            remaining=remaining,
            reset_after_us=reset_after,
            cooldown_until=until,
        )
    elif algorithm is not None:
        snapshot = RuleSnapshot(rule, algorithm, reset_after_us=reset_after, cooldown_until=until)
    else:
        snapshot = RuleSnapshot(rule, "unused", cooldown_until=until)
    return snapshot


def begin_migration(
    rule: RuleId,
    existing: PolicyMigration | None,
    *,
    current: PolicyFingerprint | None,
    to_fingerprint: PolicyFingerprint,
) -> PolicyMigration:
    """The migration record after asking to drain ``rule`` towards ``to_fingerprint``.

    No live conversion between policies is supported, so every migration
    drains: the rule refuses admissions until its state is neutral. Asking
    again for the same target is harmless.

    :param rule: The rule to migrate.
    :param existing: Its migration record, if any.
    :param current: The fingerprint the rule runs under now, stored or pending.
    :param to_fingerprint: Fingerprint of the policy to install.
    :returns: The record to store; refresh it before reporting it.
    :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
    """
    if existing is not None and existing.status in DRAINING:
        if existing.to_fingerprint != to_fingerprint:
            raise PolicyConflict(
                f"{rule} is already migrating to another policy",
                rule=rule,
                expected=to_fingerprint,
                found=existing.to_fingerprint,
            )
        else:
            migration = existing
    else:
        migration = PolicyMigration(
            rule,
            MigrationStatus.DRAINING,
            from_fingerprint=current,
            to_fingerprint=to_fingerprint,
        )
    return migration


def refresh_migration(
    migration: PolicyMigration | None,
    *,
    neutral: bool,
    horizon: EpochMicros | None,
    now: EpochMicros,
) -> PolicyMigration | None:
    """``migration`` brought up to date with whether its rule's state has drained.

    :param migration: The rule's migration record, if any.
    :param neutral: Whether the rule's state is gone or past its safe-forget horizon.
    :param horizon: The rule's safe-forget horizon, or ``None``.
    :param now: Authority epoch time.
    :returns: The record to store and report; unchanged unless draining.
    """
    if migration is None or migration.status not in DRAINING:
        refreshed = migration
    elif neutral:
        refreshed = dataclasses.replace(
            migration,
            status=MigrationStatus.READY,
            drained_after=EpochMicros(horizon if horizon is not None else now),
        )
    else:
        refreshed = dataclasses.replace(migration, drained_after=horizon)
    return refreshed


def complete_migration(rule: RuleId, migration: PolicyMigration | None) -> PolicyMigration:
    """The completed record for a drained rule, or a refusal.

    :param rule: The rule whose migration to complete.
    :param migration: Its refreshed migration record, if any.
    :returns: The record marked complete; the caller forgets the old state and metadata.
    :raises ~procrastinators.errors.PolicyConflict: No migration is ready for ``rule``.
    """
    if migration is None or migration.status is not MigrationStatus.READY:
        status = "no migration" if migration is None else migration.status.value
        raise PolicyConflict(
            f"{rule} cannot complete its migration: {status}. Its state must drain to "
            "neutral first, and a policy whose state never becomes neutral cannot be "
            "migrated live",
            rule=rule,
        )
    else:
        pass
    assert migration.to_fingerprint is not None
    completed = dataclasses.replace(migration, status=MigrationStatus.COMPLETE)
    return completed


if __name__ == "__main__":
    pass
else:
    pass
