"""Procrastinators: rate limiting for ETL libraries and pipelines.

Importing this package must remain free of side effects: no filesystem
mutation, no worker threads, and no connections.

:class:`~procrastinators.limiter.RateLimiter` is the facade: acquire quota
before doing work, as a context manager, a decorator, or a call, sync or
async, composed atomically with :meth:`~procrastinators.limiter.RateLimiter.combine`.
:func:`~procrastinators.keys.idempotent_key` derives the stable quota keys it
takes, and :meth:`~procrastinators.limiter.RateLimiter.from_config` builds one
from layered configuration (:mod:`procrastinators.config`). The five reference
algorithms live in :mod:`procrastinators.algorithms`; the memory and SQLite
backends in :mod:`procrastinators.backends`. Without a backend a limiter uses
the shared SQLite file at a stable per-user path. Service backends arrive in
later phases.

Conformance tools for testing algorithms and backends live in
:mod:`procrastinators.testing`, which this package never imports.

``docs/source/contracts.md`` is the normative specification of what these types
mean, and ``docs/source/api.md`` of the facade.
"""

__author__ = "Rohan B. Dalton"

from procrastinators.__version__ import __version__
from procrastinators.algorithms.base import BaseAlgorithm
from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.builtins import default_registry
from procrastinators.config import ConfigFile, ConfigLocations
from procrastinators.config_models import (
    DEFAULT_BACKEND,
    UNSET,
    ConfigLayer,
    ConfigSource,
    LimiterSettings,
    Provenance,
    ResolvedConfig,
    Unset,
)
from procrastinators.errors import (
    AcquireTimeout,
    BackendBusy,
    BackendError,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    IndeterminateAdmission,
    InvalidCost,
    InvalidIdentity,
    InvalidPolicy,
    PolicyConflict,
    ProcrastinatorsError,
    StateCorruption,
    UnsupportedCapability,
)
from procrastinators.keys import idempotent_key
from procrastinators.limiter import Invocation, RateLimiter
from procrastinators.models import (
    Admission,
    AdmissionRequest,
    Algorithms,
    BackendIdentity,
    Capabilities,
    Constraint,
    Cooldown,
    CoordinationScope,
    Decision,
    DiagnosticEvent,
    Durability,
    DurationMicros,
    EpochMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    Limit,
    MonotonicMicros,
    OperationBudget,
    Ownership,
    Policy,
    PolicyFingerprint,
    PolicySpec,
    QuotaIdentity,
    RemainingEstimate,
    ResourceOwnership,
    RuleId,
    RuleSnapshot,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    Snapshot,
    TokenBucketPolicy,
    Transition,
)
from procrastinators.protocols import (
    AdmissionClock,
    AdmissionObserver,
    Algorithm,
    AlgorithmSpec,
    AsyncAdmissionClock,
    AsyncBackend,
    AsyncSleeper,
    BackendSpec,
    ConfigLoader,
    ConfigStore,
    DeadlineClock,
    DiagnosticsCallback,
    EventWindow,
    LogEvent,
    MigrationStatus,
    NativeExecutorSpec,
    ObservationPoint,
    PolicyMigration,
    RuleState,
    Sleeper,
    StateCodec,
    StateRepresentation,
    StateRequirements,
    StateView,
    StoredPolicy,
    SupportsAsyncCooldown,
    SupportsAsyncPolicyAdministration,
    SupportsCooldown,
    SupportsPolicyAdministration,
    SyncBackend,
)

__all__ = [
    "DEFAULT_BACKEND",
    "UNSET",
    "AcquireTimeout",
    "Admission",
    "AdmissionClock",
    "AdmissionObserver",
    "AdmissionRequest",
    "Algorithm",
    "AlgorithmSpec",
    "Algorithms",
    "AsyncAdmissionClock",
    "AsyncBackend",
    "AsyncSleeper",
    "BackendBusy",
    "BackendError",
    "BackendIdentity",
    "BackendSpec",
    "BackendUnavailable",
    "BaseAlgorithm",
    "BaseAsyncBackend",
    "BaseSyncBackend",
    "Capabilities",
    "ClosedResource",
    "ConfigFile",
    "ConfigLayer",
    "ConfigLoader",
    "ConfigLocations",
    "ConfigSource",
    "ConfigStore",
    "ConfigurationError",
    "Constraint",
    "Cooldown",
    "CoordinationScope",
    "DeadlineClock",
    "Decision",
    "DiagnosticEvent",
    "DiagnosticsCallback",
    "Durability",
    "DurationMicros",
    "EpochMicros",
    "EventWindow",
    "FixedWindowPolicy",
    "IndeterminateAdmission",
    "InvalidCost",
    "InvalidIdentity",
    "InvalidPolicy",
    "Invocation",
    "LeakyBucketPolicy",
    "Limit",
    "LimiterSettings",
    "LogEvent",
    "MigrationStatus",
    "MonotonicMicros",
    "NativeExecutorSpec",
    "ObservationPoint",
    "OperationBudget",
    "Ownership",
    "Policy",
    "PolicyConflict",
    "PolicyFingerprint",
    "PolicyMigration",
    "PolicySpec",
    "ProcrastinatorsError",
    "Provenance",
    "QuotaIdentity",
    "RateLimiter",
    "RemainingEstimate",
    "ResolvedConfig",
    "ResourceOwnership",
    "RuleId",
    "RuleSnapshot",
    "RuleState",
    "Sleeper",
    "SlidingCounterPolicy",
    "SlidingLogPolicy",
    "Snapshot",
    "StateCodec",
    "StateCorruption",
    "StateRepresentation",
    "StateRequirements",
    "StateView",
    "StoredPolicy",
    "SupportsAsyncCooldown",
    "SupportsAsyncPolicyAdministration",
    "SupportsCooldown",
    "SupportsPolicyAdministration",
    "SyncBackend",
    "TokenBucketPolicy",
    "Transition",
    "Unset",
    "UnsupportedCapability",
    "__version__",
    "default_registry",
    "idempotent_key",
]


if __name__ == "__main__":
    pass
else:
    pass
