"""Conformance tools: fake time, fault injection, scripted backends, and shared traces.

Test support, not production code. Nothing here enforces a rate limit for
real; everything here helps prove that something else does. The package is
never imported by :mod:`procrastinators` itself, and importing it starts no
thread and opens nothing.

The pieces, in the order a backend author usually meets them:

* :class:`~procrastinators.testing.clocks.FakeTimeline` and its clocks: time
  that moves only when told to.
* :class:`~procrastinators.testing.sleepers.RecordingSleeper` and
  :class:`~procrastinators.testing.sleepers.AsyncRecordingSleeper`: waits that
  take no time and leave a record.
* :class:`~procrastinators.testing.observation.FaultInjector` and
  :class:`~procrastinators.testing.observation.Pause`: act at a chosen
  :class:`~procrastinators.protocols.ObservationPoint`.
* :class:`~procrastinators.testing.scripted.ScriptedBackend` and
  :class:`~procrastinators.testing.scripted.AsyncScriptedBackend`: replay chosen
  outcomes, for testing what sits above a backend.
* :class:`~procrastinators.testing.host.EvaluatorHost`: runs evaluators
  atomically in memory, for testing algorithms before any backend exists.
* :data:`~procrastinators.testing.catalog.TRACES` and
  :data:`~procrastinators.testing.catalog.SCENARIOS`: hand-checked histories
  every executor must reproduce.
* :func:`~procrastinators.testing.conformance.check_backend`,
  :func:`~procrastinators.testing.conformance.check_async_backend`, and
  :func:`~procrastinators.testing.conformance.check_algorithm`: the
  conformance suite.

Every name is also importable from :mod:`procrastinators.testing` itself.
"""

__author__ = "Rohan B. Dalton"

from procrastinators.testing.catalog import (
    ALGORITHM_TRACES,
    COMPOSITION_TRACES,
    CONFLICTING_PROBES,
    PROBES,
    SCENARIOS,
    T0,
    TRACES,
    traces_for,
)
from procrastinators.testing.clocks import (
    DEFAULT_EPOCH_US,
    FakeAdmissionClock,
    FakeAsyncAdmissionClock,
    FakeDeadlineClock,
    FakeTimeline,
)
from procrastinators.testing.conformance import (
    AsyncBackendCase,
    BackendCase,
    CheckResult,
    CheckStatus,
    ConformanceReport,
    Guarantee,
    check_algorithm,
    check_async_backend,
    check_backend,
)
from procrastinators.testing.guarantees import (
    Violation,
    fixed_window_violations,
    pacing_violations,
    rolling_window_violations,
    token_bucket_violations,
)
from procrastinators.testing.harness import (
    AsyncBackendSubject,
    AsyncTraceSubject,
    BackendSubject,
    TraceMismatch,
    TraceReport,
    TraceSubject,
    run_trace,
    run_trace_async,
)
from procrastinators.testing.host import EvaluatorHost
from procrastinators.testing.observation import FaultInjector, Pause
from procrastinators.testing.scripted import (
    AsyncScriptedBackend,
    ScriptedAllow,
    ScriptedBackend,
    ScriptedDeny,
    allow,
    deny,
)
from procrastinators.testing.sleepers import AsyncRecordingSleeper, RecordingSleeper
from procrastinators.testing.traces import (
    Attempt,
    Burst,
    Covers,
    Expectation,
    Finish,
    Scenario,
    Trace,
    TraceRule,
)
from procrastinators.testing.violations import ContractViolation

__all__ = [
    "ALGORITHM_TRACES",
    "COMPOSITION_TRACES",
    "CONFLICTING_PROBES",
    "DEFAULT_EPOCH_US",
    "PROBES",
    "SCENARIOS",
    "T0",
    "TRACES",
    "AsyncBackendCase",
    "AsyncBackendSubject",
    "AsyncRecordingSleeper",
    "AsyncScriptedBackend",
    "AsyncTraceSubject",
    "Attempt",
    "BackendCase",
    "BackendSubject",
    "Burst",
    "CheckResult",
    "CheckStatus",
    "ConformanceReport",
    "ContractViolation",
    "Covers",
    "EvaluatorHost",
    "Expectation",
    "FakeAdmissionClock",
    "FakeAsyncAdmissionClock",
    "FakeDeadlineClock",
    "FakeTimeline",
    "FaultInjector",
    "Finish",
    "Guarantee",
    "Pause",
    "RecordingSleeper",
    "Scenario",
    "ScriptedAllow",
    "ScriptedBackend",
    "ScriptedDeny",
    "Trace",
    "TraceMismatch",
    "TraceReport",
    "TraceRule",
    "TraceSubject",
    "Violation",
    "allow",
    "check_algorithm",
    "check_async_backend",
    "check_backend",
    "deny",
    "fixed_window_violations",
    "pacing_violations",
    "rolling_window_violations",
    "run_trace",
    "run_trace_async",
    "token_bucket_violations",
    "traces_for",
]


if __name__ == "__main__":
    pass
else:
    pass
