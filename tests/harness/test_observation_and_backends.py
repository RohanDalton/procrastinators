"""Observation points, fault injection, the evaluator host, and scripted backends.

Nobby Nobbs is not to be trusted with anything, which makes him the ideal
stand-in for an injected fault.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
from typing import TYPE_CHECKING

import pytest

from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
    IndeterminateAdmission,
    PolicyConflict,
    UnsupportedCapability,
)
from procrastinators.models import (
    AdmissionRequest,
    Algorithms,
    BackendIdentity,
    Capabilities,
    CoordinationScope,
    Decision,
    Durability,
    DurationMicros,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.protocols import AdmissionObserver, AsyncBackend, ObservationPoint, SyncBackend
from procrastinators.testing import (
    DEFAULT_EPOCH_US,
    AsyncScriptedBackend,
    ContractViolation,
    EvaluatorHost,
    FakeTimeline,
    FaultInjector,
    Pause,
    ScriptedBackend,
    TraceRule,
    allow,
    deny,
)
from procrastinators.testing.harness import DEFAULT_NAMESPACE
from tests.oracles import SlidingLogOracle, TokenBucketOracle, all_oracles

if TYPE_CHECKING:
    from procrastinators.models import PolicySpec
else:
    pass

ONE_SECOND = DurationMicros(1_000_000)
ADMITTED_POINTS = (
    ObservationPoint.BEFORE_LOCK,
    ObservationPoint.AFTER_LOAD,
    ObservationPoint.BEFORE_COMMIT,
    ObservationPoint.AFTER_COMMIT,
)


def _request(
    policy: PolicySpec, *, scope: str = "unseen", label: str = "rincewind"
) -> AdmissionRequest:
    rule = TraceRule(label, policy, scope=scope)
    request = AdmissionRequest((rule.constraint(DEFAULT_NAMESPACE),))
    return request


def test_the_injector_records_every_point_in_order(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: An observed host and a one-per-second rule.
    When:  One admission and one denial happen.
    Then:  The injector saw all four points, then only the two before a decision;
           and it satisfies the observer protocol.
    """
    expected = (*ADMITTED_POINTS, ObservationPoint.BEFORE_LOCK, ObservationPoint.AFTER_LOAD)

    host.admit(one_per_second)
    host.admit(one_per_second)
    actual = injector.points

    assert actual == expected
    assert isinstance(injector, AdmissionObserver)


def test_an_armed_fault_fires_the_requested_number_of_times(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: A fault class armed twice before the lock.
    When:  Three admissions are attempted.
    Then:  The first two fail as unavailable storage, the third is admitted.
    """
    injector.arm(ObservationPoint.BEFORE_LOCK, ConnectionError, times=2)

    for _ in range(2):
        with pytest.raises(BackendUnavailable):
            host.admit(one_per_second)

    assert host.admit(one_per_second).allowed


def test_resetting_disarms_and_forgets(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: An armed injector that has observed an admission.
    When:  It is reset and another request is admitted.
    Then:  Nothing fires and only the new admission's points are recorded.
    """
    host.admit(one_per_second)
    injector.arm(ObservationPoint.AFTER_LOAD, ConnectionError)
    expected = ADMITTED_POINTS

    injector.reset()
    host.admit(_request(SlidingLogPolicy(1, ONE_SECOND), label="twoflower"))
    actual = injector.points

    assert actual == expected


def test_a_library_error_before_commit_passes_through_unchanged(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: BackendBusy injected after state was loaded.
    When:  An admission is attempted.
    Then:  BackendBusy itself is raised — contention is not unavailability (O2) —
           and nothing was committed.
    """
    injector.arm(ObservationPoint.AFTER_LOAD, BackendBusy("the Patrician is busy"))

    with pytest.raises(BackendBusy):
        host.admit(one_per_second)

    assert host.admit(one_per_second).allowed


def test_an_indeterminate_admission_after_commit_passes_through(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: An IndeterminateAdmission injected after the commit.
    When:  An admission is attempted.
    Then:  The same exception is raised, not wrapped again.
    """
    injected = IndeterminateAdmission("the clacks went quiet")
    injector.arm(ObservationPoint.AFTER_COMMIT, injected)

    with pytest.raises(IndeterminateAdmission) as raised:
        host.admit(one_per_second)

    assert raised.value is injected


def test_an_unknown_commit_names_what_may_have_been_charged(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: A driver error injected after the commit.
    When:  An admission is attempted.
    Then:  IndeterminateAdmission names the cost and rules in doubt and keeps the cause.
    """
    injected = ConnectionError("response lost")
    injector.arm(ObservationPoint.AFTER_COMMIT, injected)
    expected = (1, one_per_second.rules, injected)

    with pytest.raises(IndeterminateAdmission) as raised:
        host.admit(one_per_second)
    actual = (raised.value.cost, raised.value.rules, raised.value.__cause__)

    assert actual == expected


def test_the_lock_is_held_where_the_contract_says(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: Observers that record whether the host's lock is held at each point.
    When:  A request is admitted.
    Then:  The lock is free before it is taken and held from the load to the commit.
    """
    held = dict()

    def note(point: ObservationPoint) -> None:
        def record(request: AdmissionRequest) -> None:
            del request
            held[point] = host.lock_held

        injector.arm(point, record)

    for point in ADMITTED_POINTS:
        note(point)
    expected = {
        ObservationPoint.BEFORE_LOCK: False,
        ObservationPoint.AFTER_LOAD: True,
        ObservationPoint.BEFORE_COMMIT: True,
        ObservationPoint.AFTER_COMMIT: True,
    }

    host.admit(one_per_second)
    actual = held

    assert actual == expected


def test_a_pause_orders_a_race_deterministically(
    host: EvaluatorHost, injector: FaultInjector, one_per_second: AdmissionRequest
) -> None:
    """
    Given: Vimes's admission paused before it takes the lock.
    When:  Carrot's admission runs to completion, then Vimes is released.
    Then:  Carrot is admitted and Vimes denied: the interleaving was chosen, not hoped for.
    """
    pause = Pause()
    injector.arm(ObservationPoint.BEFORE_LOCK, pause)
    results: dict[str, Decision] = dict()

    def vimes() -> None:
        results["vimes"] = host.admit(one_per_second)

    thread = threading.Thread(target=vimes)
    thread.start()
    pause.wait_reached()
    results["carrot"] = host.admit(one_per_second)
    pause.release()
    thread.join()
    expected = {"carrot": True, "vimes": False}

    actual = {name: decision.allowed for name, decision in results.items()}

    assert actual == expected


def test_an_unreleased_pause_gives_up_rather_than_hanging() -> None:
    """
    Given: A pause with a tiny timeout that nobody releases.
    When:  A thread reaches it.
    Then:  ContractViolation is raised instead of the test hanging.
    """
    pause = Pause(timeout_s=0.01)

    with pytest.raises(ContractViolation, match="not released"):
        pause(_request(SlidingLogPolicy(1, ONE_SECOND)))


def test_a_host_needs_distinct_algorithms(timeline: FakeTimeline) -> None:
    """
    Given: No algorithms, or two with one id.
    When:  A host is built.
    Then:  ConfigurationError is raised for each.
    """
    with pytest.raises(ConfigurationError, match="at least one"):
        EvaluatorHost([], clock=timeline.epoch_clock)
    with pytest.raises(ConfigurationError, match="share the id"):
        EvaluatorHost([SlidingLogOracle(), SlidingLogOracle()], clock=timeline.epoch_clock)


def test_a_denied_first_attempt_still_records_the_initial_state(
    host: EvaluatorHost, timeline: FakeTimeline
) -> None:
    """
    Given: An initially empty token bucket never used before.
    When:  The first attempt is denied.
    Then:  Its initial state — no tokens, refill clock started now — was recorded (P5).
    """
    request = _request(TokenBucketPolicy(2, 1, ONE_SECOND, initial_tokens=0))
    expected = (("anchor", DEFAULT_EPOCH_US), ("tokens", 0))

    decision = host.admit(request)
    actual = host.state_of(request.rules[0]).scalars

    assert not decision.allowed
    assert actual == expected


def test_a_policy_conflict_resets_nothing(host: EvaluatorHost) -> None:
    """
    Given: A rule used under one policy.
    When:  The same rule is presented under another, then under the first again.
    Then:  PolicyConflict names both fingerprints, and the stored quota is intact.
    """
    first = _request(SlidingLogPolicy(1, ONE_SECOND))
    second = _request(SlidingLogPolicy(5, ONE_SECOND))
    host.admit(first)

    with pytest.raises(PolicyConflict) as raised:
        host.admit(second)

    assert raised.value.found == first.constraints[0].fingerprint
    assert raised.value.expected == second.constraints[0].fingerprint
    assert not host.admit(first).allowed


def test_a_host_refuses_what_it_does_not_host(timeline: FakeTimeline) -> None:
    """
    Given: A host running only the token-bucket oracle.
    When:  A sliding-log request arrives.
    Then:  UnsupportedCapability is raised before any state is touched.
    """
    host = EvaluatorHost([TokenBucketOracle()], clock=timeline.epoch_clock)

    with pytest.raises(UnsupportedCapability, match="sliding_log"):
        host.admit(_request(SlidingLogPolicy(1, ONE_SECOND)))


def test_a_closed_host_refuses_everything(host: EvaluatorHost) -> None:
    """
    Given: A host closed twice.
    When:  It is asked to admit or inspect.
    Then:  Closing twice was fine and both calls raise ClosedResource.
    """
    request = _request(SlidingLogPolicy(1, ONE_SECOND))
    host.close()
    host.close()

    with pytest.raises(ClosedResource):
        host.admit(request)
    with pytest.raises(ClosedResource):
        host.inspect(request.rules)


def test_inspection_names_the_algorithm_and_promises_nothing(host: EvaluatorHost) -> None:
    """
    Given: One used rule and one unused.
    When:  Both are inspected.
    Then:  The snapshot names the used rule's algorithm, calls the other unused,
           and is advisory.
    """
    used = _request(SlidingLogPolicy(1, ONE_SECOND))
    unused = _request(SlidingLogPolicy(1, ONE_SECOND), label="unused")
    host.admit(used)
    expected = [Algorithms.SLIDING_LOG, "unused"]

    snapshot = host.inspect((*used.rules, *unused.rules))
    actual = [rule.algorithm for rule in snapshot.rules]

    assert actual == expected
    assert snapshot.advisory


def test_a_scripted_backend_replays_its_script(timeline: FakeTimeline) -> None:
    """
    Given: A script of allow, deny, a prepared decision, a callable, and an error.
    When:  Five admissions are attempted.
    Then:  Each outcome is replayed in order, admissions are stamped by the clock,
           every request is recorded, and the script is then exhausted.
    """
    request = _request(SlidingLogPolicy(1, ONE_SECOND))
    prepared = Decision.deny(request.rules, DurationMicros(9))

    def decide(seen: AdmissionRequest) -> Decision:
        decision = Decision.deny(seen.rules, DurationMicros(3))
        return decision

    backend = ScriptedBackend(
        [allow(), deny(500_000), prepared, decide, ConnectionError("nobby")],
        clock=timeline.epoch_clock,
    )
    first = backend.admit(request)
    outcomes = [backend.admit(request).retry_after_us for _ in range(3)]
    with pytest.raises(ConnectionError, match="nobby"):
        backend.admit(request)
    expected = (DEFAULT_EPOCH_US, [500_000, 9, 3], 5)

    assert first.admission is not None
    actual = (first.admission.admitted_at, outcomes, len(backend.requests))

    assert actual == expected
    backend.assert_exhausted()
    assert isinstance(backend, SyncBackend)


def test_an_exhausted_script_is_a_violation(timeline: FakeTimeline) -> None:
    """
    Given: A one-step script.
    When:  Two admissions are attempted, or it is checked for leftovers before running.
    Then:  The unexpected attempt, and the unplayed step, are each a ContractViolation.
    """
    request = _request(SlidingLogPolicy(1, ONE_SECOND))
    backend = ScriptedBackend([allow()], clock=timeline.epoch_clock)

    with pytest.raises(ContractViolation, match="never requested"):
        backend.assert_exhausted()
    backend.admit(request)
    with pytest.raises(ContractViolation, match="exhausted"):
        backend.admit(request)


def test_a_scripted_backend_still_honors_its_declarations(timeline: FakeTimeline) -> None:
    """
    Given: A scripted backend declaring only the token bucket.
    When:  A sliding-log request arrives, and later the backend is closed and used.
    Then:  The request is refused as unsupported, and use after close is refused.
    """
    capabilities = Capabilities(
        algorithms=frozenset({Algorithms.TOKEN_BUCKET}),
        coordination=CoordinationScope.IN_PROCESS,
        durability=Durability.EPHEMERAL,
    )
    backend = ScriptedBackend(
        [allow()],
        clock=timeline.epoch_clock,
        capabilities=capabilities,
        identity=BackendIdentity("scripted", "ankh", "discworld"),
    )
    request = _request(SlidingLogPolicy(1, ONE_SECOND))

    with pytest.raises(UnsupportedCapability, match="sliding_log"):
        backend.admit(request)
    backend.close()
    with pytest.raises(ClosedResource):
        backend.inspect(request.rules)


def test_a_scripted_backend_reports_observation_points(
    timeline: FakeTimeline, injector: FaultInjector
) -> None:
    """
    Given: A scripted allow then deny, observed.
    When:  Both are played.
    Then:  The allow passes all four points and the deny the first two.
    """
    request = _request(SlidingLogPolicy(1, ONE_SECOND))
    backend = ScriptedBackend([allow(), deny(1)], clock=timeline.epoch_clock, observer=injector)
    expected = (*ADMITTED_POINTS, ObservationPoint.BEFORE_LOCK, ObservationPoint.AFTER_LOAD)

    backend.admit(request)
    backend.admit(request)
    actual = injector.points

    assert actual == expected


def test_an_async_scripted_backend_can_be_cancelled_before_it_decides(
    timeline: FakeTimeline,
) -> None:
    """
    Given: An async scripted backend with one allow.
    When:  An admission is cancelled at its first yield, then retried.
    Then:  The cancellation propagates unchanged and consumed no step: the retry is
           admitted with the step the cancelled call never reached.
    """
    request = _request(SlidingLogPolicy(1, ONE_SECOND))
    backend = AsyncScriptedBackend([allow()], clock=timeline.epoch_clock)

    async def main() -> Decision:
        task = asyncio.create_task(backend.admit(request))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        decision = await backend.admit(request)
        return decision

    decision = asyncio.run(main())

    assert decision.allowed
    assert isinstance(backend, AsyncBackend)


def test_every_oracle_backend_passes_through_the_same_host(timeline: FakeTimeline) -> None:
    """
    Given: All five oracles on one host.
    When:  Its capabilities are read.
    Then:  It hosts all five built-in algorithms and holds scalars and event logs.
    """
    host = EvaluatorHost(all_oracles(), clock=timeline.epoch_clock)
    expected = (frozenset(Algorithms), frozenset({"scalars", "event_log"}))

    actual = (host.capabilities.algorithms, host.capabilities.state_representations)

    assert actual == expected


if __name__ == "__main__":
    pass
else:
    pass
