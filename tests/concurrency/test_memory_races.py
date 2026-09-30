"""Independent handles racing on one memory store obey the combined quota.

Time is frozen on a fake timeline, so the exact number of winners is known:
whatever the interleaving, a rule of ``N`` per period admits exactly ``N``
unit attempts at one instant. Threads are released together by a barrier
rather than by sleeping and hoping.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
from collections import Counter
from typing import TYPE_CHECKING

from hypothesis import given, settings
from hypothesis import strategies as st

from procrastinators.algorithms import reference_algorithms
from procrastinators.backends.memory import AsyncMemoryBackend, MemoryBackend, MemoryStore
from procrastinators.errors import PolicyConflict
from procrastinators.keys import policy_fingerprint
from procrastinators.models import (
    AdmissionRequest,
    Constraint,
    DurationMicros,
    FixedWindowPolicy,
    LeakyBucketPolicy,
    SlidingCounterPolicy,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.testing import EvaluatorHost, FakeTimeline
from tests.backends.conftest import SECOND, constraint

if TYPE_CHECKING:
    from collections.abc import Callable
else:
    pass

THREADS = 8
ATTEMPTS = 40


def _race(workers: int, work: Callable[[int], None]) -> None:
    barrier = threading.Barrier(workers)

    def run(index: int) -> None:
        barrier.wait(10)
        work(index)

    threads = [threading.Thread(target=run, args=(index,)) for index in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)


def test_the_watch_threads_share_one_quota_through_separate_handles() -> None:
    """
    Given: Eight threads, each with its own handle on one store, and a rule of 100
           per second with time frozen.
    When:  They race 320 unit attempts.
    Then:  Exactly 100 are admitted: separate handles do not mean separate quotas.
    """
    timeline = FakeTimeline()
    store = MemoryStore(clock=timeline.epoch_clock)
    request = AdmissionRequest(
        (constraint("watch-house", "sergeant", SlidingLogPolicy(100, SECOND)),)
    )
    admitted = Counter[int]()

    def work(index: int) -> None:
        handle = MemoryBackend(store)
        admitted[index] = sum(1 for _ in range(ATTEMPTS) if handle.admit(request).allowed)

    _race(THREADS, work)

    assert sum(admitted.values()) == 100


def test_racing_composed_attempts_never_debit_a_rule_partially() -> None:
    """
    Given: A vendor-wide rule of 60 and an endpoint rule of 100 composed together,
           and threads racing composed attempts while time is frozen.
    When:  The race ends.
    Then:  Exactly 60 were admitted, and the endpoint rule recorded exactly those 60:
           no losing attempt debited the endpoint before the vendor rule denied (A6).
    """
    timeline = FakeTimeline()
    store = MemoryStore(clock=timeline.epoch_clock)
    vendor = constraint("ankh", "vendor", SlidingLogPolicy(60, SECOND))
    endpoint = constraint("ankh-orders", "endpoint", SlidingLogPolicy(100, SECOND))
    request = AdmissionRequest((vendor, endpoint))
    admitted = Counter[int]()

    def work(index: int) -> None:
        handle = MemoryBackend(store)
        admitted[index] = sum(1 for _ in range(ATTEMPTS) if handle.admit(request).allowed)

    _race(THREADS, work)

    assert sum(admitted.values()) == 60
    assert sum(event.cost for event in store.state_of(endpoint.rule).events) == 60


def test_async_tasks_on_separate_handles_share_one_quota() -> None:
    """
    Given: Sixty-four tasks, each with its own async handle on one store, and a rule
           of 25 per second with time frozen.
    When:  They all attempt at once, three times each.
    Then:  Exactly 25 are admitted.
    """
    timeline = FakeTimeline()
    store = MemoryStore(clock=timeline.epoch_clock)
    request = AdmissionRequest(
        (constraint("dunmanifestin", "coach", SlidingLogPolicy(25, SECOND)),)
    )

    async def task() -> int:
        handle = AsyncMemoryBackend(store)
        count = 0
        for _ in range(3):
            count += (await handle.admit(request)).allowed
            await asyncio.sleep(0)
        return count

    async def scenario() -> int:
        counts = await asyncio.gather(*(task() for _ in range(64)))
        return sum(counts)

    assert asyncio.run(scenario()) == 25


def test_threads_and_an_event_loop_share_one_quota() -> None:
    """
    Given: Four threads using sync handles and an event loop running sixteen tasks
           on async handles, all on one store with a rule of 50 per second.
    When:  They race with time frozen.
    Then:  Exactly 50 are admitted across both modes.
    """
    timeline = FakeTimeline()
    store = MemoryStore(clock=timeline.epoch_clock)
    request = AdmissionRequest((constraint("pseudopolis", "yard", SlidingLogPolicy(50, SECOND)),))
    admitted = Counter[str]()

    async def tasks() -> int:
        async def one() -> int:
            handle = AsyncMemoryBackend(store)
            count = 0
            for _ in range(ATTEMPTS):
                count += (await handle.admit(request)).allowed
                await asyncio.sleep(0)
            return count

        counts = await asyncio.gather(*(one() for _ in range(16)))
        return sum(counts)

    def work(index: int) -> None:
        if index == 0:
            admitted["async"] = asyncio.run(tasks())
        else:
            handle = MemoryBackend(store)
            admitted[f"thread-{index}"] = sum(
                1 for _ in range(ATTEMPTS) if handle.admit(request).allowed
            )

    _race(5, work)

    assert sum(admitted.values()) == 50


def test_auditors_reject_conflicting_policies_racing_for_a_new_rule() -> None:
    """
    Given: Two threads presenting different policies for the same never-used rule.
    When:  They race to admit ten times each.
    Then:  One policy wins first contact; every attempt under the other is a
           ``PolicyConflict``, and the winner's quota was never reset (I4).
    """
    timeline = FakeTimeline()
    store = MemoryStore(clock=timeline.epoch_clock)
    policies = (SlidingLogPolicy(3, SECOND), SlidingLogPolicy(4, SECOND))
    rule = constraint("auditors", "reality", policies[0]).rule
    outcomes: dict[int, Counter[str]] = dict()

    def work(index: int) -> None:
        policy = policies[index]
        request = AdmissionRequest((Constraint(rule, policy, policy_fingerprint(policy)),))
        handle = MemoryBackend(store)
        tally = Counter[str]()
        for _ in range(10):
            try:
                tally["allowed" if handle.admit(request).allowed else "denied"] += 1
            except PolicyConflict:
                tally["conflict"] += 1
        outcomes[index] = tally

    _race(2, work)
    winner = next(index for index, tally in outcomes.items() if tally["allowed"])

    assert outcomes[1 - winner] == Counter(conflict=10)
    assert outcomes[winner] == Counter(
        allowed=policies[winner].amount, denied=10 - policies[winner].amount
    )


_POOL = (
    constraint("ankh", "fixed", FixedWindowPolicy(4, SECOND)),
    constraint("ankh", "log", SlidingLogPolicy(3, DurationMicros(700_000))),
    constraint("quirm", "bucket", TokenBucketPolicy(4, 1, DurationMicros(300_000), 1)),
    constraint("quirm", "pacing", LeakyBucketPolicy(3, SECOND, burst_tolerance=1)),
    constraint("sto-lat", "counter", SlidingCounterPolicy(4, SECOND)),
)

composed_schedules = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=900_000),
        st.integers(min_value=1, max_value=3),
        st.sets(st.integers(min_value=0, max_value=len(_POOL) - 1), min_size=1),
    ),
    min_size=1,
    max_size=40,
)


@settings(max_examples=100, deadline=None)
@given(schedule=composed_schedules)
def test_memory_state_matches_the_reference_model_after_any_composed_history(
    schedule: list[tuple[int, int, set[int]]],
) -> None:
    """
    Given: Random composed attempts over rules of all five algorithms.
    When:  Each is admitted on the memory store and on the reference evaluator host.
    Then:  Every decision agrees and, after every step, every rule's stored state is
           identical: a failed composed attempt leaves exactly the state the reference
           model leaves (Phase 6 acceptance).
    """
    timeline = FakeTimeline()
    store = MemoryStore(clock=timeline.epoch_clock)
    handle = MemoryBackend(store)
    host = EvaluatorHost(reference_algorithms(), clock=timeline.epoch_clock)
    for gap, cost, indices in schedule:
        timeline.advance(gap)
        request = AdmissionRequest(tuple(_POOL[index] for index in sorted(indices)), cost)

        actual = handle.admit(request)
        expected = host.admit(request)

        assert (actual.allowed, actual.blocking, actual.retry_after_us) == (
            expected.allowed,
            expected.blocking,
            expected.retry_after_us,
        )
        for rule in (member.rule for member in _POOL):
            assert store.state_of(rule) == host.state_of(rule)


if __name__ == "__main__":
    pass
else:
    pass
