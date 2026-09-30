"""A rig for testing limiters on fake time.

:class:`Rig` owns one fake timeline and one memory store reading it, and
registers a ``rig://`` backend family whose factory returns handles on that
store. A limiter built with ``backend="rig://"`` therefore offers both sync and
async modes, owns its handles as an address-built limiter does, and waits
through recording sleepers that advance the timeline instead of sleeping.

Both sleepers refuse to sleep while the store's lock is held, so every test
using the rig also checks that waiting happens outside the critical section
(contract W1).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from procrastinators.backends.memory import (
    MEMORY_CAPABILITIES,
    AsyncMemoryBackend,
    MemoryBackend,
    MemoryStore,
)
from procrastinators.builtins import builtin_registry
from procrastinators.capabilities import Mode
from procrastinators.keys import idempotent_key
from procrastinators.limiter import RateLimiter
from procrastinators.models import Limit
from procrastinators.protocols import BackendSpec
from procrastinators.testing import AsyncRecordingSleeper, FakeTimeline, RecordingSleeper

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from procrastinators.models import DiagnosticEvent, DurationMicros
    from procrastinators.protocols import Algorithm
    from procrastinators.registry import Registry
else:
    pass

ANKH = idempotent_key({"vendor": "ankh", "dataset": "orders"})
"""A quota key for the tests' default vendor scope."""

QUIRM = idempotent_key({"vendor": "quirm", "dataset": "cheese"})
"""A second, unrelated quota key."""


@dataclass
class Rig:
    """Fake time, one store, recording sleepers, and collected diagnostics.

    :param during: Called with each sleep's duration while a waiter is asleep,
        before time advances.
    """

    during: Callable[[DurationMicros], None] | None = None
    timeline: FakeTimeline = field(default_factory=FakeTimeline)
    events: list[DiagnosticEvent] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.store = MemoryStore(name="rig", clock=self.timeline.epoch_clock)
        self.sleeper = RecordingSleeper(
            self.timeline, during=self._during, lock_held=self._lock_held
        )
        self.async_sleeper = AsyncRecordingSleeper(
            self.timeline, during=self._during, lock_held=self._lock_held
        )
        self.registry: Registry = builtin_registry()
        self.registry.register_backend(
            BackendSpec(family="rig", factory=self._factory, capabilities=MEMORY_CAPABILITIES)
        )

    def _during(self, duration: DurationMicros) -> None:
        if self.during is not None:
            self.during(duration)
        else:
            pass

    def _lock_held(self) -> bool:
        held = self.store.lock_held
        return held

    def _factory(
        self,
        address: str,
        *,
        mode: Mode,
        namespace: str,
        algorithms: Iterable[Algorithm[Any]],
    ) -> MemoryBackend | AsyncMemoryBackend:
        del address
        self.store.host(algorithms)
        if mode is Mode.SYNC:
            handle: MemoryBackend | AsyncMemoryBackend = MemoryBackend(
                self.store, namespace=namespace
            )
        else:
            handle = AsyncMemoryBackend(self.store, namespace=namespace)
        return handle

    def limiter(self, **overrides: object) -> RateLimiter:
        """A limiter on this rig: two per second on :data:`ANKH` unless overridden."""
        arguments: dict[str, object] = {
            "key": ANKH,
            "limits": [Limit(2, per="1s")],
            "backend": "rig://",
            "registry": self.registry,
            "clock": self.timeline.deadline_clock,
            "sleeper": self.sleeper,
            "async_sleeper": self.async_sleeper,
            "on_event": self.events.append,
        }
        arguments.update(overrides)
        limiter = RateLimiter(**arguments)  # ty: ignore[invalid-argument-type]
        return limiter

    def event_types(self) -> list[str]:
        """The class name of every diagnostic delivered, in order."""
        names = [type(event).__name__ for event in self.events]
        return names


if __name__ == "__main__":
    pass
else:
    pass
