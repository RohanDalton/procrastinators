"""Observers that inject failures and interleavings at named observation points.

A backend that accepts an :class:`~procrastinators.protocols.AdmissionObserver`
calls it at each :class:`~procrastinators.protocols.ObservationPoint`. The
:class:`FaultInjector` here records every call and, when armed, acts at a chosen
point: it raises an exception (a storage failure, or a cancellation), or runs a
callable — advancing a fake clock, or holding the thread with a :class:`Pause`
until the test releases it.

This is how race and cancellation tests become deterministic: a test does not
sleep and hope two threads collide, it stops one at ``AFTER_LOAD`` and runs the
other to completion.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import threading
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, TypeAlias

from procrastinators.models import AdmissionRequest
from procrastinators.testing.violations import ContractViolation

if TYPE_CHECKING:
    from procrastinators.protocols import ObservationPoint
else:
    pass

__all__ = ["FaultAction", "FaultInjector", "Pause"]

FaultAction: TypeAlias = BaseException | type[BaseException] | Callable[[AdmissionRequest], None]
"""What an armed point does: raise an exception (instance or class) or run a callable."""


class FaultInjector:
    """Records every observation and performs armed actions; thread-safe.

    Satisfies :class:`~procrastinators.protocols.AdmissionObserver`.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._armed: dict[ObservationPoint, deque[FaultAction]] = dict()
        self._seen: list[ObservationPoint] = list()

    @property
    def points(self) -> tuple[ObservationPoint, ...]:
        """Every point observed so far, in order."""
        with self._lock:
            points = tuple(self._seen)
        return points

    def arm(self, point: ObservationPoint, action: FaultAction, *, times: int = 1) -> None:
        """Perform ``action`` the next ``times`` times ``point`` is observed.

        Actions for one point queue up in the order they were armed.

        :param point: Where to act.
        :param action: An exception instance or class to raise, or a callable
            given the request.
        :param times: How many observations of ``point`` to act on.
        :raises ValueError: ``times`` is less than one.
        """
        if times < 1:
            raise ValueError(f"an action must be armed at least once, got {times}")
        else:
            pass
        with self._lock:
            self._armed.setdefault(point, deque()).extend([action] * times)

    def reset(self) -> None:
        """Disarm every point and forget what was observed."""
        with self._lock:
            self._armed.clear()
            self._seen.clear()

    def __call__(self, point: ObservationPoint, request: AdmissionRequest) -> None:
        """Record ``point`` and perform the next action armed for it, if any.

        :param point: Where the backend is.
        :param request: The request being admitted.
        """
        with self._lock:
            self._seen.append(point)
            queue = self._armed.get(point)
            action = queue.popleft() if queue else None
        if action is None:
            pass
        elif isinstance(action, BaseException):
            raise action
        elif isinstance(action, type):
            raise action(f"injected at {point.value}")
        else:
            action(request)


class Pause:
    """An action that holds the observing thread until released.

    Arm it at a point, start the admission in a thread, wait until the pause is
    reached, act, then release it. A pause that is never released gives up
    after ``timeout_s`` and raises, so a broken test fails instead of hanging.

    :param timeout_s: How long a held thread waits for release, in real seconds.
    """

    def __init__(self, timeout_s: float = 10.0) -> None:
        self._timeout_s = timeout_s
        self._reached = threading.Event()
        self._released = threading.Event()

    @property
    def reached(self) -> bool:
        """Whether a thread has arrived at the pause."""
        reached = self._reached.is_set()
        return reached

    def __call__(self, request: AdmissionRequest) -> None:
        """Hold the calling thread until :meth:`release`.

        :param request: The request being admitted; unused.
        :raises ~procrastinators.testing.violations.ContractViolation: Not released in time.
        """
        del request
        self._reached.set()
        if not self._released.wait(self._timeout_s):
            raise ContractViolation(
                f"a paused admission was not released within {self._timeout_s}s"
            )
        else:
            pass

    def wait_reached(self, timeout_s: float = 10.0) -> None:
        """Block until a thread arrives at the pause.

        :param timeout_s: How long to wait, in real seconds.
        :raises ~procrastinators.testing.violations.ContractViolation: No thread arrived in time.
        """
        if not self._reached.wait(timeout_s):
            raise ContractViolation(f"no admission reached the pause within {timeout_s}s")
        else:
            pass

    def release(self) -> None:
        """Let the held thread continue."""
        self._released.set()


if __name__ == "__main__":
    pass
else:
    pass
