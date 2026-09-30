"""A bounded, dedicated worker thread for storage that has no native async driver.

An asynchronous handle on a blocking store — SQLite, say — must not run that
store's calls on the event loop, and must not hide a whole acquire-and-sleep
loop in a thread either. :class:`DedicatedExecutor` runs **one storage
operation at a time** on its own thread; the caller's task awaits it and does
any quota sleeping itself, on the loop, holding nothing (contract W1, W4).

**Bounded.** At most ``max_pending`` operations are queued or running; one more
is refused with :exc:`~procrastinators.errors.BackendBusy` rather than queued
without limit, and an operation that could not *start* within its budget is
skipped with the same error, having done nothing (O2).

**Cancellation.** Cancelling the awaiting task cancels an operation that has
not started, which then never runs. One that has started is not interrupted:
it may still commit after its caller is gone. That is possible consumed
capacity, never permission to run the cancelled body (O5), and it is why
closing waits for every outstanding operation to settle (L4).

**Ownership.** Whatever the operations use — a connection, typically — lives
on the worker thread and belongs to whoever submits the work. The executor
owns only its thread. It starts that thread on first use, never at
construction, and belongs to the process that created it: a forked child's
copy refuses work, because the thread it would hand work to does not exist
there (L8).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import concurrent.futures
import os
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar

from procrastinators.errors import (
    BackendBusy,
    BackendUnavailable,
    ClosedResource,
    ConfigurationError,
)

if TYPE_CHECKING:
    from collections.abc import Callable
else:
    pass

__all__ = ["DEFAULT_MAX_PENDING", "DedicatedExecutor"]

DEFAULT_MAX_PENDING: Final = 64
"""Operations an executor holds, queued or running, before refusing more."""

_NANOS_PER_MICRO: Final = 1_000

ResultT = TypeVar("ResultT")
"""What an operation returns, and so what running it produces."""


@dataclass(frozen=True, slots=True)
class _Job(Generic[ResultT]):
    operation: Callable[[], ResultT]
    future: concurrent.futures.Future[ResultT]
    start_by_ns: int | None


class DedicatedExecutor:
    """One worker thread running one blocking storage operation at a time.

    :param name: Names the worker thread, for diagnostics.
    :param max_pending: Most operations queued or running at once.
    :raises ~procrastinators.errors.ConfigurationError: ``max_pending`` is not a positive integer.
    """

    def __init__(
        self, *, name: str = "procrastinators", max_pending: int = DEFAULT_MAX_PENDING
    ) -> None:
        if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1:
            raise ConfigurationError(f"max_pending must be a positive integer, got {max_pending!r}")
        else:
            pass
        self._name = name
        self._max_pending = max_pending
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._jobs: queue.SimpleQueue[_Job[Any] | None] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._stopped: concurrent.futures.Future[None] = concurrent.futures.Future()
        self._pending = 0
        self._running = False
        self._closed = False

    @property
    def outstanding(self) -> int:
        """Operations queued or running right now."""
        with self._lock:
            count = self._pending
        return count

    @property
    def running(self) -> bool:
        """Whether an operation has started and not yet finished.

        Once true for an operation, cancelling its caller can no longer stop it.
        """
        with self._lock:
            running = self._running
        return running

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` or :meth:`aclose` has been called."""
        return self._closed

    @property
    def on_worker(self) -> bool:
        """Whether the calling thread is this executor's worker."""
        on_worker = self._thread is not None and threading.current_thread() is self._thread
        return on_worker

    def submit(
        self,
        operation: Callable[[], ResultT],
        *,
        start_within_us: int | None = None,
        bounded: bool = True,
    ) -> concurrent.futures.Future[ResultT]:
        """Queue ``operation`` to run on the worker thread.

        :param operation: The blocking work; it runs exactly once, or never.
        :param start_within_us: Skip the operation, with
            :exc:`~procrastinators.errors.BackendBusy`, if it has not started this many
            microseconds after submission; ``None`` waits as long as it takes.
        :param bounded: Whether ``max_pending`` applies. Cleanup that must run
            however busy the executor is, such as closing a connection, passes ``False``.
        :returns: A future settled with the operation's result or exception, including
            :class:`BaseException` subclasses such as cancellation.
        :raises ~procrastinators.errors.ClosedResource: The executor was closed.
        :raises ~procrastinators.errors.BackendBusy: ``max_pending`` operations are outstanding.
        :raises ~procrastinators.errors.BackendUnavailable: This is a copy made by ``fork``.
        """
        if (pid := os.getpid()) != self._pid:
            raise BackendUnavailable(
                f"executor {self._name!r} was created in process {self._pid} and cannot serve "
                f"process {pid}: its worker thread does not exist after fork (L8)"
            )
        else:
            pass
        start_by_ns = (
            None
            if start_within_us is None
            else time.monotonic_ns() + start_within_us * _NANOS_PER_MICRO
        )
        future: concurrent.futures.Future[ResultT] = concurrent.futures.Future()
        with self._lock:
            if self._closed:
                raise ClosedResource(f"executor {self._name!r} is closed")
            elif bounded and self._pending >= self._max_pending:
                raise BackendBusy(
                    f"executor {self._name!r} already holds {self._max_pending} operations; "
                    "contention is not a denial (O2)"
                )
            elif self._thread is None:
                self._thread = threading.Thread(target=self._work, name=self._name, daemon=True)
                self._thread.start()
            else:
                pass
            self._pending += 1
            self._jobs.put(_Job(operation, future, start_by_ns))
        return future

    async def run(
        self,
        operation: Callable[[], ResultT],
        *,
        start_within_us: int | None = None,
        bounded: bool = True,
    ) -> ResultT:
        """Run ``operation`` on the worker thread and await its result.

        Cancelling the awaiting task cancels the operation only if it has not
        started; a started operation runs to completion regardless (O5).

        :param operation: The blocking work.
        :param start_within_us: As for :meth:`submit`.
        :param bounded: As for :meth:`submit`.
        :returns: What ``operation`` returned.
        :raises asyncio.CancelledError: The awaiting task was cancelled, or the operation
            itself raised it.
        """
        future = self.submit(operation, start_within_us=start_within_us, bounded=bounded)
        result = await asyncio.wrap_future(future)
        return result

    def _work(self) -> None:
        while (job := self._jobs.get()) is not None:
            if not job.future.set_running_or_notify_cancel():
                self._settled()
            elif job.start_by_ns is not None and time.monotonic_ns() > job.start_by_ns:
                self._settled()
                job.future.set_exception(
                    BackendBusy(
                        f"executor {self._name!r} could not start the operation within its lock "
                        "budget; nothing was done, and contention is not a denial (O2)"
                    )
                )
            else:
                with self._lock:
                    self._running = True
                try:
                    result = job.operation()
                except BaseException as error:
                    # Forwarded, never swallowed: the submitter decides what a
                    # cancellation or interrupt inside its operation means.
                    self._settled()
                    job.future.set_exception(error)
                else:
                    self._settled()
                    job.future.set_result(result)
        self._stopped.set_result(None)

    def _settled(self) -> None:
        # Counted as finished before its future settles, so a caller woken by
        # the result never finds the executor still full of it.
        with self._lock:
            self._running = False
            self._pending -= 1

    def _stop(self) -> concurrent.futures.Future[None] | None:
        """Refuse new work and ask the worker to finish; the future settles when it has."""
        with self._lock:
            first = not self._closed
            self._closed = True
            thread = self._thread
        if thread is None:
            stopped = None
        elif first:
            self._jobs.put(None)
            stopped = self._stopped
        else:
            stopped = self._stopped
        return stopped

    def close(self) -> None:
        """Refuse new work and block until every outstanding operation has settled.

        Idempotent (L1). Called from an operation running on the worker
        itself, it only refuses new work, since waiting there would never end.
        """
        stopped = self._stop()
        if stopped is not None and not self.on_worker and os.getpid() == self._pid:
            stopped.result()
        else:
            pass

    async def aclose(self) -> None:
        """Refuse new work and await every outstanding operation, without blocking the loop."""
        stopped = self._stop()
        if stopped is not None and os.getpid() == self._pid:
            await asyncio.wrap_future(stopped)
        else:
            pass

    def __repr__(self) -> str:
        text = f"DedicatedExecutor({self._name!r}, outstanding={self.outstanding})"
        return text


if __name__ == "__main__":
    pass
else:
    pass
