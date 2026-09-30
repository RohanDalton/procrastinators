"""Base classes for backend implementations.

Inheriting is optional: :class:`~procrastinators.protocols.SyncBackend` and
:class:`~procrastinators.protocols.AsyncBackend` are structural, and a
third-party store that satisfies one is a first-class backend.

These classes supply the lifecycle bookkeeping every backend repeats — an
idempotent close, a closed check, capability validation of an incoming
request, and reporting to an optional
:class:`~procrastinators.protocols.AdmissionObserver` — and nothing else.

**There is deliberately no generic ``admit`` here.** A base implementation that
read state, called an evaluator, and wrote the result back would be a
non-atomic read-modify-write, and every backend inheriting it would advertise a
guarantee it did not have. That sequence is exactly the flaw the design
identified in an existing library's SQLite bucket. Atomicity cannot be
inherited; each backend earns it with a transaction, a lock, a script, or a
compare-and-swap loop, and ``admit`` therefore stays abstract.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from abc import ABC, abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, ClassVar

from procrastinators.capabilities import CapabilityRequirement, Mode, require_capabilities
from procrastinators.errors import (
    BackendUnavailable,
    ClosedResource,
    IndeterminateAdmission,
    ProcrastinatorsError,
)
from procrastinators.models import ResourceOwnership
from procrastinators.protocols import ObservationPoint

if TYPE_CHECKING:
    from collections.abc import Sequence

    from procrastinators.models import (
        AdmissionRequest,
        BackendIdentity,
        Capabilities,
        Decision,
        RuleId,
        Snapshot,
    )
    from procrastinators.protocols import AdmissionObserver
else:
    pass

__all__ = ["BaseAsyncBackend", "BaseSyncBackend"]


class _BackendLifecycle(ABC):
    """Shared identity, capability validation, observation, and close bookkeeping."""

    _mode: ClassVar[Mode]
    _observer: AdmissionObserver | None = None

    @property
    @abstractmethod
    def capabilities(self) -> Capabilities:
        """What this backend implements."""

    @property
    @abstractmethod
    def identity(self) -> BackendIdentity:
        """Which authority and namespace this handle addresses."""

    @property
    def ownership(self) -> ResourceOwnership:
        """Who closes the resources this backend holds.

        Defaults to owning them. A backend given an injected client must
        override this, because closing a caller's connection pool on their
        behalf is not a courtesy (contract L2).
        """
        ownership = ResourceOwnership()
        return ownership

    _closed: bool = False

    @property
    def closed(self) -> bool:
        """Whether this backend has been closed. Once ``True``, it stays ``True``."""
        return self._closed

    def _mark_closed(self) -> bool:
        """Record the close. Returns ``False`` if it had already happened.

        Closing is idempotent (contract L1), so a subclass releases its
        resources only on the first call and returns quietly afterwards.
        """
        if self._closed:
            newly_closed = False
        else:
            self._closed = True
            newly_closed = True
        return newly_closed

    def _ensure_open(self) -> None:
        """Raise if this backend was closed.

        Contract L5: a closed resource fails loudly rather than reopening, so a
        worker that closed too early does not quietly coordinate with nobody.
        """
        if self._closed:
            raise ClosedResource(f"{type(self).__name__} for {self.identity} is closed")
        else:
            pass

    def validate_request(self, request: AdmissionRequest) -> None:
        """Reject a request this backend cannot serve atomically.

        Checked before any storage work, so an unsupported combination fails
        the same way every time instead of part-way through a transaction.

        :param request: The admission request about to be served.
        :raises ~procrastinators.errors.UnsupportedCapability: An algorithm, an interface, a
            composition, or a coordination domain this backend does not implement.
        """
        require_capabilities(
            self.capabilities,
            CapabilityRequirement.for_request(request, mode=self._mode),
            backend=self.identity,
        )

    @property
    def observer(self) -> AdmissionObserver | None:
        """The hook called at each observation point, or ``None``.

        Set by a subclass that accepts one, normally for deterministic tests.
        """
        return self._observer

    def _observe(self, point: ObservationPoint, request: AdmissionRequest) -> None:
        """Report ``point`` to the observer, translating what it raises.

        A subclass calls this at each
        :class:`~procrastinators.protocols.ObservationPoint` it reaches. An
        exception before the commit becomes a storage failure that committed
        nothing; one at ``AFTER_COMMIT`` becomes an indeterminate admission,
        because the debit may already be durable. Cancellation and other
        non-:class:`Exception` errors propagate unchanged (contract O5).

        :param point: Where in the admission sequence the backend is.
        :param request: The request being admitted.
        :raises ~procrastinators.errors.BackendUnavailable: The observer failed before the commit.
        :raises ~procrastinators.errors.IndeterminateAdmission: The observer failed after it.
        """
        if (observer := self._observer) is None:
            pass
        elif point is ObservationPoint.AFTER_COMMIT:
            try:
                observer(point, request)
            except IndeterminateAdmission:
                raise
            except Exception as error:
                raise IndeterminateAdmission(
                    f"{self.identity} failed after committing; the admission may have happened",
                    cause=error,
                    cost=request.cost,
                    rules=request.rules,
                ) from error
        else:
            try:
                observer(point, request)
            except ProcrastinatorsError:
                raise
            except Exception as error:
                raise BackendUnavailable(
                    f"{self.identity} failed at {point.value}; nothing was committed",
                    cause=error,
                ) from error


class BaseSyncBackend(_BackendLifecycle, ABC):
    """Optional base for a synchronous admission authority.

    Satisfies :class:`~procrastinators.protocols.SyncBackend` once the abstract
    members are implemented. Subclasses provide ``capabilities``, ``identity``,
    :meth:`admit`, :meth:`inspect` and :meth:`close`, and inherit an
    ``ownership`` that defaults to owning every resource, a ``closed`` flag, and
    ``validate_request()``.

    Used as a context manager, entering raises
    :exc:`~procrastinators.errors.ClosedResource` if the backend is already
    closed, and exiting closes it.
    """

    _mode = Mode.SYNC

    @abstractmethod
    def admit(self, request: AdmissionRequest) -> Decision:
        """Atomically check every constraint and commit every debit, or none.

        See :meth:`procrastinators.protocols.SyncBackend.admit` for the full
        contract, including the errors an implementation raises.
        Implementations should call ``_ensure_open()`` and
        ``validate_request()`` before doing any storage work, sample authority
        time *after* acquiring locks, and commit before reporting success.

        :param request: The constraints to admit together, the cost, and the
            caller's operation budget.
        :returns: The decision. A denial is a returned value; only failures
            raise.
        """

    @abstractmethod
    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory, non-committing observation. Never a reservation.

        Contract R7: acting on the result without acquiring is a misuse,
        because another worker may consume everything it reports.

        :param rules: The rules to observe.
        """

    @abstractmethod
    def close(self) -> None:
        """Release owned resources. Must be idempotent via ``_mark_closed()``.

        Must close only what ``ownership`` says this backend owns (contract L2)
        and never deletes quota state (contract L3). Closing twice is not an
        error (contract L1).
        """

    def __enter__(self) -> BaseSyncBackend:
        """Return this backend, for use in a ``with`` block.

        :raises ~procrastinators.errors.ClosedResource: The backend was already
            closed.
        """
        self._ensure_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the backend.

        Distinct from entering a *limiter* as a context manager, which acquires
        quota and must not close storage on exit (contract L6). Any exception
        from the ``with`` block propagates unchanged.

        :param exc_type: The exception type raised in the block, or ``None``.
        :param exc: The exception raised in the block, or ``None``.
        :param traceback: Its traceback, or ``None``.
        """
        self.close()


class BaseAsyncBackend(_BackendLifecycle, ABC):
    """Optional base for an asynchronous admission authority.

    Satisfies :class:`~procrastinators.protocols.AsyncBackend` once the abstract
    members are implemented. Subclasses provide ``capabilities``, ``identity``,
    :meth:`admit`, :meth:`inspect` and :meth:`aclose`, and inherit an
    ``ownership`` that defaults to owning every resource, a ``closed`` flag, and
    ``validate_request()``.

    Used as an asynchronous context manager, entering raises
    :exc:`~procrastinators.errors.ClosedResource` if the backend is already
    closed, and exiting closes it.
    """

    _mode = Mode.ASYNC

    @abstractmethod
    async def admit(self, request: AdmissionRequest) -> Decision:
        """Atomically check every constraint and commit every debit, or none.

        Cancellation before the commit consumes nothing; cancellation after it
        may have consumed capacity and is surfaced, never refunded (O5).

        See :meth:`procrastinators.protocols.AsyncBackend.admit` for the full
        contract, including the errors an implementation raises. As with
        :meth:`BaseSyncBackend.admit`, implementations should call
        ``_ensure_open()`` and ``validate_request()`` before any storage work.

        :param request: The constraints to admit together, the cost, and the
            caller's operation budget.
        :returns: The decision. A denial is a returned value; only failures
            raise.
        """

    @abstractmethod
    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory, non-committing observation. Never a reservation.

        Contract R7: acting on the result without acquiring is a misuse,
        because another worker may consume everything it reports.

        :param rules: The rules to observe.
        """

    @abstractmethod
    async def aclose(self) -> None:
        """Release owned resources, after outstanding owned work settles (L4).

        Must be idempotent via ``_mark_closed()`` (contract L1), close only what
        ``ownership`` says this backend owns (contract L2), and never delete
        quota state (contract L3).
        """

    async def __aenter__(self) -> BaseAsyncBackend:
        """Return this backend, for use in an ``async with`` block.

        :raises ~procrastinators.errors.ClosedResource: The backend was already
            closed.
        """
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the backend by awaiting :meth:`aclose`.

        Distinct from entering a *limiter* as a context manager, which acquires
        quota and must not close storage on exit (contract L6). Any exception
        from the ``async with`` block propagates unchanged.

        :param exc_type: The exception type raised in the block, or ``None``.
        :param exc: The exception raised in the block, or ``None``.
        :param traceback: Its traceback, or ``None``.
        """
        await self.aclose()


if __name__ == "__main__":
    pass
else:
    pass
