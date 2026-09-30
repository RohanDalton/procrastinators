"""The error taxonomy.

Every failure mode named in ``docs/source/contracts.md`` maps to exactly one class
here, because callers routinely need to treat them differently: a denial with a
retry delay is ordinary, a timeout is a scheduling problem, an indeterminate
commit is a correctness problem, and a policy conflict is an operator problem.

Two rules govern this module.

*Cancellation stays cancellation.* Nothing here derives from
:class:`BaseException`, and no part of the library converts
:class:`asyncio.CancelledError` or :class:`KeyboardInterrupt` into a
:class:`ProcrastinatorsError`. A cancelled caller must not be told its quota ran
out.

*Wrapping preserves the cause.* :class:`BackendError` takes a ``cause`` keyword
and sets ``__cause__`` from it, so a driver exception survives translation even
when the raise site forgets ``from``.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from procrastinators.models import DurationMicros, PolicyFingerprint, RuleId
else:
    pass

__all__ = [
    "AcquireTimeout",
    "BackendBusy",
    "BackendError",
    "BackendUnavailable",
    "ClosedResource",
    "ConfigurationError",
    "IndeterminateAdmission",
    "InvalidCost",
    "InvalidIdentity",
    "InvalidPolicy",
    "PolicyConflict",
    "ProcrastinatorsError",
    "StateCorruption",
    "UnsupportedCapability",
]


class ProcrastinatorsError(Exception):
    """Base class for every error this library raises deliberately."""


class ConfigurationError(ProcrastinatorsError):
    """Configuration could not be resolved into a usable limiter.

    Raised for unknown fields, unreadable or malformed layers, a selected
    profile that does not exist, and contradictory settings. Configuration is
    resolved before the first admission, so this is a construction-time error.
    """


class InvalidPolicy(ConfigurationError, ValueError):
    """A rate specification cannot be normalized into a valid policy.

    Covers non-integer or non-finite amounts, booleans supplied where a number
    is required, periods outside the supported range, and algorithm parameters
    that contradict each other (for example initial tokens above capacity).
    Also derives from :class:`ValueError`, which is what a caller validating
    user input usually expects to catch.
    """


class InvalidIdentity(ConfigurationError, ValueError):
    """A scope value cannot be encoded into a stable quota identity.

    Raised for values outside the restricted JSON-like set a key may be derived
    from — an object that would need an implicit ``str()``, a non-finite float,
    a mapping with non-string keys, an out-of-range integer — and for an HMAC
    secret too short to protect anything. Identity must mean the same thing in
    every process, so anything ambiguous is refused rather than guessed at.
    """


class InvalidCost(ProcrastinatorsError, ValueError):
    """A cost is not a positive integer, or can never be admitted.

    A cost larger than a rule's capacity is *impossible*, not merely denied: it
    would otherwise produce an unbounded retry delay. Costs are validated
    before any waiting begins.
    """


class PolicyConflict(ProcrastinatorsError):
    """Stored policy metadata disagrees with the policy this caller presented.

    Two workers using one quota identity must agree on the algorithm, its
    parameters, and the state version, and a composed request must not name one
    rule twice with different policies. Changing a rate keeps the quota
    identity and changes its fingerprint, so disagreement surfaces here rather
    than silently creating fresh capacity.

    Also raised when composed constraints declare incompatible coordination
    domains, which no single atomic admission can satisfy.

    :param message: Human-readable description of the disagreement.
    :param rule: The rule whose policies disagree, when one can be named.
    :param expected: Fingerprint of the policy this caller presented.
    :param found: Fingerprint of the conflicting policy, usually the one already
        stored for ``rule``.
    """

    def __init__(
        self,
        message: str,
        *,
        rule: RuleId | None = None,
        expected: PolicyFingerprint | None = None,
        found: PolicyFingerprint | None = None,
    ) -> None:
        super().__init__(message)
        self.rule = rule
        """The rule whose policies disagree, or ``None`` if none was named."""
        self.expected = expected
        """Fingerprint of the policy this caller presented, or ``None`` if unknown."""
        self.found = found
        """Fingerprint of the conflicting policy, or ``None`` if unknown."""


class UnsupportedCapability(ConfigurationError):
    """The requested combination is not implemented by the chosen backend.

    Algorithm/backend pairs, sync versus async modes, composition, cooldowns,
    and durability guarantees are all capabilities. Unsupported combinations
    are rejected at construction rather than degraded silently at runtime.
    """


class AcquireTimeout(ProcrastinatorsError, TimeoutError):
    """The caller's deadline expired before quota became available.

    This means *quota was not granted*: nothing was consumed by the attempt
    that timed out. Distinct from :class:`BackendUnavailable`, which means the
    storage authority could not answer at all.

    Also derives from :class:`TimeoutError`, so generic timeout handling still
    catches it.

    :param message: Human-readable description of the timeout.
    :param cost: The cost that was requested and not granted.
    :param waited_us: How long the caller waited before giving up.
    :param blocking: The rules still denying when the deadline expired. Stored
        as a tuple.
    :param retry_after_us: The last advisory retry delay the authority reported.
    """

    def __init__(
        self,
        message: str,
        *,
        cost: int | None = None,
        waited_us: DurationMicros | None = None,
        blocking: Sequence[RuleId] = tuple(),
        retry_after_us: DurationMicros | None = None,
    ) -> None:
        super().__init__(message)
        self.cost = cost
        """The cost that was requested and not granted, or ``None`` if unrecorded."""
        self.waited_us = waited_us
        """Microseconds the caller waited before giving up, or ``None`` if unrecorded."""
        self.blocking = tuple(blocking)
        """The rules still denying when the deadline expired; empty if unrecorded."""
        self.retry_after_us = retry_after_us
        """The last advisory retry delay in microseconds, or ``None`` if unrecorded.

        Advisory only (contract R4): another worker may consume the capacity first.
        """


class BackendError(ProcrastinatorsError):
    """Base class for failures of the storage authority itself.

    Never raised to report a denial: a backend that answers "no" has succeeded.

    :param message: Human-readable description of the failure.
    :param cause: The underlying driver exception. When given it becomes
        ``__cause__``, so it survives translation even when the raise site
        omits ``from`` (contract O6).
    """

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause
        else:
            pass


class BackendUnavailable(BackendError):
    """The storage authority could not be reached, or refused to serve.

    Connection failures, storage-call timeouts, authentication failures, and
    exhausted contention retries all arrive here. The library fails closed: it
    never falls back to private in-process state, because doing so would
    silently drop the coordination guarantee the caller asked for.
    """


class BackendBusy(BackendError):
    """Contention prevented the atomic operation within its budget.

    Lock waits, ``SQLITE_BUSY``, and serialization failures are contention, not
    quota exhaustion, and must never be reported as a denial. Bounded retries
    happen inside the budget before this is raised.
    """


class IndeterminateAdmission(BackendError):
    """The commit may or may not have happened; the outcome is unknown.

    A lost response, a cancellation after a write was dispatched, or a worker
    killed mid-commit produces this. Quota may have been consumed. The library
    does not run the caller's body and does not refund: refunding a commit that
    did happen would hand out capacity the policy never granted.

    :param message: Human-readable description of the failure.
    :param cause: The underlying driver exception, set as ``__cause__`` as for
        :class:`BackendError`.
    :param cost: The cost whose commit is in doubt.
    :param rules: The rules that may have been charged. Stored as a tuple.
    """

    def __init__(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
        cost: int | None = None,
        rules: Sequence[RuleId] = tuple(),
    ) -> None:
        super().__init__(message, cause=cause)
        self.cost = cost
        """The cost whose commit is in doubt, or ``None`` if unrecorded."""
        self.rules = tuple(rules)
        """The rules that may have been charged; empty if unrecorded."""


class StateCorruption(BackendError):
    """Stored state could not be decoded, or decoded to an impossible value.

    Codecs fail closed: unknown versions, truncated payloads, and out-of-range
    values are errors, never an implicit reset to an empty bucket.
    """


class ClosedResource(ProcrastinatorsError):
    """The limiter or backend was closed, and this call arrived afterwards.

    Closing is idempotent; *using* a closed resource is not allowed. Raised
    rather than reopening, so a worker that closed too early fails loudly
    instead of quietly coordinating with nobody.
    """


if __name__ == "__main__":
    pass
else:
    pass
