"""The public facade: :class:`~procrastinators.limiter.RateLimiter` and its invocations.

A limiter is an immutable description — which constraints, which backend,
which default budgets — plus the waiting rules of :mod:`procrastinators.waiting`.
It holds no "current admission" (L7): every acquisition returns its own
:class:`~procrastinators.models.Admission`, so concurrent and reentrant use
cannot overwrite another call's result. Entering it as a context manager
acquires; leaving refunds nothing and closes nothing (A3, L6).

Everything is checked when the limiter is built: policies, rule identities,
the backend's capabilities for every mode it offers, and, for
:meth:`RateLimiter.combine`, that every part addresses one authority with
compatible constraints (Y2, C6). Construction opens no connection and starts
no thread.

``docs/source/api.md`` specifies this surface; ``docs/source/contracts.md`` the
behavior behind it.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import datetime as dt
import functools
import inspect
import os
import types
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, ParamSpec, TypeAlias, TypeVar, cast, overload

from procrastinators.builtins import default_registry
from procrastinators.capabilities import Mode, require_same_authority
from procrastinators.clocks import MonotonicClock, budget_for_timeout, seconds_to_micros
from procrastinators.config import FIELDS, resolve
from procrastinators.config_models import DEFAULT_BACKEND
from procrastinators.errors import (
    ClosedResource,
    ConfigurationError,
    InvalidPolicy,
    UnsupportedCapability,
)
from procrastinators.keys import scope_constraints
from procrastinators.models import (
    AdmissionRequest,
    CooldownEvent,
    Limit,
    PolicySpec,
    QuotaIdentity,
    canonical_constraints,
)
from procrastinators.policies import normalize_limit
from procrastinators.protocols import (
    AsyncBackend,
    SupportsAsyncCooldown,
    SupportsCooldown,
    SyncBackend,
)
from procrastinators.waiting import AsyncioSleeper, ThreadSleeper, Waiter, deliver, run, run_async

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from procrastinators.config import ConfigLocations
    from procrastinators.config_models import ResolvedConfig
    from procrastinators.models import (
        Admission,
        Algorithms,
        BackendIdentity,
        Constraint,
        Cooldown,
        Decision,
        OperationBudget,
        RuleId,
        Snapshot,
    )
    from procrastinators.protocols import (
        AsyncSleeper,
        DeadlineClock,
        DiagnosticsCallback,
        Sleeper,
    )
    from procrastinators.registry import Registry
else:
    pass

__all__ = ["Invocation", "RateLimiter", "Seconds"]

P = ParamSpec("P")
"""The parameters of a decorated function, which its wrapper keeps."""

R = TypeVar("R")
"""The return type of a decorated function, which its wrapper keeps."""

Seconds: TypeAlias = float | int | str | dt.timedelta
"""A public duration: seconds as a number, a string such as ``"250ms"``, or a timedelta."""

DEFAULT_NAMESPACE: Final = "default"
"""The quota namespace a limiter uses unless told otherwise."""


@dataclass(frozen=True, slots=True)
class _Handles:
    """The backend handles a limiter uses, one per mode it offers."""

    sync: SyncBackend | None
    async_: AsyncBackend | None
    owned: bool
    """Whether the limiter created them, and so closes them (L2)."""

    @property
    def identities(self) -> tuple[BackendIdentity, ...]:
        identities = tuple(
            handle.identity for handle in (self.sync, self.async_) if handle is not None
        )
        return identities


def _policy(
    entry: object,
    algorithm: Algorithms | str | None,
    options: Mapping[str, object] | None,
) -> PolicySpec:
    """One entry of ``limits`` or ``rules``: a public rate to normalize, or a policy as given."""
    if isinstance(entry, Limit):
        policy: PolicySpec = normalize_limit(entry, algorithm or "sliding_log", options)
    elif isinstance(entry, PolicySpec):
        if algorithm is not None and entry.algorithm != algorithm:
            raise InvalidPolicy(
                f"a {entry.algorithm} policy was given with algorithm={algorithm!s}; a policy "
                "object already names its algorithm"
            )
        elif options:
            raise InvalidPolicy("options apply only to Limit entries, not to policy objects")
        else:
            policy = entry
    else:
        raise InvalidPolicy(
            f"expected a Limit or a policy object, got {type(entry).__name__}: {entry!r}"
        )
    return policy


def _scope_constraints(
    scope: QuotaIdentity,
    limits: Sequence[Limit | PolicySpec] | None,
    rules: Mapping[str, Limit | PolicySpec] | None,
    algorithm: Algorithms | str | None,
    options: Mapping[str, object] | None,
) -> tuple[Constraint, ...]:
    if limits is not None and rules is not None:
        raise ConfigurationError("pass limits= or rules=, not both")
    elif limits is None and rules is None:
        raise ConfigurationError(
            "no limits: pass limits= (positional rules) or rules= (named rules); there is no "
            "default vendor rate"
        )
    elif rules is not None:
        if not isinstance(rules, Mapping) or not rules:
            raise ConfigurationError("rules= must be a non-empty mapping of rule name to limit")
        else:
            pass
        policies: Sequence[PolicySpec] | Mapping[str, PolicySpec] = {
            name: _policy(entry, algorithm, options) for name, entry in rules.items()
        }
    else:
        assert limits is not None
        if isinstance(limits, (str, bytes, Mapping)) or not isinstance(limits, Sequence):
            raise ConfigurationError(
                "limits= must be a sequence of limits; use rules= to name them"
            )
        elif not limits:
            raise ConfigurationError("limits= must not be empty")
        else:
            pass
        policies = [_policy(entry, algorithm, options) for entry in limits]
    constraints = scope_constraints(scope, policies)
    return constraints


def _is_async_backend(backend: object) -> bool:
    admit = getattr(backend, "admit", None)
    is_async = inspect.iscoroutinefunction(admit)
    return is_async


def _resolve_backend(
    backend: str | SyncBackend | AsyncBackend | None, registry: Registry, namespace: str
) -> _Handles:
    """The handles a ``backend`` argument names: created from an address, or borrowed.

    ``None`` is the shared SQLite file at the stable per-user state path, so
    processes coordinate with nothing configured. Memory is never the silent
    default: it coordinates nothing beyond one process.
    """
    backend = DEFAULT_BACKEND if backend is None else backend
    if isinstance(backend, str):
        if not (family := urllib.parse.urlsplit(backend).scheme):
            raise ConfigurationError(f"backend address {backend!r} names no backend family")
        else:
            pass
        spec = registry.backend_spec(family)
        algorithms = tuple(registry.algorithm(name) for name in sorted(registry.algorithm_ids))
        sync = async_ = None
        if spec.capabilities.supports_sync:
            sync = spec.factory(backend, mode=Mode.SYNC, namespace=namespace, algorithms=algorithms)
        else:
            pass
        if spec.capabilities.supports_async:
            async_ = spec.factory(
                backend, mode=Mode.ASYNC, namespace=namespace, algorithms=algorithms
            )
        else:
            pass
        handles = _Handles(
            cast("SyncBackend | None", sync), cast("AsyncBackend | None", async_), True
        )
    elif _is_async_backend(backend):
        handles = _Handles(None, cast("AsyncBackend", backend), owned=False)
    elif isinstance(backend, SyncBackend):
        handles = _Handles(backend, None, owned=False)
    else:
        raise ConfigurationError(
            f"backend must be an address or a backend object, got {type(backend).__name__}"
        )
    return handles


def _decorate(
    limiter: RateLimiter,
    func: Callable[P, R],
    cost: int,
    timeout: Seconds | types.EllipsisType | None,
) -> Callable[P, R]:
    """Wrap ``func`` so each call acquires first, on the path matching the function."""
    if inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func):
        raise UnsupportedCapability(
            f"cannot rate-limit generator {getattr(func, '__qualname__', func)!r}: creating a "
            "generator and iterating it happen at different times, so acquire inside the loop, "
            "around each actual request"
        )
    elif inspect.iscoroutinefunction(func):
        coroutine = cast("Callable[P, Awaitable[object]]", func)

        @functools.wraps(func)
        async def limited_coroutine(*args: P.args, **kwargs: P.kwargs) -> object:
            await limiter.acquire_async(cost, timeout)
            result = await coroutine(*args, **kwargs)
            return result

        wrapper = cast("Callable[P, R]", limited_coroutine)
    else:

        @functools.wraps(func)
        def limited(*args: P.args, **kwargs: P.kwargs) -> R:
            limiter.acquire(cost, timeout)
            result = func(*args, **kwargs)
            return result

        wrapper = limited
    return wrapper


@dataclass(frozen=True, slots=True)
class Invocation:
    """A limiter with per-call options: a context manager, async context manager, or decorator.

    Returned by ``limiter(cost=..., timeout=...)``. Immutable, and leaves the
    limiter unchanged::

        with limiter(cost=5, timeout=30):
            fetch_batch()

        @limiter(cost=5)
        def fetch_batch(): ...
    """

    limiter: RateLimiter
    """The limiter to acquire from."""

    cost: int = 1
    """The cost of each acquisition."""

    timeout: Seconds | types.EllipsisType | None = ...
    """The quota-wait budget, or ``...`` for the limiter's default."""

    def __enter__(self) -> Admission:
        """Acquire and return the admission."""
        admission = self.limiter.acquire(self.cost, self.timeout)
        return admission

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Nothing: quota is not refunded and storage is not closed (A3, L6)."""

    async def __aenter__(self) -> Admission:
        """Acquire asynchronously and return the admission."""
        admission = await self.limiter.acquire_async(self.cost, self.timeout)
        return admission

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Nothing: quota is not refunded and storage is not closed (A3, L6)."""

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Decorate ``func`` to acquire with these options before every call.

        :param func: A function or coroutine function; not a generator.
        :raises ~procrastinators.errors.UnsupportedCapability: ``func`` is a generator or
            async generator function.
        """
        wrapper = _decorate(self.limiter, func, self.cost, self.timeout)
        return wrapper


class RateLimiter:
    """Acquire quota before doing work, sync or async, alone or composed.

    ::

        limiter = RateLimiter(
            key=idempotent_key({"vendor": "ankh", "dataset": "orders"}),
            limits=[Limit(10, per="1s"), Limit(500, per="1m")],
            backend="memory://",
        )

        with limiter as admission:
            fetch_page()

    :param key: Stable quota key, normally from
        :func:`~procrastinators.keys.idempotent_key`. With ``namespace`` it forms
        the quota identity (I2).
    :param limits: The scope's rates, as :class:`~procrastinators.models.Limit`
        values or policy objects. Positional rule ids ``#0``, ``#1``, … follow
        list order, so reordering is a policy conflict (I3).
    :param rules: Explicitly named rules instead of positional ones; mutually
        exclusive with ``limits``. Recommended for managed configuration.
    :param algorithm: The algorithm every ``Limit`` is enforced with; the
        sliding log when ``None``. A policy object names its own.
    :param backend: An address such as ``"memory://"`` or
        ``"sqlite:///quota.sqlite3"``, or a backend object. An address is resolved
        through ``registry`` and its handles are owned and closed by this
        limiter; an object is borrowed and never closed (L2). ``None`` is
        ``"sqlite://"``: the shared per-user database file.
    :param namespace: The quota namespace (I1).
    :param options: Algorithm options applied to every ``Limit``, as
        :data:`~procrastinators.policies.OPTIONS_BY_ALGORITHM` lists.
    :param timeout: Default quota-wait budget in seconds: ``None`` waits
        indefinitely, ``0`` attempts once, a positive value bounds everything (B2).
    :param storage_timeout: Cap on each storage call, in seconds; never ``None`` (B3).
    :param accept_best_effort: Accept a backend whose state may vanish through
        eviction, such as Memcached. Never implied: a cache miss is not proof
        that no quota was consumed (Y4).
    :param on_event: Receives diagnostics outside critical sections (D2).
    :param registry: Resolves algorithm ids and backend families; the process
        default when ``None``.
    :param clock: Local monotonic time for deadlines; the real one when ``None``.
    :param sleeper: How synchronous acquisitions wait; real sleeps when ``None``.
    :param async_sleeper: How asynchronous acquisitions wait; ``asyncio.sleep`` when ``None``.
    :raises ~procrastinators.errors.ConfigurationError: The arguments cannot produce a limiter.
    :raises ~procrastinators.errors.InvalidPolicy: A limit, option, key, or timeout is invalid.
    :raises ~procrastinators.errors.UnsupportedCapability: The backend cannot serve these
        constraints in any mode it offers, or an algorithm or backend family is not registered.
    """

    def __init__(
        self,
        *,
        key: str,
        limits: Sequence[Limit | PolicySpec] | None = None,
        rules: Mapping[str, Limit | PolicySpec] | None = None,
        algorithm: Algorithms | str | None = None,
        backend: str | SyncBackend | AsyncBackend | None = None,
        namespace: str = DEFAULT_NAMESPACE,
        options: Mapping[str, object] | None = None,
        timeout: Seconds | None = None,
        storage_timeout: Seconds = 5.0,
        accept_best_effort: bool = False,
        on_event: DiagnosticsCallback | None = None,
        registry: Registry | None = None,
        clock: DeadlineClock | None = None,
        sleeper: Sleeper | None = None,
        async_sleeper: AsyncSleeper | None = None,
    ) -> None:
        chosen = registry or default_registry()
        constraints = _scope_constraints(
            QuotaIdentity(namespace, key), limits, rules, algorithm, options
        )
        self._setup(
            constraints=constraints,
            handles=_resolve_backend(backend, chosen, namespace),
            registry=chosen,
            timeout=timeout,
            storage_timeout=storage_timeout,
            accept_best_effort=accept_best_effort,
            on_event=on_event,
            clock=clock or MonotonicClock(),
            sleeper=sleeper or ThreadSleeper(),
            async_sleeper=async_sleeper or AsyncioSleeper(),
        )

    def _setup(
        self,
        *,
        constraints: tuple[Constraint, ...],
        handles: _Handles,
        registry: Registry,
        timeout: Seconds | None,
        storage_timeout: Seconds,
        accept_best_effort: bool,
        on_event: DiagnosticsCallback | None,
        clock: DeadlineClock,
        sleeper: Sleeper,
        async_sleeper: AsyncSleeper,
    ) -> None:
        """Validate everything and become usable; shared by ``__init__`` and :meth:`combine`."""
        for constraint in constraints:
            registry.algorithm(constraint.algorithm).validate(constraint.policy)
        for mode, handle in ((Mode.SYNC, handles.sync), (Mode.ASYNC, handles.async_)):
            if handle is not None:
                registry.validate(
                    handle.capabilities,
                    constraints,
                    mode=mode,
                    backend=handle.identity,
                    accept_best_effort=accept_best_effort,
                )
            else:
                pass
        require_same_authority(handles.identities)
        # Built once to reject a malformed timeout at construction, not at first use.
        budget_for_timeout(timeout, clock=clock, storage_timeout=storage_timeout)
        self._constraints = constraints
        self._handles = handles
        self._registry = registry
        self._timeout = timeout
        self._storage_timeout = storage_timeout
        self._accept_best_effort = accept_best_effort
        self._on_event = on_event
        self._clock = clock
        self._sleeper = sleeper
        self._async_sleeper = async_sleeper
        self._waiter = Waiter(clock=clock, on_event=on_event)
        self._closed = False
        self._config: ResolvedConfig | None = None

    @classmethod
    def from_config(
        cls,
        profile: str,
        *,
        key: str,
        project: str | None = None,
        organization: str | None = None,
        project_file: str | os.PathLike[str] | None = None,
        organization_file: str | os.PathLike[str] | None = None,
        environ: Mapping[str, str] | None = None,
        locations: ConfigLocations | None = None,
        accept_best_effort: bool = False,
        on_event: DiagnosticsCallback | None = None,
        registry: Registry | None = None,
        clock: DeadlineClock | None = None,
        sleeper: Sleeper | None = None,
        async_sleeper: AsyncSleeper | None = None,
        **overrides: object,
    ) -> RateLimiter:
        """A limiter for ``profile``, its settings resolved through every configuration layer.

        Precedence, strongest first: ``overrides``, ``PROCRASTINATORS_*``
        environment defaults, the selected project, the selected organization,
        and library defaults (G1). Resolution happens once; the limiter never
        re-reads configuration (G6), and :attr:`config` explains where each
        setting came from (G7). ``key`` is always explicit, so a changed
        configuration meets the stored policy as a
        :exc:`~procrastinators.errors.PolicyConflict`, never as fresh quota (G8)::

            limiter = RateLimiter.from_config(
                "ankh.orders", key=idempotent_key({"vendor": "ankh"}), project="etl"
            )

        :param profile: The profile to resolve, such as ``"ankh.orders"``.
        :param key: The stable quota key.
        :param project: The project id; else ``PROCRASTINATORS_PROJECT``.
        :param organization: The organization id; else ``PROCRASTINATORS_ORGANIZATION``.
        :param project_file: An explicit project file, possibly a ``pyproject.toml``.
        :param organization_file: An explicit organization file.
        :param environ: The environment; :data:`os.environ` when ``None``.
        :param locations: Where configuration files live; PlatformDirs when ``None``.
        :param accept_best_effort: Accept a best-effort backend such as Memcached (Y4);
            an argument, never a configuration field, so no file can accept it on a
            caller's behalf.
        :param on_event: Receives diagnostics outside critical sections (D2).
        :param registry: Resolves algorithm ids and backend families.
        :param clock: Local monotonic time for deadlines.
        :param sleeper: How synchronous acquisitions wait.
        :param async_sleeper: How asynchronous acquisitions wait.
        :param overrides: Configuration fields passed explicitly: ``limits``, ``rules``,
            ``algorithm``, ``backend``, ``namespace``, ``options``, ``timeout``,
            ``storage_timeout``.
        :raises ~procrastinators.errors.ConfigurationError: An unknown override, an invalid layer,
            or a profile that supplies no limits.
        :raises ~procrastinators.errors.UnsupportedCapability: The resolved backend cannot serve
            the resolved constraints.
        """
        if unknown := set(overrides) - FIELDS:
            raise ConfigurationError(
                f"unknown configuration fields {sorted(unknown)}; configurable fields are "
                f"{sorted(FIELDS)}"
            )
        else:
            pass
        resolved = resolve(
            profile,
            project=project,
            organization=organization,
            project_file=project_file,
            organization_file=organization_file,
            arguments=overrides,
            environ=environ,
            locations=locations,
        )
        settings = resolved.settings
        limiter = cls(
            key=key,
            limits=settings.limits or None,
            rules=settings.rules,
            algorithm=settings.algorithm,
            backend=settings.backend,
            namespace=settings.namespace,
            options=settings.options,
            timeout=settings.timeout,
            storage_timeout=settings.storage_timeout,
            accept_best_effort=accept_best_effort,
            on_event=on_event,
            registry=registry,
            clock=clock,
            sleeper=sleeper,
            async_sleeper=async_sleeper,
        )
        limiter._config = resolved
        return limiter

    @classmethod
    def combine(
        cls,
        *limiters: RateLimiter,
        timeout: Seconds | types.EllipsisType | None = ...,
        storage_timeout: Seconds | types.EllipsisType = ...,
        accept_best_effort: bool | types.EllipsisType = ...,
        on_event: DiagnosticsCallback | types.EllipsisType | None = ...,
    ) -> RateLimiter:
        """A limiter admitting every constraint of ``limiters`` in one atomic admission (C1).

        Identical constraints deduplicate (C2); a rule named twice with
        different policies is a :exc:`~procrastinators.errors.PolicyConflict`
        (C3), as are incompatible coordination domains (C4). Every limiter must
        address one authority (C6). All of this is checked here, not at the
        first acquisition.

        The combined limiter borrows the parts' backends: closing it closes
        nothing of theirs. Its defaults are the first limiter's unless given.

        :param limiters: At least one limiter.
        :param timeout: Default quota-wait budget; the first limiter's when omitted.
        :param storage_timeout: Per-call storage cap; the first limiter's when omitted.
        :param accept_best_effort: Whether a best-effort backend is acceptable; only when
            every part accepted it, when omitted.
        :param on_event: Diagnostics callback; the first limiter's when omitted.
        :raises ~procrastinators.errors.ConfigurationError: No limiters were given.
        :raises ~procrastinators.errors.PolicyConflict: Constraints conflict.
        :raises ~procrastinators.errors.UnsupportedCapability: The limiters address different
            authorities, or the backend cannot compose these constraints.
        :raises ~procrastinators.errors.ClosedResource: A limiter was closed.
        """
        if not limiters:
            raise ConfigurationError("combine needs at least one limiter")
        else:
            pass
        for limiter in limiters:
            limiter._ensure_open()
        first = limiters[0]
        require_same_authority(
            identity for limiter in limiters for identity in limiter._handles.identities
        )
        sync = next((limiter._handles.sync for limiter in limiters if limiter._handles.sync), None)
        async_ = next(
            (limiter._handles.async_ for limiter in limiters if limiter._handles.async_), None
        )
        combined = cls.__new__(cls)
        combined._setup(
            constraints=canonical_constraints(
                [constraint for limiter in limiters for constraint in limiter._constraints]
            ),
            handles=_Handles(sync, async_, owned=False),
            registry=first._registry,
            timeout=first._timeout if isinstance(timeout, types.EllipsisType) else timeout,
            storage_timeout=(
                first._storage_timeout
                if isinstance(storage_timeout, types.EllipsisType)
                else storage_timeout
            ),
            accept_best_effort=(
                all(limiter._accept_best_effort for limiter in limiters)
                if isinstance(accept_best_effort, types.EllipsisType)
                else accept_best_effort
            ),
            on_event=first._on_event if isinstance(on_event, types.EllipsisType) else on_event,
            clock=first._clock,
            sleeper=first._sleeper,
            async_sleeper=first._async_sleeper,
        )
        return combined

    @property
    def config(self) -> ResolvedConfig | None:
        """The resolved configuration a :meth:`from_config` limiter was built from, else ``None``.

        ``config.explain()`` says which layer supplied each setting, with
        secrets redacted (G7).
        """
        return self._config

    @property
    def constraints(self) -> tuple[Constraint, ...]:
        """Every constraint an acquisition admits together, in canonical order."""
        return self._constraints

    @property
    def rules(self) -> tuple[RuleId, ...]:
        """The rule of each constraint, in canonical order."""
        rules = tuple(constraint.rule for constraint in self._constraints)
        return rules

    @property
    def backend(self) -> BackendIdentity:
        """The identity of the authority this limiter admits against."""
        return self._handles.identities[0]

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` or :meth:`aclose` has been called."""
        return self._closed

    def _ensure_open(self) -> None:
        if self._closed:
            raise ClosedResource(f"{self!r} is closed")
        else:
            pass

    def _sync_backend(self) -> SyncBackend:
        self._ensure_open()
        if (handle := self._handles.sync) is None:
            raise UnsupportedCapability(
                f"{self.backend} offers no synchronous interface; use the async methods"
            )
        else:
            pass
        return handle

    def _async_backend(self) -> AsyncBackend:
        self._ensure_open()
        if (handle := self._handles.async_) is None:
            raise UnsupportedCapability(
                f"{self.backend} offers no asynchronous interface; wrapping blocking calls "
                "would block the event loop, so use the sync methods"
            )
        else:
            pass
        return handle

    def _budget(self, timeout: Seconds | types.EllipsisType | None) -> OperationBudget:
        budget = budget_for_timeout(
            self._timeout if isinstance(timeout, types.EllipsisType) else timeout,
            clock=self._clock,
            storage_timeout=self._storage_timeout,
        )
        return budget

    def _request(self, cost: int, timeout: Seconds | types.EllipsisType | None) -> AdmissionRequest:
        # Constructing the request validates the cost against every rule's
        # capacity, before any waiting (P3).
        request = AdmissionRequest(self._constraints, cost, self._budget(timeout))
        return request

    def acquire(
        self, cost: int = 1, timeout: Seconds | types.EllipsisType | None = ...
    ) -> Admission:
        """Wait for capacity, then return proof of one committed acquisition.

        :param cost: A positive integer within every rule's capacity.
        :param timeout: Quota-wait budget in seconds; the limiter's default when omitted.
        :returns: The admission. It has been charged and is never refunded.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` is malformed or above capacity.
        :raises ~procrastinators.errors.AcquireTimeout: The deadline passed; nothing consumed.
        :raises ~procrastinators.errors.PolicyConflict: Stored policy disagrees with this one.
        :raises ~procrastinators.errors.BackendBusy: Contention outlasted its budget.
        :raises ~procrastinators.errors.BackendUnavailable: The authority could not answer.
        :raises ~procrastinators.errors.IndeterminateAdmission: The commit outcome is unknown.
        :raises ~procrastinators.errors.ClosedResource: The limiter was closed.
        """
        backend = self._sync_backend()
        request = self._request(cost, timeout)
        admission = run(self._waiter.acquire(request, backend.identity), backend, self._sleeper)
        return admission

    async def acquire_async(
        self, cost: int = 1, timeout: Seconds | types.EllipsisType | None = ...
    ) -> Admission:
        """Wait for capacity without blocking the event loop, then return the admission.

        As :meth:`acquire`. Cancellation propagates unchanged; cancelled before
        the commit, nothing was consumed (O5).

        :param cost: A positive integer within every rule's capacity.
        :param timeout: Quota-wait budget in seconds; the limiter's default when omitted.
        :returns: The admission.
        """
        backend = self._async_backend()
        request = self._request(cost, timeout)
        admission = await run_async(
            self._waiter.acquire(request, backend.identity), backend, self._async_sleeper
        )
        return admission

    def try_acquire(self, cost: int = 1) -> Decision:
        """Make exactly one atomic attempt, without waiting for quota.

        A returned decision with ``allowed=True`` **has already been charged**;
        do not acquire again.

        :param cost: A positive integer within every rule's capacity.
        :returns: The decision; a denial is a value and consumed nothing.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` is malformed or above capacity.
        """
        backend = self._sync_backend()
        request = self._request(cost, 0)
        decision = run(self._waiter.attempt(request, backend.identity), backend, self._sleeper)
        return decision

    async def try_acquire_async(self, cost: int = 1) -> Decision:
        """The awaitable form of :meth:`try_acquire`.

        :param cost: A positive integer within every rule's capacity.
        :returns: The decision; a denial is a value and consumed nothing.
        """
        backend = self._async_backend()
        request = self._request(cost, 0)
        decision = await run_async(
            self._waiter.attempt(request, backend.identity), backend, self._async_sleeper
        )
        return decision

    def __enter__(self) -> Admission:
        """Acquire with the default cost and timeout, and return the admission."""
        admission = self.acquire()
        return admission

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Nothing: quota is not refunded and storage is not closed (A3, L6)."""

    async def __aenter__(self) -> Admission:
        """Acquire asynchronously with the default cost and timeout."""
        admission = await self.acquire_async()
        return admission

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Nothing: quota is not refunded and storage is not closed (A3, L6)."""

    @overload
    def __call__(self, func: Callable[P, R], /) -> Callable[P, R]: ...

    @overload
    def __call__(
        self, *, cost: int = 1, timeout: Seconds | types.EllipsisType | None = ...
    ) -> Invocation: ...

    def __call__(
        self,
        func: Callable[P, R] | None = None,
        /,
        *,
        cost: int = 1,
        timeout: Seconds | types.EllipsisType | None = ...,
    ) -> Callable[P, R] | Invocation:
        """Decorate ``func``, or return an :class:`Invocation` carrying per-call options.

        ``@limiter`` and ``@limiter(cost=…)`` both work. The wrapper keeps the
        function's metadata and acquires on the path matching it: an async
        function is never wrapped in a blocking acquire.

        :param func: The function to decorate, when used as ``@limiter``.
        :param cost: The cost of each acquisition.
        :param timeout: The quota-wait budget; the limiter's default when omitted.
        :raises ~procrastinators.errors.UnsupportedCapability: ``func`` is a generator or async
            generator function.
        :raises ~procrastinators.errors.InvalidCost: ``cost`` is malformed or above capacity.
        """
        self._ensure_open()
        self._request(cost, timeout)
        if func is None:
            result: Callable[P, R] | Invocation = Invocation(self, cost, timeout)
        else:
            result = _decorate(self, func, cost, timeout)
        return result

    def inspect(self) -> Snapshot:
        """An advisory snapshot of every rule. Never a reservation (R7).

        :raises ~procrastinators.errors.ClosedResource: The limiter was closed.
        """
        snapshot = self._sync_backend().inspect(self.rules)
        return snapshot

    async def inspect_async(self) -> Snapshot:
        """The awaitable form of :meth:`inspect`."""
        snapshot = await self._async_backend().inspect(self.rules)
        return snapshot

    def _cooldown_scope(self, scope: QuotaIdentity | None) -> QuotaIdentity:
        scopes = sorted({rule.scope for rule in self.rules})
        if scope is not None:
            chosen = scope
        elif len(scopes) == 1:
            chosen = scopes[0]
        else:
            raise ConfigurationError(
                f"this limiter spans {len(scopes)} scopes; pass scope= to say which one the "
                "vendor asked to pause"
            )
        return chosen

    def _cooled(self, cooldown: Cooldown) -> Cooldown:
        deliver(
            self._on_event,
            CooldownEvent(
                at=self._clock.now(),
                scope=cooldown.scope,
                until=cooldown.until,
                reason=cooldown.reason,
            ),
        )
        return cooldown

    def defer_for(
        self, duration: Seconds, *, scope: QuotaIdentity | None = None, reason: str = ""
    ) -> Cooldown:
        """Extend a shared cooldown with ``max(existing, new)`` (K2).

        The supported path for a vendor's ``Retry-After``: every limiter on the
        scope then waits, and no quota event is fabricated (K4).

        :param duration: The least length of the pause, in seconds.
        :param scope: The quota to pause; required when this limiter spans several.
        :param reason: Recorded on the cooldown.
        :returns: The cooldown in force, which may be longer than requested.
        :raises ~procrastinators.errors.UnsupportedCapability: The backend has no cooldowns.
        """
        backend = self._sync_backend()
        if not (isinstance(backend, SupportsCooldown) and backend.capabilities.supports_cooldowns):
            raise UnsupportedCapability(f"{self.backend} does not support shared cooldowns")
        else:
            pass
        cooldown = backend.defer_for(
            self._cooldown_scope(scope),
            seconds_to_micros(duration, what="duration"),
            reason=reason,
        )
        applied = self._cooled(cooldown)
        return applied

    async def defer_for_async(
        self, duration: Seconds, *, scope: QuotaIdentity | None = None, reason: str = ""
    ) -> Cooldown:
        """The awaitable form of :meth:`defer_for`."""
        backend = self._async_backend()
        if not (
            isinstance(backend, SupportsAsyncCooldown) and backend.capabilities.supports_cooldowns
        ):
            raise UnsupportedCapability(f"{self.backend} does not support shared cooldowns")
        else:
            pass
        cooldown = await backend.defer_for(
            self._cooldown_scope(scope),
            seconds_to_micros(duration, what="duration"),
            reason=reason,
        )
        applied = self._cooled(cooldown)
        return applied

    def close(self) -> None:
        """Stop this limiter and close its owned synchronous backend. Idempotent (L1).

        A borrowed backend is left alone (L2) and no quota state is deleted
        (L3). An owned asynchronous handle needs awaiting, which is what
        :meth:`aclose` is for.
        """
        self._closed = True
        if self._handles.owned and self._handles.sync is not None:
            self._handles.sync.close()
        else:
            pass

    async def aclose(self) -> None:
        """Stop this limiter and close every owned backend handle. Idempotent (L1)."""
        self._closed = True
        if self._handles.owned:
            if self._handles.sync is not None:
                self._handles.sync.close()
            else:
                pass
            if self._handles.async_ is not None:
                await self._handles.async_.aclose()
            else:
                pass
        else:
            pass

    def __repr__(self) -> str:
        rules = ", ".join(str(constraint) for constraint in self._constraints)
        text = f"RateLimiter([{rules}] on {self.backend})"
        return text


if __name__ == "__main__":
    pass
else:
    pass
