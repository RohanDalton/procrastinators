"""Synchronous and asynchronous handles on a Redis or Valkey store.

A handle owns the driver client it creates from its store's address and
closes it when closed; a client passed in is borrowed and never closed
(contract L2). Clients are created on first use, so constructing a handle
connects to nothing. The driver's connection pools replace their connections
in a forked child, so a handle used after ``fork`` coordinates with its parent
through the server (L8).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio
import threading
import urllib.parse
from typing import TYPE_CHECKING, ClassVar, Final

from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.backends.redis.store import RedisStore
from procrastinators.backends.redis.transport import AsyncTransport, SyncTransport
from procrastinators.capabilities import Mode
from procrastinators.errors import BackendUnavailable, ConfigurationError
from procrastinators.models import DurationMicros, Ownership, ResourceOwnership
from procrastinators.protocols import ObservationPoint

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from procrastinators.backends.redis.store import Call, ScriptOperation, ScriptResultT
    from procrastinators.backends.redis.transport import AsyncClient, SyncClient
    from procrastinators.models import (
        AdmissionRequest,
        BackendIdentity,
        Capabilities,
        Cooldown,
        Decision,
        PolicyFingerprint,
        QuotaIdentity,
        RuleId,
        Snapshot,
    )
    from procrastinators.protocols import (
        AdmissionObserver,
        Algorithm,
        PolicyMigration,
        StoredPolicy,
    )
else:
    pass

__all__ = ["AsyncRedisBackend", "RedisBackend", "redis_backend"]

DEFAULT_TIMEOUT_US: Final = DurationMicros(5_000_000)
"""Reply budget for inspection, cooldowns, and administration."""

_TRUE: Final = frozenset({"1", "true", "yes", "on"})
_FALSE: Final = frozenset({"0", "false", "no", "off"})


def _require_driver() -> None:
    try:
        import redis  # noqa: F401
    except ImportError as error:
        raise BackendUnavailable(
            "the redis and valkey backends need the redis driver: install procrastinators[redis]",
            cause=error,
        ) from error
    else:
        pass


def _sync_client(store: RedisStore) -> SyncClient:
    """A synchronous client for ``store``, created and so owned by the caller."""
    _require_driver()
    import redis
    import redis.cluster

    url, options = store.client_arguments()
    if store.cluster:
        client: SyncClient = redis.cluster.RedisCluster.from_url(url, **options)
    else:
        client = redis.Redis.from_url(url, **options)
    return client


def _async_client(store: RedisStore) -> AsyncClient:
    """An asyncio client for ``store``, created and so owned by the caller."""
    _require_driver()
    import redis.asyncio
    import redis.asyncio.cluster

    url, options = store.client_arguments()
    if store.cluster:
        client: AsyncClient = redis.asyncio.cluster.RedisCluster.from_url(url, **options)
    else:
        client = redis.asyncio.Redis.from_url(url, **options)
    return client


class _RedisHandle:
    """What the sync and async handles share: the store, namespace, observer, and client."""

    _mode: ClassVar[Mode]

    def __init__(
        self,
        store: RedisStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        borrowed: bool = False,
    ) -> None:
        self._store = store
        self._identity = store.identity(namespace)
        self._observer = observer
        self._borrowed = borrowed
        self._verified = not store.require_noeviction
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._active_calls = 0
        self._close_complete = False

    @property
    def store(self) -> RedisStore:
        """The authority this handle addresses."""
        return self._store

    @property
    def capabilities(self) -> Capabilities:
        """Shared-service, service-durable, composing, with cooldowns and administration."""
        capabilities = self._store.capabilities(self._mode)
        return capabilities

    @property
    def identity(self) -> BackendIdentity:
        """``redis://host:port/db#namespace``, or ``valkey://`` likewise."""
        return self._identity

    @property
    def ownership(self) -> ResourceOwnership:
        """The client is owned unless it was injected (L2)."""
        ownership = ResourceOwnership(
            client=Ownership.BORROWED if self._borrowed else Ownership.OWNED
        )
        return ownership

    def __repr__(self) -> str:
        text = f"{type(self).__name__}({self._identity})"
        return text


class RedisBackend(_RedisHandle, BaseSyncBackend):
    """A synchronous handle on a :class:`~procrastinators.backends.redis.store.RedisStore`.

    Satisfies :class:`~procrastinators.protocols.SyncBackend`,
    :class:`~procrastinators.protocols.SupportsCooldown`, and
    :class:`~procrastinators.protocols.SupportsPolicyAdministration`. The
    observer sees ``before_lock`` before a script is sent and ``after_commit``
    once an admission's reply arrives; loading and committing happen inside one
    script on the server, where no observer can run.

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called before sending and after an admission commits.
    :param client: A ``redis.Redis`` or ``redis.cluster.RedisCluster`` to borrow; one
        created from the store's address and owned when ``None``.
    """

    _mode = Mode.SYNC

    def __init__(
        self,
        store: RedisStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        client: SyncClient | None = None,
    ) -> None:
        super().__init__(store, namespace=namespace, observer=observer, borrowed=client is not None)
        self._active = None if client is None else SyncTransport(client)

    def _transport(self) -> SyncTransport:
        if (transport := self._active) is None:
            transport = self._active = SyncTransport(_sync_client(self._store))
        else:
            pass
        return transport

    def _execute(
        self, operation: ScriptOperation[ScriptResultT], timeout_us: int = DEFAULT_TIMEOUT_US
    ) -> ScriptResultT:
        with self._condition:
            self._ensure_open()
            transport = self._transport()
            self._active_calls += 1
        try:

            def verify(call: Call) -> None:
                if self._store.require_noeviction and (not self._verified or self._store.cluster):
                    transport.execute(self._store.check_eviction(call.slot), timeout_us=timeout_us)
                    if not self._store.cluster:
                        self._verified = True
                    else:
                        pass
                else:
                    pass

            result = transport.execute(operation, timeout_us=timeout_us, before_call=verify)
        finally:
            with self._condition:
                self._active_calls -= 1
                self._condition.notify_all()
        return result

    def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        :param request: The request.
        :returns: The decision; a denial is a value.
        :raises ~procrastinators.errors.PolicyConflict: A rule is stored under another policy,
            or is being migrated.
        :raises ~procrastinators.errors.UnsupportedCapability: No native executor is verified for
            a constraint, rules fall in several cluster slots, or the server can evict.
        :raises ~procrastinators.errors.BackendBusy: The server was running another script.
        :raises ~procrastinators.errors.BackendUnavailable: Nothing ran.
        :raises ~procrastinators.errors.IndeterminateAdmission: The reply was lost after sending,
            or the observer failed after the commit.
        :raises ~procrastinators.errors.StateCorruption: Stored state is malformed.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        self._ensure_open()
        self.validate_request(request)
        self._store.validate(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = self._execute(
            self._store.admit(request, self._identity), request.budget.storage_timeout_us
        )
        if decision.allowed:
            self._observe(ObservationPoint.AFTER_COMMIT, request)
        else:
            pass
        return decision

    def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        snapshot = self._execute(self._store.inspect(rules, self._identity))
        return snapshot

    def defer_for(
        self, scope: QuotaIdentity, duration: DurationMicros, *, reason: str = ""
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on the cooldown.
        :returns: The cooldown now in force, which may be longer than requested.
        :raises ~procrastinators.errors.ClosedResource: This handle was closed.
        """
        cooldown = self._execute(self._store.defer(scope, duration, reason))
        return cooldown

    def stored_policy(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> StoredPolicy | None:
        """The policy metadata stored for ``rule``, if any.

        :param rule: The rule to look up.
        :param coordination_domain: The domain its constraints declare, when this process
            has not admitted it.
        """
        stored = self._execute(
            self._store.stored_policy(rule, coordination_domain=coordination_domain)
        )
        return stored

    def begin_migration(
        self,
        rule: RuleId,
        *,
        to_fingerprint: PolicyFingerprint,
        to_state_version: int,
        coordination_domain: str | None = None,
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :param coordination_domain: As for :meth:`stored_policy`.
        :raises ~procrastinators.errors.PolicyConflict: The rule is already migrating elsewhere.
        """
        migration = self._execute(
            self._store.begin_migration(
                rule, to_fingerprint, to_state_version, coordination_domain=coordination_domain
            )
        )
        return migration

    def migration_status(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        :param coordination_domain: As for :meth:`stored_policy`.
        """
        migration = self._execute(
            self._store.migration_status(rule, coordination_domain=coordination_domain)
        )
        return migration

    def complete_migration(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        :param coordination_domain: As for :meth:`stored_policy`.
        :raises ~procrastinators.errors.PolicyConflict: The rule is not ready.
        """
        migration = self._execute(
            self._store.complete_migration(rule, coordination_domain=coordination_domain)
        )
        return migration

    def close(self) -> None:
        """Close an owned client; a borrowed one is left open. Idempotent (L1, L2).

        Quota state on the server is untouched (L3).
        """
        with self._condition:
            first = self._mark_closed()
            if first:
                while self._active_calls:
                    self._condition.wait()
                transport, self._active = self._active, None
            else:
                while not self._close_complete:
                    self._condition.wait()
        if first:
            try:
                if transport is not None and not self._borrowed:
                    transport.client.close()
                else:
                    pass
            finally:
                with self._condition:
                    self._close_complete = True
                    self._condition.notify_all()
        else:
            pass


class AsyncRedisBackend(_RedisHandle, BaseAsyncBackend):
    """An asynchronous handle on a :class:`~procrastinators.backends.redis.store.RedisStore`.

    Uses the driver's native asyncio client, so nothing blocks the event loop.
    Satisfies :class:`~procrastinators.protocols.AsyncBackend`,
    :class:`~procrastinators.protocols.SupportsAsyncCooldown`, and
    :class:`~procrastinators.protocols.SupportsAsyncPolicyAdministration`.
    Cancelled before its script is sent, an admission consumes nothing;
    cancelled after, it may have committed without its caller (O5).

    :param store: The authority to address.
    :param namespace: The handle's quota namespace, shown in its identity.
    :param observer: Called before sending and after an admission commits.
    :param client: A ``redis.asyncio.Redis`` or ``redis.asyncio.cluster.RedisCluster`` to
        borrow; one created from the store's address and owned when ``None``.
    """

    _mode = Mode.ASYNC

    def __init__(
        self,
        store: RedisStore,
        *,
        namespace: str = "default",
        observer: AdmissionObserver | None = None,
        client: AsyncClient | None = None,
    ) -> None:
        super().__init__(store, namespace=namespace, observer=observer, borrowed=client is not None)
        self._active = None if client is None else AsyncTransport(client)
        self._idle = asyncio.Event()
        self._idle.set()
        self._close_task: asyncio.Task[None] | None = None

    def _transport(self) -> AsyncTransport:
        if (transport := self._active) is None:
            transport = self._active = AsyncTransport(_async_client(self._store))
        else:
            pass
        return transport

    async def _execute(
        self, operation: ScriptOperation[ScriptResultT], timeout_us: int = DEFAULT_TIMEOUT_US
    ) -> ScriptResultT:
        with self._lock:
            self._ensure_open()
            transport = self._transport()
            self._active_calls += 1
            self._idle.clear()
        try:

            async def verify(call: Call) -> None:
                if self._store.require_noeviction and (not self._verified or self._store.cluster):
                    await transport.execute(
                        self._store.check_eviction(call.slot), timeout_us=timeout_us
                    )
                    if not self._store.cluster:
                        self._verified = True
                    else:
                        pass
                else:
                    pass

            result = await transport.execute(operation, timeout_us=timeout_us, before_call=verify)
        finally:
            with self._lock:
                self._active_calls -= 1
                if self._active_calls == 0:
                    self._idle.set()
                else:
                    pass
        return result

    async def admit(self, request: AdmissionRequest) -> Decision:
        """Admit every constraint of ``request`` atomically, or none.

        As :meth:`RedisBackend.admit`, awaited.

        :param request: The request.
        :returns: The decision; a denial is a value.
        """
        self._ensure_open()
        self.validate_request(request)
        self._store.validate(request)
        self._observe(ObservationPoint.BEFORE_LOCK, request)
        decision = await self._execute(
            self._store.admit(request, self._identity), request.budget.storage_timeout_us
        )
        if decision.allowed:
            self._observe(ObservationPoint.AFTER_COMMIT, request)
        else:
            pass
        return decision

    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot:
        """Advisory observation of ``rules``. Never a reservation (R7).

        :param rules: The rules to observe.
        """
        snapshot = await self._execute(self._store.inspect(rules, self._identity))
        return snapshot

    async def defer_for(
        self, scope: QuotaIdentity, duration: DurationMicros, *, reason: str = ""
    ) -> Cooldown:
        """Extend ``scope``'s cooldown to at least ``now + duration``, atomically (K2).

        :param scope: The quota to pause.
        :param duration: The least length of the pause, in microseconds.
        :param reason: Recorded on the cooldown.
        """
        cooldown = await self._execute(self._store.defer(scope, duration, reason))
        return cooldown

    async def stored_policy(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> StoredPolicy | None:
        """The policy metadata stored for ``rule``, if any.

        :param rule: The rule to look up.
        :param coordination_domain: As for :meth:`RedisBackend.stored_policy`.
        """
        stored = await self._execute(
            self._store.stored_policy(rule, coordination_domain=coordination_domain)
        )
        return stored

    async def begin_migration(
        self,
        rule: RuleId,
        *,
        to_fingerprint: PolicyFingerprint,
        to_state_version: int,
        coordination_domain: str | None = None,
    ) -> PolicyMigration:
        """Stop admissions on ``rule`` and drain it towards a new policy.

        :param rule: The rule to migrate.
        :param to_fingerprint: Fingerprint of the policy to install.
        :param to_state_version: State version of the policy to install.
        :param coordination_domain: As for :meth:`RedisBackend.stored_policy`.
        """
        migration = await self._execute(
            self._store.begin_migration(
                rule, to_fingerprint, to_state_version, coordination_domain=coordination_domain
            )
        )
        return migration

    async def migration_status(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> PolicyMigration | None:
        """Progress of ``rule``'s migration, if any.

        :param rule: The rule whose migration to report.
        :param coordination_domain: As for :meth:`RedisBackend.stored_policy`.
        """
        migration = await self._execute(
            self._store.migration_status(rule, coordination_domain=coordination_domain)
        )
        return migration

    async def complete_migration(
        self, rule: RuleId, *, coordination_domain: str | None = None
    ) -> PolicyMigration:
        """Install the new policy once ``rule`` has drained.

        :param rule: The rule whose migration to complete.
        :param coordination_domain: As for :meth:`RedisBackend.stored_policy`.
        """
        migration = await self._execute(
            self._store.complete_migration(rule, coordination_domain=coordination_domain)
        )
        return migration

    async def aclose(self) -> None:
        """Close an owned client; a borrowed one is left open. Idempotent (L1, L2).

        Quota state on the server is untouched (L3).
        """
        with self._lock:
            if self._mark_closed():
                transport, self._active = self._active, None
                self._close_task = asyncio.create_task(self._finish_close(transport))
            else:
                pass
        if self._close_task is not None:
            await asyncio.shield(self._close_task)
        else:
            pass

    async def _finish_close(self, transport: AsyncTransport | None) -> None:
        await self._idle.wait()
        if transport is not None and not self._borrowed:
            await transport.client.aclose()
        else:
            pass


def _flag(value: str, *, name: str, address: str) -> bool:
    if value.lower() in _TRUE:
        flag = True
    elif value.lower() in _FALSE:
        flag = False
    else:
        raise ConfigurationError(f"{name} must be true or false in {address!r}, got {value!r}")
    return flag


def redis_backend(
    address: str,
    *,
    mode: Mode,
    namespace: str = "default",
    algorithms: Iterable[Algorithm[object]] = tuple(),
) -> RedisBackend | AsyncRedisBackend:
    """A handle on the Redis or Valkey deployment an address names.

    ``redis://host:6379/0``, ``rediss://`` for TLS, and ``valkey://`` or
    ``valkeys://`` likewise. Query parameters ``cluster``, ``prefix``, and
    ``require_noeviction`` configure the store; any others are passed to the
    driver. Registered as the factory of the ``redis``, ``rediss``, ``valkey``,
    and ``valkeys`` families.

    :param address: The deployment's address, credentials included if needed; they never
        reach the handle's identity.
    :param mode: Which interface the handle offers.
    :param namespace: The handle's quota namespace.
    :param algorithms: Ignored beyond the built-ins: only native executors run in the server.
    :raises ~procrastinators.errors.ConfigurationError: The address is malformed.
    """
    del algorithms
    parts = urllib.parse.urlsplit(address)
    settings = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    cluster = _flag(settings.pop("cluster", "false"), name="cluster", address=address)
    require_noeviction = _flag(
        settings.pop("require_noeviction", "true"), name="require_noeviction", address=address
    )
    prefix = settings.pop("prefix", None)
    remaining = urllib.parse.urlencode(settings, quote_via=urllib.parse.quote)
    rebuilt = urllib.parse.urlunsplit(parts._replace(query=remaining))
    store = RedisStore(
        rebuilt,
        cluster=cluster,
        require_noeviction=require_noeviction,
        **({"prefix": prefix} if prefix is not None else dict()),
    )
    if mode is Mode.SYNC:
        handle: RedisBackend | AsyncRedisBackend = RedisBackend(store, namespace=namespace)
    else:
        handle = AsyncRedisBackend(store, namespace=namespace)
    return handle


if __name__ == "__main__":
    pass
else:
    pass
