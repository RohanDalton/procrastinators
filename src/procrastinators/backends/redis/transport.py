"""Sending a store's calls to Redis or Valkey, and saying what a failure means.

Admission is sent on a connection taken straight from the driver's pool,
bypassing the client's own retry logic: a client that resends a command after
a lost reply would run an admission twice. Everything else about a client —
authentication, TLS, pool sizing — is left to the driver.

What a failure means depends on when it happened:

* Before the command was sent — no connection, no route to the slot — nothing
  ran: :exc:`~procrastinators.errors.BackendUnavailable`, which a waiter may
  retry within its budget.
* A refusal the server makes before running a script — ``NOSCRIPT``,
  ``MOVED``, ``OOM``, ``READONLY``, ``BUSY``, and their kind — also ran
  nothing. ``NOSCRIPT`` is answered by loading the script and sending it once
  more, ``MOVED`` by refreshing the cluster's slot map and doing the same.
* Anything after the command may have been received — a timeout, a dropped
  connection, a script error — leaves an admission in doubt:
  :exc:`~procrastinators.errors.IndeterminateAdmission`, never permission and
  never a refund (contract O4).

An asynchronous call cancelled while its reply is outstanding abandons the
connection rather than returning it to the pool with a reply still to come,
and the cancellation propagates unchanged (O5).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

from typing import TYPE_CHECKING, Final, TypeAlias

from procrastinators.errors import (
    BackendBusy,
    BackendError,
    BackendUnavailable,
    IndeterminateAdmission,
)
from procrastinators.models import USECS_PER_SECOND

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from redis import Redis
    from redis.asyncio import Redis as AsyncRedis
    from redis.asyncio.cluster import ClusterNode as AsyncClusterNode
    from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
    from redis.asyncio.connection import AbstractConnection as AsyncConnection
    from redis.asyncio.connection import ConnectionPool as AsyncConnectionPool
    from redis.cluster import RedisCluster
    from redis.connection import ConnectionPool

    from procrastinators.backends.redis.store import Call, Reply, ScriptOperation, ScriptResultT

    SyncClient: TypeAlias = Redis | RedisCluster
    AsyncClient: TypeAlias = AsyncRedis | AsyncRedisCluster
else:
    pass

__all__ = ["AsyncClient", "AsyncTransport", "SyncClient", "SyncTransport", "translate"]

_REFUSALS: Final = frozenset(
    {
        "ASK",
        "CLUSTERDOWN",
        "CROSSSLOT",
        "LOADING",
        "MASTERDOWN",
        "MISCONF",
        "MOVED",
        "NOAUTH",
        "NOPERM",
        "NOREPLICAS",
        "NOSCRIPT",
        "OOM",
        "READONLY",
        "TRYAGAIN",
        "WRONGPASS",
    }
)
"""Error codes a server answers with instead of running a command."""

_BUSY: Final = "BUSY"
_ROUTES: Final = 2


def _code(error: BaseException) -> str:
    """The error code a server reply began with, recovered when the driver dropped it."""
    import redis.exceptions as driver

    if isinstance(error, driver.NoScriptError):
        code = "NOSCRIPT"
    elif isinstance(error, driver.MovedError):
        code = "MOVED"
    elif isinstance(error, driver.AskError):
        code = "ASK"
    elif isinstance(error, driver.TryAgainError):
        code = "TRYAGAIN"
    elif isinstance(error, (driver.ClusterDownError, driver.MasterDownError)):
        code = "CLUSTERDOWN"
    elif isinstance(error, driver.BusyLoadingError):
        code = "LOADING"
    elif isinstance(error, driver.ReadOnlyError):
        code = "READONLY"
    elif isinstance(error, driver.OutOfMemoryError):
        code = "OOM"
    elif isinstance(error, (driver.NoPermissionError, driver.AuthenticationError)):
        code = "NOPERM"
    elif isinstance(error, driver.ClusterCrossSlotError):
        code = "CROSSSLOT"
    else:
        code = str(error).split(" ", 1)[0].upper()
    return code


def translate(error: BaseException, call: Call, *, sent: bool) -> BackendError:
    """The library error a driver error means for ``call``.

    :param error: What the driver raised.
    :param call: The call it raised for.
    :param sent: Whether the command may have reached the server.
    """
    import redis.exceptions as driver

    what = "admitting" if call.admission is not None else "calling"
    if isinstance(error, driver.ResponseError) and (code := _code(error)) in _REFUSALS:
        translated: BackendError = BackendUnavailable(
            f"{what}: the server refused without running anything ({code}): {error}",
            cause=error,
        )
    elif isinstance(error, driver.ResponseError) and code == _BUSY:
        translated = BackendBusy(
            f"{what}: the server is busy running another script; contention is not a "
            f"denial (O2): {error}",
            cause=error,
        )
    elif not sent:
        translated = BackendUnavailable(f"{what}: nothing was sent: {error}", cause=error)
    elif call.admission is not None:
        translated = IndeterminateAdmission(
            f"admitting: the reply was lost or the script failed ({error}); the admission may "
            "have happened",
            cause=error,
            cost=call.admission.cost,
            rules=call.admission.rules,
        )
    elif call.writes:
        translated = BackendUnavailable(
            f"{what}: the reply was lost or the script failed, and the change may have been "
            f"applied: {error}",
            cause=error,
        )
    else:
        translated = BackendUnavailable(f"{what}: {error}", cause=error)
    return translated


def _failures() -> tuple[type[BaseException], ...]:
    import redis.exceptions as driver

    failures = (driver.RedisError, OSError)
    return failures


def _command(call: Call) -> tuple[str, ...]:
    if call.script is None:
        command = call.command
    else:
        command = ("EVALSHA", call.script.sha, str(len(call.keys)), *call.keys, *call.args)
    return command


def _seconds(micros: int) -> float:
    seconds = micros / USECS_PER_SECOND
    return seconds


class SyncTransport:
    """Sends calls on a synchronous ``redis.Redis`` or ``redis.cluster.RedisCluster``.

    :param client: The driver client.
    """

    def __init__(self, client: SyncClient) -> None:
        self._client = client

    @property
    def client(self) -> SyncClient:
        """The driver client calls are sent on."""
        return self._client

    def execute(
        self,
        operation: ScriptOperation[ScriptResultT],
        *,
        timeout_us: int,
        before_call: Callable[[Call], None] | None = None,
    ) -> ScriptResultT:
        """Run ``operation`` to completion, sending each of its calls in turn.

        :param operation: A store operation.
        :param timeout_us: How long to wait for each reply.
        :returns: What the operation returns.
        """
        try:
            call = next(operation)
            while True:
                if before_call is not None:
                    before_call(call)
                else:
                    pass
                reply = self.run(call, timeout_us=timeout_us)
                call = operation.send(reply)
        except StopIteration as stop:
            result = stop.value
        return result

    def _pool(self, slot: int) -> ConnectionPool:
        from redis.cluster import RedisCluster

        if isinstance(client := self._client, RedisCluster):
            node = client.nodes_manager.get_node_from_slot(slot)
            if (connection := node.redis_connection) is None:
                client.nodes_manager.initialize()
                connection = client.nodes_manager.get_node_from_slot(slot).redis_connection
            else:
                pass
            assert connection is not None
            pool = connection.connection_pool
        else:
            pool = client.connection_pool
        return pool

    def run(self, call: Call, *, timeout_us: int) -> Reply:
        """Send one call and return its reply.

        :param call: The call.
        :param timeout_us: How long to wait for the reply.
        :raises ~procrastinators.errors.BackendError: As the module notes describe.
        """
        import redis.exceptions as driver
        from redis.cluster import RedisCluster

        for route in range(_ROUTES):
            try:
                reply = self._send(call, timeout_us)
            except driver.MovedError as error:
                if route + 1 < _ROUTES and isinstance(client := self._client, RedisCluster):
                    client.nodes_manager.initialize()
                else:
                    raise translate(error, call, sent=True) from error
            else:
                break
        else:
            raise AssertionError("unreachable")
        return reply

    def _send(self, call: Call, timeout_us: int) -> Reply:
        import redis.exceptions as driver

        try:
            pool = self._pool(call.slot)
            connection = pool.get_connection()
        except _failures() as error:
            raise translate(error, call, sent=False) from error
        sent = False
        try:
            for attempt in range(2):
                sent = True
                connection.send_command(*_command(call))
                try:
                    reply = connection.read_response(timeout=_seconds(timeout_us))
                except driver.NoScriptError:
                    if attempt or call.script is None:
                        raise
                    else:
                        connection.send_command("SCRIPT", "LOAD", call.script.source)
                        connection.read_response(timeout=_seconds(timeout_us))
                        sent = False
                except driver.ResponseError as error:
                    if call.script is None:
                        reply = error
                        break
                    else:
                        raise
                else:
                    break
            else:
                raise AssertionError("unreachable")
        except driver.MovedError:
            raise
        except driver.ResponseError as error:
            raise translate(error, call, sent=sent) from error
        except _failures() as error:
            connection.disconnect()
            raise translate(error, call, sent=sent) from error
        except BaseException:
            connection.disconnect()
            raise
        finally:
            pool.release(connection)
        return reply

    def close(self) -> None:
        """Close the client and its connections."""
        self._client.close()


class AsyncTransport:
    """Sends calls on a ``redis.asyncio.Redis`` or ``redis.asyncio.cluster.RedisCluster``.

    :param client: The driver client.
    """

    def __init__(self, client: AsyncClient) -> None:
        self._client = client
        self._initialized = False

    @property
    def client(self) -> AsyncClient:
        """The driver client calls are sent on."""
        return self._client

    async def execute(
        self,
        operation: ScriptOperation[ScriptResultT],
        *,
        timeout_us: int,
        before_call: Callable[[Call], Awaitable[None]] | None = None,
    ) -> ScriptResultT:
        """Run ``operation`` to completion, awaiting each of its calls in turn.

        :param operation: A store operation.
        :param timeout_us: How long to wait for each reply.
        :returns: What the operation returns.
        """
        try:
            call = next(operation)
            while True:
                if before_call is not None:
                    await before_call(call)
                else:
                    pass
                reply = await self.run(call, timeout_us=timeout_us)
                call = operation.send(reply)
        except StopIteration as stop:
            result = stop.value
        return result

    async def run(self, call: Call, *, timeout_us: int) -> Reply:
        """Send one call and return its reply.

        :param call: The call.
        :param timeout_us: How long to wait for the reply.
        :raises ~procrastinators.errors.BackendError: As the module notes describe.
        """
        import redis.exceptions as driver
        from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

        for route in range(_ROUTES):
            try:
                reply = await self._send(call, timeout_us)
            except driver.MovedError as error:
                if route + 1 < _ROUTES and isinstance(client := self._client, AsyncRedisCluster):
                    await client.nodes_manager.initialize()
                else:
                    raise translate(error, call, sent=True) from error
            else:
                break
        else:
            raise AssertionError("unreachable")
        return reply

    async def _acquire(
        self, slot: int
    ) -> tuple[AsyncClusterNode | AsyncConnectionPool, AsyncConnection]:
        from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster

        if isinstance(client := self._client, AsyncRedisCluster):
            if not self._initialized:
                await client.initialize()
                self._initialized = True
            else:
                pass
            node = client.nodes_manager.get_node_from_slot(slot)
            connection: AsyncConnection = node.acquire_connection()
            try:
                await connection.connect()
            except BaseException:
                node.release(connection)
                raise
            owner: AsyncClusterNode | AsyncConnectionPool = node
        else:
            pool = client.connection_pool
            connection = await pool.get_connection()
            owner = pool
        acquired = (owner, connection)
        return acquired

    @staticmethod
    async def _release(
        owner: AsyncClusterNode | AsyncConnectionPool, connection: AsyncConnection
    ) -> None:
        from redis.asyncio.cluster import ClusterNode as AsyncClusterNode

        if isinstance(owner, AsyncClusterNode):
            owner.release(connection)  # ty: ignore[invalid-argument-type]
        else:
            await owner.release(connection)

    async def _send(self, call: Call, timeout_us: int) -> Reply:
        import redis.exceptions as driver

        try:
            owner, connection = await self._acquire(call.slot)
        except _failures() as error:
            raise translate(error, call, sent=False) from error
        sent = False
        try:
            for attempt in range(2):
                sent = True
                await connection.send_command(*_command(call))
                try:
                    reply = await connection.read_response(timeout=_seconds(timeout_us))
                except driver.NoScriptError:
                    if attempt or call.script is None:
                        raise
                    else:
                        await connection.send_command("SCRIPT", "LOAD", call.script.source)
                        await connection.read_response(timeout=_seconds(timeout_us))
                        sent = False
                except driver.ResponseError as error:
                    if call.script is None:
                        reply = error
                        break
                    else:
                        raise
                else:
                    break
            else:
                raise AssertionError("unreachable")
        except driver.MovedError:
            raise
        except driver.ResponseError as error:
            raise translate(error, call, sent=sent) from error
        except (TimeoutError, *_failures()) as error:
            await connection.disconnect(nowait=True)
            raise translate(error, call, sent=sent) from error
        except BaseException:
            # Cancelled with a reply outstanding: the connection cannot be reused.
            await connection.disconnect(nowait=True)
            raise
        finally:
            await self._release(owner, connection)
        return reply

    async def aclose(self) -> None:
        """Close the client and its connections."""
        await self._client.aclose()


if __name__ == "__main__":
    pass
else:
    pass
