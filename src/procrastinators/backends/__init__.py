"""Admission authorities.

The base classes a custom backend may inherit, and every built-in authority:

* :class:`~procrastinators.backends.memory.MemoryStore` coordinates one
  process; ``memory://`` addresses resolve to process-wide stores.
* :class:`~procrastinators.backends.sqlite.SQLiteStore` coordinates every
  process on a machine through one database file; ``sqlite://`` addresses name
  it, and its asynchronous handles run their storage calls on a
  :class:`~procrastinators.backends.executor.DedicatedExecutor`.
* :class:`~procrastinators.backends.redis.store.RedisStore` coordinates workers on any
  machine through Redis or Valkey, running each algorithm as a native Lua
  executor.
* :class:`~procrastinators.backends.postgres.PostgresStore` does the same
  through a PostgreSQL database.
* :class:`~procrastinators.backends.memcached.MemcachedStore` shares
  constant-state quotas through Memcached, with the weaker best-effort guarantee.

Importing any of them imports no driver; a service backend needs its driver
only when a handle first connects.
"""

__author__ = "Rohan B. Dalton"

from procrastinators.backends.base import BaseAsyncBackend, BaseSyncBackend
from procrastinators.backends.executor import DedicatedExecutor
from procrastinators.backends.memcached import (
    AsyncMemcachedBackend,
    MemcachedBackend,
    MemcachedStore,
)
from procrastinators.backends.memory import AsyncMemoryBackend, MemoryBackend, MemoryStore
from procrastinators.backends.postgres import AsyncPostgresBackend, PostgresBackend, PostgresStore
from procrastinators.backends.redis import AsyncRedisBackend, RedisBackend, RedisStore
from procrastinators.backends.sqlite import AsyncSQLiteBackend, SQLiteBackend, SQLiteStore

__all__ = [
    "AsyncMemcachedBackend",
    "AsyncMemoryBackend",
    "AsyncPostgresBackend",
    "AsyncRedisBackend",
    "AsyncSQLiteBackend",
    "BaseAsyncBackend",
    "BaseSyncBackend",
    "DedicatedExecutor",
    "MemcachedBackend",
    "MemcachedStore",
    "MemoryBackend",
    "MemoryStore",
    "PostgresBackend",
    "PostgresStore",
    "RedisBackend",
    "RedisStore",
    "SQLiteBackend",
    "SQLiteStore",
]


if __name__ == "__main__":
    pass
else:
    pass
