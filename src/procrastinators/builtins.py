"""The registry of everything that ships: five algorithms and every built-in backend family.

:func:`default_registry` is what a :class:`~procrastinators.limiter.RateLimiter`
resolves algorithm ids and backend addresses against unless given another. It
is created on first use, not at import, and a third-party algorithm or backend
family registered with it becomes available to every limiter in the process
built afterwards — registered explicitly, never discovered (see
:mod:`procrastinators.registry`).
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import threading
from typing import Final

from procrastinators.algorithms import builtin_specs
from procrastinators.backends.memcached import MEMCACHED_CAPABILITIES, memcached_backend
from procrastinators.backends.memory import MEMORY_CAPABILITIES, memory_backend
from procrastinators.backends.postgres import POSTGRES_CAPABILITIES, postgres_backend
from procrastinators.backends.redis import (
    REDIS_CAPABILITIES,
    native_executor_specs,
    redis_backend,
)
from procrastinators.backends.sqlite import SQLITE_CAPABILITIES, sqlite_backend
from procrastinators.protocols import BackendSpec
from procrastinators.registry import Registry

__all__ = ["builtin_registry", "default_registry"]

POSTGRES_FAMILIES: Final = ("postgresql", "postgres")
"""The address schemes of the PostgreSQL backend; identities always say ``postgresql``."""

REDIS_FAMILIES: Final = ("redis", "rediss", "valkey", "valkeys")
"""The address schemes of the Redis and Valkey backend, plain and over TLS."""

_default_lock: Final = threading.Lock()
_default: list[Registry] = list()


def builtin_registry() -> Registry:
    """A fresh registry: the five reference algorithms and every built-in backend family.

    ``memory`` and ``sqlite``; ``redis``, ``rediss``, ``valkey``, and ``valkeys``
    with a native executor for each algorithm; ``postgresql`` and ``postgres``;
    and the best-effort ``memcached``. Registering a family imports no driver: a service backend's
    driver is needed only when a handle connects.

    For callers who want their own registrations kept apart from the
    process-wide :func:`default_registry`.
    """
    registry = Registry()
    for spec in builtin_specs():
        registry.register_algorithm(spec)
    registry.register_backend(
        BackendSpec(family="memory", factory=memory_backend, capabilities=MEMORY_CAPABILITIES)
    )
    registry.register_backend(
        BackendSpec(family="sqlite", factory=sqlite_backend, capabilities=SQLITE_CAPABILITIES)
    )
    for family in POSTGRES_FAMILIES:
        registry.register_backend(
            BackendSpec(family=family, factory=postgres_backend, capabilities=POSTGRES_CAPABILITIES)
        )
    registry.register_backend(
        BackendSpec(
            family="memcached", factory=memcached_backend, capabilities=MEMCACHED_CAPABILITIES
        )
    )
    for family in REDIS_FAMILIES:
        registry.register_backend(
            BackendSpec(family=family, factory=redis_backend, capabilities=REDIS_CAPABILITIES)
        )
        for spec in native_executor_specs(family):
            registry.register_native_executor(spec)
    return registry


def default_registry() -> Registry:
    """The process-wide registry limiters use by default, created on first call."""
    with _default_lock:
        if not _default:
            _default.append(builtin_registry())
        else:
            pass
        registry = _default[0]
    return registry


if __name__ == "__main__":
    pass
else:
    pass
