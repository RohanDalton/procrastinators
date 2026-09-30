"""Static checks must tell sync implementations from async ones.

Phase 2 acceptance: the two are separate protocols precisely so that passing one
where the other is required is caught before it reaches production, where the
symptom would be an un-awaited coroutine that admitted nothing.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import inspect
from typing import TYPE_CHECKING

import pytest

from procrastinators import protocols

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.typing.conftest import TypeCheckRunner
else:
    pass

BACKENDS = """
from collections.abc import Sequence

from procrastinators import (
    AdmissionRequest,
    AsyncBackend,
    BackendIdentity,
    Capabilities,
    Decision,
    RuleId,
    Snapshot,
    SyncBackend,
)


class Blocking:
    @property
    def capabilities(self) -> Capabilities: raise NotImplementedError
    @property
    def identity(self) -> BackendIdentity: raise NotImplementedError
    def admit(self, request: AdmissionRequest) -> Decision: raise NotImplementedError
    def inspect(self, rules: Sequence[RuleId]) -> Snapshot: raise NotImplementedError
    def close(self) -> None: raise NotImplementedError


class Awaiting:
    @property
    def capabilities(self) -> Capabilities: raise NotImplementedError
    @property
    def identity(self) -> BackendIdentity: raise NotImplementedError
    async def admit(self, request: AdmissionRequest) -> Decision: raise NotImplementedError
    async def inspect(self, rules: Sequence[RuleId]) -> Snapshot: raise NotImplementedError
    async def aclose(self) -> None: raise NotImplementedError


class Forgetful:
    @property
    def capabilities(self) -> Capabilities: raise NotImplementedError
    @property
    def identity(self) -> BackendIdentity: raise NotImplementedError
    def admit(self, request: AdmissionRequest) -> Decision: raise NotImplementedError


good_sync: SyncBackend = Blocking()
good_async: AsyncBackend = Awaiting()
wrong_way_round: AsyncBackend = Blocking()  # E: a blocking store is not async storage
other_way: SyncBackend = Awaiting()  # E: coroutines are not a synchronous backend
incomplete: SyncBackend = Forgetful()  # E: inspect and close are missing
"""

ASYNC_PROTOCOL_NAMES = (
    "AsyncBackend",
    "SupportsAsyncCooldown",
    "AsyncSleeper",
    "AsyncAdmissionClock",
)


def _protocol_members() -> dict[str, tuple[str, str, str]]:
    """Every public callable on every exported protocol, keyed by ``Protocol.member``."""
    found: dict[str, tuple[str, str, str]] = dict()
    for protocol_name in protocols.__all__:
        protocol = getattr(protocols, protocol_name)
        # Protocol classes carry _is_protocol; issubclass against Protocol
        # itself is not a check type checkers accept.
        if isinstance(protocol, type) and getattr(protocol, "_is_protocol", False):
            for member, value in vars(protocol).items():
                function = value.fget if isinstance(value, property) else value
                if not member.startswith("_") and callable(function):
                    # Read raw annotation text: protocols.py defers its imports, so
                    # resolving them here would need the TYPE_CHECKING namespace.
                    annotations = getattr(function, "__annotations__", dict())
                    returns = annotations.get("return", "<missing>")
                    found[f"{protocol_name}.{member}"] = (protocol_name, member, returns)
                else:
                    pass
        else:
            pass
    return found


PROTOCOL_MEMBERS = _protocol_members()


@pytest.fixture(params=list(PROTOCOL_MEMBERS))
def protocol_member(request: pytest.FixtureRequest) -> tuple[str, str, str]:
    """A ``(protocol, member, return annotation)`` triple for one protocol method."""
    member = PROTOCOL_MEMBERS[request.param]
    return member


@pytest.fixture(params=ASYNC_PROTOCOL_NAMES)
def async_protocol(request: pytest.FixtureRequest) -> type:
    protocol: type = getattr(protocols, request.param)
    return protocol


def test_the_checker_will_not_let_a_blocking_store_pose_as_async(
    run_type_checker: TypeCheckRunner, marked_lines: Callable[[str], list[int]]
) -> None:
    """
    Given: Sync, async, and incomplete backend classes assigned to both protocols.
    When:  ty checks the assignments.
    Then:  Exactly the marked wrong-way-round and incomplete assignments are rejected.
    """
    expected = marked_lines(BACKENDS)
    report = run_type_checker(BACKENDS)
    actual = report.error_lines
    assert actual == expected, report.output


def test_no_method_returns_a_value_or_an_awaitable(protocol_member: tuple[str, str, str]) -> None:
    """
    Given: Any public method declared on an exported protocol.
    When:  Its return annotation is read.
    Then:  It is annotated, and returns neither an ``Awaitable`` nor a ``Coroutine``,
           so no method returns ``T | Awaitable[T]``. Such a union pushes the
           sync/async decision onto every caller, and tempts an implementation
           into checking at runtime whether it got a coroutine back, which is how
           a blocking driver ends up mislabelled as async storage.
    """
    protocol, member, returns = protocol_member
    assert returns != "<missing>", f"{protocol}.{member} has no return annotation"
    assert "Awaitable" not in returns, f"{protocol}.{member} returns {returns}"
    assert "Coroutine" not in returns, f"{protocol}.{member} returns {returns}"


def test_the_two_backend_protocols_do_not_overlap_by_accident() -> None:
    """
    Given: The SyncBackend and AsyncBackend protocols.
    When:  Their public member names are compared.
    Then:  Each has its own close method and AsyncBackend lacks the sync one, so a
           class satisfying one cannot accidentally satisfy the other.
    """
    sync_members = {name for name in dir(protocols.SyncBackend) if not name.startswith("_")}
    async_members = {name for name in dir(protocols.AsyncBackend) if not name.startswith("_")}
    assert "close" in sync_members
    assert "aclose" in async_members
    assert "close" not in async_members


def test_every_async_protocol_declares_coroutines(async_protocol: type) -> None:
    """
    Given: Any of the async protocols.
    When:  Its public methods are inspected.
    Then:  It declares at least one method, and every one is a coroutine function.
    """
    methods = [
        value
        for name, value in vars(async_protocol).items()
        if not name.startswith("_") and inspect.isfunction(value)
    ]
    assert methods
    assert all(inspect.iscoroutinefunction(method) for method in methods), async_protocol


if __name__ == "__main__":
    pass
else:
    pass
