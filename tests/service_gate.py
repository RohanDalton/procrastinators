"""A pytest plugin: service tests may skip locally, but a required service job may not.

Service integration tests carry ``@pytest.mark.service("redis")`` (or
``valkey``, ``postgres``, ``memcached``) and call the ``service_available``
fixture with a reachability check. Locally, an unreachable server skips the
test so the suite stays useful on a laptop.

A CI job that exists to test one service names it with ``--require-service``
(or the ``PROCRASTINATORS_REQUIRE_SERVICES`` environment variable, comma
separated). For a required service, an unreachable server *fails* the test
instead of skipping it, and the session fails if not one test of that service
passed — so a misconfigured job cannot go green by skipping everything
(Phase 3: "a backend's required CI job must not pass by skipping all its
tests").
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import os
from collections import Counter
from typing import TYPE_CHECKING, Protocol

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
else:
    pass

REQUIRE_OPTION = "--require-service"
REQUIRE_ENVIRONMENT = "PROCRASTINATORS_REQUIRE_SERVICES"
_SERVICES_BY_TEST = pytest.StashKey[dict[str, frozenset[str]]]()
_PASSES = pytest.StashKey[Counter[str]]()


class ServiceGate(Protocol):
    def __call__(self, name: str, reachable: bool | Callable[[], bool], reason: str = "") -> None:
        """Skip, or fail when ``name`` is required, unless the service is reachable."""
        ...


def required_services(config: pytest.Config) -> frozenset[str]:
    """The services this run must actually exercise."""
    from_option: list[str] = config.getoption("require_service") or list()
    from_environment = os.environ.get(REQUIRE_ENVIRONMENT, "")
    names = {name.strip() for name in [*from_option, *from_environment.split(",")] if name.strip()}
    return frozenset(names)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        REQUIRE_OPTION,
        action="append",
        dest="require_service",
        metavar="NAME",
        help="fail, rather than skip, when this service is unreachable or none of its tests pass",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "service(name): integration test requiring the named external service",
    )
    config.stash[_SERVICES_BY_TEST] = dict()
    config.stash[_PASSES] = Counter()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    services = config.stash[_SERVICES_BY_TEST]
    for item in items:
        names = frozenset(name for marker in item.iter_markers("service") for name in marker.args)
        if names:
            services[item.nodeid] = names
        else:
            pass


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> object:
    report: pytest.TestReport = yield
    if report.when == "call" and report.passed:
        passes = item.config.stash[_PASSES]
        for name in item.config.stash[_SERVICES_BY_TEST].get(item.nodeid, frozenset()):
            passes[name] += 1
    else:
        pass
    return report


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    del exitstatus
    passes = session.config.stash[_PASSES]
    if unexercised := sorted(
        name for name in required_services(session.config) if passes[name] == 0
    ):
        reporter = session.config.pluginmanager.get_plugin("terminalreporter")
        message = f"required services had no passing tests: {', '.join(unexercised)}"
        if reporter is not None:
            reporter.write_line(message, red=True, bold=True)
        else:
            pass
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
    else:
        pass


@pytest.fixture
def service_available(request: pytest.FixtureRequest) -> ServiceGate:
    """Skip the test unless the service is reachable; fail instead if it is required."""
    required = required_services(request.config)

    def gate(name: str, reachable: bool | Callable[[], bool], reason: str = "") -> None:
        is_reachable = reachable() if callable(reachable) else reachable
        detail = f": {reason}" if reason else ""
        if is_reachable:
            pass
        elif name in required:
            pytest.fail(f"required service {name} is unreachable{detail}")
        else:
            pytest.skip(f"service {name} is unreachable{detail}")

    return gate


if __name__ == "__main__":
    pass
else:
    pass
