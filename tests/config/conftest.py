"""Fixtures for configuration tests.

Every test gets its own user and site configuration directories under
``tmp_path`` and an empty environment, so nothing reads or writes the real
PlatformDirs locations or the real ``PROCRASTINATORS_*`` variables.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import uuid
from typing import TYPE_CHECKING

import pytest

from procrastinators.config import ConfigFile, ConfigLocations
from procrastinators.config_models import ConfigSource

if TYPE_CHECKING:
    from pathlib import Path
else:
    pass

PROJECT = "etl"
"""The project id tests select."""

ORGANIZATION = "guild"
"""The organization id tests select."""

PROFILE = "ankh.orders"
"""The profile tests resolve."""


def write(path: Path, text: str) -> Path:
    """Write ``text`` to ``path``, creating its directory, and return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def locations(tmp_path: Path) -> ConfigLocations:
    """User and site configuration directories that exist only for this test."""
    fresh = ConfigLocations(user_config=tmp_path / "user", site_config=tmp_path / "site")
    return fresh


@pytest.fixture
def environ() -> dict[str, str]:
    """An environment with no ``PROCRASTINATORS_*`` variables, for tests to fill."""
    fresh: dict[str, str] = dict()
    return fresh


@pytest.fixture
def project_store(locations: ConfigLocations) -> ConfigFile:
    """The selected project's file, not yet written."""
    store = ConfigFile(locations.project(PROJECT), ConfigSource.PROJECT)
    return store


@pytest.fixture
def organization_store(locations: ConfigLocations) -> ConfigFile:
    """The selected organization's user file, not yet written."""
    store = ConfigFile(locations.organization(ORGANIZATION), ConfigSource.ORGANIZATION)
    return store


@pytest.fixture
def store_name() -> str:
    """A memory store name no other test shares."""
    name = f"config-{uuid.uuid4().hex}"
    return name


if __name__ == "__main__":
    pass
else:
    pass
