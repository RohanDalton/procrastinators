"""``RateLimiter.from_config``: configured limiters behave as constructed ones (G1, G6, G8)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import asyncio

import pytest

from procrastinators.config import ConfigLocations, project_file
from procrastinators.config_models import ConfigSource
from procrastinators.errors import ConfigurationError, PolicyConflict, UnsupportedCapability
from procrastinators.limiter import RateLimiter
from procrastinators.models import Algorithms, Limit
from tests.config.conftest import PROFILE, PROJECT
from tests.limiters import ANKH, Rig


@pytest.fixture
def configured(locations: ConfigLocations) -> ConfigLocations:
    """Locations whose project gives the profile two per second on a sliding log."""
    project_file(PROJECT, locations=locations).set_profile(
        PROFILE, limits=[Limit(2, per="1s")], timeout=0
    )
    return locations


def _rig_limiter(
    rig: Rig, locations: ConfigLocations, environ: dict[str, str], **overrides: object
) -> RateLimiter:
    limiter = RateLimiter.from_config(
        PROFILE,
        key=ANKH,
        project=PROJECT,
        environ=environ,
        locations=locations,
        registry=rig.registry,
        clock=rig.timeline.deadline_clock,
        sleeper=rig.sleeper,
        async_sleeper=rig.async_sleeper,
        backend="rig://",
        **overrides,  # ty: ignore[invalid-argument-type]
    )
    return limiter


def test_a_configured_limiter_enforces_the_profiles_limits(
    rig: Rig, configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A project profile of two per second with timeout 0.
    When:  A limiter from that profile acquires three times at one instant.
    Then:  Two are admitted, and the third attempt is denied without waiting.
    """
    limiter = _rig_limiter(rig, configured, environ)
    expected = [True, True, False]

    actual = [limiter.try_acquire().allowed for _ in range(3)]

    assert actual == expected


def test_a_configured_limiter_decorates_and_enters(
    rig: Rig, configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A configured limiter with its timeout overridden to wait indefinitely.
    When:  It decorates a function called three times, then is entered as a context.
    Then:  Every call runs, waiting on fake time for the third and fourth.
    """
    limiter = _rig_limiter(rig, configured, environ, timeout=None)
    calls: list[int] = list()

    @limiter
    def fetch_page(page: int) -> int:
        calls.append(page)
        return page

    pages = [fetch_page(page) for page in range(3)]
    with limiter as admission:
        pass

    assert pages == [0, 1, 2]
    assert calls == [0, 1, 2]
    assert admission.cost == 1
    assert rig.sleeper.sleeps


def test_a_configured_limiter_works_asynchronously(
    rig: Rig, configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A configured limiter.
    When:  It is entered asynchronously.
    Then:  The admission is returned, as with a constructed limiter.
    """
    limiter = _rig_limiter(rig, configured, environ)

    async def enter() -> int:
        async with limiter as admission:
            cost = admission.cost
        return cost

    assert asyncio.run(enter()) == 1


def test_the_limiter_remembers_where_its_settings_came_from(
    rig: Rig, configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A limiter from configuration, with an environment algorithm and an argument backend.
    When:  Its configuration is explained.
    Then:  G7: each setting names its layer, and a constructed limiter has no configuration.
    """
    environ["PROCRASTINATORS_ALGORITHM"] = "fixed_window"
    limiter = _rig_limiter(rig, configured, environ)
    config = limiter.config

    assert config is not None
    assert config.settings.algorithm is Algorithms.FIXED_WINDOW
    assert config.source_of("algorithm") is ConfigSource.ENVIRONMENT
    assert config.source_of("backend") is ConfigSource.ARGUMENTS
    assert config.source_of("limits") is ConfigSource.PROJECT
    assert rig.limiter().config is None


def test_arguments_beat_every_layer(
    rig: Rig, configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: Project and environment limits, and limits passed as an override.
    When:  The limiter is built.
    Then:  G1: the override's single rule of one is what is enforced.
    """
    environ["PROCRASTINATORS_LIMITS"] = "5/1s"
    limiter = _rig_limiter(rig, configured, environ, limits=[Limit(1, per="1s")])
    expected = [True, False]

    actual = [limiter.try_acquire().allowed for _ in range(2)]

    assert actual == expected


def test_a_profile_without_limits_cannot_build_a_limiter(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: No layer supplying limits for the profile.
    When:  A limiter is built from it.
    Then:  ConfigurationError: there is no default vendor rate.
    """
    with pytest.raises(ConfigurationError, match="supplies no limits"):
        RateLimiter.from_config(PROFILE, key=ANKH, environ=environ, locations=locations)


def test_unknown_overrides_are_refused(
    configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: An override that is not a configuration field.
    When:  A limiter is built.
    Then:  G5: ConfigurationError naming it.
    """
    with pytest.raises(ConfigurationError, match="retries"):
        RateLimiter.from_config(
            PROFILE, key=ANKH, project=PROJECT, environ=environ, locations=configured, retries=3
        )


def test_capabilities_are_checked_before_the_first_admission(
    configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A configured backend family nobody registered.
    When:  A limiter is built.
    Then:  UnsupportedCapability at construction, not at the first acquisition.
    """
    environ["PROCRASTINATORS_BACKEND"] = "clacks://tower"

    with pytest.raises(UnsupportedCapability, match="clacks"):
        RateLimiter.from_config(
            PROFILE, key=ANKH, project=PROJECT, environ=environ, locations=configured
        )


def test_a_changed_configuration_conflicts_rather_than_granting_fresh_quota(
    configured: ConfigLocations, environ: dict[str, str], store_name: str
) -> None:
    """
    Given: A worker that admitted under the project's two per second on a shared store.
    When:  The project is edited to five per second and a second worker starts from it.
    Then:  G8: the second worker meets PolicyConflict; the edit is not a new quota identity.
    """
    environ["PROCRASTINATORS_BACKEND"] = f"memory://{store_name}"
    before = RateLimiter.from_config(
        PROFILE, key=ANKH, project=PROJECT, environ=environ, locations=configured
    )
    assert before.try_acquire().allowed
    project_file(PROJECT, locations=configured).set_profile(PROFILE, limits=[Limit(5, per="1s")])

    after = RateLimiter.from_config(
        PROFILE, key=ANKH, project=PROJECT, environ=environ, locations=configured
    )

    with pytest.raises(PolicyConflict):
        after.try_acquire()


def test_configuration_is_resolved_once(
    rig: Rig, configured: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A configured limiter.
    When:  The project file is edited afterwards.
    Then:  G6: the limiter keeps enforcing what it resolved.
    """
    limiter = _rig_limiter(rig, configured, environ)
    project_file(PROJECT, locations=configured).set_profile(PROFILE, limits=[Limit(9, per="1s")])
    expected = [True, True, False]

    actual = [limiter.try_acquire().allowed for _ in range(3)]

    assert actual == expected
    assert limiter.config is not None
    assert limiter.config.settings.limits == (Limit(2, per="1s"),)


if __name__ == "__main__":
    pass
else:
    pass
