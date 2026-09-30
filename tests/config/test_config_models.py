"""Contract tests for configuration layers and resolution (section 12)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import copy

import pytest

from procrastinators import (
    UNSET,
    Algorithms,
    ConfigLayer,
    ConfigSource,
    ConfigurationError,
    Limit,
    LimiterSettings,
    Provenance,
    ResolvedConfig,
    Unset,
)
from procrastinators.config_models import REDACTED, redact

UNUSABLE_SETTINGS: dict[str, dict[str, object]] = {
    "algorithm-as-string": {"algorithm": "sliding_log"},
    "negative-timeout": {"timeout": -1},
    "boolean-timeout": {"timeout": True},
    "zero-storage-timeout": {"storage_timeout": 0},
    "negative-storage-timeout": {"storage_timeout": -5},
}

SECRET_FIELDS: dict[str, tuple[str, str]] = {
    "password": ("password", "luggage"),
    "api-key": ("api_key", "sekrit"),
    "vendor-token": ("vendor_token", "sekrit"),
    "credential": ("credential", "sekrit"),
}


@pytest.fixture(params=list(UNUSABLE_SETTINGS))
def unusable_settings(request: pytest.FixtureRequest) -> dict[str, object]:
    """Keyword arguments ``LimiterSettings`` must refuse."""
    settings = copy.deepcopy(UNUSABLE_SETTINGS[request.param])
    return settings


@pytest.fixture(params=list(SECRET_FIELDS))
def secret_field(request: pytest.FixtureRequest) -> tuple[str, str]:
    """A ``(field name, value)`` pair whose name marks it as a secret."""
    field = SECRET_FIELDS[request.param]
    return field


@pytest.fixture
def resolved_with_secrets() -> ResolvedConfig:
    """A resolution whose provenance includes a credentialed URL and an API key."""
    resolved = ResolvedConfig(
        settings=LimiterSettings(timeout=30),
        provenance=(
            Provenance("timeout", ConfigSource.ARGUMENTS, "constructor", 30),
            Provenance("backend", ConfigSource.PROJECT, "ankh.toml", "redis://u:p@host:6379/0"),
            Provenance("api_key", ConfigSource.ENVIRONMENT, "PROCRASTINATORS_API_KEY", "sekrit"),
        ),
    )
    return resolved


def test_the_precedence_the_goals_asked_for() -> None:
    """
    Given: The configuration sources.
    When:  They are sorted.
    Then:  G1: arguments beat environment beat project beat organization beat defaults.
    """
    expected = [
        ConfigSource.ARGUMENTS,
        ConfigSource.ENVIRONMENT,
        ConfigSource.PROJECT,
        ConfigSource.ORGANIZATION,
        ConfigSource.LIBRARY_DEFAULT,
    ]
    actual = sorted(ConfigSource)
    assert actual == expected
    assert ConfigSource.ARGUMENTS < ConfigSource.ORGANIZATION


def test_unset_is_a_singleton_that_reads_as_absence() -> None:
    """
    Given: The ``Unset`` type and its ``UNSET`` instance.
    When:  ``Unset`` is constructed, and ``UNSET`` is tested for truth and repr'd.
    Then:  Construction returns ``UNSET`` itself, which is falsy and reprs as ``UNSET``.
    """
    assert Unset() is UNSET
    assert not UNSET  # ty: ignore[redundant-condition]
    assert repr(UNSET) == "UNSET"


def test_an_explicit_null_is_not_the_same_as_saying_nothing() -> None:
    """
    Given: One layer that never mentions ``timeout`` and one that sets it to None.
    When:  Each is asked for ``timeout``.
    Then:  G2: the silent layer yields UNSET and the deliberate one yields None, which
           is what lets a project file override an org default back to 'no timeout'.
    """
    silent = ConfigLayer(ConfigSource.PROJECT, {"algorithm": "sliding_log"})
    deliberate = ConfigLayer(ConfigSource.PROJECT, {"timeout": None})

    assert silent.get("timeout") is UNSET
    assert deliberate.get("timeout") is None
    assert deliberate.get("timeout") is not UNSET


def test_a_layer_cannot_be_edited_after_it_is_read() -> None:
    """
    Given: A layer built from a mutable mapping.
    When:  The source mapping is changed, and the layer's own values are assigned to.
    Then:  G6: the layer keeps its original value and refuses the assignment, since
           resolved configuration is applied once, not mutated underneath a worker.
    """
    values = {"timeout": 30}
    layer = ConfigLayer(ConfigSource.PROJECT, values)
    values["timeout"] = 1
    assert layer.get("timeout") == 30
    with pytest.raises(TypeError):
        layer.values["timeout"] = 1  # ty: ignore[invalid-assignment]


def test_configuration_keys_are_strings() -> None:
    """
    Given: A mapping with a non-string key.
    When:  A layer is built from it.
    Then:  ConfigurationError is raised.
    """
    with pytest.raises(ConfigurationError):
        ConfigLayer(ConfigSource.PROJECT, {1: "one"})  # ty: ignore[invalid-argument-type]


def test_settings_default_to_an_exact_policy_and_no_invented_rates() -> None:
    """
    Given: No arguments.
    When:  ``LimiterSettings`` is constructed.
    Then:  G1: it uses the exact sliding-log policy and no limits, because the library
           supplies behavior defaults, never a vendor's quota.
    """
    settings = LimiterSettings()
    assert settings.algorithm is Algorithms.SLIDING_LOG
    assert settings.limits == tuple()


def test_limits_are_kept_as_an_immutable_whole() -> None:
    """
    Given: Two limits passed as a list.
    When:  ``LimiterSettings`` is constructed.
    Then:  G4: both are kept as a tuple, since the strongest layer that sets limits
           supplies all of them.
    """
    settings = LimiterSettings(limits=[Limit(10, per="1s"), Limit(500, per="1m")])
    assert isinstance(settings.limits, tuple)
    assert len(settings.limits) == 2


def test_unusable_settings_are_refused(unusable_settings: dict[str, object]) -> None:
    """
    Given: Any unusable setting (wrong type, negative, or zero where positive is needed).
    When:  ``LimiterSettings`` is constructed with it.
    Then:  ConfigurationError is raised.
    """
    with pytest.raises(ConfigurationError):
        LimiterSettings(**unusable_settings)  # ty: ignore[invalid-argument-type]


def test_an_unbounded_quota_wait_still_bounds_each_storage_call() -> None:
    """
    Given: Settings with ``timeout=None``.
    When:  They are constructed.
    Then:  B3: the timeout stays None while storage_timeout remains a positive bound.
    """
    settings = LimiterSettings(timeout=None)
    assert settings.timeout is None
    assert settings.storage_timeout > 0


def test_a_field_that_sounds_like_a_secret_is_redacted(secret_field: tuple[str, str]) -> None:
    """
    Given: Any field whose name sounds like a secret.
    When:  Its value is redacted.
    Then:  G7: the whole value is replaced with the redaction marker.
    """
    name, value = secret_field
    expected = REDACTED
    actual = redact(name, value)
    assert actual == expected


def test_a_password_hidden_inside_a_backend_url_is_redacted_too() -> None:
    """
    Given: A ``backend`` URL carrying credentials.
    When:  It is redacted.
    Then:  G7: the credentials are masked even though 'backend' is an innocuous name.
    """
    expected = f"redis://{REDACTED}@localhost:6379/0"
    actual = redact("backend", "redis://rincewind:luggage@localhost:6379/0")
    assert actual == expected


@pytest.mark.parametrize("parameter", ["password", "pass%77ord", "PASSWORD"])
def test_a_password_in_a_backend_query_is_redacted(parameter: str) -> None:
    """
    Given: A backend URL whose query carries a password, possibly with an encoded key.
    When:  It is prepared for display.
    Then:  The password value is replaced by the redaction marker.
    """
    actual = redact("backend", f"redis://localhost/0?{parameter}=topsecret&db=1")

    assert isinstance(actual, str)
    assert "topsecret" not in actual
    assert f"{parameter}={REDACTED}" in actual


def test_explain_hides_a_password_supplied_in_a_backend_query() -> None:
    """
    Given: A resolved backend URL carrying a query password from the environment.
    When:  The configuration explains its provenance.
    Then:  The explanation does not reveal the password.
    """
    resolved = ResolvedConfig(
        settings=LimiterSettings(),
        provenance=(
            Provenance(
                "backend",
                ConfigSource.ENVIRONMENT,
                "PROCRASTINATORS_BACKEND",
                "redis://localhost/0?password=topsecret",
            ),
        ),
    )

    explanation = "\n".join(resolved.explain())

    assert "topsecret" not in explanation
    assert REDACTED in explanation


def test_a_backend_url_without_credentials_is_left_alone() -> None:
    """
    Given: A ``backend`` URL with no credentials in it.
    When:  It is redacted.
    Then:  G7: the URL comes back unchanged.
    """
    expected = "sqlite:///./quota.sqlite3"
    actual = redact("backend", "sqlite:///./quota.sqlite3")
    assert actual == expected


def test_explain_says_where_each_value_came_from_without_leaking_it(
    resolved_with_secrets: ResolvedConfig,
) -> None:
    """
    Given: A resolution whose provenance includes secrets.
    When:  It is explained.
    Then:  G7: each value is attributed to its source and no secret appears in the text.
    """
    lines = resolved_with_secrets.explain()

    assert "sekrit" not in "\n".join(lines)
    assert "u:p" not in "\n".join(lines)
    assert lines[0].startswith("timeout = 30 from arguments (constructor)")


def test_source_of_names_the_layer_that_set_a_field(
    resolved_with_secrets: ResolvedConfig,
) -> None:
    """
    Given: A resolution whose ``backend`` came from the project layer.
    When:  The source of ``backend`` is requested.
    Then:  The project layer is named.
    """
    expected = ConfigSource.PROJECT
    actual = resolved_with_secrets.source_of("backend")
    assert actual is expected


def test_source_of_an_unset_field_is_none(resolved_with_secrets: ResolvedConfig) -> None:
    """
    Given: A resolution in which nothing set a given field.
    When:  The source of that field is requested.
    Then:  None is returned.
    """
    assert resolved_with_secrets.source_of("nothing-set-this") is None


def test_a_field_cannot_come_from_two_places_at_once() -> None:
    """
    Given: Provenance naming the same field from two sources.
    When:  A resolution is built from it.
    Then:  ConfigurationError is raised explaining a field may appear at most once.
    """
    with pytest.raises(ConfigurationError, match="at most once"):
        ResolvedConfig(
            settings=LimiterSettings(),
            provenance=(
                Provenance("timeout", ConfigSource.ARGUMENTS),
                Provenance("timeout", ConfigSource.PROJECT),
            ),
        )


if __name__ == "__main__":
    pass
else:
    pass
