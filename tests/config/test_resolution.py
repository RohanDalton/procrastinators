"""Layer resolution: precedence, nulls, wholesale rates, selection, and explanation (G1-G9)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import copy
from typing import TYPE_CHECKING

import pytest

from procrastinators.config import ConfigLocations, explain, resolve
from procrastinators.config_models import DEFAULT_BACKEND, ConfigSource
from procrastinators.errors import ConfigurationError
from procrastinators.models import Algorithms, Limit
from tests.config.conftest import ORGANIZATION, PROFILE, PROJECT, write

if TYPE_CHECKING:
    from pathlib import Path
else:
    pass

LAYERS = ("arguments", "environment", "project", "organization")

# For each scalar field: the value each layer sets (as that layer spells it),
# the value each resolves to, and the library default.
PRECEDENCE: dict[str, dict[str, object]] = {
    "timeout": {
        "spelled": {"arguments": 1, "environment": "2", "project": 3, "organization": "4s"},
        "resolved": {"arguments": 1.0, "environment": 2.0, "project": 3.0, "organization": 4.0},
        "default": None,
    },
    "storage_timeout": {
        "spelled": {"arguments": 1.5, "environment": "250ms", "project": 3, "organization": 4},
        "resolved": {"arguments": 1.5, "environment": 0.25, "project": 3.0, "organization": 4.0},
        "default": 5.0,
    },
    "algorithm": {
        "spelled": {
            "arguments": Algorithms.FIXED_WINDOW,
            "environment": "token_bucket",
            "project": "leaky_bucket",
            "organization": "sliding_counter",
        },
        "resolved": {
            "arguments": Algorithms.FIXED_WINDOW,
            "environment": Algorithms.TOKEN_BUCKET,
            "project": Algorithms.LEAKY_BUCKET,
            "organization": Algorithms.SLIDING_COUNTER,
        },
        "default": Algorithms.SLIDING_LOG,
    },
    "namespace": {
        "spelled": {
            "arguments": "ankh",
            "environment": "morpork",
            "project": "sto",
            "organization": "lat",
        },
        "resolved": {
            "arguments": "ankh",
            "environment": "morpork",
            "project": "sto",
            "organization": "lat",
        },
        "default": "default",
    },
    "backend": {
        "spelled": {
            "arguments": "memory://a",
            "environment": "memory://b",
            "project": "memory://c",
            "organization": "memory://d",
        },
        "resolved": {
            "arguments": "memory://a",
            "environment": "memory://b",
            "project": "memory://c",
            "organization": "memory://d",
        },
        "default": DEFAULT_BACKEND,
    },
}

SOURCES = {
    "arguments": ConfigSource.ARGUMENTS,
    "environment": ConfigSource.ENVIRONMENT,
    "project": ConfigSource.PROJECT,
    "organization": ConfigSource.ORGANIZATION,
    "default": ConfigSource.LIBRARY_DEFAULT,
}

BAD_FILES: dict[str, str] = {
    "no-version": "[defaults]\ntimeout = 1\n",
    "future-version": "version = 2\n",
    "boolean-version": "version = true\n",
    "unknown-top-level": "version = 1\n[vendors]\nankh = 1\n",
    "unknown-field": "version = 1\n[defaults]\nretries = 3\n",
    "unknown-limit-key": (
        'version = 1\n[profiles."ankh.orders"]\nlimits = [{ amount = 1, per = "1s", burst = 2 }]\n'
    ),
    "limit-without-per": 'version = 1\n[profiles."ankh.orders"]\nlimits = [{ amount = 1 }]\n',
    "bad-algorithm": 'version = 1\n[defaults]\nalgorithm = "hex"\n',
    "limits-and-rules": (
        'version = 1\n[defaults]\nlimits = [{ amount = 1, per = "1s" }]\n'
        'rules = { a = { amount = 1, per = "1s" } }\n'
    ),
    "null-storage-timeout": 'version = 1\n[defaults]\nstorage_timeout = "none"\n',
    "profile-not-a-table": 'version = 1\n[profiles]\n"ankh.orders" = 3\n',
    "not-toml": "version = = 1\n",
}

BAD_ENVIRONMENTS: dict[str, dict[str, str]] = {
    "unknown-variable": {"PROCRASTINATORS_TIMEOT": "3"},
    "empty-value": {"PROCRASTINATORS_TIMEOUT": ""},
    "malformed-rate": {"PROCRASTINATORS_LIMITS": "ten per second"},
    "rate-without-period": {"PROCRASTINATORS_LIMITS": "10/"},
    "rule-without-name": {"PROCRASTINATORS_RULES": "=10/1s"},
    "option-without-value": {"PROCRASTINATORS_OPTIONS": "capacity"},
    "limits-and-rules": {
        "PROCRASTINATORS_LIMITS": "10/1s",
        "PROCRASTINATORS_RULES": "burst=10/1s",
    },
    "unknown-algorithm": {"PROCRASTINATORS_ALGORITHM": "hex"},
    "negative-timeout": {"PROCRASTINATORS_TIMEOUT": "-1"},
}


@pytest.fixture(params=list(PRECEDENCE))
def field(request: pytest.FixtureRequest) -> str:
    """Each scalar configuration field."""
    return request.param


@pytest.fixture(params=[*LAYERS, "default"])
def strongest(request: pytest.FixtureRequest) -> str:
    """The strongest layer that sets the field under test."""
    return request.param


@pytest.fixture(params=list(BAD_FILES))
def bad_file(request: pytest.FixtureRequest) -> str:
    """A project file that must be refused."""
    return BAD_FILES[request.param]


@pytest.fixture(params=list(BAD_ENVIRONMENTS))
def bad_environment(request: pytest.FixtureRequest) -> dict[str, str]:
    """An environment that must be refused."""
    environment = copy.deepcopy(BAD_ENVIRONMENTS[request.param])
    return environment


def _toml(value: object) -> str:
    rendered = f'"{value}"' if isinstance(value, str) else str(value)
    return rendered


def test_the_strongest_layer_that_sets_a_field_wins(
    field: str,
    strongest: str,
    locations: ConfigLocations,
    environ: dict[str, str],
    tmp_path: Path,
) -> None:
    """
    Given: Any scalar field set by every layer from the strongest one down.
    When:  The profile is resolved.
    Then:  G1: the strongest layer's value wins and provenance names that layer.
    """
    data = PRECEDENCE[field]
    spelled = data["spelled"]
    assert isinstance(spelled, dict)
    present = LAYERS[LAYERS.index(strongest) :] if strongest in LAYERS else tuple()
    arguments: dict[str, object] = {"limits": [Limit(10, per="1s")]}
    if "arguments" in present:
        arguments[field] = spelled["arguments"]
    else:
        pass
    if "environment" in present:
        environ[f"PROCRASTINATORS_{field.upper()}"] = str(spelled["environment"])
    else:
        pass
    for layer, path in (
        ("project", locations.project(PROJECT)),
        ("organization", locations.organization(ORGANIZATION)),
    ):
        if layer in present:
            write(path, f"version = 1\n[defaults]\n{field} = {_toml(spelled[layer])}\n")
        else:
            pass
    resolved_values = data["resolved"]
    assert isinstance(resolved_values, dict)
    expected = (
        resolved_values[strongest] if strongest in LAYERS else data["default"],
        SOURCES[strongest],
    )

    resolved = resolve(
        PROFILE,
        project=PROJECT,
        organization=ORGANIZATION,
        arguments=arguments,
        environ=environ,
        locations=locations,
    )
    actual = (getattr(resolved.settings, field), resolved.source_of(field))

    assert actual == expected


def test_a_project_default_beats_an_organization_profile(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: An organization profile setting a timeout and a project [defaults] setting another.
    When:  The profile is resolved.
    Then:  G1: layer precedence comes first; within the project, profile beats defaults.
    """
    write(
        locations.organization(ORGANIZATION),
        f'version = 1\n[profiles."{PROFILE}"]\ntimeout = 60\n'
        'limits = [{ amount = 5, per = "1s" }]\n',
    )
    write(locations.project(PROJECT), "version = 1\n[defaults]\ntimeout = 10\n")
    expected = (10.0, ConfigSource.PROJECT, (Limit(5, per="1s"),))

    resolved = resolve(
        PROFILE, project=PROJECT, organization=ORGANIZATION, environ=environ, locations=locations
    )
    actual = (resolved.settings.timeout, resolved.source_of("timeout"), resolved.settings.limits)

    assert actual == expected


def test_a_profile_overrides_its_own_files_defaults(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A project file whose defaults and profile both set the algorithm.
    When:  The profile and another profile are resolved.
    Then:  The profile's value wins for it; the other profile falls back to the defaults.
    """
    write(
        locations.project(PROJECT),
        'version = 1\n[defaults]\nalgorithm = "fixed_window"\n'
        'limits = [{ amount = 1, per = "1s" }]\n'
        f'[profiles."{PROFILE}"]\nalgorithm = "token_bucket"\n',
    )
    expected = (Algorithms.TOKEN_BUCKET, Algorithms.FIXED_WINDOW)

    actual = tuple(
        resolve(name, project=PROJECT, environ=environ, locations=locations).settings.algorithm
        for name in (PROFILE, "quirm.cheese")
    )

    assert actual == expected


def test_an_explicit_null_overrides_a_weaker_timeout(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: An organization timeout of 60 and a project timeout of "none".
    When:  The profile is resolved.
    Then:  G2: the project's explicit null wins — wait indefinitely — rather than falling through.
    """
    write(locations.organization(ORGANIZATION), "version = 1\n[defaults]\ntimeout = 60\n")
    write(locations.project(PROJECT), 'version = 1\n[defaults]\ntimeout = "none"\n')
    expected = (None, ConfigSource.PROJECT)

    resolved = resolve(
        PROFILE,
        project=PROJECT,
        organization=ORGANIZATION,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )
    actual = (resolved.settings.timeout, resolved.source_of("timeout"))

    assert actual == expected


def test_an_unset_timeout_falls_through(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: An organization timeout of 60 and a project that never mentions a timeout.
    When:  The profile is resolved.
    Then:  G2: the organization's value survives.
    """
    write(locations.organization(ORGANIZATION), "version = 1\n[defaults]\ntimeout = 60\n")
    write(locations.project(PROJECT), 'version = 1\n[defaults]\nnamespace = "sto"\n')
    expected = 60.0

    actual = resolve(
        PROFILE,
        project=PROJECT,
        organization=ORGANIZATION,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    ).settings.timeout

    assert actual == expected


def test_the_environment_spells_null_too(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A project timeout of 60 and PROCRASTINATORS_TIMEOUT=none.
    When:  The profile is resolved.
    Then:  G2: the environment's explicit null wins.
    """
    write(locations.project(PROJECT), "version = 1\n[defaults]\ntimeout = 60\n")
    environ["PROCRASTINATORS_TIMEOUT"] = "none"

    resolved = resolve(
        PROFILE,
        project=PROJECT,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert resolved.settings.timeout is None
    assert resolved.source_of("timeout") is ConfigSource.ENVIRONMENT


def test_the_strongest_rates_replace_weaker_ones_wholesale(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: An organization profile with three positional limits and a project profile with one.
    When:  The profile is resolved.
    Then:  G4: only the project's single limit survives; lists never merge element-wise.
    """
    write(
        locations.organization(ORGANIZATION),
        f'version = 1\n[profiles."{PROFILE}"]\nlimits = [{{ amount = 1, per = "1s" }}, '
        '{ amount = 2, per = "1m" }, { amount = 3, per = "1h" }]\n',
    )
    write(
        locations.project(PROJECT),
        f'version = 1\n[profiles."{PROFILE}"]\nlimits = [{{ amount = 9, per = "1s" }}]\n',
    )
    expected = (Limit(9, per="1s"),)

    actual = resolve(
        PROFILE, project=PROJECT, organization=ORGANIZATION, environ=environ, locations=locations
    ).settings.limits

    assert actual == expected


def test_named_rules_replace_positional_limits_as_one_value(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: Organization positional limits and a project profile with named rules.
    When:  The profile is resolved.
    Then:  G4: the project's rules supply the rates and no positional limit survives.
    """
    write(
        locations.organization(ORGANIZATION),
        f'version = 1\n[profiles."{PROFILE}"]\nlimits = [{{ amount = 1, per = "1s" }}]\n',
    )
    write(
        locations.project(PROJECT),
        f'version = 1\n[profiles."{PROFILE}"]\n'
        'rules = { burst = { amount = 5, per = "1s" } }\n',
    )
    expected = (tuple(), {"burst": Limit(5, per="1s")}, ConfigSource.PROJECT)

    resolved = resolve(
        PROFILE, project=PROJECT, organization=ORGANIZATION, environ=environ, locations=locations
    )
    actual = (
        resolved.settings.limits,
        dict(resolved.settings.rules or dict()),
        resolved.source_of("rules"),
    )

    assert actual == expected
    assert resolved.source_of("limits") is None


def test_a_profile_without_rates_names_itself(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: No layer that supplies limits for the profile.
    When:  It is resolved.
    Then:  ConfigurationError naming the profile: there is no default vendor rate.
    """
    with pytest.raises(ConfigurationError, match=r"'ankh\.orders' supplies no limits"):
        resolve(PROFILE, environ=environ, locations=locations)


def test_environment_rates_rules_and_options_parse(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: Rules and token-bucket options spelled in the environment.
    When:  The profile is resolved.
    Then:  They parse into named limits and typed options.
    """
    environ.update(
        {
            "PROCRASTINATORS_RULES": "burst=10/1s, sustained=500/1m",
            "PROCRASTINATORS_ALGORITHM": "token_bucket",
            "PROCRASTINATORS_OPTIONS": "capacity=20, initial_tokens=0",
        }
    )
    expected = (
        {"burst": Limit(10, per="1s"), "sustained": Limit(500, per="1m")},
        {"capacity": 20, "initial_tokens": 0},
    )

    settings = resolve(PROFILE, environ=environ, locations=locations).settings
    actual = (dict(settings.rules or dict()), dict(settings.options))

    assert actual == expected


def test_bad_environments_are_refused(
    bad_environment: dict[str, str], locations: ConfigLocations
) -> None:
    """
    Given: Any malformed, empty, contradictory, or unknown PROCRASTINATORS_ variable.
    When:  A profile is resolved.
    Then:  G5: ConfigurationError, rather than a silently ignored setting.
    """
    with pytest.raises(ConfigurationError):
        resolve(
            PROFILE,
            arguments={"limits": [Limit(1)]},
            environ=bad_environment,
            locations=locations,
        )


def test_the_service_gate_variable_is_reserved_not_rejected(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: PROCRASTINATORS_REQUIRE_SERVICES, which belongs to the test tooling.
    When:  A profile is resolved.
    Then:  Configuration ignores it rather than calling it unknown.
    """
    environ["PROCRASTINATORS_REQUIRE_SERVICES"] = "redis"
    expected = (Limit(1),)

    actual = resolve(
        PROFILE, arguments={"limits": [Limit(1)]}, environ=environ, locations=locations
    ).settings.limits

    assert actual == expected


def test_bad_files_are_refused(
    bad_file: str, locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: Any project file with an unknown key, table, version, or an invalid value.
    When:  The project is selected and a profile resolved.
    Then:  G5: ConfigurationError naming the file.
    """
    path = write(locations.project(PROJECT), bad_file)

    with pytest.raises(ConfigurationError, match=path.name):
        resolve(
            PROFILE,
            project=PROJECT,
            arguments={"limits": [Limit(1)]},
            environ=environ,
            locations=locations,
        )


def test_unknown_arguments_are_refused(locations: ConfigLocations, environ: dict[str, str]) -> None:
    """
    Given: An argument that names no configuration field.
    When:  A profile is resolved.
    Then:  G5: ConfigurationError.
    """
    with pytest.raises(ConfigurationError, match="unknown field"):
        resolve(
            PROFILE,
            arguments={"limits": [Limit(1)], "retries": 3},
            environ=environ,
            locations=locations,
        )


def test_options_that_do_not_fit_the_resolved_algorithm_are_refused(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: An organization setting token-bucket options and a project switching to a sliding log.
    When:  The profile is resolved.
    Then:  Changing the algorithm re-validates everything: ConfigurationError naming the sources.
    """
    write(
        locations.organization(ORGANIZATION),
        'version = 1\n[defaults]\nalgorithm = "token_bucket"\noptions = { capacity = 20 }\n',
    )
    write(locations.project(PROJECT), 'version = 1\n[defaults]\nalgorithm = "sliding_log"\n')

    with pytest.raises(ConfigurationError, match=r"options = .* from organization"):
        resolve(
            PROFILE,
            project=PROJECT,
            organization=ORGANIZATION,
            arguments={"limits": [Limit(10)]},
            environ=environ,
            locations=locations,
        )


def test_selection_comes_from_the_environment_when_not_passed(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: PROCRASTINATORS_PROJECT naming a project with a profile.
    When:  The profile is resolved without naming a project.
    Then:  G3: the environment's selection is used.
    """
    path = write(
        locations.project(PROJECT),
        f'version = 1\n[profiles."{PROFILE}"]\nlimits = [{{ amount = 7, per = "1s" }}]\n',
    )
    environ["PROCRASTINATORS_PROJECT"] = PROJECT

    resolved = resolve(PROFILE, environ=environ, locations=locations)

    assert resolved.settings.limits == (Limit(7, per="1s"),)
    assert str(path) in str(resolved.provenance)


def test_nothing_is_discovered_without_a_selection(
    locations: ConfigLocations, environ: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: A project file in the working directory and in the user location, but no selection.
    When:  The profile is resolved.
    Then:  G3: neither is read; a worker's behavior does not depend on where it started.
    """
    body = f'version = 1\n[profiles."{PROFILE}"]\nlimits = [{{ amount = 7, per = "1s" }}]\n'
    write(locations.project(PROJECT), body)
    workdir = locations.user_config.parent / "workdir"
    write(workdir / "pyproject.toml", "[tool.procrastinators]\n" + body)
    monkeypatch.chdir(workdir)

    with pytest.raises(ConfigurationError, match="supplies no limits"):
        resolve(PROFILE, environ=environ, locations=locations)


def test_a_selected_project_without_a_file_contributes_nothing(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A selected project whose file does not exist.
    When:  A profile is resolved.
    Then:  It resolves from the other layers, and no project layer is recorded.
    """
    resolved = resolve(
        PROFILE,
        project=PROJECT,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert ConfigSource.PROJECT not in {layer.source for layer in resolved.layers}


def test_an_explicit_file_that_does_not_exist_is_an_error(
    locations: ConfigLocations, environ: dict[str, str], tmp_path: Path
) -> None:
    """
    Given: An explicitly named organization file that does not exist.
    When:  A profile is resolved.
    Then:  ConfigurationError: a named file that is missing is a mistake, not an absent layer.
    """
    with pytest.raises(ConfigurationError, match="does not exist"):
        resolve(
            PROFILE,
            organization_file=tmp_path / "nowhere.toml",
            arguments={"limits": [Limit(1)]},
            environ=environ,
            locations=locations,
        )


def test_a_selected_pyproject_reads_its_tool_table(
    locations: ConfigLocations, environ: dict[str, str], tmp_path: Path
) -> None:
    """
    Given: A pyproject.toml with a [tool.procrastinators] table, named by
           PROCRASTINATORS_PROJECT_FILE.
    When:  A profile is resolved.
    Then:  The table is the project layer.
    """
    path = write(
        tmp_path / "repo" / "pyproject.toml",
        '[project]\nname = "ankh"\n\n[tool.procrastinators]\nversion = 1\n'
        f'[tool.procrastinators.profiles."{PROFILE}"]\nlimits = [{{ amount = 3, per = "1s" }}]\n',
    )
    environ["PROCRASTINATORS_PROJECT_FILE"] = str(path)
    expected = ((Limit(3, per="1s"),), ConfigSource.PROJECT)

    resolved = resolve(PROFILE, environ=environ, locations=locations)
    actual = (resolved.settings.limits, resolved.source_of("limits"))

    assert actual == expected


def test_a_pyproject_without_the_table_contributes_nothing(
    locations: ConfigLocations, environ: dict[str, str], tmp_path: Path
) -> None:
    """
    Given: A selected pyproject.toml without a [tool.procrastinators] table.
    When:  A profile is resolved.
    Then:  No project layer contributes.
    """
    path = write(tmp_path / "pyproject.toml", '[project]\nname = "ankh"\n')

    resolved = resolve(
        PROFILE,
        project_file=path,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert ConfigSource.PROJECT not in {layer.source for layer in resolved.layers}


def test_the_site_organization_file_is_preferred_when_present(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: Both a site and a user organization file.
    When:  The organization is selected.
    Then:  The site file is read, deterministically.
    """
    write(locations.organization(ORGANIZATION), 'version = 1\n[defaults]\nnamespace = "user"\n')
    site = write(
        locations.organization(ORGANIZATION, site=True),
        'version = 1\n[defaults]\nnamespace = "site"\n',
    )

    resolved = resolve(
        PROFILE,
        organization=ORGANIZATION,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert resolved.settings.namespace == "site"
    assert str(site) in str(resolved.provenance)


def test_the_user_organization_file_is_used_without_a_site_file(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: Only a user organization file.
    When:  The organization is selected.
    Then:  The user file is read.
    """
    write(locations.organization(ORGANIZATION), 'version = 1\n[defaults]\nnamespace = "user"\n')

    resolved = resolve(
        PROFILE,
        organization=ORGANIZATION,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert resolved.settings.namespace == "user"


def test_an_explicit_organization_path_beats_both_locations(
    locations: ConfigLocations, environ: dict[str, str], tmp_path: Path
) -> None:
    """
    Given: Site and user organization files, and an explicit path in the environment.
    When:  The organization is selected.
    Then:  The explicit file is read.
    """
    write(
        locations.organization(ORGANIZATION, site=True),
        'version = 1\n[defaults]\nnamespace = "site"\n',
    )
    explicit = write(tmp_path / "shared.toml", 'version = 1\n[defaults]\nnamespace = "shared"\n')
    environ["PROCRASTINATORS_ORGANIZATION_FILE"] = str(explicit)

    resolved = resolve(
        PROFILE,
        organization=ORGANIZATION,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert resolved.settings.namespace == "shared"


@pytest.mark.parametrize("unsafe", ["../escape", "a/b", "", ".hidden", "a..b", "etl\n"])
def test_selection_ids_cannot_escape_their_directory(
    unsafe: str, locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A project id that is not a plain file name.
    When:  It is selected.
    Then:  ConfigurationError, before any path is built from it.
    """
    with pytest.raises(ConfigurationError):
        resolve(
            PROFILE,
            project=unsafe,
            arguments={"limits": [Limit(1)]},
            environ=environ,
            locations=locations,
        )


def test_a_file_cannot_hold_a_password(locations: ConfigLocations, environ: dict[str, str]) -> None:
    """
    Given: A project file whose backend URL carries a password.
    When:  It is selected.
    Then:  G9: ConfigurationError pointing at PROCRASTINATORS_BACKEND.
    """
    write(
        locations.project(PROJECT),
        'version = 1\n[defaults]\nbackend = "redis://rincewind:luggage@localhost:6379/0"\n',
    )

    with pytest.raises(ConfigurationError, match="PROCRASTINATORS_BACKEND"):
        resolve(
            PROFILE,
            project=PROJECT,
            arguments={"limits": [Limit(1)]},
            environ=environ,
            locations=locations,
        )


def test_a_user_without_a_password_is_fine_in_a_file(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A project backend URL naming a user but no password.
    When:  It is selected.
    Then:  It resolves.
    """
    write(locations.project(PROJECT), 'version = 1\n[defaults]\nbackend = "redis://ops@cache/0"\n')

    resolved = resolve(
        PROFILE,
        project=PROJECT,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )

    assert resolved.settings.backend == "redis://ops@cache/0"


def test_explain_attributes_every_field_and_redacts_secrets(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A password-bearing backend in the environment, a project timeout, and argument limits.
    When:  The profile is explained.
    Then:  G7: one line per field, strongest source first, naming each origin, with no password.
    """
    project = write(locations.project(PROJECT), "version = 1\n[defaults]\ntimeout = 30\n")
    environ["PROCRASTINATORS_BACKEND"] = "redis://rincewind:luggage@localhost:6379/0"

    lines = explain(
        PROFILE,
        project=PROJECT,
        arguments={"limits": [Limit(1)]},
        environ=environ,
        locations=locations,
    )
    text = "\n".join(lines)
    sources = [ConfigSource[line.split(" from ", 1)[1].split(" ", 1)[0].upper()] for line in lines]

    assert "luggage" not in text
    assert (
        "backend = 'redis://***@localhost:6379/0' from environment (PROCRASTINATORS_BACKEND)"
        in text
    )
    assert f"timeout = 30.0 from project ({project})" in text
    assert sources == sorted(sources)
    assert len(lines) == 8


def test_resolution_is_immutable(locations: ConfigLocations, environ: dict[str, str]) -> None:
    """
    Given: A resolved configuration.
    When:  Its rules or options are assigned to.
    Then:  G6: they refuse; resolved configuration is applied once.
    """
    environ["PROCRASTINATORS_RULES"] = "burst=10/1s"
    settings = resolve(PROFILE, environ=environ, locations=locations).settings

    with pytest.raises(TypeError):
        settings.options["capacity"] = 3  # ty: ignore[invalid-assignment]
    with pytest.raises(TypeError):
        settings.rules["other"] = Limit(1)  # ty: ignore[invalid-assignment]


if __name__ == "__main__":
    pass
else:
    pass
