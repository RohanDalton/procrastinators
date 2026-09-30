"""Configuration files: the narrow TOML writer, atomic saves, and revision checks (G5, G9, G10)."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import datetime as dt
import threading
import tomllib
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

import procrastinators.config as config_module
from procrastinators.config import (
    ConfigFile,
    ConfigLocations,
    organization_file,
    project_file,
    render_toml,
    resolve,
)
from procrastinators.config_models import UNSET, ConfigLayer, ConfigSource
from procrastinators.errors import ConfigurationError
from procrastinators.models import Algorithms, Limit
from tests.config.conftest import ORGANIZATION, PROFILE, PROJECT, write

WRITERS = 6

KEYS = st.text(max_size=12)
SCALARS = (
    st.text(max_size=20)
    | st.integers(min_value=-(2**63), max_value=2**63 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.booleans()
)


def _nested(children: st.SearchStrategy[object]) -> st.SearchStrategy[object]:
    nested = st.lists(children, max_size=4) | st.dictionaries(KEYS, children, max_size=4)
    return nested


VALUES = st.recursive(SCALARS, _nested, max_leaves=12)
TABLES = st.dictionaries(KEYS, VALUES, max_size=5)
DOCUMENTS = st.fixed_dictionaries(
    {"version": st.just(1)},
    optional={"defaults": TABLES, "profiles": st.dictionaries(KEYS, TABLES, max_size=4)},
)

ORDERS_DOCUMENT: dict[str, object] = {
    "version": 1,
    "defaults": {"algorithm": "sliding_log", "timeout": 60},
    "profiles": {
        "ankh.orders": {"limits": [{"amount": 10, "per": "1s"}, {"amount": 500, "per": "1m"}]},
    },
}


@pytest.fixture
def orders_layer() -> ConfigLayer:
    """A project layer holding the design's example document."""
    layer = ConfigLayer(ConfigSource.PROJECT, ORDERS_DOCUMENT)
    return layer


@given(DOCUMENTS)
def test_the_writer_round_trips_through_tomllib(document: dict[str, object]) -> None:
    """
    Given: Any document of the configuration's shape, with arbitrary keys and values.
    When:  It is written and read back by tomllib.
    Then:  The same document comes back: the narrow writer needs no TOML library.
    """
    expected = document

    actual = tomllib.loads(render_toml(document))

    assert actual == expected


@pytest.mark.parametrize(
    "document",
    [
        {"version": 1, "defaults": {"0\n": list()}},
        {"version": 1, "profiles": {"0\n": dict()}},
    ],
    ids=["defaults-key", "profile-name"],
)
def test_a_key_ending_in_a_newline_is_quoted(document: dict[str, object]) -> None:
    """
    Given: A key whose only unusual character is a trailing newline.
    When:  The document is written and read back.
    Then:  The key was quoted and escaped, so it comes back unchanged.
    """
    expected = document

    actual = tomllib.loads(render_toml(document))

    assert actual == expected


@pytest.mark.parametrize("value", [float("inf"), float("nan"), 2**64, dt.date(2026, 1, 1)])
def test_the_writer_refuses_what_toml_cannot_hold(value: object) -> None:
    """
    Given: A value TOML configuration cannot store.
    When:  It is written.
    Then:  ConfigurationError, rather than a file tomllib would reject or misread.
    """
    with pytest.raises(ConfigurationError):
        render_toml({"version": 1, "defaults": {"value": value}})


def test_a_saved_file_loads_back_with_its_revision(
    project_store: ConfigFile, orders_layer: ConfigLayer
) -> None:
    """
    Given: The design's example document.
    When:  It is saved to a new project file and loaded back.
    Then:  The document and revision agree, and the file is readable TOML.
    """
    saved = project_store.save(orders_layer)
    loaded = project_store.load()

    assert loaded is not None
    assert loaded.revision == saved.revision
    assert loaded.revision is not None
    assert loaded.revision.startswith("sha256:")
    assert loaded.origin == str(project_store.path)
    assert tomllib.loads(project_store.path.read_text()) == ORDERS_DOCUMENT


def test_saving_over_a_changed_file_is_refused(
    project_store: ConfigFile, orders_layer: ConfigLayer
) -> None:
    """
    Given: A file read by one editor and then changed by another.
    When:  The first editor saves with the revision it read.
    Then:  G10: ConfigurationError, and the second editor's change survives.
    """
    first = project_store.save(orders_layer)
    project_store.save(
        ConfigLayer(ConfigSource.PROJECT, {"version": 1, "defaults": {"timeout": 5}}),
        expected_revision=first.revision,
    )

    with pytest.raises(ConfigurationError, match="changed since it was read"):
        project_store.save(orders_layer, expected_revision=first.revision)
    loaded = project_store.load()
    assert loaded is not None
    assert loaded.values["defaults"] == {"timeout": 5}


def test_saving_a_new_file_over_an_existing_one_is_refused(
    project_store: ConfigFile, orders_layer: ConfigLayer
) -> None:
    """
    Given: An existing file.
    When:  A save claims the file should not exist yet.
    Then:  G10: ConfigurationError.
    """
    project_store.save(orders_layer)

    with pytest.raises(ConfigurationError, match="changed since it was read"):
        project_store.save(orders_layer)


def test_racing_writers_lose_no_update(project_store: ConfigFile) -> None:
    """
    Given: Six threads each adding their own profile to one file, retrying on conflict.
    When:  They race.
    Then:  G10: every profile is present afterwards; no save overwrote another's.
    """
    barrier = threading.Barrier(WRITERS)
    failures: list[BaseException] = list()

    def add(index: int) -> None:
        barrier.wait(10)
        for _ in range(200):
            try:
                project_store.set_profile(f"watch-{index}", timeout=index + 1)
            except ConfigurationError:
                continue
            else:
                break
        else:
            failures.append(RuntimeError(f"writer {index} never saved"))

    threads = [threading.Thread(target=add, args=(index,)) for index in range(WRITERS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    expected = {f"watch-{index}" for index in range(WRITERS)}

    loaded = project_store.load()
    assert loaded is not None
    actual = set(loaded.values["profiles"])  # ty: ignore[invalid-argument-type]

    assert failures == list()
    assert actual == expected


def test_a_failed_save_leaves_the_file_and_no_temporary_behind(
    project_store: ConfigFile, orders_layer: ConfigLayer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: A saved file, and a disk that fails while flushing the next save.
    When:  The next save runs.
    Then:  G10: ConfigurationError, the original file is intact, and no temporary file remains.
    """
    first = project_store.save(orders_layer)
    before = project_store.path.read_bytes()

    def failing_fsync(descriptor: int) -> None:
        del descriptor
        raise OSError("the Luggage ate the disk")

    monkeypatch.setattr(config_module.os, "fsync", failing_fsync)

    with pytest.raises(ConfigurationError, match="Luggage"):
        project_store.save(
            ConfigLayer(ConfigSource.PROJECT, {"version": 1}), expected_revision=first.revision
        )
    leftovers = [path.name for path in project_store.path.parent.iterdir() if ".tmp" in path.name]

    assert project_store.path.read_bytes() == before
    assert leftovers == list()


def test_a_password_is_never_saved(project_store: ConfigFile) -> None:
    """
    Given: A document whose backend URL carries a password.
    When:  It is saved.
    Then:  G9: ConfigurationError, and no file is created.
    """
    layer = ConfigLayer(
        ConfigSource.PROJECT,
        {"version": 1, "defaults": {"backend": "redis://rincewind:luggage@localhost/0"}},
    )

    with pytest.raises(ConfigurationError, match="password"):
        project_store.save(layer)
    assert not project_store.path.exists()


@pytest.mark.parametrize(
    "backend",
    [
        "redis://localhost/0?password=topsecret",
        "redis://localhost/0?pass%77ord=topsecret",
        "postgresql://localhost/quota?password=topsecret",
    ],
)
def test_a_query_password_is_never_saved(project_store: ConfigFile, backend: str) -> None:
    """
    Given: A file layer with a backend URL carrying a query password.
    When:  The layer is saved.
    Then:  Saving fails before the secret is written to disk.
    """
    layer = ConfigLayer(ConfigSource.PROJECT, {"version": 1, "defaults": {"backend": backend}})

    with pytest.raises(ConfigurationError, match="password"):
        project_store.save(layer)
    assert not project_store.path.exists()


def test_an_invalid_document_is_never_saved(project_store: ConfigFile) -> None:
    """
    Given: A document with an unknown field.
    When:  It is saved.
    Then:  G5: ConfigurationError, and no file is created.
    """
    layer = ConfigLayer(ConfigSource.PROJECT, {"version": 1, "defaults": {"retries": 3}})

    with pytest.raises(ConfigurationError, match="unknown field"):
        project_store.save(layer)
    assert not project_store.path.exists()


def test_a_layer_is_saved_only_to_its_own_kind_of_file(
    project_store: ConfigFile,
) -> None:
    """
    Given: An organization layer.
    When:  It is saved to a project file.
    Then:  ConfigurationError.
    """
    with pytest.raises(ConfigurationError, match="organization layer"):
        project_store.save(ConfigLayer(ConfigSource.ORGANIZATION, {"version": 1}))


def test_a_pyproject_is_never_rewritten(tmp_path: Path, orders_layer: ConfigLayer) -> None:
    """
    Given: A project file that is a pyproject.toml.
    When:  A layer is saved to it.
    Then:  ConfigurationError; the project's metadata is the user's to edit.
    """
    store = ConfigFile(tmp_path / "pyproject.toml", ConfigSource.PROJECT)

    with pytest.raises(ConfigurationError, match="pyproject"):
        store.save(orders_layer)


def test_only_project_and_organization_layers_are_files(tmp_path: Path) -> None:
    """
    Given: The environment source.
    When:  A configuration file is built for it.
    Then:  ConfigurationError.
    """
    with pytest.raises(ConfigurationError):
        ConfigFile(tmp_path / "env.toml", ConfigSource.ENVIRONMENT)


def test_helpers_build_a_profile_that_resolves(
    locations: ConfigLocations, environ: dict[str, str]
) -> None:
    """
    Given: A project file written through set_defaults and set_profile with Python values.
    When:  The profile is resolved.
    Then:  The saved values come back, and the file holds plain TOML.
    """
    store = project_file(PROJECT, locations=locations)
    store.set_defaults(algorithm=Algorithms.TOKEN_BUCKET, timeout=dt.timedelta(seconds=90))
    store.set_profile(PROFILE, limits=[Limit(10, per="1s")], options={"capacity": 20})
    expected = ((Limit(10, per="1s"),), Algorithms.TOKEN_BUCKET, 90.0, {"capacity": 20})

    settings = resolve(PROFILE, project=PROJECT, environ=environ, locations=locations).settings
    actual = (settings.limits, settings.algorithm, settings.timeout, dict(settings.options))

    assert actual == expected
    assert 'algorithm = "token_bucket"' in store.path.read_text()


def test_setting_rules_replaces_limits_and_unset_removes_a_field(
    project_store: ConfigFile,
) -> None:
    """
    Given: A profile with positional limits and a timeout.
    When:  Rules are set and the timeout is set to UNSET.
    Then:  G4: the limits are gone, the rules are present, and the timeout is removed.
    """
    project_store.set_profile(PROFILE, limits=[Limit(1)], timeout=30)
    saved = project_store.set_profile(PROFILE, rules={"burst": Limit(2)}, timeout=UNSET)
    expected = {"rules": {"burst": {"amount": 2, "per": "1s"}}}

    profiles = saved.values["profiles"]
    assert not isinstance(profiles, str)
    actual = config_module._thaw(profiles[PROFILE])  # ty: ignore[not-subscriptable]

    assert actual == expected


def test_an_explicit_null_timeout_is_saved_as_none(project_store: ConfigFile) -> None:
    """
    Given: A profile set with timeout=None.
    When:  The file is read.
    Then:  G2: it spells the null as "none", since TOML has no null.
    """
    project_store.set_profile(PROFILE, timeout=None)

    assert 'timeout = "none"' in project_store.path.read_text()


def test_removing_a_profile(project_store: ConfigFile) -> None:
    """
    Given: A file with two profiles.
    When:  One is removed, and then removed again.
    Then:  The other survives, and removing a missing profile is an error.
    """
    project_store.set_profile("ankh.orders", timeout=1)
    project_store.set_profile("quirm.cheese", timeout=2)
    saved = project_store.remove_profile("ankh.orders")

    assert set(saved.values["profiles"]) == {"quirm.cheese"}  # ty: ignore[invalid-argument-type]
    with pytest.raises(ConfigurationError, match="no profile"):
        project_store.remove_profile("ankh.orders")


def test_helpers_refuse_limits_and_rules_together(project_store: ConfigFile) -> None:
    """
    Given: A profile update naming both limits and rules.
    When:  It is applied.
    Then:  G4: ConfigurationError, and nothing is written.
    """
    with pytest.raises(ConfigurationError, match="one value"):
        project_store.set_profile(PROFILE, limits=[Limit(1)], rules={"a": Limit(1)})
    assert not project_store.path.exists()


def test_organization_files_default_to_the_user_location(locations: ConfigLocations) -> None:
    """
    Given: An organization id, with and without site= and an explicit path.
    When:  Its file is requested.
    Then:  The user location by default, the site location on request, and the path when given.
    """
    explicit = locations.user_config / "elsewhere.toml"
    expected = (
        locations.organization(ORGANIZATION),
        locations.organization(ORGANIZATION, site=True),
        explicit,
    )

    actual = (
        organization_file(ORGANIZATION, locations=locations).path,
        organization_file(ORGANIZATION, site=True, locations=locations).path,
        organization_file(ORGANIZATION, path=explicit, locations=locations).path,
    )

    assert actual == expected


def test_default_locations_come_from_platformdirs() -> None:
    """
    Given: The default locations.
    When:  They are computed.
    Then:  Both are named for the library.
    """
    locations = ConfigLocations.default()

    assert locations.user_config.name == "procrastinators"
    assert locations.site_config.name == "procrastinators"


def test_a_file_with_invalid_utf8_is_refused(locations: ConfigLocations) -> None:
    """
    Given: A project file that is not UTF-8.
    When:  It is loaded.
    Then:  ConfigurationError naming the file.
    """
    path = locations.project(PROJECT)
    path.parent.mkdir(parents=True)
    path.write_bytes(b"version = 1\n# \xff\xfe\n")
    store = ConfigFile(path, ConfigSource.PROJECT)

    with pytest.raises(ConfigurationError, match="not valid TOML"):
        store.load()


def test_a_missing_file_loads_as_no_layer(project_store: ConfigFile) -> None:
    """
    Given: A project file that does not exist.
    When:  It is loaded.
    Then:  None: an absent layer, not an error.
    """
    assert project_store.load() is None


def test_a_relative_path_is_made_absolute_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Given: A relative configuration path.
    When:  A file store is built and the working directory then changes.
    Then:  The store keeps the absolute path it resolved at construction.
    """
    monkeypatch.chdir(tmp_path)
    written = write(tmp_path / "ankh.toml", "version = 1\n")
    store = ConfigFile(Path(written.name), ConfigSource.PROJECT)
    monkeypatch.chdir(tmp_path.parent)

    assert store.path == tmp_path / "ankh.toml"
    assert store.load() is not None


if __name__ == "__main__":
    pass
else:
    pass
