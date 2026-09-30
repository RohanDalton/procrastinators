"""Layered configuration: finding, reading, resolving, explaining, and saving it.

:mod:`procrastinators.config_models` says what a resolved configuration *is*;
this module does the work of producing one.

**Precedence** (contract G1), strongest first: explicit arguments,
``PROCRASTINATORS_*`` environment defaults, the selected project, the selected
organization, and the library's behavior defaults. Values merge field by
field, except the rates — ``limits`` or ``rules`` — which the strongest layer
setting either supplies wholesale (G4), and ``options``, which belong to one
algorithm and are replaced as a whole.

**Selection is explicit** (G3). A project or organization is chosen by argument
or environment variable, never by the directory a worker started in. Their
files live where :class:`ConfigLocations` says, which is PlatformDirs unless a
caller injects something else.

**Files** are TOML with an explicit ``version``::

    version = 1

    [defaults]
    algorithm = "sliding_log"
    timeout = 60

    [profiles."ankh.orders"]
    limits = [{ amount = 10, per = "1s" }, { amount = 500, per = "1m" }]

Unknown keys, tables, and versions are rejected (G5). TOML has no null, so the
string ``"none"`` is the explicit null for ``timeout`` (G2). Saving is atomic
and guarded by a revision check under a file lock (G10), and a file never
stores a password (G9). The writer is deliberately narrow: it writes exactly
this schema, and every save is parsed back with :mod:`tomllib` before it
replaces anything.

Nothing here reads the environment or touches the filesystem at import time.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import contextlib
import datetime as dt
import hashlib
import math
import os
import re
import tempfile
import tomllib
import urllib.parse
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

import platformdirs

from procrastinators.config_models import (
    DEFAULT_BACKEND,
    UNSET,
    ConfigLayer,
    ConfigSource,
    LimiterSettings,
    Provenance,
    ResolvedConfig,
)
from procrastinators.errors import ConfigurationError, InvalidPolicy
from procrastinators.models import (
    MAX_DURATION_US,
    USECS_PER_SECOND,
    Algorithms,
    Limit,
    duration_to_micros,
)
from procrastinators.policies import normalize_limit, resolve_algorithm

try:
    import fcntl
except ImportError:  # pragma: no cover - platforms without POSIX locks
    fcntl = None
else:
    pass

__all__ = [
    "APP_NAME",
    "CONFIG_VERSION",
    "ENVIRONMENT_PREFIX",
    "FIELDS",
    "LIBRARY_DEFAULTS",
    "RESERVED_ENVIRONMENT",
    "ConfigFile",
    "ConfigLocations",
    "explain",
    "organization_file",
    "project_file",
    "render_toml",
    "resolve",
    "validate_document",
]

APP_NAME: Final = "procrastinators"
"""The application name PlatformDirs locations are derived from."""

CONFIG_VERSION: Final = 1
"""The only configuration file version this library reads and writes."""

ENVIRONMENT_PREFIX: Final = "PROCRASTINATORS_"
"""Prefix of every environment variable configuration reads."""

RESERVED_ENVIRONMENT: Final = frozenset({"PROCRASTINATORS_REQUIRE_SERVICES"})
"""Prefixed variables that belong to development tooling, which configuration ignores."""

FIELDS: Final = frozenset(
    {
        "algorithm",
        "backend",
        "limits",
        "namespace",
        "options",
        "rules",
        "storage_timeout",
        "timeout",
    }
)
"""Every configurable field, in files, the environment, and arguments alike."""

LIBRARY_DEFAULTS: Final[Mapping[str, object]] = MappingProxyType(
    {
        "algorithm": Algorithms.SLIDING_LOG,
        "backend": DEFAULT_BACKEND,
        "namespace": "default",
        "options": MappingProxyType(dict()),
        "storage_timeout": 5.0,
        "timeout": None,
    }
)
"""The weakest layer: behavior defaults only, and never a rate (G1)."""

NULL_SPELLING: Final = "none"
"""How a file or environment variable spells an explicit null, which TOML lacks (G2)."""

_RATES: Final = frozenset({"limits", "rules"})
_TOP_LEVEL: Final = frozenset({"version", "defaults", "profiles"})
_LIMIT_KEYS: Final = frozenset({"amount", "per"})
_ID_PATTERN: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
_BARE_KEY: Final = re.compile(r"[A-Za-z0-9_-]+")
_ENVIRONMENT_FIELDS: Final = {
    f"{ENVIRONMENT_PREFIX}{name.upper()}": name for name in sorted(FIELDS)
}
_SELECTION: Final = {
    f"{ENVIRONMENT_PREFIX}PROJECT": "project",
    f"{ENVIRONMENT_PREFIX}ORGANIZATION": "organization",
    f"{ENVIRONMENT_PREFIX}PROJECT_FILE": "project_file",
    f"{ENVIRONMENT_PREFIX}ORGANIZATION_FILE": "organization_file",
}
_PYPROJECT: Final = "pyproject.toml"
_PYPROJECT_TABLE: Final = ("tool", APP_NAME)
_ESCAPES: Final = {
    '"': '\\"',
    "\\": "\\\\",
    "\b": "\\b",
    "\t": "\\t",
    "\n": "\\n",
    "\f": "\\f",
    "\r": "\\r",
}


#
# Field values. Every layer's raw values — TOML-native from files, strings
# from the environment, Python objects from arguments — are normalized here,
# so resolution compares like with like and a bad value is reported once,
# naming where it came from.


def _fail(where: str, message: str) -> ConfigurationError:
    error = ConfigurationError(f"{where}: {message}")
    return error


def _seconds(raw: object, *, where: str, nullable: bool) -> float | None:
    """A timeout in seconds, from a number, a duration string, a timedelta, or the null."""
    if raw is None or (isinstance(raw, str) and raw.strip().lower() == NULL_SPELLING):
        if not nullable:
            raise _fail(where, "must be a duration, not null: every storage call is bounded (B3)")
        else:
            seconds = None
    elif isinstance(raw, bool) or not isinstance(raw, (int, float, str, dt.timedelta)):
        raise _fail(where, f"must be a number of seconds or a duration string, got {raw!r}")
    else:
        try:
            micros = duration_to_micros(raw)
        except InvalidPolicy as error:
            raise _fail(where, str(error)) from error
        if micros > MAX_DURATION_US:
            raise _fail(where, f"must be at most {MAX_DURATION_US // USECS_PER_SECOND} seconds")
        elif micros == 0 and not nullable:
            raise _fail(where, "must be positive")
        else:
            seconds = micros / USECS_PER_SECOND
    return seconds


def _limit(entry: object, *, where: str) -> Limit:
    """One rate, from a :class:`~procrastinators.models.Limit` or an ``{amount, per}`` table."""
    if isinstance(entry, Limit):
        limit = entry
    elif isinstance(entry, Mapping):
        if unknown := set(entry) - _LIMIT_KEYS:
            raise _fail(where, f"unknown keys {sorted(map(str, unknown))}; a limit has amount, per")
        elif missing := _LIMIT_KEYS - set(entry):
            raise _fail(where, f"a limit needs {sorted(missing)}")
        else:
            pass
        try:
            limit = Limit(entry["amount"], per=entry["per"])
        except InvalidPolicy as error:
            raise _fail(where, str(error)) from error
    else:
        raise _fail(where, f"expected a limit such as {{ amount = 10, per = '1s' }}, got {entry!r}")
    return limit


def _option(value: object, *, where: str) -> object:
    if isinstance(value, dt.timedelta):
        micros = duration_to_micros(value)
        seconds = micros // USECS_PER_SECOND if micros % USECS_PER_SECOND == 0 else None
        option: object = seconds if seconds is not None else micros / USECS_PER_SECOND
    elif isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise _fail(where, f"an option is an integer, number, or string, got {value!r}")
    else:
        option = value
    return option


def _normalize(name: str, raw: object, *, where: str) -> object:
    """``raw`` as the value resolution works with for field ``name``.

    :raises ~procrastinators.errors.ConfigurationError: ``name`` is unknown or ``raw`` invalid.
    """
    where = f"{where}: {name}"
    if name == "algorithm":
        if not isinstance(raw, (Algorithms, str)):
            raise _fail(where, f"must be an algorithm name, got {raw!r}")
        else:
            pass
        try:
            value: object = resolve_algorithm(raw)
        except InvalidPolicy as error:
            raise _fail(where, str(error)) from error
    elif name in ("backend", "namespace"):
        if not isinstance(raw, str) or not raw.strip():
            raise _fail(where, f"must be a non-empty string, got {raw!r}")
        else:
            value = raw
    elif name == "timeout":
        value = _seconds(raw, where=where, nullable=True)
    elif name == "storage_timeout":
        value = _seconds(raw, where=where, nullable=False)
    elif name == "limits":
        if isinstance(raw, (str, bytes, Mapping)) or not isinstance(raw, Sequence):
            raise _fail(where, "must be a list of limits; use rules to name them")
        elif not raw:
            raise _fail(where, "must not be empty; there is no default vendor rate")
        else:
            value = tuple(
                _limit(entry, where=f"{where}[{index}]") for index, entry in enumerate(raw)
            )
    elif name == "rules":
        if not isinstance(raw, Mapping) or not raw:
            raise _fail(where, "must be a non-empty table of rule name to limit")
        else:
            pass
        for rule in raw:
            if not isinstance(rule, str) or not rule:
                raise _fail(where, f"rule names must be non-empty strings, got {rule!r}")
            else:
                pass
        value = MappingProxyType(
            {rule: _limit(entry, where=f"{where}.{rule}") for rule, entry in raw.items()}
        )
    elif name == "options":
        if not isinstance(raw, Mapping):
            raise _fail(where, f"must be a table of option name to value, got {raw!r}")
        else:
            pass
        for option in raw:
            if not isinstance(option, str) or not option:
                raise _fail(where, f"option names must be non-empty strings, got {option!r}")
            else:
                pass
        value = MappingProxyType(
            {key: _option(item, where=f"{where}.{key}") for key, item in raw.items()}
        )
    else:
        raise _fail(where, f"unknown field; configurable fields are {sorted(FIELDS)} (G5)")
    return value


def _number(seconds: float) -> int | float:
    number = int(seconds) if float(seconds).is_integer() else seconds
    return number


def _native_per(per: object) -> object:
    if isinstance(per, dt.timedelta):
        native: object = _number(duration_to_micros(per) / USECS_PER_SECOND)
    else:
        native = per
    return native


def _native_limit(limit: Limit) -> dict[str, object]:
    native = {"amount": limit.amount, "per": _native_per(limit.per)}
    return native


def _native(name: str, value: object) -> object:
    """A normalized field value as the TOML-native value a file stores."""
    if isinstance(value, Algorithms):
        native: object = value.value
    elif name == "timeout" and value is None:
        native = NULL_SPELLING
    elif name in ("timeout", "storage_timeout") and isinstance(value, float):
        native = _number(value)
    elif name == "limits" and isinstance(value, tuple):
        native = [_native_limit(limit) for limit in value]
    elif name == "rules" and isinstance(value, Mapping):
        native = {rule: _native_limit(limit) for rule, limit in value.items()}
    elif name == "options" and isinstance(value, Mapping):
        native = dict(value)
    else:
        native = value
    return native


def _reject_password(backend: object, *, where: str) -> None:
    """Refuse a backend URL carrying a password in a file layer (G9)."""
    if isinstance(backend, str):
        parts = urllib.parse.urlsplit(backend)
        has_password = parts.password is not None or any(
            key.lower() in {"password", "passwd"}
            for key, _ in urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        )
    else:
        has_password = False
    if has_password:
        raise _fail(
            where,
            "backend carries a password; distributable configuration stores credential "
            "references, not passwords (G9). Supply the full address through "
            f"{ENVIRONMENT_PREFIX}BACKEND or an argument instead",
        )
    else:
        pass


#
# Documents: the validated contents of one TOML file.


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        frozen: object = MappingProxyType({key: _freeze(item) for key, item in value.items()})
    elif isinstance(value, (list, tuple)):
        frozen = tuple(_freeze(item) for item in value)
    else:
        frozen = value
    return frozen


def _thaw(value: object) -> object:
    if isinstance(value, Mapping):
        thawed: object = {key: _thaw(item) for key, item in value.items()}
    elif isinstance(value, (list, tuple)):
        thawed = [_thaw(item) for item in value]
    else:
        thawed = value
    return thawed


def _validate_fields(table: object, *, where: str) -> None:
    if not isinstance(table, Mapping):
        raise _fail(where, f"must be a table, got {table!r}")
    elif set(table) >= _RATES:
        raise _fail(where, "sets both limits and rules; they are one value (G4)")
    else:
        pass
    for name, raw in table.items():
        _normalize(name, raw, where=where)
        if name == "backend":
            _reject_password(raw, where=f"{where}: backend")
        else:
            pass


def validate_document(document: object, *, origin: str) -> Mapping[str, object]:
    """Check a configuration document and return it frozen.

    :param document: The parsed TOML, or a document about to be written.
    :param origin: Where it came from, named in errors.
    :returns: The document with every nested table and array made immutable.
    :raises ~procrastinators.errors.ConfigurationError: A missing or unknown ``version``, an
        unknown key or table, an invalid value, or a password in a backend URL.
    """
    if not isinstance(document, Mapping):
        raise _fail(origin, "a configuration document is a table")
    elif unknown := set(document) - _TOP_LEVEL:
        raise _fail(origin, f"unknown top-level keys {sorted(map(str, unknown))} (G5)")
    elif (version := document.get("version")) is None:
        raise _fail(origin, f"missing version; write version = {CONFIG_VERSION}")
    elif isinstance(version, bool) or version != CONFIG_VERSION:
        raise _fail(origin, f"version {version!r} is not supported; only {CONFIG_VERSION} is")
    else:
        pass
    if "defaults" in document:
        _validate_fields(document["defaults"], where=f"{origin}: [defaults]")
    else:
        pass
    profiles = document.get("profiles", dict())
    if not isinstance(profiles, Mapping):
        raise _fail(origin, "profiles must be a table of profile tables")
    else:
        pass
    for name, table in profiles.items():
        if not isinstance(name, str) or not name:
            raise _fail(origin, f"profile names must be non-empty strings, got {name!r}")
        else:
            _validate_fields(table, where=f'{origin}: [profiles."{name}"]')
    frozen = _freeze(document)
    assert isinstance(frozen, Mapping)
    return frozen


def _profile_fields(document: Mapping[str, object], profile: str) -> dict[str, object]:
    """One file's raw values for ``profile``: its defaults overlaid by the profile's own."""
    defaults = document.get("defaults", dict())
    profiles = document.get("profiles", dict())
    assert isinstance(defaults, Mapping)
    assert isinstance(profiles, Mapping)
    chosen = profiles.get(profile, dict())
    assert isinstance(chosen, Mapping)
    fields = dict(defaults)
    if _RATES & set(chosen):
        for name in _RATES:
            fields.pop(name, None)
    else:
        pass
    fields.update(chosen)
    return fields


#
# The narrow TOML writer.


def _escape(text: str) -> str:
    characters = list()
    for character in text:
        if (escaped := _ESCAPES.get(character)) is not None:
            characters.append(escaped)
        elif ord(character) < 0x20 or ord(character) == 0x7F:
            characters.append(f"\\u{ord(character):04X}")
        else:
            characters.append(character)
    quoted = '"' + "".join(characters) + '"'
    return quoted


def _key(key: object) -> str:
    if not isinstance(key, str):
        raise ConfigurationError(f"TOML keys are strings, got {key!r}")
    else:
        pass
    rendered = key if _BARE_KEY.fullmatch(key) else _escape(key)
    return rendered


def _value(value: object) -> str:
    if isinstance(value, bool):
        rendered = "true" if value else "false"
    elif isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise ConfigurationError(f"TOML integers are 64-bit, got {value}")
        else:
            rendered = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigurationError(f"TOML configuration stores finite numbers, got {value!r}")
        else:
            rendered = repr(value)
    elif isinstance(value, str):
        rendered = _escape(value)
    elif isinstance(value, Mapping):
        items = ", ".join(f"{_key(key)} = {_value(item)}" for key, item in value.items())
        rendered = f"{{ {items} }}" if items else "{}"
    elif isinstance(value, (list, tuple)):
        rendered = "[" + ", ".join(_value(item) for item in value) + "]"
    else:
        raise ConfigurationError(f"cannot write {type(value).__name__} to TOML: {value!r}")
    return rendered


def _section(header: str, table: object) -> list[str]:
    if not isinstance(table, Mapping):
        raise ConfigurationError(f"[{header}] must be a table, got {table!r}")
    else:
        pass
    lines = ["", f"[{header}]"]
    lines.extend(f"{_key(key)} = {_value(item)}" for key, item in table.items())
    return lines


def render_toml(document: Mapping[str, object]) -> str:
    """Write a configuration document as TOML.

    Narrow by design: top-level values, then each top-level table as a section
    whose values are written inline, except ``profiles``, whose tables each get
    a ``[profiles."name"]`` section. That is exactly the configuration schema,
    and :mod:`tomllib` reads it back unchanged.

    :param document: The document to write.
    :returns: The TOML text, ending in a newline.
    :raises ~procrastinators.errors.ConfigurationError: A value TOML cannot hold, such as a
        non-finite float, an integer beyond 64 bits, or an unsupported type.
    """
    lines = [
        f"{_key(key)} = {_value(value)}"
        for key, value in document.items()
        if not isinstance(value, Mapping)
    ]
    for key, value in document.items():
        if not isinstance(value, Mapping):
            pass
        elif key == "profiles" and value:
            for name, table in value.items():
                lines.extend(_section(f"{_key(key)}.{_key(name)}", table))
        else:
            lines.extend(_section(_key(key), value))
    text = "\n".join(lines).lstrip("\n") + "\n"
    return text


#
# Locations and files.


def _revision(payload: bytes) -> str:
    revision = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    return revision


def _checked_id(value: object, *, what: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERN.fullmatch(value) or ".." in value:
        raise ConfigurationError(
            f"{what} id {value!r} must start with a letter or digit and contain only letters, "
            "digits, '.', '_', and '-', without '..'"
        )
    else:
        pass
    return value


@dataclass(frozen=True, slots=True)
class ConfigLocations:
    """Where project and organization files live.

    :meth:`default` asks PlatformDirs, without creating anything; tests and
    unusual deployments inject their own directories.
    """

    user_config: Path
    """The per-user configuration directory."""

    site_config: Path
    """The machine-wide configuration directory."""

    @classmethod
    def default(cls) -> ConfigLocations:
        """PlatformDirs' user and site configuration directories for this library."""
        locations = cls(
            user_config=platformdirs.user_config_path(APP_NAME),
            site_config=platformdirs.site_config_path(APP_NAME),
        )
        return locations

    def project(self, project: str) -> Path:
        """``<user config>/projects/<project>.toml``.

        :param project: The project id.
        :raises ~procrastinators.errors.ConfigurationError: The id is not a safe file name.
        """
        path = self.user_config / "projects" / f"{_checked_id(project, what='project')}.toml"
        return path

    def organization(self, organization: str, *, site: bool = False) -> Path:
        """``<user or site config>/organizations/<organization>.toml``.

        :param organization: The organization id.
        :param site: The machine-wide file rather than the user's.
        :raises ~procrastinators.errors.ConfigurationError: The id is not a safe file name.
        """
        base = self.site_config if site else self.user_config
        name = f"{_checked_id(organization, what='organization')}.toml"
        path = base / "organizations" / name
        return path


@contextlib.contextmanager
def _file_lock(path: Path) -> Iterator[None]:
    """Hold an exclusive lock on ``path``'s sidecar lock file, where the platform has one."""
    if fcntl is None:
        yield
    else:
        lock_path = path.with_name(f"{path.name}.lock")
        with lock_path.open("a") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _fsync_directory(directory: Path) -> None:
    # Directory fsync makes the rename itself durable; not every platform allows it.
    with contextlib.suppress(OSError):
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class ConfigFile:
    """One TOML configuration file, read and written atomically.

    Satisfies :class:`~procrastinators.protocols.ConfigStore`. A layer loaded
    from it carries the validated document as its values, the path as its
    origin, and a digest of the file's bytes as its revision. A file named
    ``pyproject.toml`` is read from its ``[tool.procrastinators]`` table and
    cannot be saved: rewriting someone's project metadata is not this
    library's job.

    :param path: The file; ``~`` is expanded and a relative path made absolute once.
    :param source: :attr:`~procrastinators.config_models.ConfigSource.PROJECT` or
        :attr:`~procrastinators.config_models.ConfigSource.ORGANIZATION`.
    :raises ~procrastinators.errors.ConfigurationError: ``source`` is not a file layer.
    """

    path: Path
    """The absolute path of the file."""

    source: ConfigSource
    """The precedence level the file's layer occupies."""

    def __post_init__(self) -> None:
        if self.source not in (ConfigSource.PROJECT, ConfigSource.ORGANIZATION):
            raise ConfigurationError(
                f"a configuration file is a project or organization layer, not {self.source!r}"
            )
        else:
            pass
        object.__setattr__(self, "path", Path(self.path).expanduser().absolute())

    @property
    def is_pyproject(self) -> bool:
        """Whether this is a ``pyproject.toml``, read from its ``[tool.procrastinators]`` table."""
        is_pyproject = self.path.name == _PYPROJECT
        return is_pyproject

    def _read(self) -> tuple[bytes, str] | None:
        try:
            payload = self.path.read_bytes()
        except FileNotFoundError:
            result = None
        except OSError as error:
            raise ConfigurationError(f"cannot read {self.path}: {error}") from error
        else:
            result = (payload, _revision(payload))
        return result

    def load(self) -> ConfigLayer | None:
        """This file's layer, or ``None`` when the file (or its pyproject table) is absent.

        :raises ~procrastinators.errors.ConfigurationError: The file is unreadable, not valid
            TOML, or fails validation.
        """
        if (read := self._read()) is None:
            layer = None
        else:
            payload, revision = read
            try:
                parsed: object = tomllib.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
                raise ConfigurationError(f"{self.path} is not valid TOML: {error}") from error
            if self.is_pyproject:
                for part in _PYPROJECT_TABLE:
                    parsed = parsed.get(part) if isinstance(parsed, Mapping) else None
            else:
                pass
            if parsed is None:
                layer = None
            else:
                document = validate_document(parsed, origin=str(self.path))
                layer = ConfigLayer(self.source, document, str(self.path), revision)
        return layer

    def save(self, layer: ConfigLayer, *, expected_revision: str | None = None) -> ConfigLayer:
        """Atomically replace the file with ``layer``'s document (G10).

        Under an exclusive lock, the current revision must equal
        ``expected_revision`` (``None``: the file must not exist). The document
        is written to a temporary file beside the destination, flushed to disk,
        parsed back and compared, and only then renamed over the destination.

        :param layer: A layer whose values are a configuration document.
        :param expected_revision: The revision last read, or ``None`` for a new file.
        :returns: The saved layer, carrying its new revision.
        :raises ~procrastinators.errors.ConfigurationError: The revision did not match, the
            document is invalid or carries a password, the layer is for another source, the file
            is a ``pyproject.toml``, or the destination is not writable.
        """
        if self.is_pyproject:
            raise ConfigurationError(f"{self.path} is a pyproject.toml; edit it by hand")
        elif layer.source is not self.source:
            raise ConfigurationError(
                f"a {layer.source.label} layer cannot be saved to the {self.source.label} "
                f"file {self.path}"
            )
        else:
            pass
        document = validate_document(layer.values, origin=str(self.path))
        payload = render_toml(document).encode("utf-8")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with _file_lock(self.path):
                current = self._read()
                found = None if current is None else current[1]
                if found != expected_revision:
                    raise ConfigurationError(
                        f"{self.path} changed since it was read (expected revision "
                        f"{expected_revision}, found {found}); reload and apply the change again"
                    )
                else:
                    pass
                self._replace(payload, document)
        except OSError as error:
            raise ConfigurationError(f"cannot write {self.path}: {error}") from error
        saved = ConfigLayer(self.source, document, str(self.path), _revision(payload))
        return saved

    def _replace(self, payload: bytes, document: Mapping[str, object]) -> None:
        descriptor, temporary = tempfile.mkstemp(
            dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
        )
        replaced = False
        try:
            with os.fdopen(descriptor, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            reread = tomllib.loads(Path(temporary).read_text(encoding="utf-8"))
            if _thaw(validate_document(reread, origin=temporary)) != _thaw(document):
                raise ConfigurationError(
                    f"writing {self.path} would not round-trip; nothing was replaced"
                )
            else:
                pass
            Path(temporary).replace(self.path)
            replaced = True
            _fsync_directory(self.path.parent)
        finally:
            if not replaced:
                with contextlib.suppress(FileNotFoundError):
                    Path(temporary).unlink()
            else:
                pass

    def _update(self, change: Callable[[dict[str, object]], None]) -> ConfigLayer:
        current = self.load()
        document = _thaw(current.values) if current is not None else {"version": CONFIG_VERSION}
        assert isinstance(document, dict)
        change(document)
        saved = self.save(
            ConfigLayer(self.source, document),
            expected_revision=None if current is None else current.revision,
        )
        return saved

    def set_defaults(self, **fields: object) -> ConfigLayer:
        """Set fields in the file's ``[defaults]``, keeping the others.

        Setting ``limits`` removes ``rules`` and vice versa, since they are one
        value (G4); a field given as :data:`~procrastinators.config_models.UNSET`
        is removed. Loads, changes, and saves against the loaded revision.

        :param fields: Configurable fields, as Python values or TOML-native ones.
        :returns: The saved layer.
        :raises ~procrastinators.errors.ConfigurationError: An unknown field, an invalid value,
            or a concurrent change.
        """
        updates = _updates(fields, where=f"{self.path}: [defaults]")

        def change(document: dict[str, object]) -> None:
            table = document.setdefault("defaults", dict())
            assert isinstance(table, dict)
            _apply(table, updates)

        saved = self._update(change)
        return saved

    def set_profile(self, profile: str, **fields: object) -> ConfigLayer:
        """Set fields in ``[profiles."<profile>"]``, creating it if needed.

        As :meth:`set_defaults`, for one profile.

        :param profile: The profile's name.
        :param fields: Configurable fields.
        :returns: The saved layer.
        :raises ~procrastinators.errors.ConfigurationError: An invalid name, field, or value, or
            a concurrent change.
        """
        if not isinstance(profile, str) or not profile:
            raise ConfigurationError(f"profile names must be non-empty strings, got {profile!r}")
        else:
            pass
        updates = _updates(fields, where=f'{self.path}: [profiles."{profile}"]')

        def change(document: dict[str, object]) -> None:
            profiles = document.setdefault("profiles", dict())
            assert isinstance(profiles, dict)
            table = profiles.setdefault(profile, dict())
            _apply(table, updates)

        saved = self._update(change)
        return saved

    def remove_profile(self, profile: str) -> ConfigLayer:
        """Delete ``[profiles."<profile>"]``.

        :param profile: The profile's name.
        :returns: The saved layer.
        :raises ~procrastinators.errors.ConfigurationError: The profile does not exist, or a
            concurrent change.
        """

        def change(document: dict[str, object]) -> None:
            profiles = document.get("profiles", dict())
            assert isinstance(profiles, dict)
            if profile not in profiles:
                raise ConfigurationError(f"{self.path} has no profile {profile!r}")
            else:
                del profiles[profile]

        saved = self._update(change)
        return saved


def _updates(fields: Mapping[str, object], *, where: str) -> dict[str, object]:
    """Validated TOML-native values for ``fields``; :data:`UNSET` marks a removal."""
    if {name for name, value in fields.items() if value is not UNSET} >= _RATES:
        raise _fail(where, "sets both limits and rules; they are one value (G4)")
    else:
        pass
    updates = dict()
    for name, raw in fields.items():
        if raw is UNSET:
            if name not in FIELDS:
                raise _fail(f"{where}: {name}", "unknown field (G5)")
            else:
                updates[name] = UNSET
        else:
            updates[name] = _native(name, _normalize(name, raw, where=where))
    return updates


def _apply(table: dict[str, object], updates: Mapping[str, object]) -> None:
    for name, value in updates.items():
        if value is UNSET:
            table.pop(name, None)
        else:
            if name in _RATES:
                for rate in _RATES:
                    table.pop(rate, None)
            else:
                pass
            table[name] = value


def project_file(project: str, *, locations: ConfigLocations | None = None) -> ConfigFile:
    """The user's file for ``project``, at ``<user config>/projects/<project>.toml``.

    :param project: The project id.
    :param locations: Where files live; PlatformDirs when ``None``.
    :raises ~procrastinators.errors.ConfigurationError: The id is not a safe file name.
    """
    chosen = locations or ConfigLocations.default()
    store = ConfigFile(chosen.project(project), ConfigSource.PROJECT)
    return store


def organization_file(
    organization: str,
    *,
    path: str | os.PathLike[str] | None = None,
    site: bool = False,
    locations: ConfigLocations | None = None,
) -> ConfigFile:
    """A file for ``organization``: ``path`` if given, else the user's or the machine's.

    For reading, resolution picks the site file when it exists and the user's
    otherwise; for writing, say which with ``site``. Writing the site file
    needs whatever filesystem permissions the caller has; nothing is escalated.

    :param organization: The organization id.
    :param path: An explicit file, overriding the locations.
    :param site: The machine-wide file rather than the user's.
    :param locations: Where files live; PlatformDirs when ``None``.
    :raises ~procrastinators.errors.ConfigurationError: The id is not a safe file name.
    """
    if path is not None:
        target = Path(path)
    else:
        chosen = locations or ConfigLocations.default()
        target = chosen.organization(organization, site=site)
    store = ConfigFile(target, ConfigSource.ORGANIZATION)
    return store


#
# The environment.


def _environment_rate(text: str, *, where: str) -> dict[str, object]:
    amount, separator, per = text.partition("/")
    if not separator or not amount.strip().isdigit() or not per.strip():
        raise _fail(where, f"{text!r} is not a rate; write amount/period, such as 10/1s")
    else:
        pass
    rate: dict[str, object] = {"amount": int(amount.strip()), "per": per.strip()}
    return rate


def _environment_pairs(text: str, *, where: str) -> list[tuple[str, str]]:
    pairs = list()
    for item in text.split(","):
        name, separator, value = item.partition("=")
        if not separator or not name.strip() or not value.strip():
            raise _fail(where, f"{item.strip()!r} is not name=value")
        else:
            pairs.append((name.strip(), value.strip()))
    return pairs


def _environment_value(name: str, text: str, *, where: str) -> object:
    """The raw value environment text ``text`` spells for field ``name``."""
    if name == "limits":
        value: object = [_environment_rate(item.strip(), where=where) for item in text.split(",")]
    elif name == "rules":
        value = {
            rule: _environment_rate(rate, where=where)
            for rule, rate in _environment_pairs(text, where=where)
        }
    elif name == "options":
        value = {
            option: int(item) if item.isdigit() else item
            for option, item in _environment_pairs(text, where=where)
        }
    else:
        value = text
    return value


@dataclass(frozen=True, slots=True)
class _Environment:
    """What the ``PROCRASTINATORS_*`` variables say: field values and selections."""

    fields: Mapping[str, object]
    selections: Mapping[str, str]


def _read_environment(environ: Mapping[str, str]) -> _Environment:
    fields = dict()
    selections = dict()
    for variable, text in sorted(environ.items()):
        if not variable.startswith(ENVIRONMENT_PREFIX) or variable in RESERVED_ENVIRONMENT:
            continue
        elif not text.strip():
            raise ConfigurationError(
                f"{variable} is empty; unset it, or write '{NULL_SPELLING}' for an explicit null"
            )
        elif (name := _ENVIRONMENT_FIELDS.get(variable)) is not None:
            fields[name] = _normalize(
                name, _environment_value(name, text, where=variable), where=variable
            )
        elif (selection := _SELECTION.get(variable)) is not None:
            selections[selection] = text.strip()
        else:
            known = sorted([*_ENVIRONMENT_FIELDS, *_SELECTION])
            raise ConfigurationError(f"unknown environment variable {variable}; known: {known}")
    if set(fields) >= _RATES:
        raise ConfigurationError(
            f"{ENVIRONMENT_PREFIX}LIMITS and {ENVIRONMENT_PREFIX}RULES are one value; set one (G4)"
        )
    else:
        pass
    environment = _Environment(MappingProxyType(fields), MappingProxyType(selections))
    return environment


#
# Resolution.


@dataclass(frozen=True, slots=True)
class _FieldLayer:
    """One layer's normalized values for the selected profile."""

    source: ConfigSource
    values: Mapping[str, object]
    origin: str | None
    revision: str | None = None

    def origin_of(self, name: str) -> str | None:
        if self.source is ConfigSource.ENVIRONMENT:
            origin: str | None = f"{ENVIRONMENT_PREFIX}{name.upper()}"
        else:
            origin = self.origin
        return origin

    def as_config_layer(self) -> ConfigLayer:
        layer = ConfigLayer(self.source, self.values, self.origin, self.revision)
        return layer


def _file_layer(store: ConfigFile, profile: str) -> _FieldLayer | None:
    if (layer := store.load()) is None:
        field_layer = None
    else:
        origin = str(store.path)
        raw = _profile_fields(layer.values, profile)
        values = {name: _normalize(name, value, where=origin) for name, value in raw.items()}
        field_layer = _FieldLayer(store.source, MappingProxyType(values), origin, layer.revision)
    return field_layer


def _explicit_file(path: str | os.PathLike[str], source: ConfigSource) -> ConfigFile:
    store = ConfigFile(Path(path), source)
    if not store.path.is_file():
        raise ConfigurationError(
            f"the {source.label} configuration file {store.path} was selected but does not exist"
        )
    else:
        pass
    return store


def _project_store(
    project: str | None,
    path: str | os.PathLike[str] | None,
    locations: ConfigLocations,
) -> ConfigFile | None:
    if path is not None:
        store: ConfigFile | None = _explicit_file(path, ConfigSource.PROJECT)
    elif project is not None:
        store = ConfigFile(locations.project(project), ConfigSource.PROJECT)
    else:
        store = None
    return store


def _organization_store(
    organization: str | None,
    path: str | os.PathLike[str] | None,
    locations: ConfigLocations,
) -> ConfigFile | None:
    if path is not None:
        store: ConfigFile | None = _explicit_file(path, ConfigSource.ORGANIZATION)
    elif organization is not None:
        site = locations.organization(organization, site=True)
        chosen = site if site.is_file() else locations.organization(organization)
        store = ConfigFile(chosen, ConfigSource.ORGANIZATION)
    else:
        store = None
    return store


def _check_rates(settings: LimiterSettings, provenance: Mapping[str, Provenance]) -> None:
    """Validate every rate against the resolved algorithm and options, naming their sources."""
    limits = settings.limits or tuple((settings.rules or dict()).values())
    for limit in limits:
        try:
            normalize_limit(limit, settings.algorithm, settings.options)
        except InvalidPolicy as error:
            sources = ", ".join(
                str(provenance[name])
                for name in ("algorithm", "options", "limits", "rules")
                if name in provenance
            )
            raise ConfigurationError(f"{error}; resolved from {sources}") from error


def resolve(
    profile: str,
    *,
    project: str | None = None,
    organization: str | None = None,
    project_file: str | os.PathLike[str] | None = None,
    organization_file: str | os.PathLike[str] | None = None,
    arguments: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
    locations: ConfigLocations | None = None,
) -> ResolvedConfig:
    """Resolve ``profile`` through every layer, once, into immutable settings (G1, G6).

    Selections given here beat the ``PROCRASTINATORS_PROJECT``,
    ``_ORGANIZATION``, ``_PROJECT_FILE``, and ``_ORGANIZATION_FILE``
    variables. A selected project or organization without a file contributes
    nothing; an explicitly named file that does not exist is an error.

    :param profile: The profile to resolve, such as ``"ankh.orders"``.
    :param project: The project id, or ``None``.
    :param organization: The organization id, or ``None``.
    :param project_file: An explicit project file, possibly a ``pyproject.toml``.
    :param organization_file: An explicit organization file.
    :param arguments: Field values passed explicitly: the strongest layer.
    :param environ: The environment; :data:`os.environ` when ``None``.
    :param locations: Where files live; PlatformDirs when ``None``.
    :returns: The settings, the provenance of every field, and the contributing layers.
    :raises ~procrastinators.errors.ConfigurationError: An unknown field or variable, an invalid
        value, a missing selected file, no rates for the profile, or rates the resolved
        algorithm and options cannot enforce.
    """
    if not isinstance(profile, str) or not profile:
        raise ConfigurationError(f"profile must be a non-empty string, got {profile!r}")
    else:
        pass
    environment = _read_environment(os.environ if environ is None else environ)
    chosen = locations or ConfigLocations.default()
    selections = environment.selections
    project_store = _project_store(
        project if project is not None else selections.get("project"),
        project_file if project_file is not None else selections.get("project_file"),
        chosen,
    )
    organization_store = _organization_store(
        organization if organization is not None else selections.get("organization"),
        organization_file if organization_file is not None else selections.get("organization_file"),
        chosen,
    )
    argument_values = dict(arguments or dict())
    if set(argument_values) >= _RATES:
        raise ConfigurationError("pass limits= or rules=, not both")
    else:
        pass
    layers = [
        _FieldLayer(
            ConfigSource.ARGUMENTS,
            MappingProxyType(
                {
                    name: _normalize(name, value, where="arguments")
                    for name, value in argument_values.items()
                }
            ),
            "arguments",
        ),
        _FieldLayer(ConfigSource.ENVIRONMENT, environment.fields, "environment"),
    ]
    for store in (project_store, organization_store):
        if store is not None and (layer := _file_layer(store, profile)) is not None:
            layers.append(layer)
        else:
            pass
    layers.append(_FieldLayer(ConfigSource.LIBRARY_DEFAULT, LIBRARY_DEFAULTS, None))

    resolved: dict[str, object] = dict()
    provenance: dict[str, Provenance] = {
        "profile": Provenance("profile", ConfigSource.ARGUMENTS, "arguments", profile)
    }
    for name in sorted(FIELDS - _RATES):
        for layer in layers:
            if name in layer.values:
                resolved[name] = layer.values[name]
                provenance[name] = Provenance(
                    name, layer.source, layer.origin_of(name), layer.values[name]
                )
                break
            else:
                pass
        else:
            raise ConfigurationError(f"no layer, not even the library defaults, set {name}")
    for layer in layers:
        if rates := _RATES & set(layer.values):
            (name,) = rates
            resolved[name] = layer.values[name]
            provenance[name] = Provenance(
                name, layer.source, layer.origin_of(name), layer.values[name]
            )
            break
        else:
            pass
    else:
        raise ConfigurationError(
            f"profile {profile!r} supplies no limits: set limits or rules in its project or "
            f"organization profile, {ENVIRONMENT_PREFIX}LIMITS, or pass limits=; there is no "
            "default vendor rate"
        )
    settings = LimiterSettings(
        limits=resolved.get("limits", tuple()),  # ty: ignore[invalid-argument-type]
        rules=resolved.get("rules"),  # ty: ignore[invalid-argument-type]
        algorithm=resolved["algorithm"],  # ty: ignore[invalid-argument-type]
        backend=resolved["backend"],  # ty: ignore[invalid-argument-type]
        namespace=resolved["namespace"],  # ty: ignore[invalid-argument-type]
        timeout=resolved["timeout"],  # ty: ignore[invalid-argument-type]
        storage_timeout=resolved["storage_timeout"],  # ty: ignore[invalid-argument-type]
        options=resolved["options"],  # ty: ignore[invalid-argument-type]
        profile=profile,
    )
    _check_rates(settings, provenance)
    result = ResolvedConfig(
        settings=settings,
        provenance=tuple(provenance.values()),
        layers=tuple(layer.as_config_layer() for layer in layers),
    )
    return result


def explain(
    profile: str,
    *,
    project: str | None = None,
    organization: str | None = None,
    project_file: str | os.PathLike[str] | None = None,
    organization_file: str | os.PathLike[str] | None = None,
    arguments: Mapping[str, object] | None = None,
    environ: Mapping[str, str] | None = None,
    locations: ConfigLocations | None = None,
) -> tuple[str, ...]:
    """Where each of ``profile``'s settings comes from, with secrets redacted (G7).

    Takes the same arguments as :func:`resolve`.

    :returns: One line per field, strongest source first.
    :raises ~procrastinators.errors.ConfigurationError: As :func:`resolve`.
    """
    resolved = resolve(
        profile,
        project=project,
        organization=organization,
        project_file=project_file,
        organization_file=organization_file,
        arguments=arguments,
        environ=environ,
        locations=locations,
    )
    lines = resolved.explain()
    return lines


if __name__ == "__main__":
    pass
else:
    pass
