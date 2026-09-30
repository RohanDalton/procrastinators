"""Configuration layers, resolution results, and provenance.

These types describe *what a resolved configuration is*, not how it is found.
Discovery, TOML parsing, PlatformDirs locations, and atomic saves arrive in
Phase 9; this module fixes the meaning those mechanics must implement.

Two distinctions carry most of the weight.

*Unset is not null.* A layer that never mentions ``timeout`` must fall through
to the layer below it. A layer that sets ``timeout = null`` has said something:
wait indefinitely. Collapsing the two would make it impossible for a project
file to override an organization default back to "no timeout", so the absent
case is :data:`UNSET` and the explicit case is ``None``.

*Provenance survives resolution.* A caller debugging a limiter needs to know
which file supplied which value, and needs that answer without a password in
it, so every resolved field remembers its source and secrets are redacted on
the way out.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import operator
import re
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum
from types import MappingProxyType
from typing import Final, Literal, TypeVar, overload

from procrastinators.errors import ConfigurationError
from procrastinators.models import Algorithms, Limit

__all__ = [
    "DEFAULT_BACKEND",
    "UNSET",
    "ConfigLayer",
    "ConfigSource",
    "LimiterSettings",
    "Provenance",
    "ResolvedConfig",
    "Unset",
    "redact",
]

T = TypeVar("T")
"""Type of the caller-supplied ``default`` in :meth:`ConfigLayer.get`."""

DEFAULT_BACKEND: Final = "sqlite://"
"""The backend a limiter uses when nothing selects one: the shared SQLite file.

It lives at a stable per-user state path, so separate processes run by one
user coordinate without configuring anything. Memory is never the silent
default, because it coordinates nothing beyond one process.
"""


class Unset:
    """Type of :data:`UNSET`: "this layer said nothing about that field".

    A singleton: constructing it again returns the same instance, including
    after pickling, so identity checks such as ``value is UNSET`` are reliable.
    It is falsy, but callers should test identity rather than truthiness,
    because ``None``, ``0`` and ``""`` are falsy too and mean something.
    """

    _instance: Unset | None = None

    def __new__(cls) -> Unset:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        else:
            pass
        return cls._instance

    def __bool__(self) -> Literal[False]:
        """Always ``False``."""
        return False

    def __repr__(self) -> str:
        return "UNSET"

    def __reduce__(self) -> str:
        return "UNSET"


UNSET: Final = Unset()
"""Absence of a value, as distinct from an explicit ``None`` (contract G2).

The sole instance of :class:`Unset`. A layer field that is :data:`UNSET` falls
through to the layer below; one that is ``None`` has chosen the null meaning.
"""


class ConfigSource(IntEnum):
    """Where a value came from, ordered by precedence.

    Lower is stronger. The order is the one the goals ask for: explicit
    arguments beat environment defaults, which beat the selected project, which
    beats the selected organization, which beats the library's own behavior
    defaults. There is no layer for invented vendor rates.
    """

    ARGUMENTS = 0
    """Arguments passed explicitly to the constructor. The strongest layer."""
    ENVIRONMENT = 1
    """``PROCRASTINATORS_*`` environment defaults."""
    PROJECT = 2
    """The explicitly selected project's configuration."""
    ORGANIZATION = 3
    """The explicitly selected organization's configuration."""
    LIBRARY_DEFAULT = 4
    """The library's own behavior defaults. The weakest layer, and never a rate."""

    @property
    def label(self) -> str:
        """Lower-case member name for display, for example ``"library_default"``."""
        label = self.name.lower()
        return label


@dataclass(frozen=True, slots=True)
class ConfigLayer:
    """One contributing layer: its source, where it came from, and its values.

    ``origin`` is a human-readable pointer for ``explain()`` — a file path, the
    environment-variable prefix, ``"constructor"``. ``revision`` is whatever the
    store uses to detect a lost update on save, and is ``None`` for layers that
    are not written back.

    Values are held behind a read-only view, but a layer is only as immutable as
    what a caller puts in it; nested containers should already be immutable.

    :raises ~procrastinators.errors.ConfigurationError: ``source`` is not a
        :class:`ConfigSource`, or a key in ``values`` is not a string.
    """

    source: ConfigSource
    """Which precedence level this layer occupies.

    Must be a :class:`ConfigSource` member, or
    :exc:`~procrastinators.errors.ConfigurationError` is raised.
    """
    values: Mapping[str, object]
    """Field names mapped to the values this layer sets.

    Keys must be strings, or :exc:`~procrastinators.errors.ConfigurationError`
    is raised. Copied on construction and exposed as a read-only
    :class:`~types.MappingProxyType`. A field this layer does not mention is
    simply absent.
    """
    origin: str | None = None
    """Human-readable pointer to where the layer came from, or ``None``."""
    revision: str | None = None
    """Store-specific revision for lost-update detection on save, or ``None``."""

    def __post_init__(self) -> None:
        if not isinstance(self.source, ConfigSource):
            raise ConfigurationError(f"unknown configuration source: {self.source!r}")
        else:
            pass
        for key in self.values:
            if not isinstance(key, str):
                raise ConfigurationError(f"configuration keys must be strings, got {key!r}")
            else:
                pass
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    @overload
    def get(self, name: str) -> object | Unset: ...

    @overload
    def get(self, name: str, default: T) -> object | T: ...

    def get(self, name: str, default: object = UNSET) -> object:
        """Return the value this layer sets for ``name``, or :data:`UNSET`.

        An explicit ``None`` in the layer is returned as ``None``, which is the
        whole point of having a separate sentinel.

        :param name: The field to look up.
        :param default: Returned instead when this layer does not set ``name``.
            Defaults to :data:`UNSET`.
        :returns: The layer's value for ``name``, which may be ``None``, or
            ``default`` if the layer does not mention it.
        """
        value = self.values.get(name, default)
        return value


@dataclass(frozen=True, slots=True)
class LimiterSettings:
    """The immutable, fully resolved settings one limiter runs on.

    ``limits`` is replaced as a whole by the strongest layer that sets it, never
    merged element-wise. Merging two lists of rates from different files would
    produce a policy nobody wrote and, because rule identity is positional,
    would silently change which stored rule each entry addresses.

    :raises ~procrastinators.errors.ConfigurationError: ``algorithm`` is not an
        :class:`~procrastinators.models.Algorithms` member, or ``timeout`` or
        ``storage_timeout`` is out of range.
    """

    limits: Sequence[Limit] = tuple()
    """The rates for this scope, in order. Stored as a tuple.

    Positional rule names follow this order, so reordering it is a policy
    conflict (contract I3). Empty by default: there is no default vendor rate.
    """
    algorithm: Algorithms = Algorithms.SLIDING_LOG
    """The one algorithm applied to every limit in the scope.

    Must be an :class:`~procrastinators.models.Algorithms` member, or
    :exc:`~procrastinators.errors.ConfigurationError` is raised.
    """
    backend: str | None = None
    """Backend address, such as ``"sqlite:///./quota.sqlite3"``, or ``None``.

    May carry credentials in its userinfo, which is why :func:`redact` inspects
    its value and not only its name.
    """
    namespace: str = "default"
    """The environment or quota domain; workers in different namespaces never
    share counters (contract I1)."""
    timeout: float | None = None
    """Default quota-wait budget in seconds (contract B2).

    ``None`` waits indefinitely, ``0`` attempts once, and a positive value bounds
    the whole attempt. Must be a non-negative number (not a :class:`bool`) or
    ``None``, or :exc:`~procrastinators.errors.ConfigurationError` is raised.
    """
    storage_timeout: float = 5.0
    """Cap in seconds on each individual storage call (contract B3).

    Unlike :attr:`timeout` it cannot be ``None``. Must be a positive number (not
    a :class:`bool`), or :exc:`~procrastinators.errors.ConfigurationError` is
    raised: an unbounded quota wait still does not license an unbounded storage
    call.
    """
    profile: str | None = None
    """Name of the selected configuration profile, or ``None``."""
    rules: Mapping[str, Limit] | None = None
    """Explicitly named rates instead of positional :attr:`limits`, or ``None``.

    Held behind a read-only view. ``limits`` and ``rules`` are one value for
    precedence purposes (contract G4): a scope has one or the other, never both.
    """
    options: Mapping[str, object] = MappingProxyType(dict())
    """Algorithm options applied to every rate, replaced wholesale by the strongest layer.

    Held behind a read-only view; which keys apply depends on :attr:`algorithm`,
    and the facade validates them against it.
    """

    def __post_init__(self) -> None:
        object.__setattr__(self, "limits", tuple(self.limits))
        if self.rules is not None:
            if not isinstance(self.rules, Mapping):
                raise ConfigurationError(f"rules must be a mapping, got {self.rules!r}")
            elif self.limits:
                raise ConfigurationError(
                    "settings carry limits or rules, not both: they are one value (G4)"
                )
            else:
                object.__setattr__(self, "rules", MappingProxyType(dict(self.rules)))
        else:
            pass
        if not isinstance(self.options, Mapping):
            raise ConfigurationError(f"options must be a mapping, got {self.options!r}")
        else:
            object.__setattr__(self, "options", MappingProxyType(dict(self.options)))
        if not isinstance(self.algorithm, Algorithms):
            raise ConfigurationError(
                f"algorithm must be one of {[algorithm.value for algorithm in Algorithms]}, "
                f"got {self.algorithm!r}"
            )
        else:
            pass
        for name in ("timeout", "storage_timeout"):
            if (value := getattr(self, name)) is None:
                continue
            elif isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ConfigurationError(f"{name} must be a number or null, got {value!r}")
            elif value < 0:
                raise ConfigurationError(f"{name} must not be negative, got {value!r}")
            else:
                pass
        if self.storage_timeout <= 0:
            raise ConfigurationError(
                "storage_timeout must be positive: an unbounded quota wait still does not "
                "license an unbounded storage call"
            )
        else:
            pass


_SECRET_NAME_PATTERN: Final = re.compile(
    r"(pass(word)?|secret|token|api[_-]?key|credential|auth)", re.IGNORECASE
)
_USERINFO_PATTERN: Final = re.compile(r"(?P<scheme>[a-zA-Z][\w+.-]*://)(?P<userinfo>[^/@]+)@")
_QUERY_PARAMETER_PATTERN: Final = re.compile(r"([?&])([^=&#\s]+)=([^&#\s]*)")
REDACTED: Final = "***"
"""Placeholder :func:`redact` substitutes for a secret value or URL userinfo."""


def redact(name: str, value: object) -> object:
    """Return ``value`` with anything secret removed, for display only.

    Two cases matter in practice: a field whose *name* says it holds a secret,
    and a backend URL that carries userinfo. The second is why redaction cannot
    be a name check alone — ``backend`` is an innocuous name for a string that
    may contain a password.

    :param name: The field name. If it mentions a password, secret, token, API
        key, credential, or auth (case-insensitively), the whole value is
        replaced with :data:`REDACTED`.
    :param value: The value to display. When ``name`` is not secret and
        ``value`` is a string, the userinfo of every URL in it
        (``scheme://userinfo@``) is replaced with :data:`REDACTED`.
    :returns: :data:`REDACTED`, the string with userinfo replaced, or ``value``
        unchanged.
    """
    if _SECRET_NAME_PATTERN.search(name):
        redacted: object = REDACTED
    elif isinstance(value, str):
        redacted = _USERINFO_PATTERN.sub(rf"\g<scheme>{REDACTED}@", value)
        redacted = _QUERY_PARAMETER_PATTERN.sub(_redact_query_parameter, redacted)
    else:
        redacted = value
    return redacted


def _redact_query_parameter(match: re.Match[str]) -> str:
    key = match.group(2)
    if _SECRET_NAME_PATTERN.search(urllib.parse.unquote_plus(key)):
        redacted = f"{match.group(1)}{key}={REDACTED}"
    else:
        redacted = match.group(0)
    return redacted


@dataclass(frozen=True, slots=True)
class Provenance:
    """Where one resolved field's value came from.

    Its string form is one line of :meth:`ResolvedConfig.explain`, and uses
    :attr:`display_value`, never the raw :attr:`value`.
    """

    field: str
    """Name of the resolved field, such as ``"timeout"``."""
    source: ConfigSource
    """The layer that supplied the value."""
    origin: str | None = None
    """That layer's :attr:`ConfigLayer.origin`, or ``None``."""
    value: object = None
    """The raw resolved value. May contain secrets; display :attr:`display_value`."""

    @property
    def display_value(self) -> object:
        """The value with secrets removed. The only form safe to log."""
        display_value = redact(self.field, self.value)
        return display_value

    def __str__(self) -> str:
        where = f"{self.source.label}" + (f" ({self.origin})" if self.origin else "")
        description = f"{self.field} = {_display(self.display_value)} from {where}"
        return description


def _display(value: object) -> str:
    """``value`` as ``explain()`` shows it: rates as a vendor writes them, the rest as reprs."""
    if isinstance(value, Limit):
        text = str(value)
    elif isinstance(value, (tuple, list)):
        text = "[" + ", ".join(_display(item) for item in value) + "]"
    elif isinstance(value, Mapping):
        text = "{" + ", ".join(f"{key}: {_display(item)}" for key, item in value.items()) + "}"
    else:
        text = repr(value)
    return text


@dataclass(frozen=True, slots=True)
class ResolvedConfig:
    """Immutable settings plus the provenance of every field that produced them.

    Handed to the facade once. A limiter does not re-read configuration between
    acquisitions: a rate that changed underneath a running worker is a policy
    migration, not a value refresh.

    :raises ~procrastinators.errors.ConfigurationError: ``provenance`` records
        the same field more than once.
    """

    settings: LimiterSettings
    """The settings the limiter runs on."""
    provenance: tuple[Provenance, ...] = tuple()
    """One record per resolved field. Stored as a tuple.

    A field recorded twice raises
    :exc:`~procrastinators.errors.ConfigurationError`.
    """
    layers: tuple[ConfigLayer, ...] = tuple()
    """The layers that contributed to resolution. Stored as a tuple."""

    def __post_init__(self) -> None:
        object.__setattr__(self, "provenance", tuple(self.provenance))
        object.__setattr__(self, "layers", tuple(self.layers))
        seen = {entry.field for entry in self.provenance}
        if len(seen) != len(self.provenance):
            raise ConfigurationError("provenance must record each field at most once")
        else:
            pass

    def source_of(self, field_name: str) -> ConfigSource | None:
        """Which layer supplied ``field_name``, or ``None`` if unrecorded.

        :param field_name: The resolved field to look up.
        """
        for entry in self.provenance:
            if entry.field == field_name:
                source: ConfigSource | None = entry.source
                break
            else:
                pass
        else:
            source = None
        return source

    def explain(self) -> tuple[str, ...]:
        """Redacted, ordered lines describing where each setting came from.

        Ordered strongest source first, then by field name within a source
        (contract G7).
        """
        ordered = sorted(self.provenance, key=operator.attrgetter("source", "field"))
        lines = tuple(str(entry) for entry in ordered)
        return lines


if __name__ == "__main__":
    pass
else:
    pass
