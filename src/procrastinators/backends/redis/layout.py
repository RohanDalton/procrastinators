"""Where a rule's state lives in a Redis or Valkey keyspace.

Every key is ``<prefix>:{<tag>}:<kind>:<name>``. The braces are a hash tag:
Redis Cluster places a key by the text between them alone, so every key with
one tag lands in one hash slot and one script can touch them all.

* A single server tags by scope. In a cluster, all quota keys share one tag so
  rules and cooldowns can be checked in the same atomic script. A domain does
  not change a rule's placement.

Names are percent-encoded, keeping only unreserved characters, so no
namespace, quota key, rule name, or domain can forge a brace or a separator
and two different rules never share a key.

Nothing here performs I/O.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import hashlib
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from procrastinators.errors import ConfigurationError

if TYPE_CHECKING:
    from procrastinators.models import Constraint, QuotaIdentity, RuleId
else:
    pass

__all__ = [
    "DEFAULT_PREFIX",
    "LAYOUT_VERSION",
    "SLOTS",
    "KeyLayout",
    "RuleKeys",
    "key_slot",
]

LAYOUT_VERSION: Final = "1"
"""The version of this key layout, stored with every rule and checked by every script."""

DEFAULT_PREFIX: Final = "procrastinators"
"""What every key starts with unless a store is given another prefix."""

SLOTS: Final = 16384
"""Hash slots in a Redis Cluster."""

_UNRESERVED: Final = "-._~"
_CRC16_POLYNOMIAL: Final = 0x1021


def _crc16_table() -> tuple[int, ...]:
    table = list()
    for byte in range(256):
        crc = byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ _CRC16_POLYNOMIAL) if crc & 0x8000 else crc << 1
        table.append(crc & 0xFFFF)
    built = tuple(table)
    return built


_CRC16: Final = _crc16_table()


def _crc16(data: bytes) -> int:
    """CRC-16/XMODEM, the checksum Redis Cluster places keys by."""
    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFF) ^ _CRC16[((crc >> 8) ^ byte) & 0xFF]
    return crc


def key_slot(key: str) -> int:
    """The Redis Cluster hash slot of ``key``, honoring its hash tag.

    :param key: A key.
    :returns: A slot from 0 to 16383.
    """
    encoded = key.encode()
    if (opening := encoded.find(b"{")) != -1 and (
        closing := encoded.find(b"}", opening + 1)
    ) > opening + 1:
        hashed = encoded[opening + 1 : closing]
    else:
        hashed = encoded
    slot = _crc16(hashed) % SLOTS
    return slot


def _encoded(name: str) -> str:
    encoded = urllib.parse.quote(name, safe=_UNRESERVED)
    return encoded


@dataclass(frozen=True, slots=True)
class RuleKeys:
    """The three keys holding one rule, all in one hash slot."""

    meta: str
    """Policy metadata, migration record, and last observed time; never expires (L10)."""

    state: str
    """Scalar state, its safe-forget horizon, and event-log counters; expires at the horizon."""

    events: str
    """Weighted log events, one member per admission; expires with the state."""

    @property
    def all(self) -> tuple[str, str, str]:
        """The keys in the order every script expects them."""
        keys = (self.meta, self.state, self.events)
        return keys


@dataclass(frozen=True, slots=True)
class KeyLayout:
    """The keys of rules and cooldowns under one prefix.

    :raises ~procrastinators.errors.ConfigurationError: ``prefix`` is empty or holds a brace,
        which would move every key's hash tag.
    """

    prefix: str = DEFAULT_PREFIX
    """Starts every key; separates deployments sharing one server."""

    def __post_init__(self) -> None:
        if not isinstance(self.prefix, str) or not self.prefix:
            raise ConfigurationError(f"a key prefix must be a non-empty string: {self.prefix!r}")
        elif "{" in self.prefix or "}" in self.prefix:
            raise ConfigurationError(
                f"a key prefix must not hold braces, which would move every key's hash tag: "
                f"{self.prefix!r}"
            )
        else:
            pass

    @property
    def cluster_tag(self) -> str:
        """One cluster slot per key prefix, shared by its rules and cooldowns."""
        digest = hashlib.sha256(self.prefix.encode()).hexdigest()[:16]
        tag = f"p.{digest}"
        return tag

    @staticmethod
    def scope_tag(scope: QuotaIdentity) -> str:
        """The hash tag of a scope's cooldown, and of its rules without a domain."""
        tag = f"q.{_encoded(scope.namespace)}.{_encoded(scope.key)}"
        return tag

    @classmethod
    def rule_tag(cls, constraint: Constraint) -> str:
        """The scope tag of a rule outside a cluster."""
        tag = cls.scope_tag(constraint.rule.scope)
        return tag

    def rule_keys(self, rule: RuleId, tag: str) -> RuleKeys:
        """The keys holding ``rule`` under hash tag ``tag``.

        :param rule: The rule.
        :param tag: Its hash tag, from :meth:`rule_tag` or :meth:`scope_tag`.
        """
        name = f"{_encoded(rule.scope.namespace)}:{_encoded(rule.scope.key)}:{_encoded(rule.name)}"
        base = f"{self.prefix}:{{{tag}}}"
        keys = RuleKeys(f"{base}:m:{name}", f"{base}:s:{name}", f"{base}:e:{name}")
        return keys

    def cooldown_key(self, scope: QuotaIdentity, *, cluster: bool = False) -> str:
        """The key holding ``scope``'s cooldown."""
        name = f"{_encoded(scope.namespace)}:{_encoded(scope.key)}"
        tag = self.cluster_tag if cluster else self.scope_tag(scope)
        key = f"{self.prefix}:{{{tag}}}:c:{name}"
        return key


if __name__ == "__main__":
    pass
else:
    pass
