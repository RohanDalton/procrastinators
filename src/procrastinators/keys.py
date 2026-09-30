"""Stable identity: canonical encoding, quota keys, and policy fingerprints.

Three things are derived here, and each must mean the same thing in every
process, on every machine, after every restart (contract I8):

* A **quota key** names what is limited. It is a digest of the caller's scope
  mapping — vendor, dataset, endpoint, credential *reference* — and never of a
  rate, so changing a rate addresses the same stored quota (I2).
* A **policy fingerprint** names how it is limited: algorithm, parameters,
  state version, and fingerprint schema (I4). It is stored beside the quota and
  checked at every admission, so two workers that disagree find out.
* **Constraints** pair the two, with rule names that are either explicit or
  positional (I3).

Everything goes through one canonical, versioned byte encoding of a restricted
JSON-like value set. Python's ``hash()``, ``repr()``, ``str()`` of arbitrary
objects, and punctuation replacement are never used: they differ between
processes, between versions, or between values that should be distinct.

.. rubric:: Canonical encoding, version 1

The encoding starts with ``procrastinators/identity/v1`` and a NUL byte, then
encodes one value:

======================  ================================================
Value                   Encoding
======================  ================================================
``None``                ``N``
``True`` / ``False``    ``T`` / ``F``
``int``                 ``I`` decimal ``;`` (64-bit signed range only)
``float``               ``D`` :meth:`float.hex` ``;`` (finite; ``-0.0`` is ``0.0``)
``str``                 ``S`` UTF-8 length ``:`` UTF-8 bytes
list or tuple           ``L`` count ``:`` items, in order
mapping                 ``M`` count ``:`` key/value pairs, keys as ``str``,
                        sorted by their UTF-8 bytes
======================  ================================================

Types stay distinct: ``1``, ``1.0``, ``"1"`` and ``True`` all encode
differently. Strings are not Unicode-normalized, because two spellings a
vendor treats as different identifiers must not collide. Anything else —
bytes, sets, paths, dates, UUIDs, arbitrary objects — is rejected with
:exc:`~procrastinators.errors.InvalidIdentity`, so the caller converts it
explicitly and deliberately.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import dataclasses
import hashlib
import hmac
import math
import operator
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Final

from procrastinators.errors import InvalidIdentity, InvalidPolicy
from procrastinators.models import (
    Constraint,
    PolicyFingerprint,
    RuleId,
    canonical_constraints,
)

if TYPE_CHECKING:
    from procrastinators.models import PolicySpec, QuotaIdentity
else:
    pass

__all__ = [
    "FINGERPRINT_SCHEMA",
    "IDENTITY_ENCODING_VERSION",
    "MIN_SECRET_BYTES",
    "canonical_encode",
    "idempotent_key",
    "policy_fingerprint",
    "policy_parameters",
    "scope_constraints",
    "scope_fingerprint",
]

IDENTITY_ENCODING_VERSION: Final = 1
"""Version of the canonical encoding. Changing the encoding means a new version."""

FINGERPRINT_SCHEMA: Final = 1
"""Version of what a policy fingerprint covers, itself covered by the fingerprint."""

MIN_SECRET_BYTES: Final = 16
"""Shortest HMAC secret accepted, in bytes."""

_ENCODING_PREFIX: Final = f"procrastinators/identity/v{IDENTITY_ENCODING_VERSION}\x00".encode()
_KEY_PREFIX: Final = "k1-"
_HMAC_KEY_PREFIX: Final = "k1h-"
_FINGERPRINT_PREFIX: Final = "p1-"
_MAX_DEPTH: Final = 32
_MAX_ENCODED_BYTES: Final = 64 * 1024
_MIN_INT: Final = -(2**63)
_MAX_INT: Final = 2**63 - 1


def canonical_encode(value: object) -> bytes:
    """Encode ``value`` into the canonical, versioned byte form described above.

    Equal inputs always produce equal bytes, in any process and on any
    platform, and inputs that differ in type or content never do.

    :param value: ``None``, a ``bool``, ``int``, finite ``float``, ``str``, or a
        list, tuple, or string-keyed mapping of those, nested at most 32 deep.
    :returns: The encoding, at most 64 KiB.
    :raises ~procrastinators.errors.InvalidIdentity: ``value`` contains an unsupported type, a
        non-string mapping key, a non-finite float, an integer outside the 64-bit signed range, a
        string that is not valid Unicode, or is nested or sized beyond the bounds.
    """
    buffer = bytearray(_ENCODING_PREFIX)
    _encode_into(buffer, value, depth=0, path="$")
    encoded = bytes(buffer)
    return encoded


def _encode_into(buffer: bytearray, value: object, *, depth: int, path: str) -> None:
    if depth > _MAX_DEPTH:
        raise InvalidIdentity(f"{path}: nested more than {_MAX_DEPTH} levels deep")
    elif value is None:
        buffer += b"N"
    elif isinstance(value, bool):
        buffer += b"T" if value else b"F"
    elif isinstance(value, int):
        if not _MIN_INT <= value <= _MAX_INT:
            raise InvalidIdentity(f"{path}: integer {value} is outside the 64-bit signed range")
        else:
            pass
        buffer += b"I%d;" % int(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidIdentity(f"{path}: float {value!r} is not finite")
        else:
            pass
        # -0.0 == 0.0 in Python, so they must not name different quotas.
        normalized = 0.0 if value == 0 else float(value)
        buffer += b"D" + normalized.hex().encode("ascii") + b";"
    elif isinstance(value, str):
        _encode_str(buffer, value, path=path)
    elif isinstance(value, Mapping):
        entries = list()
        for key, item in value.items():
            if not isinstance(key, str):
                raise InvalidIdentity(
                    f"{path}: mapping keys must be str, got {type(key).__name__} {key!r}"
                )
            else:
                pass
            entries.append((_utf8(key, path=f"{path}.{key}"), key, item))
        entries.sort(key=operator.itemgetter(0))
        buffer += b"M%d:" % len(entries)
        for _, key, item in entries:
            _encode_str(buffer, key, path=path)
            _encode_into(buffer, item, depth=depth + 1, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        buffer += b"L%d:" % len(value)
        for index, item in enumerate(value):
            _encode_into(buffer, item, depth=depth + 1, path=f"{path}[{index}]")
    else:
        raise InvalidIdentity(
            f"{path}: {type(value).__name__} cannot be part of a stable identity; convert it "
            "explicitly to a str, int, float, bool, None, list, or str-keyed mapping"
        )
    if len(buffer) > _MAX_ENCODED_BYTES:
        raise InvalidIdentity(f"{path}: identity encoding exceeds {_MAX_ENCODED_BYTES} bytes")
    else:
        pass


def _utf8(value: str, *, path: str) -> bytes:
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise InvalidIdentity(f"{path}: string is not valid Unicode: {value!r}") from error
    return encoded


def _encode_str(buffer: bytearray, value: str, *, path: str) -> None:
    encoded = _utf8(value, path=path)
    buffer += b"S%d:" % len(encoded) + encoded


def idempotent_key(scope: Mapping[str, object], *, secret: bytes | None = None) -> str:
    """A stable quota key for ``scope``: the same mapping gives the same key everywhere.

    "Idempotent" means stable identity, not request deduplication. The key is a
    digest, so it is safe to store and log, and it never contains rate values:
    pass what is being limited, not how fast.

    Prefer an account or credential *reference* to a raw API key. An ordinary
    digest does not protect a low-entropy secret — anyone can hash candidate
    keys and compare — so when identity must derive from a credential, pass a
    ``secret`` shared by every worker and the key becomes an HMAC.

    :param scope: A non-empty mapping such as ``{"vendor": "ankh", "dataset":
        "orders"}``, holding only values :func:`canonical_encode` accepts.
    :param secret: An HMAC key of at least :data:`MIN_SECRET_BYTES` bytes, or
        ``None`` for a plain SHA-256 digest.
    :returns: ``"k1-"`` and 64 hex digits, or ``"k1h-"`` and 64 hex digits when
        keyed with ``secret``.
    :raises ~procrastinators.errors.InvalidIdentity: ``scope`` is not a non-empty mapping, holds an
        unsupported value, or ``secret`` is not bytes of sufficient length.
    """
    if not isinstance(scope, Mapping):
        raise InvalidIdentity(f"a quota scope must be a mapping, got {type(scope).__name__}")
    elif not scope:
        raise InvalidIdentity("a quota scope must name at least one dimension")
    else:
        pass
    encoded = canonical_encode(scope)
    if secret is None:
        key = _KEY_PREFIX + hashlib.sha256(encoded).hexdigest()
    elif not isinstance(secret, (bytes, bytearray)):
        raise InvalidIdentity(
            f"an HMAC secret must be bytes, got {type(secret).__name__}; encode it explicitly"
        )
    elif len(secret) < MIN_SECRET_BYTES:
        raise InvalidIdentity(f"an HMAC secret must be at least {MIN_SECRET_BYTES} bytes")
    else:
        key = _HMAC_KEY_PREFIX + hmac.new(bytes(secret), encoded, hashlib.sha256).hexdigest()
    return key


def policy_parameters(policy: PolicySpec) -> Mapping[str, object]:
    """The parameters a policy's fingerprint covers.

    A policy's ``fingerprint_parameters()`` method wins when it has one; a
    dataclass policy otherwise contributes its fields.

    :param policy: Any built-in or third-party policy.
    :returns: A mapping of parameter names to canonical values.
    :raises ~procrastinators.errors.InvalidPolicy: The policy is neither a dataclass nor provides
        ``fingerprint_parameters()``, or that method returns something other than a mapping.
    """
    if (hook := getattr(policy, "fingerprint_parameters", None)) is not None:
        parameters = hook()
    elif dataclasses.is_dataclass(policy) and not isinstance(policy, type):
        parameters = {
            field.name: getattr(policy, field.name) for field in dataclasses.fields(policy)
        }
    else:
        raise InvalidPolicy(
            f"{type(policy).__name__} must be a dataclass or provide fingerprint_parameters() "
            "so its fingerprint covers its parameters"
        )
    if not isinstance(parameters, Mapping):
        raise InvalidPolicy(
            f"{type(policy).__name__}.fingerprint_parameters() must return a mapping, "
            f"got {type(parameters).__name__}"
        )
    else:
        pass
    return parameters


def _policy_document(policy: PolicySpec) -> dict[str, object]:
    algorithm = policy.algorithm
    state_version = policy.state_version
    if not isinstance(algorithm, str) or not algorithm:
        raise InvalidPolicy(f"a policy's algorithm id must be a non-empty str, got {algorithm!r}")
    elif isinstance(state_version, bool) or not isinstance(state_version, int):
        raise InvalidPolicy(f"a policy's state_version must be an int, got {state_version!r}")
    else:
        pass
    document = {
        "algorithm": str(algorithm),
        "state_version": state_version,
        "parameters": dict(policy_parameters(policy)),
    }
    return document


def _fingerprint(document: dict[str, object]) -> PolicyFingerprint:
    try:
        encoded = canonical_encode({"schema": FINGERPRINT_SCHEMA, **document})
    except InvalidIdentity as error:
        raise InvalidPolicy(f"policy parameters cannot be fingerprinted: {error}") from error
    fingerprint = PolicyFingerprint(_FINGERPRINT_PREFIX + hashlib.sha256(encoded).hexdigest())
    return fingerprint


def policy_fingerprint(policy: PolicySpec) -> PolicyFingerprint:
    """Fingerprint one policy: algorithm, parameters, state version, and schema.

    Deliberately separate from the quota key. A rate change keeps the key and
    changes the fingerprint, so it surfaces as a
    :exc:`~procrastinators.errors.PolicyConflict` rather than as fresh quota.

    :param policy: Any built-in or third-party policy.
    :returns: ``"p1-"`` and 64 hex digits.
    :raises ~procrastinators.errors.InvalidPolicy: The policy's id, state version, or parameters
        cannot be fingerprinted.
    """
    fingerprint = _fingerprint(_policy_document(policy))
    return fingerprint


def scope_fingerprint(policies: Sequence[PolicySpec]) -> PolicyFingerprint:
    """Fingerprint a whole ordered list of policies for one scope.

    Used for positional rules: every rule in the list carries this one
    fingerprint, so reordering, adding, removing, or changing any limit is a
    policy conflict on every rule, never a silent repointing of stored state
    (contract I3).

    :param policies: The scope's policies in list order.
    :returns: ``"p1-"`` and 64 hex digits.
    :raises ~procrastinators.errors.InvalidPolicy: ``policies`` is empty or a policy cannot be
        fingerprinted.
    """
    if not policies:
        raise InvalidPolicy("a scope needs at least one policy")
    else:
        pass
    fingerprint = _fingerprint({"scope": [_policy_document(policy) for policy in policies]})
    return fingerprint


def scope_constraints(
    scope: QuotaIdentity,
    policies: Sequence[PolicySpec] | Mapping[str, PolicySpec],
    *,
    coordination_domain: str | None = None,
) -> tuple[Constraint, ...]:
    """Build the constraints for one scope, named and fingerprinted consistently.

    A sequence gets positional rule names ``#0``, ``#1``, … sharing one
    :func:`scope_fingerprint`. A mapping gets its keys as explicit rule names,
    each with its own :func:`policy_fingerprint`, so named rules survive
    reordering and one rule's change does not disturb another's.

    :param scope: The quota the rules belong to.
    :param policies: The scope's policies, as a list or keyed by rule name.
    :param coordination_domain: The partition every rule's state must live in,
        or ``None``.
    :returns: The constraints in canonical order.
    :raises ~procrastinators.errors.InvalidPolicy: ``policies`` is empty or a string, a rule name
        is invalid, or a policy cannot be fingerprinted.
    """
    if isinstance(policies, (str, bytes)):
        raise InvalidPolicy("policies must be a sequence or mapping of policies, not a string")
    elif isinstance(policies, Mapping):
        constraints = [
            Constraint(RuleId(scope, name), policy, policy_fingerprint(policy), coordination_domain)
            for name, policy in policies.items()
        ]
    else:
        ordered = list(policies)
        fingerprint = scope_fingerprint(ordered)
        constraints = [
            Constraint(RuleId.positional(scope, index), policy, fingerprint, coordination_domain)
            for index, policy in enumerate(ordered)
        ]
    canonical = canonical_constraints(constraints)
    return canonical


if __name__ == "__main__":
    pass
else:
    pass
