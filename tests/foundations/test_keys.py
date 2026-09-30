"""Stable identity: canonical encoding, quota keys, HMAC keys, and fingerprints.

Phase 4 acceptance: identities survive process restarts, policy changes preserve
identity but alter fingerprints, normalized equivalent inputs agree, and invalid
encodings fail predictably. The golden fixtures freeze the encoding: if one of
these tests fails after a change, every stored quota in the world just moved.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import copy
import datetime as dt
import json
import math
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

import pytest
from hypothesis import given
from hypothesis import strategies as st

from procrastinators import models
from procrastinators.errors import InvalidIdentity, InvalidPolicy, PolicyConflict
from procrastinators.keys import (
    MIN_SECRET_BYTES,
    canonical_encode,
    idempotent_key,
    policy_fingerprint,
    policy_parameters,
    scope_constraints,
    scope_fingerprint,
)
from procrastinators.models import (
    DurationMicros,
    FixedWindowPolicy,
    Limit,
    QuotaIdentity,
    RuleId,
    SlidingLogPolicy,
    TokenBucketPolicy,
)
from procrastinators.policies import normalize_limit

GOLDEN_PATH = Path(__file__).with_name("identity_golden.json")
ONE_SECOND = DurationMicros(1_000_000)
SECRET = bytes(range(32))

# Recomputed in a fresh interpreter under a different hash seed.
_RECOMPUTE = """
import json, sys
from procrastinators import models
from procrastinators.keys import canonical_encode, idempotent_key, policy_fingerprint

golden = json.loads(sys.stdin.read())
result = {
    "hex": canonical_encode(golden["encoding"]["scope"]).hex(),
    "keys": [idempotent_key(entry["scope"]) for entry in golden["keys"]],
    "hmac_keys": [
        idempotent_key(entry["scope"], secret=bytes.fromhex(entry["secret_hex"]))
        for entry in golden["hmac_keys"]
    ],
    "fingerprints": [
        policy_fingerprint(
            getattr(models, entry["policy"])(*entry["arguments"], **entry["keywords"])
        )
        for entry in golden["fingerprints"]
    ],
}
print(json.dumps(result))
"""


def _containers(children: st.SearchStrategy[object]) -> st.SearchStrategy[object]:
    containers = st.lists(children, max_size=4) | st.dictionaries(
        st.text(max_size=8), children, max_size=4
    )
    return containers


JSON_VALUES = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**63), max_value=2**63 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    _containers,
    max_leaves=12,
)
SCOPES = st.dictionaries(st.text(min_size=1, max_size=8), JSON_VALUES, min_size=1, max_size=5)


def _nested(depth: int) -> object:
    value: object = "deep"
    for _ in range(depth):
        value = [value]
    return value


UNSUPPORTED_VALUES = {
    "bytes": b"ankh",
    "set": {"ankh"},
    "path": Path("/ankh"),
    "date": dt.date(2026, 9, 29),
    "uuid": uuid.UUID(int=0),
    "object": object(),
    "nan": math.nan,
    "infinity": math.inf,
    "huge int": 2**63,
    "int key": {1: "one"},
    "lone surrogate": "\ud800",
    "too deep": _nested(40),
    "too large": "x" * (65 * 1024),
}


@pytest.fixture(scope="session")
def golden() -> dict[str, object]:
    """The frozen fixtures. Loaded once; tests deep-copy before using mutable parts."""
    with GOLDEN_PATH.open("rt") as fh:
        data: dict[str, object] = json.load(fh)
    return data


@pytest.fixture
def golden_copy(golden: dict[str, object]) -> dict[str, object]:
    data = copy.deepcopy(golden)
    return data


@pytest.fixture(params=list(UNSUPPORTED_VALUES))
def unsupported(request: pytest.FixtureRequest) -> object:
    value = UNSUPPORTED_VALUES[request.param]
    return value


def test_keys_match_the_golden_fixtures(golden_copy: dict[str, object]) -> None:
    """
    Given: Scopes and frozen keys recorded when the encoding was defined.
    When:  The keys are recomputed in this process.
    Then:  Every one matches.
    """
    entries = golden_copy["keys"]
    assert isinstance(entries, list)
    expected = [entry["key"] for entry in entries]

    actual = [idempotent_key(entry["scope"]) for entry in entries]

    assert actual == expected


def test_the_encoding_bytes_are_frozen(golden_copy: dict[str, object]) -> None:
    """
    Given: A scope and the exact bytes its encoding had when version 1 was defined.
    When:  It is encoded again.
    Then:  The bytes are identical.
    """
    encoding = golden_copy["encoding"]
    assert isinstance(encoding, dict)
    expected = bytes.fromhex(encoding["hex"])

    actual = canonical_encode(encoding["scope"])

    assert actual == expected


def test_lu_tze_computes_the_same_keys_in_another_process(
    golden_copy: dict[str, object], project_root: Path
) -> None:
    """
    Given: The golden fixtures.
    When:  Keys, HMAC keys, encoding, and fingerprints are recomputed in a fresh
           interpreter with a different hash seed.
    Then:  Every value matches the fixtures: identity survives a restart (I8).
    """
    keys = golden_copy["keys"]
    hmac_keys = golden_copy["hmac_keys"]
    fingerprints = golden_copy["fingerprints"]
    encoding = golden_copy["encoding"]
    assert isinstance(keys, list)
    assert isinstance(hmac_keys, list)
    assert isinstance(fingerprints, list)
    assert isinstance(encoding, dict)
    expected = {
        "hex": encoding["hex"],
        "keys": [entry["key"] for entry in keys],
        "hmac_keys": [entry["key"] for entry in hmac_keys],
        "fingerprints": [entry["fingerprint"] for entry in fingerprints],
    }

    result = subprocess.run(
        [sys.executable, "-B", "-c", _RECOMPUTE],
        input=json.dumps(golden_copy),
        capture_output=True,
        text=True,
        cwd=project_root,
        env={**os.environ, "PYTHONHASHSEED": "271828"},
        check=True,
    )
    actual = json.loads(result.stdout)

    assert actual == expected


def test_fingerprints_match_the_golden_fixtures(golden_copy: dict[str, object]) -> None:
    """
    Given: Built-in policies and their frozen fingerprints.
    When:  The fingerprints are recomputed.
    Then:  Every one matches.
    """
    entries = golden_copy["fingerprints"]
    assert isinstance(entries, list)
    expected = [entry["fingerprint"] for entry in entries]

    actual = [
        policy_fingerprint(
            getattr(models, entry["policy"])(*entry["arguments"], **entry["keywords"])
        )
        for entry in entries
    ]

    assert actual == expected


def test_hmac_keys_match_the_golden_fixtures(golden_copy: dict[str, object]) -> None:
    """
    Given: Scopes, a shared secret, and frozen HMAC keys.
    When:  The keys are recomputed with that secret.
    Then:  Every one matches, and differs from the plain key of the same scope.
    """
    entries = golden_copy["hmac_keys"]
    assert isinstance(entries, list)
    expected = [entry["key"] for entry in entries]

    actual = [
        idempotent_key(entry["scope"], secret=bytes.fromhex(entry["secret_hex"]))
        for entry in entries
    ]

    assert actual == expected
    assert all(key.startswith("k1h-") for key in actual)
    assert {idempotent_key(entry["scope"]) for entry in entries}.isdisjoint(actual)


@given(scope=SCOPES)
def test_mapping_order_never_changes_a_key(scope: dict[str, object]) -> None:
    """
    Given: Any supported scope.
    When:  Its keys are inserted in reverse order.
    Then:  The key is unchanged: mapping keys are sorted in the encoding.
    """
    expected = idempotent_key(scope)

    actual = idempotent_key(dict(reversed(list(scope.items()))))

    assert actual == expected


@given(scope=SCOPES)
def test_encoding_is_deterministic(scope: dict[str, object]) -> None:
    """
    Given: Any supported scope.
    When:  It is encoded twice, once from a deep copy.
    Then:  The bytes are identical.
    """
    expected = canonical_encode(scope)

    actual = canonical_encode(copy.deepcopy(scope))

    assert actual == expected


DISTINCT_VALUES = {
    "int": 1,
    "float": 1.0,
    "string": "1",
    "bool": True,
    "none": None,
    "list": [1],
    "tuple-as-list-of-two": [1, 1],
    "nested": {"1": 1},
}


def test_types_are_never_confused() -> None:
    """
    Given: 1, 1.0, "1", True, None, [1], [1, 1], and {"1": 1}.
    When:  Each is the value of the same scope key.
    Then:  All eight keys differ: no implicit conversion merges two identities.
    """
    expected = len(DISTINCT_VALUES)

    actual = len({idempotent_key({"account": value}) for value in DISTINCT_VALUES.values()})

    assert actual == expected


def test_lists_keep_their_order_and_tuples_are_lists() -> None:
    """
    Given: A list, the same list reversed, and the same items as a tuple.
    When:  Each is encoded.
    Then:  Order matters, and a tuple encodes exactly as a list.
    """
    forward = canonical_encode(["vimes", "carrot"])

    assert forward != canonical_encode(["carrot", "vimes"])
    assert forward == canonical_encode(("vimes", "carrot"))


def test_negative_zero_is_zero() -> None:
    """
    Given: 0.0 and -0.0, which Python considers equal.
    When:  Each is encoded.
    Then:  They encode identically, so equal scopes cannot name different quotas.
    """
    expected = canonical_encode({"offset": 0.0})

    actual = canonical_encode({"offset": -0.0})

    assert actual == expected


def test_punctuation_is_not_replaced() -> None:
    """
    Given: Two names that collide if punctuation is replaced with underscores.
    When:  Keys are derived from each.
    Then:  The keys differ (the requests-ratelimiter collision the design cites).
    """
    first = idempotent_key({"vendor": "ankh.morpork"})

    actual = idempotent_key({"vendor": "ankh_morpork"})

    assert actual != first


def test_the_auditors_reject_what_has_no_stable_meaning(unsupported: object) -> None:
    """
    Given: A value with no single stable encoding, or beyond the bounds.
    When:  A key is derived from a scope containing it.
    Then:  InvalidIdentity is raised, which is also a ValueError.
    """
    with pytest.raises(InvalidIdentity) as raised:
        idempotent_key({"value": unsupported})

    assert isinstance(raised.value, ValueError)


@pytest.mark.parametrize("scope", [dict(), ["ankh"], "ankh"], ids=["empty", "list", "string"])
def test_a_scope_must_be_a_non_empty_mapping(scope: object) -> None:
    """
    Given: An empty mapping, a list, or a string as the scope.
    When:  A key is derived.
    Then:  InvalidIdentity is raised.
    """
    with pytest.raises(InvalidIdentity, match="scope"):
        idempotent_key(scope)  # ty: ignore[invalid-argument-type]


@pytest.mark.parametrize(
    "secret",
    [b"short", "a string secret of plenty of length", bytes(MIN_SECRET_BYTES - 1)],
    ids=["too short", "str", "one byte short"],
)
def test_an_hmac_secret_must_be_real_bytes_of_real_length(secret: object) -> None:
    """
    Given: A short secret, or a str.
    When:  An HMAC key is derived with it.
    Then:  InvalidIdentity is raised: a weak secret must not look protected.
    """
    with pytest.raises(InvalidIdentity, match="secret"):
        idempotent_key({"vendor": "ankh"}, secret=secret)  # ty: ignore[invalid-argument-type]


def test_a_rate_change_keeps_the_key_and_changes_the_fingerprint() -> None:
    """
    Given: One scope with a ten-per-second policy, then a twenty-per-second one.
    When:  The key and fingerprints are derived.
    Then:  The key is the same — same stored quota — and the fingerprints differ, so
           the change surfaces as a conflict rather than fresh capacity (I2, I4).
    """
    key = idempotent_key({"vendor": "ankh", "dataset": "orders"})
    before = policy_fingerprint(SlidingLogPolicy(10, ONE_SECOND))

    after = policy_fingerprint(SlidingLogPolicy(20, ONE_SECOND))

    assert idempotent_key({"dataset": "orders", "vendor": "ankh"}) == key
    assert after != before


def test_equivalent_policies_fingerprint_identically() -> None:
    """
    Given: A token bucket with default initial tokens and one explicitly full;
           and one limit spelled two ways.
    When:  Each is fingerprinted.
    Then:  Equivalent policies agree, so equivalent workers do not conflict.
    """
    implicit = policy_fingerprint(TokenBucketPolicy(10, 1, ONE_SECOND))
    explicit = policy_fingerprint(TokenBucketPolicy(10, 1, ONE_SECOND, initial_tokens=10))
    spelled = policy_fingerprint(normalize_limit(Limit(10, per="1s")))
    written = policy_fingerprint(normalize_limit(Limit(10, per=dt.timedelta(milliseconds=1000))))

    assert explicit == implicit
    assert written == spelled


def test_the_algorithm_is_part_of_the_fingerprint() -> None:
    """
    Given: A fixed window and a sliding log with identical parameters.
    When:  Each is fingerprinted.
    Then:  They differ.
    """
    window = policy_fingerprint(FixedWindowPolicy(10, ONE_SECOND))

    actual = policy_fingerprint(SlidingLogPolicy(10, ONE_SECOND))

    assert actual != window


@dataclass(frozen=True)
class HexPolicy:
    """A third-party dataclass policy."""

    spells: int

    algorithm: ClassVar[str] = "unseen.hex"
    state_version: ClassVar[int] = 1

    @property
    def capacity(self) -> int:
        return self.spells


class OpaquePolicy:
    """A third-party policy that is not a dataclass and offers no parameters."""

    algorithm = "unseen.opaque"
    state_version = 1
    capacity = 1


class ExplainedPolicy(OpaquePolicy):
    """The same, but it states its parameters."""

    def fingerprint_parameters(self) -> dict[str, int]:
        parameters = {"capacity": 1}
        return parameters


def test_a_third_party_dataclass_policy_fingerprints_by_its_fields() -> None:
    """
    Given: A third-party dataclass policy.
    When:  Its parameters and fingerprints for two values are read.
    Then:  The parameters are its fields, and different values fingerprint differently.
    """
    expected = {"spells": 3}

    actual = policy_parameters(HexPolicy(3))

    assert actual == expected
    assert policy_fingerprint(HexPolicy(3)) != policy_fingerprint(HexPolicy(4))


def test_an_opaque_policy_must_explain_itself() -> None:
    """
    Given: A non-dataclass policy with and without fingerprint_parameters().
    When:  Each is fingerprinted.
    Then:  The silent one raises InvalidPolicy; the explained one is fingerprinted.
    """
    with pytest.raises(InvalidPolicy, match="fingerprint_parameters"):
        policy_fingerprint(OpaquePolicy())

    assert policy_fingerprint(ExplainedPolicy()).startswith("p1-")


def test_positional_rules_share_one_scope_fingerprint() -> None:
    """
    Given: Two limits for one scope as a list.
    When:  Constraints are built.
    Then:  They are named #0 and #1 and both carry the scope fingerprint (I3).
    """
    scope = QuotaIdentity("discworld", idempotent_key({"vendor": "ankh"}))
    policies = [
        SlidingLogPolicy(10, ONE_SECOND),
        SlidingLogPolicy(500, DurationMicros(60 * 1_000_000)),
    ]
    fingerprint = scope_fingerprint(policies)
    expected = [(RuleId(scope, "#0"), fingerprint), (RuleId(scope, "#1"), fingerprint)]

    constraints = scope_constraints(scope, policies)
    actual = [(constraint.rule, constraint.fingerprint) for constraint in constraints]

    assert actual == expected


def test_reordering_a_positional_list_is_a_conflict_not_fresh_quota() -> None:
    """
    Given: Constraints for a two-limit list, and for the same list reordered.
    When:  The two are composed.
    Then:  PolicyConflict: rule #0 now names a different policy (I3).
    """
    scope = QuotaIdentity("discworld", "ankh")
    fast, slow = SlidingLogPolicy(10, ONE_SECOND), SlidingLogPolicy(500, DurationMicros(60_000_000))
    before = scope_constraints(scope, [fast, slow])
    after = scope_constraints(scope, [slow, fast])

    with pytest.raises(PolicyConflict):
        models.canonical_constraints([*before, *after])


def test_named_rules_survive_reordering() -> None:
    """
    Given: The same named rules supplied in two orders.
    When:  Constraints are built.
    Then:  They are identical, and each rule has its own policy fingerprint.
    """
    scope = QuotaIdentity("discworld", "ankh")
    fast, slow = SlidingLogPolicy(10, ONE_SECOND), SlidingLogPolicy(500, DurationMicros(60_000_000))
    expected = scope_constraints(scope, {"burst": fast, "daily": slow})

    actual = scope_constraints(scope, {"daily": slow, "burst": fast})

    assert actual == expected
    assert [constraint.fingerprint for constraint in actual] == [
        policy_fingerprint(fast),
        policy_fingerprint(slow),
    ]


@pytest.mark.parametrize(
    "policies", [list(), dict(), "#0"], ids=["empty list", "empty map", "string"]
)
def test_a_scope_needs_real_policies(policies: object) -> None:
    """
    Given: No policies, or a string.
    When:  Constraints are built.
    Then:  InvalidPolicy is raised.
    """
    with pytest.raises(InvalidPolicy):
        scope_constraints(QuotaIdentity("discworld", "ankh"), policies)  # ty: ignore[invalid-argument-type]


if __name__ == "__main__":
    pass
else:
    pass
