"""The one exception the conformance tools raise."""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

__all__ = ["ContractViolation"]


class ContractViolation(AssertionError):
    """An implementation broke a rule of ``docs/source/contracts.md``.

    An :class:`AssertionError`, so a test runner reports it as a failed
    expectation rather than an error in the test itself. The message names what
    was expected and what happened.
    """


if __name__ == "__main__":
    pass
else:
    pass
