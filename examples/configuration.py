"""
Layered configuration, saved and read with PlatformDirs locations, and its provenance.

An organization file sets defaults, a project file sets the rates for a
profile, the environment overrides the timeout, and an argument picks the
backend. ``explain()`` then says where every setting came from; a secret, such
as a password in a backend URL, would be shown redacted.

The example writes to a temporary directory; without ``locations=`` the same
files would live in PlatformDirs' user and site configuration directories.
"""

from __future__ import annotations

__author__ = "Rohan B. Dalton"

import tempfile
from pathlib import Path

from procrastinators import ConfigLocations, Limit, RateLimiter, idempotent_key
from procrastinators.config import organization_file, project_file


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        locations = ConfigLocations(user_config=root / "user", site_config=root / "site")
        
        organization_file("unseen-university", locations=locations).set_defaults(
            algorithm="sliding_log", timeout=60, namespace="production"
        )
        
        project_file("library-etl", locations=locations).set_profile(
            "ankh.orders", limits=[Limit(10, per="1s"), Limit(500, per="1m")]
        )
        
        environment = {
            "PROCRASTINATORS_TIMEOUT": "30",
            "PROCRASTINATORS_ORGANIZATION": "unseen-university",
        }

        
        limiter = RateLimiter.from_config(
            "ankh.orders",
            key=idempotent_key({"vendor": "ankh", "dataset": "orders"}),
            project="library-etl",
            environ=environment,
            locations=locations,
            backend=f"sqlite:///{root / 'quota.sqlite3'}",
        )

        with limiter:
            print("fetched a page")
        
            if limiter.config is None:
                raise ValueError

        print("\n".join(limiter.config.explain()))


if __name__ == "__main__":
    main()
