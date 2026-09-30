# Configuration

Limiters can take their settings from layered configuration instead of
constructor arguments. `procrastinators.config` finds, reads, resolves,
explains, and saves it; `RateLimiter.from_config` builds a limiter from the
result. Rule references like **G1** point at [the contracts](contracts.md).

```python
from procrastinators import RateLimiter, idempotent_key

limiter = RateLimiter.from_config(
    "ankh.orders",
    key=idempotent_key({"vendor": "ankh", "dataset": "orders"}),
    project="etl",
)

with limiter:
    fetch_page()

print("\n".join(limiter.config.explain()))
```

## Precedence

Strongest first (**G1**):

| Layer | Where it comes from |
| --- | --- |
| Arguments | `from_config(..., timeout=30)` or `resolve(..., arguments={...})`. |
| Environment | `PROCRASTINATORS_*` variables. |
| Project | The selected project's file. |
| Organization | The selected organization's file. |
| Library defaults | Behavior only: `sliding_log`, the default SQLite backend, namespace `default`, no timeout, a 5-second storage timeout, no options. Never a rate. |

Values merge field by field: the strongest layer that sets a field supplies it.
Within one file, a profile's fields override that file's `[defaults]`; across
files, layer precedence comes first, so a project's `[defaults]` beats an
organization's profile.

Two fields are replaced wholesale. The **rates** — `limits` (positional) or
`rules` (named) — are one value: the strongest layer that sets either supplies
all of them, and a single layer cannot set both (**G4**). Merging lists would
produce a policy nobody wrote and, because positional rule identity follows list
order, silently repoint stored rules. **`options`** belong to one algorithm and
are replaced as a whole; after resolution every rate is re-validated against the
resolved algorithm and options, and an error names the layers involved.

There is no default vendor rate. A profile no layer gives rates to is a
`ConfigurationError` naming it.

## Fields

| Field | Value |
| --- | --- |
| `backend` | An address such as `"sqlite://"`, `"sqlite:///./quota.sqlite3"`, or `"memory://ankh"`. |
| `algorithm` | `sliding_log`, `fixed_window`, `sliding_counter`, `token_bucket`, or `leaky_bucket`. |
| `namespace` | The quota domain (**I1**). |
| `timeout` | Quota-wait budget: seconds or a duration string such as `"250ms"`; `"none"` waits indefinitely (**B2**). |
| `storage_timeout` | Per-storage-call cap, positive; never null (**B3**). |
| `limits` | A list of `{ amount = 10, per = "1s" }` tables. |
| `rules` | A table of rule name to `{ amount, per }`. Recommended for managed configuration (**I3**). |
| `options` | Algorithm options: `epoch_offset`, `capacity`, `initial_tokens`, `burst_tolerance`. |

Unknown fields, tables, limit keys, and versions are rejected, not ignored
(**G5**).

### Unset is not null

TOML has no null, so the string `"none"` is the explicit null for `timeout`, in
files and in the environment alike (**G2**). A layer that never mentions
`timeout` falls through to the next; a layer that says `timeout = "none"` has
chosen to wait indefinitely, and overrides a weaker layer's number.

## Files

Project and organization files share one schema:

```toml
version = 1

[defaults]
algorithm = "sliding_log"
timeout = 60

[profiles."ankh.orders"]
limits = [{ amount = 10, per = "1s" }, { amount = 500, per = "1m" }]

[profiles."quirm.cheese"]
algorithm = "token_bucket"
rules = { burst = { amount = 20, per = "1s" } }
options = { capacity = 40 }
```

### Selection and locations

Selection is explicit (**G3**): a project or organization is named by argument
or environment variable, never discovered from the working directory, so a
worker's behavior does not depend on where it started. Locations come from
[PlatformDirs](https://platformdirs.readthedocs.io/en/latest/api.html) through
`ConfigLocations.default()`, which creates nothing:

```text
user_config_path("procrastinators")/
    projects/<project>.toml
    organizations/<organization>.toml

site_config_path("procrastinators")/
    organizations/<organization>.toml
```

- **Project:** an explicit `project_file` (or `PROCRASTINATORS_PROJECT_FILE`),
  else `projects/<project>.toml` in the user directory. An explicit file named
  `pyproject.toml` is read from its `[tool.procrastinators]` table.
- **Organization:** an explicit `organization_file` (or
  `PROCRASTINATORS_ORGANIZATION_FILE`), else the site file if it exists, else the
  user file.

A selected project or organization without a file contributes nothing. An
explicitly named file that does not exist is an error. Ids are plain file
names: letters, digits, `.`, `_`, and `-`, starting with a letter or digit.

PlatformDirs supplies locations, not distribution: organization files are
deployed or synchronized by whatever manages the machines, and writing the site
file needs the caller's own filesystem permissions. Nothing is fetched or
escalated.

### Saving

`ConfigFile` reads and writes one file; `project_file()` and
`organization_file()` build one for an id:

```python
from procrastinators import Limit
from procrastinators.config import project_file

etl = project_file("etl")
etl.set_defaults(timeout=60)
etl.set_profile("ankh.orders", limits=[Limit(10, per="1s"), Limit(500, per="1m")])
```

Saves are atomic (**G10**). Under an exclusive lock on a sidecar `.lock` file,
the file's current revision — a SHA-256 of its bytes — must match the revision
the caller read; the document is written to a temporary file beside the
destination, flushed to disk, parsed back and compared, and only then renamed
over it. A concurrent change is a `ConfigurationError` rather than a lost
update. `set_defaults`, `set_profile`, and `remove_profile` load, change, and
save against the loaded revision; `ConfigFile.save(layer, expected_revision=…)`
is the underlying operation. Setting `limits` removes `rules` and vice versa,
and a field set to `UNSET` is removed.

The writer is deliberately narrow — exactly this schema, with no TOML library
dependency — and every save is read back with `tomllib` before it replaces
anything. A `pyproject.toml` is never rewritten.

### Credentials

Distributable configuration stores credential references, not passwords
(**G9**). A file whose backend URL carries a password in its userinfo is refused
on load and on save; supply such an address through `PROCRASTINATORS_BACKEND`
or an argument. A user name alone is fine.

## Environment

| Variable | Meaning |
| --- | --- |
| `PROCRASTINATORS_BACKEND` | `backend`. |
| `PROCRASTINATORS_ALGORITHM` | `algorithm`. |
| `PROCRASTINATORS_NAMESPACE` | `namespace`. |
| `PROCRASTINATORS_TIMEOUT` | `timeout`, such as `30`, `"250ms"`, or `none`. |
| `PROCRASTINATORS_STORAGE_TIMEOUT` | `storage_timeout`. |
| `PROCRASTINATORS_LIMITS` | `limits`, as `10/1s, 500/1m`. |
| `PROCRASTINATORS_RULES` | `rules`, as `burst=10/1s, sustained=500/1m`. |
| `PROCRASTINATORS_OPTIONS` | `options`, as `capacity=20, initial_tokens=0`; all-digit values are integers. |
| `PROCRASTINATORS_PROJECT` | Selects the project. |
| `PROCRASTINATORS_ORGANIZATION` | Selects the organization. |
| `PROCRASTINATORS_PROJECT_FILE` | An explicit project file. |
| `PROCRASTINATORS_ORGANIZATION_FILE` | An explicit organization file. |

Environment values are defaults for every profile. An empty value or an
unknown `PROCRASTINATORS_*` variable is a `ConfigurationError`, so a typo fails
loudly. `PROCRASTINATORS_REQUIRE_SERVICES` is reserved for the test tooling
([development](development.md)) and ignored by configuration. The environment is
read when configuration is resolved, never at import; every function takes an
`environ` mapping for tests.

## Resolving and explaining

```python
from procrastinators.config import explain, resolve

resolved = resolve("ankh.orders", project="etl", arguments={"timeout": 30})
resolved.settings.limits        # the immutable LimiterSettings
resolved.source_of("timeout")   # ConfigSource.ARGUMENTS
print("\n".join(explain("ankh.orders", project="etl")))
```

```text
profile = 'ankh.orders' from arguments (arguments)
backend = 'redis://***@cache:6379/0' from environment (PROCRASTINATORS_BACKEND)
limits = (...) from project (/home/ops/.config/procrastinators/projects/etl.toml)
timeout = 60.0 from project (/home/ops/.config/procrastinators/projects/etl.toml)
algorithm = 'sliding_log' from library_default
...
```

Resolution happens once and its result is immutable (**G6**): a rate that
changes underneath a running worker is a policy migration, not a value refresh.
`explain` reports every field's layer and origin, strongest first, with secrets
redacted both by field name and by stripping userinfo from backend URLs
(**G7**).

Configuration never creates a new quota identity (**G8**). The quota key is
always passed explicitly, so a worker started after a profile's rates changed
meets the stored policy as a `PolicyConflict`, and the change goes through an
explicit migration (**L12**).
