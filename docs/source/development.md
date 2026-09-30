# Development

## Environment

Checks run in an isolated environment managed by [Pixi](https://pixi.sh/);
`pixi run` creates and syncs `.pixi/` from `pyproject.toml` on demand, so no
command below expects an activated shell or a globally installed tool.

```bash
pixi install -e dev    # the project (editable) plus the `lint` and `test` features
```

The minimum supported interpreter is Python 3.11. The full supported matrix is
validated in CI rather than locally.

## Local checks

| Command | Checks |
| --- | --- |
| `pixi run -e dev ruff format .` | Apply formatting. |
| `pixi run -e dev ruff format --check .` | Verify formatting without editing. |
| `pixi run -e dev ruff check .` | Lint. Add `--fix` to apply safe fixes. |
| `pixi run -e dev ty check` | Static typing over `src/` and `tests/`; warnings fail the check. Paths come from `pyproject.toml`. |
| `pixi run -e dev pytest` | The unit suite. The `typing/` tests run `ty`, so they need the `dev` environment. |
| `pixi run -e dev sphinx-build -W -n -b html docs/source docs/build/html` | The HTML documentation. The API reference is generated from docstrings by autodoc and autosummary; any warning, including an unresolved cross-reference, fails the build. |

Run all four before proposing a change:

```bash
pixi run -e dev bash -c 'ruff format --check . && ruff check . && ty check && pytest'
```

Tests import `procrastinators` as an installed package, never by relative path,
so the layout the suite exercises is the layout users get.

## Unit tests versus service integration tests

The default `pytest` run is the **unit suite**: deterministic, offline, and with
no external process. It must pass on a laptop with nothing installed but the
`dev` environment. Fake clocks and controlled barriers keep it that way —
timing assertions must not depend on real sleeps or scheduling luck.

**Service integration tests** exercise a real Redis, Valkey, PostgreSQL, or
Memcached server, and carry the `service` marker:

```python
@pytest.mark.service
def test_susan_sto_helit_holds_the_line_across_processes() -> None: ...
```

They are excluded from the default run by the `-m "not service"` entry in
`addopts`. Select them explicitly (a command-line `-m` overrides the default):

```bash
pixi run -e dev pytest -m service                  # only the service tests
pixi run -e dev pytest -m "service or not service" # everything
```

A service test names its service — `@pytest.mark.service("redis")` — and calls
the `service_available` fixture, which skips when the server is unreachable so
a local run stays useful. That allowance is local only: each backend's
**required CI job must not pass by skipping all of its tests**. A job passes
`--require-service NAME` (or sets `PROCRASTINATORS_REQUIRE_SERVICES`), which
turns an unreachable server into a failure and fails the session if that
service had no passing test. The jobs are listed in
[the conformance guide](conformance.md#required-service-jobs).

Tests are grouped by concern under `tests/`: `contracts/`, `foundations/`,
`harness/`, `algorithms/`, `backends/`, `concurrency/`, `failures/`, `config/`,
`integrations/`, and `typing/`. Tests accompany the implementation they cover
rather than being deferred to a final phase. Property-based tests use
`hypothesis`.

`tests/oracles.py` holds deliberately naive models of the five algorithms, used
to cross-check the shared traces and the reference algorithms, and
`tests/doubles.py` the deliberately broken backends the conformance suite must
catch. `tests/limiters.py` provides the `rig` fixture: a limiter on fake time
whose sleepers advance a fake timeline and refuse to sleep while the memory
store's lock is held. Neither may be imported from
`src/`. The conformance tools themselves live in `procrastinators.testing`; see
[the conformance guide](conformance.md).

`tests/foundations/identity_golden.json` freezes the key and fingerprint
encodings. Never regenerate it to make a test pass: a changed golden value
means every stored quota key or fingerprint in the world has moved, which is a
new encoding version, not a fix.

Discworld names are encouraged for tests and fixtures — `test_lu_tze_does_not_refund_a_failed_request`
reads better than `test_case_7`.

## Building

```bash
pixi run -e build hatch build    # sdist and wheel into dist/
```

Runtime dependencies stay minimal: PlatformDirs for configuration support, plus
optional drivers. Backend and integration extras are declared in the phase that
implements them; SQLite comes from the standard library and never needs one.

## Local release check

The local ETL release gate builds the package, installs it into a clean
environment, and runs the examples against the installed copy:

```bash
pixi run -e build hatch build /tmp/procrastinators-dist
python -m venv /tmp/procrastinators-venv
/tmp/procrastinators-venv/bin/pip install /tmp/procrastinators-dist/*.whl
for example in examples/*.py; do /tmp/procrastinators-venv/bin/python -I "$example"; done
```

## Service backends locally

`just services` starts the services in [`docker-compose.yaml`](../../docker-compose.yaml):
Redis, Valkey, a three-node Redis Cluster, PostgreSQL, and Memcached. It uses
Podman Compose or Docker Compose and waits for the cluster to become ready.
`just services-down` removes the containers and their test data volumes.
The cluster uses host networking so its nodes and the host-side tests can all
reach the same loopback addresses; this setup requires a Linux container host.
The other services bind only to the host's loopback interface, on the ports
the service tests use by default.
Each test isolates itself — a key prefix, a schema, an item prefix — and
cleans up, so the servers can be shared.

| Service | Default address | Override |
| --- | --- | --- |
| Redis | `redis://localhost:16379/0` | `PROCRASTINATORS_REDIS_URL` |
| Valkey | `valkey://localhost:16380/0` | `PROCRASTINATORS_VALKEY_URL` |
| Redis Cluster | `redis://127.0.0.1:17000/0`; `just services` prints it | `PROCRASTINATORS_REDIS_CLUSTER_URL` |
| PostgreSQL | `postgresql://procrastinators:procrastinators@localhost:15432/procrastinators` | `PROCRASTINATORS_POSTGRES_URL` |
| Memcached | `memcached://127.0.0.1:11311` | `PROCRASTINATORS_MEMCACHED_URL` |

```bash
just services
PROCRASTINATORS_REDIS_CLUSTER_URL=redis://127.0.0.1:17000/0 \
    just test -m "service or not service" --require-service redis --require-service valkey \
    --require-service redis-cluster --require-service postgres --require-service memcached
```

`benchmarks/contention.py` measures admission throughput and latency against
any address; [Backends](backends.md#measurements) records what it showed.

SQLite tests run on the unit suite: they need only a local filesystem.
`tests/concurrency/test_sqlite_processes.py` spawns real processes (and forks,
where the platform can), so it takes a few seconds.
