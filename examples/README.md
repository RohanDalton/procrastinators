# Examples

Each script runs on its own, writes only to a temporary directory, and exits
non-zero if the behavior it demonstrates does not hold.

| Script | Shows |
| --- | --- |
| `shared_quota.py` | Decorated functions, a context manager, and a separately built limiter spending one quota. |
| `coroutines.py` | Concurrent coroutines sharing a quota while the event loop stays responsive. |
| `processes.py` | Spawned processes sharing one quota through a SQLite file. |
| `distributed.py` | Spawned workers sharing one quota through Redis, Valkey, or PostgreSQL (`PROCRASTINATORS_EXAMPLE_BACKEND`); says so and exits cleanly when no server is reachable. |
| `weighted.py` | Weighted acquisitions (`cost=`) for batch calls. |
| `configuration.py` | Organization and project files, environment defaults, `from_config`, and `explain()` provenance. |

```bash
for example in examples/*.py; do python "$example"; done
```
