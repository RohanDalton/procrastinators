run := "pixi run -e dev"

[doc("Build the HTML documentation, failing on any warning")]
docs *args="":
    {{ run }} sphinx-build -W -n --keep-going -b html docs/source docs/build/html {{ args }}

[doc("Automatically format files")]
format *args=".":
    {{ run }} ruff check --fix {{ args }}
    {{ run }} ruff format {{ args }}

[doc("Check linting and types without modifying files")]
lint *args=".":
    {{ run }} ruff check {{ args }}
    just --justfile {{justfile()}} typecheck {{ args }}

[doc("Run the test suite")]
test *args="tests":
    {{ run }} pytest {{ args }}

[doc("Typecheck the code")]
typecheck *args=".":
    {{ run }} ty check {{ args }}

[doc("Start Redis, Valkey, a Redis Cluster, PostgreSQL, and Memcached for the service tests")]
services:
    ./.scripts/compose up -d
    ./.scripts/compose run --rm cluster-init
    @echo "PROCRASTINATORS_REDIS_CLUSTER_URL=redis://127.0.0.1:17000/0"

[doc("Stop the service containers and remove their test data")]
services-down:
    ./.scripts/compose down --volumes --remove-orphans
