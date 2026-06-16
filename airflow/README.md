# Airflow Integration — Step 1 (BashOperator)

The simplest possible Airflow + dbt integration: one DAG, one `BashOperator` task, running `dbt build` against the `airbnb/` project. No Docker, no Astro CLI, no Cosmos yet — those come later.

## Why this approach
- Apache Airflow has no native Windows support, but it runs fine on Linux/macOS — and the existing devcontainer/Codespace is already Linux-based, so it's the easiest way to run this with zero extra setup, on any OS.
- `dbt` and `airflow` share the same Python venv (managed by the root `pyproject.toml`/`uv.lock`), so `BashOperator` can just shell out to `dbt build` directly — no volume mounts, no Snowflake provider needed. The connection details already live in `airbnb/profiles.yml`.

## Usage
From the repo root:
```bash
uv sync                              # installs apache-airflow alongside dbt
. airflow/set-airflow-env.sh         # sets AIRFLOW_HOME + ports for this session
uv run airflow standalone            # first run also runs `airflow db migrate`
```
Open the URL printed in the terminal (default `http://localhost:8081` per `set-airflow-env.sh`), log in with **admin / admin**, and trigger the `dbt_build_bash_operator` DAG.

**Login:** Airflow 3 dropped `airflow users create` (that was a FAB auth manager command; the default now is `SimpleAuthManager`, which normally auto-generates a random per-user password on first run). `set-airflow-env.sh` pre-seeds `admin`/`admin` into the generated-passwords file so login is always predictable for the course.

**Note:** if port 8080 is already taken on your machine (e.g. by Docker Desktop), `set-airflow-env.sh` moves Airflow's API server to 8081 instead.

## Next step: Cosmos
Because `dbt` already runs in the same venv as Airflow, upgrading to [astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/) later is purely additive — add the package, add a second DAG using `DbtDag` with `ExecutionMode.LOCAL` and the existing `airbnb/profiles.yml`. No folder restructuring required.
