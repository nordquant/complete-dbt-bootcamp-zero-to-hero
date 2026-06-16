# Airflow + dbt Integration — Step 1 (BashOperator)

Reproducible record of how the first Airflow integration step was added to this repo, so an AI agent (or a human) can redo it exactly, or understand what was decided and why.

## Goal
Simplest possible Airflow + dbt integration: one DAG, one `BashOperator` task, running `dbt build` against the existing `airbnb/` dbt project. Cosmos integration is explicitly deferred to a later lesson.

## Decisions made (in order, with reasoning)

1. **No Astro CLI, no Docker.** Astro CLI is Docker-Compose-based; students won't have Docker installed. This was the deciding constraint.
2. **No Snowflake Airflow provider needed.** `BashOperator` only shells out to the `dbt` CLI; `dbt` itself owns the Snowflake connection via `airbnb/profiles.yml` (which already has the full connection — account, key, passphrase — inline). Nothing Airflow-side needs to know about Snowflake for this step.
3. **WSL2 was considered and rejected as the primary path for Windows.** Apache Airflow has no native Windows support (Linux/macOS only), and WSL2 technically satisfies "Linux" — but a student's Windows-native `uv` install does not carry over into WSL2 (Windows `uv` manages Windows-style venvs; WSL2 needs its own separate `uv` install). That's an avoidable extra setup step.
4. **Primary recommended environment: the existing devcontainer/Codespace.** It's already Linux-based and already has `uv` + `dbt` preinstalled identically for every student regardless of host OS — zero extra setup. Running natively on the host (Mac terminal directly, or WSL2 for people who already use it) remains possible but is a secondary/optional path, not what's documented.
5. **`dbt` and `airflow` share the same Python venv** (the existing root `pyproject.toml`/`uv.lock`, which already has `dbt-core`/`dbt-snowflake`/Dagster deps). This avoids a second venv, mirrors the existing `dbt_dagster_project/` pattern (which also has no deps of its own — it just relies on the root venv), and means no volume mounts or container path translation are needed: `dbt` and `airflow` see the exact same filesystem.
6. **New sibling folder `airflow/`** at repo root, alongside `airbnb/`, `dbt_dagster_project/`, `my_dbt_dagster_project/`.
7. **`apache-airflow` version: left unpinned (`>=2.10`) and let `uv` resolve the newest compatible version.** This matters because the existing `dbt-autofix` dependency requires `rich>=14.0.0`, which is incompatible with `apache-airflow` 2.x's pinned `flask-appbuilder`/`rich` chain. Pinning to `apache-airflow~=2.10.0` or `<3.0` both failed to resolve. Leaving it unbounded let the resolver land on **Apache Airflow 3.2.2** (the latest release at the time), which doesn't have that conflict. Confirmed with the user that Airflow 3, latest version, is what they want.
8. **`AIRFLOW_HOME` is repo-relative** (`airflow/.airflow_home/`, gitignored) rather than the default `~/airflow`, so each student's Airflow metadata/db/logs stay self-contained inside the repo instead of polluting their home directory.
9. **Airflow's API server port was moved from 8080 to 8081** because port 8080 is commonly already taken (in this environment, by Docker Desktop). Airflow 3's scheduler/executor also talks to an internal "execution API" that must be pointed at the same custom port — just changing `AIRFLOW__API__PORT` is not enough; `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` must also be updated, or the scheduler crashes trying to reach the wrong port (this was discovered by actually running it and hitting `httpx.HTTPStatusError: Redirect response '308 Permanent Redirect'` errors in the scheduler log).
10. **Future Cosmos migration** was scoped in advance (not implemented now) to confirm this design doesn't create rework later: because `dbt` already runs in the same venv as Airflow, Cosmos's `ExecutionMode.LOCAL` is the natural next step — it shells out to local `dbt` just like `BashOperator` does, just one task per model. It would reuse the same `airbnb/profiles.yml` directly via `ProfileConfig(profiles_yml_filepath=...)`, no `ProfileMapping`/env-var translation needed, no folder restructuring.

## Files created/changed

### 1. `pyproject.toml` (root) — added one dependency line

```toml
[project]
name = "dbt-bootcamp"
version = "1.11.0"
requires-python = ">=3.10,<3.14"
dependencies = [
    "dbt-core~=1.11.0",
    "dbt-snowflake~=1.11.0",
    "dbt-autofix~=0.18.0",
    "dagster-dbt~=0.28.0",
    "dagster-webserver~=1.12.0",
    "apache-airflow>=2.10",
]
```

Then: `uv sync` (resolved to `apache-airflow==3.2.2`).

### 2. `airflow/dags/dbt_build_dag.py` (new file)

```python
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# airflow/dags/dbt_build_dag.py -> repo root is two levels up
REPO_ROOT = Path(__file__).resolve().parents[2]
AIRBNB_DIR = REPO_ROOT / "airbnb"

with DAG(
    dag_id="dbt_build_bash_operator",
    description="Simplest possible Airflow + dbt integration: one BashOperator running `dbt build`.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "bash-operator"],
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f'cd "{AIRBNB_DIR}" && dbt build',
    )
```

Note: in Airflow 3, `BashOperator` moved to the `standard` provider package (`airflow.providers.standard.operators.bash`). Importing from the old `airflow.operators.bash` path still works but emits a `DeprecatedImportWarning`.

### 3. `airflow/set-airflow-env.sh` (new file)

```bash
#!/usr/bin/env bash
# Run this from the repo root, before running Airflow:
#   . airflow/set-airflow-env.sh
# !! Do this every time you open a new terminal, as env vars are not persisted !!

export AIRFLOW_HOME="$(pwd)/airflow/.airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT=8081
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:8081/execution"

# Airflow 3 has no `airflow users create` (that was FAB auth manager only).
# The default SimpleAuthManager auto-generates a random per-user password on
# first run and stores it in this file - pre-seed it so login is always admin/admin.
mkdir -p "$AIRFLOW_HOME"
PASSWORDS_FILE="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
if [ ! -f "$PASSWORDS_FILE" ]; then
  echo '{"admin": "admin"}' > "$PASSWORDS_FILE"
fi
```

Mirrors the existing `set-env.sh` pattern (source-before-use, not persisted across terminals). Deliberately uses `$(pwd)` instead of self-locating via `${BASH_SOURCE[0]}`/`dirname`, because that pattern resolved incorrectly when sourced under `zsh` (zsh's `BASH_SOURCE` compatibility shim behaved differently than bash's during testing) — so the script requires being sourced from the repo root, documented in the comment.

### 4. `airflow/README.md` (new file)

```markdown
# Airflow Integration — Step 1 (BashOperator)

The simplest possible Airflow + dbt integration: one DAG, one `BashOperator` task, running `dbt build` against the `airbnb/` project. No Docker, no Astro CLI, no Cosmos yet — those come later.

## Why this approach
- Apache Airflow has no native Windows support, but it runs fine on Linux/macOS — and the existing devcontainer/Codespace is already Linux-based, so it's the easiest way to run this with zero extra setup, on any OS.
- `dbt` and `airflow` share the same Python venv (managed by the root `pyproject.toml`/`uv.lock`), so `BashOperator` can just shell out to `dbt build` directly — no volume mounts, no Snowflake provider needed. The connection details already live in `airbnb/profiles.yml`.

## Usage
From the repo root:
\`\`\`bash
uv sync                              # installs apache-airflow alongside dbt
. airflow/set-airflow-env.sh         # sets AIRFLOW_HOME + ports for this session
uv run airflow standalone            # first run also runs `airflow db migrate`
\`\`\`
Open the URL printed in the terminal (default `http://localhost:8081` per `set-airflow-env.sh`), log in with the admin password printed in the standalone output, and trigger the `dbt_build_bash_operator` DAG.

**Note:** if port 8080 is already taken on your machine (e.g. by Docker Desktop), `set-airflow-env.sh` moves Airflow's API server to 8081 instead.

## Next step: Cosmos
Because `dbt` already runs in the same venv as Airflow, upgrading to [astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/) later is purely additive — add the package, add a second DAG using `DbtDag` with `ExecutionMode.LOCAL` and the existing `airbnb/profiles.yml`. No folder restructuring required.
```

### 5. `.gitignore` (root) — appended

```
# Airflow (instructional integration)
airflow/.airflow_home/
```

## Reproduction steps (exact commands run, in order)

```bash
cd dev-repo

# 1. Add the dependency (edit pyproject.toml as shown above), then:
uv sync

# 2. Create the DAG folder + file (content above)
mkdir -p airflow/dags
# write airflow/dags/dbt_build_dag.py

# 3. Create the env-setup script (content above), make executable
chmod +x airflow/set-airflow-env.sh

# 4. gitignore the local Airflow home
echo -e "\n# Airflow (instructional integration)\nairflow/.airflow_home/" >> .gitignore

# 5. First-time DB setup
. airflow/set-airflow-env.sh
uv run airflow db migrate

# 6. Run it
uv run airflow standalone
# -> note the admin password printed to the terminal, e.g.:
#    "Password for user 'admin': <random>"
# -> open http://localhost:8081

# 7. In another terminal (with the same env sourced), trigger + check the DAG
. airflow/set-airflow-env.sh
uv run airflow dags unpause dbt_build_bash_operator
uv run airflow dags trigger dbt_build_bash_operator
uv run airflow dags state dbt_build_bash_operator "<run_id from trigger output>"
```

## Verification performed
Ran the full flow above for real (not just import-checked):
- `uv run airflow dags list-import-errors` → no errors
- `uv run airflow dags list` → `dbt_build_bash_operator` registered
- Triggered the DAG, polled `airflow dags state` until `success`
- Confirmed the task log contained genuine `dbt build` output against Snowflake (model runs, tests, not a connection error), ending in:
  `Finished running 1 exposure, 2 incremental models, 1 project hook, 1 seed, 2 snapshots, 4 table models, 22 data tests, 1 unit test, 1 view model ... Done. PASS=33 WARN=1 ERROR=0 SKIP=0 NO-OP=1 TOTAL=35`
- Killed all Airflow processes (`scheduler`, `triggerer`, `dag-processor`, `api-server`) afterward to leave a clean state.

## Known gotchas hit during setup (so they aren't rediscovered)
- `apache-airflow~=2.10.0` (and any 2.x pin) fails `uv sync` due to a `rich` version conflict with `dbt-autofix`. Leave the version unpinned (`>=2.10`) and let the resolver pick — it landed on 3.2.2.
- Port 8080 may already be bound by something else on the host (Docker Desktop did this in testing) — `airflow standalone`'s API server then silently fails to bind while everything else starts, and the scheduler crashes with a confusing `httpx.HTTPStatusError` / `TypeError: HTTPStatusError.__init__() missing 2 required keyword-only arguments` instead of a clear "port in use" message. Check `lsof -i :8080` if this happens.
- Moving the API server port requires setting **both** `AIRFLOW__API__PORT` and `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` in Airflow 3 — the scheduler's internal execution-API client doesn't automatically follow the API port setting.
- A self-locating `set-airflow-env.sh` using `${BASH_SOURCE[0]}`/`dirname` resolved to the wrong directory when sourced under `zsh` — use `$(pwd)`-relative paths and document "source from repo root" instead.
- `airflow users create` (the classic way to set a fixed admin password) **doesn't exist in Airflow 3** — that command belonged to the FAB auth manager, which is no longer the default. Airflow 3's default `SimpleAuthManager` auto-generates a random password per user on first run and stores it in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` (only for usernames not already present in that file — it never overwrites existing entries). Fix: pre-seed that file with `{"admin": "admin"}` before the first `airflow standalone` run; `set-airflow-env.sh` now does this automatically. Verified by curling `POST /auth/token` with `admin`/`admin` and getting back a valid JWT.

# Step 2
- open the airflow pane (localhost)
- go to dag
- show the dag
- start the dag
- look at the output
