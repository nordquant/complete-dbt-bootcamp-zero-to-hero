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
