from datetime import datetime
from airflow import DAG
from pathlib import Path
from airflow.providers.standard.operators.bash import BashOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
AIRBNB_DIR = REPO_ROOT / "airbnb"

with DAG(
    dag_id="dbt_build_bash_operator",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f'cd "{AIRBNB_DIR}" && dbt build',
    )