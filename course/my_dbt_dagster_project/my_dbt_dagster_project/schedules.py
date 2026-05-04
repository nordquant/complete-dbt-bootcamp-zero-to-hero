from dagster_dbt import build_schedule_from_dbt_selection

from .assets import airbnb_dbt_assets

schedules = [
    build_schedule_from_dbt_selection(
        [airbnb_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        # what do you want to run for dbt model, currently it is all models
        dbt_select="fqn:*",
    ),
]
