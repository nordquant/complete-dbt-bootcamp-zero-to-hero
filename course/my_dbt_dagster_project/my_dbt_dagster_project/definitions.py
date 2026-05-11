from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import airbnb_dbt_assets, dbtlearn_partitioned_dbt_assets
from .project import airbnb_project
from .schedules import schedules

defs = Definitions(
    assets=[airbnb_dbt_assets, dbtlearn_partitioned_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=airbnb_project),
    },
)
