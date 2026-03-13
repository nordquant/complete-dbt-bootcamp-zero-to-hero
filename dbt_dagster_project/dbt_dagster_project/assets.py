import json
from typing import Any, Mapping

from dagster import (AssetExecutionContext, DailyPartitionsDefinition,
                     OpExecutionContext)
from dagster_dbt import (DagsterDbtTranslator, DbtCliResource, dbt_assets,
                         default_metadata_from_dbt_resource_props)

from .constants import dbt_manifest_path


@dbt_assets(manifest=dbt_manifest_path, exclude="fct_reviews")
def dbtlearn_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# many dbt assets use an incremental approach to avoid
# re-processing all data on each run
# this approach can be modelled in dagster using partitions
daily_partitions = DailyPartitionsDefinition(start_date="2022-01-24")

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_metadata(self, dbt_resource_props: Mapping[str, Any]) -> Mapping[str, Any]:
        metadata = {"partition_expr": "date"}
        default_metadata = default_metadata_from_dbt_resource_props(dbt_resource_props)
        return {**default_metadata, **metadata}


@dbt_assets(manifest=dbt_manifest_path, 
            select="fct_reviews",
            partitions_def=daily_partitions,
            dagster_dbt_translator=CustomDagsterDbtTranslator())
def dbtlearn_partitioned_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    (
        first_partition,
        last_partition,
    ) = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )
    dbt_vars = {"start_date": str(first_partition), "end_date": str(last_partition)}
    dbt_args = ["build", "--vars", json.dumps(dbt_vars)]
    dbt_cli_task = dbt.cli(dbt_args, context=context, raise_on_error=False)
    
    yield from dbt_cli_task.stream()
