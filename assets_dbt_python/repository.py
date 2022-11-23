import os

from assets_dbt_python.assets import raw_data
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from dagster_duckdb import build_duckdb_io_manager
from dagster_duckdb_pandas import DuckDBPandasTypeHandler

from dagster import (
    load_assets_from_package_module,
    repository,
    with_resources,
    DailyPartitionsDefinition,
    define_asset_job,
    AssetSelection, 
    build_schedule_from_partitioned_job
)
from dagster._utils import file_relative_path

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_project")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_project/config")


raw_data_assets = load_assets_from_package_module(
    raw_data,
    group_name="raw_data",
    key_prefix=["raw_data"],
)

partition_def = DailyPartitionsDefinition(start_date = "2022-10-01")

def map_dagster_partition_to_dbt_run_var(partition_str): 
    # for the dagster partition "2022-10-01"
    # this runs a dbt command 
    # dbt run --vars '{"pt": "2022-10-01"}' ... other dbt flags
    return {"pt": partition_str}

dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    DBT_PROFILES_DIR,
    partitions_def=partition_def,
    partition_key_to_vars_fn = map_dagster_partition_to_dbt_run_var
)

update_dbt_job = define_asset_job(
    name = "update_dbt_job",
    # select by asset groups, which for dbt are mapped to model folders / schemas by default
    selection = AssetSelection.groups("cleaned", "analytics", "marketing"),
    partitions_def=partition_def
)

update_dbt_daily = build_schedule_from_partitioned_job(
    update_dbt_job
)


@repository
def assets_dbt_python():
    duckdb_io_manager = build_duckdb_io_manager(type_handlers=[DuckDBPandasTypeHandler()])
    return with_resources(
        dbt_assets + raw_data_assets,
        resource_defs={
            # this io_manager allows us to load dbt models as pandas dataframes
            "io_manager": duckdb_io_manager.configured(
                {"database": os.path.join(DBT_PROJECT_DIR, "example.duckdb")}
            ),
            # this resource is used to execute dbt cli commands
            "dbt": dbt_cli_resource.configured(
                {"project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR}
            ),
        }
    ) + [update_dbt_job] + [update_dbt_daily]
    
