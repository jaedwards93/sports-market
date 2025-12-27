from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

dbt_resource = DbtCliResource(
    project_dir=r"C:\Users\jorda\repos\sports-market\dbt\sports_market",
    profiles_dir=r"C:\Users\jorda\.dbt",
    dbt_executable=r"C:\Users\jorda\repos\sports-market\.venv\Scripts\dbt.exe",
)


@dbt_assets(
    manifest=Path("dbt/sports_market/target/manifest.json"),
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
