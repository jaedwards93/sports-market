from dagster import Definitions
from .the_odds_api.assets import batch_id, raw_sports, raw_odds
from .dbt.assets import my_dbt_assets, dbt_resource
from dagster_dbt import DbtCliResource


defs = Definitions(
    assets=[batch_id, raw_sports, raw_odds, my_dbt_assets],
    resources={
        "dbt": dbt_resource,
    },
)