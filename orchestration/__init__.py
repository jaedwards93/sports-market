from dagster import Definitions
from .the_odds_api.assets import batch_id, raw_sports, raw_odds, raw_scores, raw_events
from .dbt.assets import my_dbt_assets, dbt_resource


defs = Definitions(
    assets=[batch_id, raw_sports, raw_odds, raw_scores, raw_events, my_dbt_assets],
    resources={
        "dbt": dbt_resource,
    },
)