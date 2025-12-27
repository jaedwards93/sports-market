from dagster import asset
from ingest.the_odds_api.endpoints.sports import sports_ingest
from ingest.the_odds_api.endpoints.odds import odds_ingest
from util.batches import get_new_batch


@asset
def batch_id() -> int:
    return get_new_batch()


@asset(
    deps=[batch_id],
)
def raw_sports(batch_id: int):
    sports_ingest(batch_id)


@asset(
    deps=[batch_id],
)
def raw_odds(batch_id: int):
    sports_list = ['americanfootball_nfl', 'basketball_nba']

    for sport in sports_list:
        odds_ingest(batch_id, sport)
