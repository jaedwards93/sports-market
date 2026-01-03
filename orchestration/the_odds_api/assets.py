from dagster import asset
from ingest.the_odds_api.endpoints.sports import sports_ingest
from ingest.the_odds_api.endpoints.odds import odds_ingest
from ingest.the_odds_api.endpoints.scores import scores_ingest
from ingest.the_odds_api.endpoints.events import events_ingest
from util.batches import get_new_batch
from .sport_key_ingest_config import sports_key_list


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
    for sport in sports_key_list:
        odds_ingest(batch_id, sport)


@asset(
    deps=[batch_id],
)
def raw_scores(batch_id: int):
    for sport in sports_key_list:
        scores_ingest(batch_id, sport)


@asset(
    deps=[batch_id],
)
def raw_events(batch_id: int):
    for sport in sports_key_list:
        events_ingest(batch_id, sport)