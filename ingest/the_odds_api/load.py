from ingest.the_odds_api.endpoints.sports import *
from ingest.the_odds_api.endpoints.odds import *
from util.batches import get_new_batch


def load():
    batch_id = get_new_batch()
    sports_ingest(batch_id)
    odds_ingest(batch_id, 'americanfootball_nfl')


load()