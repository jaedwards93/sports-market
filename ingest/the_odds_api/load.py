from ingest.the_odds_api.endpoints.sports import sports
from ingest.base import get_new_batch


def load():
    batch_id = get_new_batch()
    sports(batch_id)


load()