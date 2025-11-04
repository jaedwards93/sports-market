import dotenv as e
import os

e.load_dotenv()

schema_roles_list = {
    'raw_the_odds_api': {os.getenv('SPORTS_MARKET_RAW_USER_NAME'): os.getenv('SPORTS_MARKET_RAW_USER_PASSWORD')},
    'etl_reference': {os.getenv('SPORTS_MARKET_RAW_USER_NAME'): os.getenv('SPORTS_MARKET_RAW_USER_PASSWORD')},
    'stg_sports_market': {os.getenv('SPORTS_MARKET_STG_USER_NAME'): os.getenv('SPORTS_MARKET_STG_USER_PASSWORD')},
    'nds_sports_market': {os.getenv('SPORTS_MARKET_NDS_USER_NAME'): os.getenv('SPORTS_MARKET_NDS_USER_PASSWORD')},
}
