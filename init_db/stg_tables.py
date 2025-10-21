from init_db.ddl.tables.stg_the_odds_api import table_list as the_odds_api_list
from init_db.ddl.tables.stg_reference import table_list as reference_list

table_list = {
    'stg_the_odds_api': the_odds_api_list,
    'stg_reference': reference_list
}