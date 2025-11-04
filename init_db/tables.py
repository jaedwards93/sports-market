from init_db.ddl.tables.raw_the_odds_api import table_list as the_odds_api_list
from init_db.ddl.tables.etl_reference import table_list as reference_list

table_list = {
    'raw_the_odds_api': the_odds_api_list,
    'etl_reference': reference_list
}