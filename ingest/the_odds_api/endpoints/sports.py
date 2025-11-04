from util.request import ApiCall
from util.postgres import PostgresConnection
import json


def sports(batch_id: int):
    sports_api_call = ApiCall(key_name='the_odds_api_key', url='https://api.the-odds-api.com/v4/sports')
    results = sports_api_call.run()

    pg_conn = PostgresConnection(host_name='PG_HOST', port='PG_PORT', user_name='SPORTS_MARKET_RAW_USER_NAME',
                                 user_password='SPORTS_MARKET_RAW_USER_PASSWORD', db_name='PG_DB_NAME', auto_commit=True)

    pg_cursor = pg_conn.cursor

    insert_sql = """
            INSERT INTO raw_the_odds_api.sports (json, batch_id)
            VALUES (%s, %s)
        """

    for row in results:
        pg_cursor.execute(insert_sql, (json.dumps(row), batch_id))

    pg_conn.close_cursor()
    pg_conn.close_connection()