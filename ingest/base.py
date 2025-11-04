from util.postgres import PostgresConnection


def get_new_batch():
    pg_conn = PostgresConnection(host_name='PG_HOST', port='PG_PORT', user_name='SPORTS_MARKET_RAW_USER_NAME',
                                 user_password='SPORTS_MARKET_RAW_USER_PASSWORD', db_name='PG_DB_NAME',
                                 auto_commit=True)

    pg_curs = pg_conn.cursor

    pg_curs.execute("""
        WITH new_batch_id_cte AS (
            SELECT nextval('sports_market.etl_reference.seq_batches') batch_id
        ), insert_new_batch_id_cte AS (
            INSERT INTO sports_market.etl_reference.batches (batch_id)
            SELECT batch_id
            FROM new_batch_id_cte
        )
        SELECT batch_id
        FROM new_batch_id_cte
        """)

    return pg_curs.fetchone()
