from util.request import ApiCall
import os
import psycopg2
from psycopg2 import sql
from util.postgres import PostgresConnection


def sports_call_test():
    sports_call = ApiCall(key_name='the_odds_api_key', url='https://api.the-odds-api.com/v4/sports')
    results = sports_call.run()

    for result in results:
        print(result)


def postgres_connection_test():
    # Load connection parameters (you can also use python-dotenv)
    DB_HOST = os.getenv("PG_HOST")
    DB_PORT = os.getenv("PG_PORT")
    DEFAULT_DB_NAME = 'postgres'
    SPORTS_MARKET_DB_NAME = os.getenv("PG_DB_NAME")
    DB_USER = os.getenv("PG_DB_ADMIN_USER")
    DB_PASSWORD = os.getenv("PB_DB_ADMIN_USER_PASSWORD")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        dbname=DEFAULT_DB_NAME,
        password=DB_PASSWORD,
    )

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")

    results = cursor.fetchall()

    for result in results:
        print(result[0])


def postgres_connection_class_test():
    pg_conn = PostgresConnection(
        host_name='PG_HOST',
        port='PG_PORT',
        user_name='PG_DB_ADMIN_USER',
        user_password='PB_DB_ADMIN_USER_PASSWORD',
        db_name=None
    )

    pg_cursor = pg_conn.cursor
    pg_cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    results = pg_cursor.fetchall()

    for result in results:
        print(result[0])


sports_call_test()
