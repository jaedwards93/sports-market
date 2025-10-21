from util.postgres import PostgresConnection
from util.custom_errors import ConfigError
from init_db.stg_tables import table_list
import os
import dotenv as e

e.load_dotenv()


def create_database(connection: PostgresConnection, database_name: str):
    cursor = connection.cursor
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
    result = cursor.fetchone()

    if not result:
        cursor.execute(f"""
            CREATE DATABASE {database_name} WITH
            OWNER = {connection.user_name}
            ENCODING = 'UTF8'
            LC_COLLATE = 'English_United States.1252'
            LC_CTYPE = 'English_United States.1252'
            LOCALE_PROVIDER = 'libc'
            TABLESPACE = pg_default
            CONNECTION LIMIT = -1
            IS_TEMPLATE = False;
            """)


def create_role(connection: PostgresConnection, database_name: str, role_name: str, role_password: str):
    cursor = connection.cursor
    cursor.execute(f"""SELECT 1 FROM pg_roles WHERE rolname = '{role_name}';'""")
    result = cursor.fetchone()

    if not result:
        cursor.execute(f""" 
        CREATE ROLE {role_name}
        WITH LOGIN
        PASSWORD
        '{role_password}';
        """)

    cursor.execute(f"""GRANT ALL PRIVILEGES ON DATABASE {database_name} TO {role_name};""")


def create_schema(connection: PostgresConnection, database_name: str, schema_name: str):
    cursor = connection.cursor
    cursor.execute(f"""
        SELECT 1
        FROM information_schema.schemata
        WHERE catalog_name = '{database_name}'
            AND schema_name = '{schema_name}'
        """)
    result = cursor.fetchone()

    if not result:
        cursor.execute(f"""
            CREATE SCHEMA {schema_name}
            AUTHORIZATION {connection.user_name}""")


def create_table(connection: PostgresConnection, database_name: str, schema_name: str, table_name: str, table_definition: str):
    cursor = connection.cursor
    cursor.execute(f"""
        SELECT 1
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_catalog = '{database_name}'
          AND table_schema = '{schema_name}'
          AND table_name = '{table_name}'
        """)
    result = cursor.fetchone()
    if not result:
        cursor.execute(table_definition)


def run():
    # DB bootstrap variables
    sports_market_database_name = os.getenv("SPORTS_MARKET_DB_NAME")
    sports_market_stg_user_name = os.getenv("SPORTS_MARKET_STG_USER_NAME")
    sports_market_stg_user_password = os.getenv("SPORTS_MARKET_STG_USER_PASSWORD")

    # Initial connection to default postgres db
    pg_admin_conn = PostgresConnection(host_name='PG_HOST', port='PG_PORT', user_name='PG_DB_ADMIN_USER',
                                       user_password='PB_DB_ADMIN_USER_PASSWORD', db_name=None, auto_commit=True)

    # Create sports-market database
    if not sports_market_database_name:
        raise ConfigError(f"""sports_market_database_name={sports_market_database_name}""")
    create_database(pg_admin_conn, sports_market_database_name)

    # Once database created, close connection and use new database
    pg_admin_conn.close_cursor()
    pg_admin_conn.close_connection()
    pg_admin_conn = PostgresConnection(host_name='PG_HOST', port='PG_PORT', user_name='PG_DB_ADMIN_USER',
                                       user_password='PB_DB_ADMIN_USER_PASSWORD',
                                       db_name='SPORTS_MARKET_DB_NAME',
                                       auto_commit=True)

    # create bronze-level schemas
    bronze_schema_list = [
        'stg_the_odds_api',
        'stg_reference'
    ]

    if not bronze_schema_list:
        raise AttributeError("""Empty list: bronze_schema_list""")
    for schema in bronze_schema_list:
        create_schema(pg_admin_conn, pg_admin_conn.db_name, schema)

    # create the odds api bronze tables
    for schema, table in table_list.items():
        table_name, table_definition = next(iter(table.items()))
        create_table(pg_admin_conn, sports_market_database_name, schema, table_name, table_definition)

    # Create database roles
    # if not sports_market_stg_user_name or not sports_market_stg_user_password:
    #     raise ConfigError(f"""
    #         Missing database config variables:
    #         sports_market_stg_user_name={sports_market_stg_user_name}
    #         sports_market_stg_user_password={sports_market_stg_user_password}
    #         """)
    # create_role(pg_admin_conn, sports_market_database_name, sports_market_stg_user_name, sports_market_stg_user_password)

    # Close PostgresConnection
    pg_admin_conn.close_cursor()
    pg_admin_conn.close_connection()


run()
