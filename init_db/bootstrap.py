from util.postgres import PostgresConnection
from util.custom_errors import ConfigError
from init_db.schemas import schema_list
from init_db.tables import table_list
from init_db.schema_roles import schema_roles_list
from init_db.sequences import sequence_list
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
            OWNER = {connection.user_name};
            """)
        print(f'Created database: {database_name}')


def create_schema(connection: PostgresConnection, schema_name: str):
    cursor = connection.cursor
    database_name = connection.db_name
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
        print(f'Created schema: {database_name}.{schema_name}')


def create_table(connection: PostgresConnection, schema_name: str, table_name: str, table_definition: str):
    cursor = connection.cursor
    database_name = connection.db_name
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
        print(f'Created table: {database_name}.{schema_name}.{table_name}')


def create_database_role(connection: PostgresConnection, role_name: str, role_password: str):
    cursor = connection.cursor
    database_name = connection.db_name
    cursor.execute(f"""SELECT 1 FROM pg_roles WHERE rolname = '{role_name}';'""")
    result = cursor.fetchone()

    if not result:
        cursor.execute(f""" 
        CREATE ROLE {role_name}
        WITH LOGIN
        PASSWORD
        '{role_password}';
        """)
        print(f'Created role: {database_name}.{role_name}')

    cursor.execute(f"""GRANT ALL PRIVILEGES ON DATABASE {database_name} TO {role_name};""")


def create_schema_role(connection: PostgresConnection, schema_name: str, role_name: str, role_password: str):
    cursor = connection.cursor
    cursor.execute(f"""SELECT 1 FROM pg_roles WHERE rolname = '{role_name}';""")
    result = cursor.fetchone()

    if not result:
        cursor.execute(f""" 
        CREATE ROLE {role_name}
        WITH LOGIN
        PASSWORD
        '{role_password}';
        """)
        print(f'Created role: {role_name}')

    cursor.execute(f"""GRANT USAGE, CREATE ON SCHEMA {schema_name} TO {role_name};""")
    cursor.execute(f"""ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role_name};""")
    cursor.execute(f"""ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT USAGE, SELECT, UPDATE ON SEQUENCES  TO {role_name};""")
    cursor.execute(f"""ALTER DEFAULT PRIVILEGES IN SCHEMA {schema_name} GRANT EXECUTE ON FUNCTIONS  TO {role_name};""")
    print(f'Granted privileges: {schema_name}.{role_name}')


def create_sequence(connection: PostgresConnection, schema_name: str, sequence_name: str):
    cursor = connection.cursor
    database_name = connection.db_name
    cursor.execute(f"""	
        SELECT 1
        FROM information_schema.sequences
        WHERE sequence_catalog = '{database_name}'
            AND sequence_schema = '{schema_name}'
            AND sequence_name = '{sequence_name}';
        """)

    result = cursor.fetchone()

    if not result:
        cursor.execute(f"""
            CREATE SEQUENCE {database_name}.{schema_name}.{sequence_name}
            INCREMENT 1
            START 1;
            """)
        print(f'Created sequence: {database_name}.{schema_name}.{sequence_name}')


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

    # Create schemas
    if not schema_list:
        raise ConfigError("""Empty list: stg_schema_list""")
    for schema in schema_list:
        create_schema(pg_admin_conn, schema)

    # Create schema roles
    for schema, schema_creds in schema_roles_list.items():
        for role_name, role_password in schema_creds.items():
            create_schema_role(pg_admin_conn, schema, role_name, role_password)

    # create the odds api bronze tables
    for schema, table in table_list.items():
        table_name, table_definition = next(iter(table.items()))
        create_table(pg_admin_conn, schema, table_name, table_definition)

    # create bronze sequences
    for schema, sequence in sequence_list.items():
        create_sequence(pg_admin_conn, schema, sequence[0])

    # Close PostgresConnection
    pg_admin_conn.close_cursor()
    pg_admin_conn.close_connection()


run()
