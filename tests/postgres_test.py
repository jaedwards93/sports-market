from util.postgres import PostgresConnection


def test_postgres_connection_returns_data():
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

    assert pg_cursor.query is not None
    assert "postgres" in results[:][0]

    pg_conn.close_cursor()
    pg_conn.close_connection()