import os
import psycopg2
import dotenv as e
from typing import Optional
from util.custom_errors import ConfigError

e.load_dotenv()


class PostgresConnection:
    def __init__(self, host_name: str, port: int, user_name: str, user_password: str,
                 db_name: Optional[str] = 'postgres', auto_commit: Optional[bool] = False):
        self.host_name = os.getenv(host_name)
        self.port = os.getenv(port)
        self.user_name = os.getenv(user_name)
        self.user_password = os.getenv(user_password)
        self.db_name = 'postgres' if not db_name or db_name == 'postgres' else os.getenv(db_name)

        if not self.host_name or not self.port or not self.user_name or not self.user_password or not self.db_name:
            raise ConfigError(f"Missing configuration. Current values:\n"
                              f"host_name={self.host_name},\n"
                              f"port={self.port},\n"
                              f"user_name={self.user_name},\n"
                              f"user_password={self.user_password},\n"
                              f"db_name={self.db_name}")

        self.connection = psycopg2.connect(
            host=self.host_name,
            port=self.port,
            user=self.user_name,
            password=self.user_password,
            dbname=self.db_name
        )
        if auto_commit:
            self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def close_cursor(self):
        if self.cursor:
            self.cursor.close()

    def close_connection(self):
        if self.connection:
            self.connection.close()

