from logging import Logger
from pathlib import Path
from typing import Any, Dict, Optional
import sqlite3

from dbutils.pooled_db import PooledDB
from sqlalchemy.engine.url import make_url

from dbtr.server.lib.logger import get_logger

POOL = None


class Database:
    """
    Handles database operations.

    It manages the connection pool, executes queries, and handles transactions.
    The class uses a context manager to ensure that database connections are properly
    opened and closed. It supports SQLite, PostgreSQL, and MySQL databases.

    Attributes:
        connection_string (str): The database connection string.
        logger (Logger): The logger instance for logging messages.
        url (URL): The parsed URL object of the connection string.
        pool (PooledDB): The connection pool for database connections.
        conn (Connection): The current database connection.
    """


    def __init__(self, connection_string: str, logger: Logger = None):
        self.connection_string = connection_string
        self.logger = logger or get_logger(level="INFO")

        self.url = make_url(self.connection_string)

        self.logger.debug("Creating connection pool")
        global POOL
        POOL = POOL or self._create_pool()  # Makes the pool a singleton
        self.pool = POOL
        self.conn = None

    def __enter__(self) -> "Database":
        self.logger.debug("Getting connection from pool")
        self.conn = self.pool.connection()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        if self.conn:
            if exc_type:
                self.logger.error(
                    "Transaction failed", exc_info=(exc_type, exc_value, traceback)
                )
                self.conn.rollback()
            else:
                self.conn.commit()
            self.logger.debug("Returning connection to pool")
            self.conn.close()
            self.conn = None

    def execute(self, query: str, params: Optional[tuple] = None) -> Any:
        cursor = self.conn.cursor()
        try:
            self.logger.debug(f"Executing query: {query}")
            cursor.execute(query, params or ())
            return cursor
        except Exception as e:
            cursor.close()
            self.logger.exception("Query execution failed", exc_info=e)
            raise

    def fetchone(self, query: str, params: Optional[tuple] = None) -> Optional[Dict]:
        cursor = self.execute(query, params)
        try:
            row = cursor.fetchone()
            return dict(row) if row else {}
        finally:
            cursor.close()

    def fetchall(self, query: str, params: Optional[tuple] = None) -> list:
        cursor = self.execute(query, params)
        try:
            return cursor.fetchall()
        finally:
            cursor.close()

    def initialize_schema(self):
        try:
            self.logger.debug("Initializing database schema")
            sql_script = Path(__file__).parent.joinpath("db_init.sql").read_text()
            for statement in sql_script.split(";"):
                self.execute(statement)
            self.logger.info(
                f"Database schema initialized successfully for {self.url.drivername}"
            )
        except Exception as e:
            self.logger.exception("Schema initialization failed", exc_info=e)
            raise

    def run_script(self, path: Path):
        try:
            self.logger.debug(f"Running Database script at {str(path)}")
            sql_script = path.read_text()
            for statement in sql_script.split(";"):
                self.execute(statement)
            self.logger.info(
                f"Successfuly ran script at {path} for {self.url.drivername}"
            )
        except Exception as e:
            self.logger.exception(
                f"Failed to execute the script {path} for {self.url.drivername}",
                exc_info=e,
            )
            raise

    def _create_pool(self) -> PooledDB:
        Path(self.connection_string.replace("sqlite:///", "")).parent.mkdir(
            parents=True, exist_ok=True
        )

        def creator():
            con = sqlite3.connect(self.connection_string.replace("sqlite:///", ""), check_same_thread=False)
            con.row_factory = sqlite3.Row
            return con

        return PooledDB(
            creator=creator,
            maxconnections=5,
        )
