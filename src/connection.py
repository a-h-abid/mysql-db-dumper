"""
Database connection management for MySQL Database Dumper.
"""

import logging
import time
from typing import Optional, Any

import mysql.connector
from mysql.connector import Error as MySQLError

from .models import ColumnInfo


class DatabaseConnection:
    """Manages MySQL database connections with context manager support."""

    DEFAULT_PORT = 3306
    DEFAULT_CHARSET = 'utf8mb4'
    DEFAULT_CONNECT_TIMEOUT = 30
    DEFAULT_READ_TIMEOUT = 300  # 5 minutes for large result sets
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY = 2  # seconds

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: int = DEFAULT_READ_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_delay: int = DEFAULT_RETRY_DELAY
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None

    def __enter__(self) -> "DatabaseConnection":
        """Context manager entry - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close connection."""
        self.disconnect()

    def connect(self) -> None:
        """Establish database connection with retry logic for transient failures."""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    charset=self.DEFAULT_CHARSET,
                    use_unicode=True,
                    connection_timeout=self.connect_timeout,
                    read_timeout=self.read_timeout,
                    autocommit=True
                )
                logging.info(f"Connected to {self.host}:{self.port}/{self.database or 'N/A'}")
                return
            except MySQLError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    logging.warning(
                        f"Connection attempt {attempt + 1}/{self.max_retries} failed: {e}. "
                        f"Retrying in {self.retry_delay} seconds..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    logging.error(f"Failed to connect after {self.max_retries} attempts: {e}")

        raise last_error if last_error else MySQLError("Failed to connect to database")

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.debug("Database connection closed")

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list[tuple]:
        """Execute a query and return results."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def get_cursor(self, buffered: bool = False):
        """Get a cursor for streaming large results.

        Args:
            buffered: If False (default), uses server-side cursor for memory-efficient
                     streaming of large result sets. If True, uses buffered cursor.
        """
        return self.connection.cursor(buffered=buffered)

    def get_tables(self) -> list[str]:
        """Get list of all tables in the current database."""
        results = self.execute_query("SHOW TABLES")
        return [row[0] for row in results]

    def get_table_columns(self, table: str) -> list[ColumnInfo]:
        """Get column information for a table."""
        results = self.execute_query(f"DESCRIBE `{table}`")
        return [
            ColumnInfo(
                name=row[0],
                type=row[1],
                nullable=row[2],
                key=row[3],
                default=row[4],
                extra=row[5]
            )
            for row in results
        ]

    def get_create_table(self, table: str) -> str:
        """Get CREATE TABLE statement."""
        results = self.execute_query(f"SHOW CREATE TABLE `{table}`")
        return results[0][1]

    def get_row_count(self, table: str, where_clause: Optional[str] = None) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) FROM `{table}`"
        if where_clause:
            query += f" WHERE {where_clause}"
        results = self.execute_query(query)
        return results[0][0]
