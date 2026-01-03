"""
Database connection management for MySQL Database Dumper.
"""

import logging
from typing import Optional, Any

import mysql.connector
from mysql.connector import Error as MySQLError

from .models import ColumnInfo


class DatabaseConnection:
    """Manages MySQL database connections with context manager support."""

    DEFAULT_PORT = 3306
    DEFAULT_CHARSET = 'utf8mb4'

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def __enter__(self) -> "DatabaseConnection":
        """Context manager entry - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close connection."""
        self.disconnect()

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.DEFAULT_CHARSET,
                use_unicode=True
            )
            logging.info(f"Connected to {self.host}:{self.port}/{self.database or 'N/A'}")
        except MySQLError as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

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
