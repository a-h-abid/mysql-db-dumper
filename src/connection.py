"""
Database connection management for MySQL Database Dumper.
"""

import logging
from typing import Optional, Any

import mysql.connector
from mysql.connector import Error as MySQLError

from .models import ColumnInfo, coerce_optional_int


def quote_identifier(name: str) -> str:
    """Quote a MySQL identifier, escaping embedded backticks."""
    return f"`{str(name).replace('`', '``')}`"


def validate_where_clause(where_clause: Optional[str]) -> None:
    """Reject WHERE fragments that can terminate or comment out the query."""
    if not where_clause:
        return

    forbidden_tokens = (';', '--', '#', '/*', '*/')
    if any(token in where_clause for token in forbidden_tokens):
        raise ValueError("where_clause contains unsupported SQL separator or comment token")


class DatabaseConnection:
    """Manages MySQL database connections with context manager support."""

    DEFAULT_PORT = 3306
    DEFAULT_CHARSET = 'utf8mb4'
    DEFAULT_CONNECT_TIMEOUT = 30

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        ssl_ca: Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None,
        ssl_verify_cert: Optional[bool] = None,
    ):
        self.host = host
        self.port = coerce_optional_int(port, 'port')
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout = coerce_optional_int(connect_timeout, 'connect_timeout')
        self.ssl_options = {
            key: value for key, value in {
                'ssl_ca': ssl_ca,
                'ssl_cert': ssl_cert,
                'ssl_key': ssl_key,
                'ssl_verify_cert': ssl_verify_cert,
            }.items()
            if value is not None
        }
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
            connect_kwargs = {
                'host': self.host,
                'port': self.port,
                'user': self.user,
                'password': self.password,
                'database': self.database,
                'charset': self.DEFAULT_CHARSET,
                'use_unicode': True,
                'connection_timeout': self.connect_timeout,
                **self.ssl_options,
            }
            self.connection = mysql.connector.connect(**connect_kwargs)
            logging.info(f"Connected to {self.host}:{self.port}/{self.database or 'N/A'}")
        except MySQLError as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.debug("Database connection closed")

    def start_consistent_snapshot(self) -> None:
        """Begin a REPEATABLE READ transaction with a consistent snapshot.

        Gives every table in the dump the same point-in-time view, matching
        `mysqldump --single-transaction`. InnoDB only.
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            logging.debug("Started consistent-snapshot transaction")
        finally:
            cursor.close()

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
        results = self.execute_query(f"DESCRIBE {quote_identifier(table)}")
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
        results = self.execute_query(f"SHOW CREATE TABLE {quote_identifier(table)}")
        return results[0][1]

    def get_row_count(self, table: str, where_clause: Optional[str] = None) -> int:
        """Get row count for a table."""
        validate_where_clause(where_clause)
        query = f"SELECT COUNT(*) FROM {quote_identifier(table)}"
        if where_clause:
            query += f" WHERE {where_clause}"
        results = self.execute_query(query)
        return results[0][0]
