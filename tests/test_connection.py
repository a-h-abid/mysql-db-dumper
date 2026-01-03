"""
Unit tests for connection.py
"""

from unittest import mock

import pytest

from src.connection import DatabaseConnection
from src.models import ColumnInfo


class TestDatabaseConnection:
    """Tests for DatabaseConnection class."""

    def test_init(self):
        """Test connection initialization."""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb"
        )
        assert conn.host == "localhost"
        assert conn.port == 3306
        assert conn.user == "root"
        assert conn.password == "secret"
        assert conn.database == "testdb"
        assert conn.connection is None

    def test_default_constants(self):
        """Test default constants."""
        assert DatabaseConnection.DEFAULT_PORT == 3306
        assert DatabaseConnection.DEFAULT_CHARSET == 'utf8mb4'

    @mock.patch('src.connection.mysql.connector.connect')
    def test_connect(self, mock_connect):
        """Test database connection establishment."""
        mock_connection = mock.MagicMock()
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb"
        )
        conn.connect()

        mock_connect.assert_called_once_with(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb",
            charset='utf8mb4',
            use_unicode=True
        )
        assert conn.connection == mock_connection

    @mock.patch('src.connection.mysql.connector.connect')
    def test_disconnect(self, mock_connect):
        """Test database disconnection."""
        mock_connection = mock.MagicMock()
        mock_connection.is_connected.return_value = True
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret"
        )
        conn.connect()
        conn.disconnect()

        mock_connection.close.assert_called_once()

    @mock.patch('src.connection.mysql.connector.connect')
    def test_disconnect_not_connected(self, mock_connect):
        """Test disconnect when not connected."""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret"
        )
        # Should not raise any errors
        conn.disconnect()

    @mock.patch('src.connection.mysql.connector.connect')
    def test_context_manager(self, mock_connect):
        """Test context manager usage."""
        mock_connection = mock.MagicMock()
        mock_connection.is_connected.return_value = True
        mock_connect.return_value = mock_connection

        with DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb"
        ) as conn:
            assert conn.connection == mock_connection

        mock_connection.close.assert_called_once()

    @mock.patch('src.connection.mysql.connector.connect')
    def test_execute_query(self, mock_connect):
        """Test query execution."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [("row1",), ("row2",)]

        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret"
        )
        conn.connect()
        result = conn.execute_query("SELECT * FROM test")

        assert result == [("row1",), ("row2",)]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", None)
        mock_cursor.close.assert_called_once()

    @mock.patch('src.connection.mysql.connector.connect')
    def test_execute_query_with_params(self, mock_connect):
        """Test query execution with parameters."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [("row1",)]

        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret"
        )
        conn.connect()
        result = conn.execute_query(
            "SELECT * FROM test WHERE id = %s",
            (1,)
        )

        mock_cursor.execute.assert_called_once_with(
            "SELECT * FROM test WHERE id = %s",
            (1,)
        )

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_tables(self, mock_connect):
        """Test getting list of tables."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("users",), ("orders",), ("products",)
        ]

        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb"
        )
        conn.connect()
        tables = conn.get_tables()

        assert tables == ["users", "orders", "products"]
        mock_cursor.execute.assert_called_once_with("SHOW TABLES", None)

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_table_columns(self, mock_connect):
        """Test getting column information for a table."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ("name", "varchar(255)", "YES", "", None, ""),
            ("created_at", "datetime", "YES", "", "CURRENT_TIMESTAMP", ""),
        ]

        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            database="testdb"
        )
        conn.connect()
        columns = conn.get_table_columns("users")

        assert len(columns) == 3
        assert isinstance(columns[0], ColumnInfo)
        assert columns[0].name == "id"
        assert columns[0].type == "int(11)"
        assert columns[0].key == "PRI"
        assert columns[1].name == "name"
        assert columns[1].nullable == "YES"

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_cursor(self, mock_connect):
        """Test getting a cursor."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret"
        )
        conn.connect()

        # Default (unbuffered)
        cursor = conn.get_cursor()
        mock_connection.cursor.assert_called_with(buffered=False)

        # Buffered
        cursor = conn.get_cursor(buffered=True)
        mock_connection.cursor.assert_called_with(buffered=True)

    @mock.patch('src.connection.mysql.connector.connect')
    def test_connect_error(self, mock_connect):
        """Test connection error handling."""
        from mysql.connector import Error as MySQLError
        mock_connect.side_effect = MySQLError("Connection refused")

        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="wrong_password"
        )

        with pytest.raises(MySQLError):
            conn.connect()
