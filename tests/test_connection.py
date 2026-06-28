"""
Unit tests for connection.py
"""

from unittest import mock

import pytest

from src.connection import DatabaseConnection, quote_identifier
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
            use_unicode=True,
            connection_timeout=30
        )
        assert conn.connection == mock_connection

    @mock.patch('src.connection.mysql.connector.connect')
    def test_connect_with_tls_options(self, mock_connect):
        """TLS options are passed through when configured."""
        mock_connection = mock.MagicMock()
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="db.example.com",
            port=3306,
            user="root",
            password="secret",
            database="testdb",
            ssl_ca="/certs/ca.pem",
            ssl_cert="/certs/client-cert.pem",
            ssl_key="/certs/client-key.pem",
            ssl_verify_cert=True,
        )
        conn.connect()

        mock_connect.assert_called_once_with(
            host="db.example.com",
            port=3306,
            user="root",
            password="secret",
            database="testdb",
            charset='utf8mb4',
            use_unicode=True,
            connection_timeout=30,
            ssl_ca="/certs/ca.pem",
            ssl_cert="/certs/client-cert.pem",
            ssl_key="/certs/client-key.pem",
            ssl_verify_cert=True,
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
    def test_metadata_queries_escape_table_identifier(self, mock_connect):
        """Backticks in table identifiers are doubled in metadata queries."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        conn.get_table_columns("odd`table")

        mock_cursor.execute.assert_called_once_with("DESCRIBE `odd``table`", None)

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

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_create_table(self, mock_connect):
        """Test getting CREATE TABLE statement."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("users", "CREATE TABLE `users` (`id` int NOT NULL) ENGINE=InnoDB")
        ]
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        result = conn.get_create_table("users")

        assert result == "CREATE TABLE `users` (`id` int NOT NULL) ENGINE=InnoDB"

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_create_table_escapes_table_identifier(self, mock_connect):
        """SHOW CREATE TABLE escapes embedded backticks in table names."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [
            ("odd`table", "CREATE TABLE `odd``table` (`id` int)")
        ]
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        result = conn.get_create_table("odd`table")

        assert result == "CREATE TABLE `odd``table` (`id` int)"
        mock_cursor.execute.assert_called_once_with(
            "SHOW CREATE TABLE `odd``table`", None
        )

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_row_count(self, mock_connect):
        """Test getting row count without WHERE clause."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [(42,)]
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        count = conn.get_row_count("users")

        assert count == 42

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_row_count_with_where(self, mock_connect):
        """Test getting row count with WHERE clause."""
        mock_cursor = mock.MagicMock()
        mock_cursor.fetchall.return_value = [(10,)]
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        count = conn.get_row_count("users", where_clause="active = 1")

        assert count == 10
        mock_cursor.execute.assert_called_once_with(
            "SELECT COUNT(*) FROM `users` WHERE active = 1", None
        )

    @mock.patch('src.connection.mysql.connector.connect')
    def test_get_row_count_rejects_dangerous_where_clause(self, mock_connect):
        """Row-count WHERE fragments cannot include statement separators."""
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock.MagicMock()
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()

        with pytest.raises(ValueError, match="where_clause"):
            conn.get_row_count("users", where_clause="active = 1; DROP TABLE users")

    def test_quote_identifier_escapes_backticks(self):
        """Identifier quoting doubles embedded backticks."""
        assert quote_identifier("odd`name") == "`odd``name`"

    def test_init_with_custom_timeouts(self):
        """Test connection initialization with custom timeout."""
        conn = DatabaseConnection(
            host="localhost",
            port=3306,
            user="root",
            password="secret",
            connect_timeout=60,
        )
        assert conn.connect_timeout == 60

    def test_default_timeout_constants(self):
        """Test default timeout constants."""
        assert DatabaseConnection.DEFAULT_CONNECT_TIMEOUT == 30

    @mock.patch('src.connection.mysql.connector.connect')
    def test_start_consistent_snapshot(self, mock_connect):
        """Snapshot helper issues REPEATABLE READ + START TRANSACTION."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        conn.start_consistent_snapshot()

        executed = [call.args[0] for call in mock_cursor.execute.call_args_list]
        assert "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ" in executed
        assert "START TRANSACTION WITH CONSISTENT SNAPSHOT" in executed
        mock_cursor.close.assert_called_once()
