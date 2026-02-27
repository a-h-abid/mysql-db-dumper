"""
Unit tests for table_dumper.py
"""

import gzip
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from src.models import ColumnInfo, DumpSettings, OutputFormat, TableStats
from src.table_dumper import TableDumper


class TestTableDumper:
    """Tests for TableDumper class."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock database connection."""
        conn = mock.MagicMock()
        conn.get_table_columns.return_value = [
            ColumnInfo("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ColumnInfo("name", "varchar(255)", "YES", "", None, ""),
            ColumnInfo("created_at", "datetime", "YES", "", None, ""),
        ]
        conn.get_create_table.return_value = "CREATE TABLE `users` (...)"
        return conn

    @pytest.fixture
    def output_settings(self):
        """Default output settings."""
        return {
            "directory": "./dumps",
            "format": "sql",
            "compress": False,
            "batch_size": 1000
        }

    def test_init(self, mock_connection, output_settings):
        """Test TableDumper initialization."""
        dumper = TableDumper(mock_connection, output_settings)
        assert dumper.connection == mock_connection
        assert dumper.output_settings == output_settings
        assert dumper.batch_size == 1000

    def test_init_default_batch_size(self, mock_connection):
        """Test default batch size when not specified."""
        dumper = TableDumper(mock_connection, {})
        assert dumper.batch_size == TableDumper.DEFAULT_BATCH_SIZE


class TestBuildSelectQuery:
    """Tests for _build_select_query method."""

    @pytest.fixture
    def dumper(self):
        """Create a TableDumper instance."""
        mock_conn = mock.MagicMock()
        return TableDumper(mock_conn, {})

    def test_basic_query(self, dumper):
        """Test basic SELECT query."""
        settings = DumpSettings()
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert query == "SELECT `id`, `name` FROM `users`"

    def test_query_with_where(self, dumper):
        """Test query with WHERE clause."""
        settings = DumpSettings(where_clause="status = 'active'")
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "WHERE status = 'active'" in query

    def test_query_with_order_by(self, dumper):
        """Test query with ORDER BY."""
        settings = DumpSettings(order_by="id", order_direction="ASC")
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "ORDER BY `id` ASC" in query

    def test_query_with_order_desc(self, dumper):
        """Test query with DESC ordering."""
        settings = DumpSettings(order_by="created_at", order_direction="DESC")
        query = dumper._build_select_query(
            "users", ["id", "name", "created_at"], settings
        )
        assert "ORDER BY `created_at` DESC" in query

    def test_query_with_limit(self, dumper):
        """Test query with LIMIT."""
        settings = DumpSettings(row_limit=1000)
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "LIMIT 1000" in query

    def test_query_with_zero_limit(self, dumper):
        """Test query with zero LIMIT."""
        settings = DumpSettings(row_limit=0)
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "LIMIT 0" in query

    def test_query_with_all_options(self, dumper):
        """Test query with all options."""
        settings = DumpSettings(
            row_limit=500,
            order_by="id",
            order_direction="DESC",
            where_clause="active = 1"
        )
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "WHERE active = 1" in query
        assert "ORDER BY `id` DESC" in query
        assert "LIMIT 500" in query

    def test_order_by_invalid_column_ignored(self, dumper):
        """Test that order_by with non-existent column is ignored."""
        settings = DumpSettings(order_by="nonexistent", order_direction="ASC")
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "ORDER BY" not in query

    def test_order_direction_without_order_by_warns(self, dumper, caplog):
        """Test that setting order_direction without order_by logs a warning."""
        import logging
        caplog.set_level(logging.WARNING)
        settings = DumpSettings(order_direction="DESC")  # No order_by!
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "ORDER BY" not in query
        assert "order_direction" in caplog.text
        assert "order_by" in caplog.text

    def test_column_quoting(self, dumper):
        """Test that column names are properly quoted."""
        settings = DumpSettings()
        query = dumper._build_select_query(
            "users", ["user-id", "first name"], settings
        )
        assert "`user-id`" in query
        assert "`first name`" in query


class TestTypeFormatters:
    """Tests for type formatters used in SQL generation."""

    @pytest.fixture
    def dumper(self):
        """Create a TableDumper instance."""
        mock_conn = mock.MagicMock()
        return TableDumper(mock_conn, {})

    def test_format_none(self, dumper):
        """Test NULL formatting."""
        result = dumper._type_formatters[type(None)](None)
        assert result == "NULL"

    def test_format_bool_true(self, dumper):
        """Test boolean True formatting."""
        result = dumper._type_formatters[bool](True)
        assert result == "1"

    def test_format_bool_false(self, dumper):
        """Test boolean False formatting."""
        result = dumper._type_formatters[bool](False)
        assert result == "0"

    def test_format_int(self, dumper):
        """Test integer formatting."""
        result = dumper._type_formatters[int](42)
        assert result == "42"

    def test_format_float(self, dumper):
        """Test float formatting."""
        result = dumper._type_formatters[float](3.14159)
        assert result == "3.14159"

    def test_format_bytes(self, dumper):
        """Test bytes formatting as hex."""
        result = dumper._type_formatters[bytes](b'\x00\xff\xab')
        assert result == "X'00ffab'"

    def test_format_datetime(self, dumper):
        """Test datetime formatting."""
        dt = datetime(2024, 1, 15, 10, 30, 45)
        result = dumper._type_formatters[datetime](dt)
        assert result == "'2024-01-15 10:30:45'"


class TestOpenOutputFile:
    """Tests for _open_output_file method."""

    @pytest.fixture
    def dumper_no_compress(self):
        """Create a TableDumper without compression."""
        mock_conn = mock.MagicMock()
        return TableDumper(mock_conn, {"compress": False})

    @pytest.fixture
    def dumper_with_compress(self):
        """Create a TableDumper with compression."""
        mock_conn = mock.MagicMock()
        return TableDumper(mock_conn, {"compress": True})

    def test_open_without_compression(self, dumper_no_compress):
        """Test opening file without compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            result_path, handle = dumper_no_compress._open_output_file(
                output_path, append=False
            )
            handle.close()

            assert result_path == output_path
            assert not str(result_path).endswith('.gz')

    def test_open_with_compression(self, dumper_with_compress):
        """Test opening file with compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            result_path, handle = dumper_with_compress._open_output_file(
                output_path, append=False
            )
            handle.close()

            assert str(result_path).endswith('.gz')
            assert result_path == Path(str(output_path) + '.gz')

    def test_open_append_mode(self, dumper_no_compress):
        """Test opening file in append mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"

            # Write initial content
            result_path, handle = dumper_no_compress._open_output_file(
                output_path, append=False
            )
            handle.write("initial")
            handle.close()

            # Append more content
            result_path, handle = dumper_no_compress._open_output_file(
                output_path, append=True
            )
            handle.write("appended")
            handle.close()

            # Verify content
            content = output_path.read_text()
            assert "initial" in content
            assert "appended" in content


class TestDumpTable:
    """Tests for dump_table method."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock database connection."""
        conn = mock.MagicMock()
        conn.get_table_columns.return_value = [
            ColumnInfo("id", "int(11)", "NO", "PRI", None, "auto_increment"),
            ColumnInfo("name", "varchar(255)", "YES", "", None, ""),
        ]
        conn.get_create_table.return_value = "CREATE TABLE `users` (`id` int, `name` varchar(255))"
        return conn

    def test_dump_table_error_handling(self, mock_connection):
        """Test error handling during dump."""
        mock_connection.get_table_columns.side_effect = Exception("Connection lost")

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.SQL
            )

            assert stats.success is False
            assert stats.error == "Connection lost"

    def test_dump_table_returns_stats(self, mock_connection):
        """Test that dump_table returns TableStats."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.SQL
            )

            assert isinstance(stats, TableStats)
            assert stats.table == "users"

    def test_dump_table_sql_with_data(self, mock_connection):
        """Test dumping table as SQL with actual data."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(
            return_value=iter([(1, "Alice"), (2, "Bob")])
        )
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.SQL
            )

            assert stats.success is True
            assert stats.rows_dumped == 2

            content = output_path.read_text()
            assert "INSERT INTO `users`" in content
            assert "CREATE TABLE" in content
            assert "DROP TABLE IF EXISTS" in content

    def test_dump_table_csv_with_data(self, mock_connection):
        """Test dumping table as CSV with actual data."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(
            return_value=iter([(1, "Alice"), (2, "Bob")])
        )
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.csv"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.CSV
            )

            assert stats.success is True
            assert stats.rows_dumped == 2

            content = output_path.read_text()
            assert "id,name" in content

    def test_dump_table_sql_batched(self, mock_connection):
        """Test SQL dump with multiple batches."""
        # Create enough rows to trigger batch writing
        rows = [(i, f"User{i}") for i in range(5)]
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter(rows))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False, "batch_size": 2})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.SQL
            )

            assert stats.success is True
            assert stats.rows_dumped == 5
            content = output_path.read_text()
            # Should have multiple INSERT statements due to batching
            assert content.count("INSERT INTO") >= 2

    def test_dump_table_csv_batched(self, mock_connection):
        """Test CSV dump with multiple batches."""
        rows = [(i, f"User{i}") for i in range(10)]
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter(rows))
        mock_connection.get_cursor.return_value = mock_cursor

        # Override CSV_BATCH_SIZE for testing
        dumper = TableDumper(mock_connection, {"compress": False})
        dumper.CSV_BATCH_SIZE = 3

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.csv"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.CSV
            )

            assert stats.success is True
            assert stats.rows_dumped == 10

    def test_dump_table_compressed(self, mock_connection):
        """Test dumping table with gzip compression."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(
            return_value=iter([(1, "Alice")])
        )
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": True})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings()

            stats = dumper.dump_table(
                "users", output_path, settings, OutputFormat.SQL
            )

            assert stats.success is True
            assert stats.file_path.endswith('.gz')


class TestFormatSqlValue:
    """Tests for _format_sql_value method."""

    @pytest.fixture
    def dumper(self):
        mock_conn = mock.MagicMock()
        return TableDumper(mock_conn, {})

    def test_format_string(self, dumper):
        """Test string formatting with escaping."""
        result = dumper._format_sql_value("hello")
        assert result == "'hello'"

    def test_format_string_with_quotes(self, dumper):
        """Test string with single quotes is escaped."""
        result = dumper._format_sql_value("it's")
        assert result == "'it\\'s'"

    def test_format_string_with_backslash(self, dumper):
        """Test string with backslash is escaped."""
        result = dumper._format_sql_value("path\\to")
        assert result == "'path\\\\to'"

    def test_format_string_with_newlines(self, dumper):
        """Test string with newlines is escaped."""
        result = dumper._format_sql_value("line1\nline2\rline3")
        assert result == "'line1\\nline2\\rline3'"

    def test_format_none_via_method(self, dumper):
        """Test None formatting via the method."""
        result = dumper._format_sql_value(None)
        assert result == "NULL"

    def test_format_bool_via_method(self, dumper):
        """Test bool formatting via the method."""
        assert dumper._format_sql_value(True) == "1"
        assert dumper._format_sql_value(False) == "0"

    def test_format_int_via_method(self, dumper):
        """Test int formatting via the method."""
        assert dumper._format_sql_value(42) == "42"

    def test_format_datetime_via_method(self, dumper):
        """Test datetime formatting via the method."""
        dt = datetime(2024, 6, 15, 12, 30, 0)
        result = dumper._format_sql_value(dt)
        assert result == "'2024-06-15 12:30:00'"


class TestWriteInsertBatch:
    """Tests for _write_insert_batch method."""

    @pytest.fixture
    def dumper(self):
        mock_conn = mock.MagicMock()
        return TableDumper(mock_conn, {})

    def test_write_empty_batch(self, dumper):
        """Test writing empty batch does nothing."""
        import io
        fh = io.StringIO()
        dumper._write_insert_batch(fh, "users", "`id`, `name`", [])
        assert fh.getvalue() == ""

    def test_write_single_row_batch(self, dumper):
        """Test writing a single row batch."""
        import io
        fh = io.StringIO()
        dumper._write_insert_batch(fh, "users", "`id`, `name`", [(1, "Alice")])
        content = fh.getvalue()
        assert "INSERT INTO `users`" in content
        assert "1" in content
        assert "'Alice'" in content

    def test_write_multi_row_batch(self, dumper):
        """Test writing a multi-row batch."""
        import io
        fh = io.StringIO()
        dumper._write_insert_batch(
            fh, "users", "`id`, `name`",
            [(1, "Alice"), (2, "Bob")]
        )
        content = fh.getvalue()
        assert "INSERT INTO `users`" in content
        assert content.count(",\n") == 1  # Two rows joined by comma
