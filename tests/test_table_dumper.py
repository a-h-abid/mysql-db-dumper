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

    def test_negative_row_limit_warns_and_is_unlimited(self, dumper, caplog):
        """A negative row_limit emits no LIMIT and warns the user."""
        import logging
        caplog.set_level(logging.WARNING)
        settings = DumpSettings(row_limit=-5)
        query = dumper._build_select_query("users", ["id"], settings)
        assert "LIMIT" not in query
        assert "negative row_limit" in caplog.text

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

    def test_invalid_order_direction_defaults_to_asc(self, dumper, caplog):
        """An invalid order_direction is rejected and falls back to ASC."""
        import logging
        caplog.set_level(logging.WARNING)
        settings = DumpSettings(order_by="id", order_direction="SIDEWAYS")
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "ORDER BY `id` ASC" in query
        assert "invalid order_direction" in caplog.text

    def test_column_quoting(self, dumper):
        """Test that column names are properly quoted."""
        settings = DumpSettings()
        query = dumper._build_select_query(
            "users", ["user-id", "first name"], settings
        )
        assert "`user-id`" in query
        assert "`first name`" in query

    def test_identifiers_escape_embedded_backticks(self, dumper):
        """Embedded backticks in table and column names are doubled."""
        settings = DumpSettings(order_by="created`at")
        query = dumper._build_select_query(
            "odd`table", ["id", "created`at"], settings
        )

        assert query == (
            "SELECT `id`, `created``at` FROM `odd``table` "
            "ORDER BY `created``at` ASC"
        )

    def test_dangerous_where_clause_rejected(self, dumper):
        """Multi-statement WHERE fragments are rejected before execution."""
        settings = DumpSettings(where_clause="active = 1; DROP TABLE users")

        with pytest.raises(ValueError, match="where_clause"):
            dumper._build_select_query("users", ["id"], settings)


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

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_format_non_finite_float_rejected(self, dumper, value):
        """NaN and infinity cannot be dumped as bare SQL tokens."""
        with pytest.raises(ValueError, match="non-finite float"):
            dumper._type_formatters[float](value)

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
        """Non-append writes go to a .partial temp file; final is the plain path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            final, write_path, handle = dumper_no_compress._open_output_file(
                output_path, append=False
            )
            handle.close()

            assert final == output_path
            assert write_path == Path(str(output_path) + ".partial")
            assert not str(final).endswith(".gz")

    def test_open_with_compression(self, dumper_with_compress):
        """Compression adds .gz to final, and the temp file is .gz.partial."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            final, write_path, handle = dumper_with_compress._open_output_file(
                output_path, append=False
            )
            handle.close()

            assert str(final).endswith(".gz")
            assert final == Path(str(output_path) + ".gz")
            assert write_path == Path(str(output_path) + ".gz.partial")

    def test_open_append_mode(self, dumper_no_compress):
        """Append mode targets the final file directly (no .partial)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            output_path.write_text("initial")  # simulate an existing single-file dump

            final, write_path, handle = dumper_no_compress._open_output_file(
                output_path, append=True
            )
            handle.write("appended")
            handle.close()

            assert write_path == final == output_path
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

    def test_partial_dump_omits_drop_table(self, mock_connection):
        """A row_limited dump must NOT emit DROP TABLE (restore would destroy data)."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings(row_limit=10)  # partial -> unsafe to recreate

            stats = dumper.dump_table("users", output_path, settings, OutputFormat.SQL)

            content = output_path.read_text()
            assert stats.success is True
            assert "DROP TABLE" not in content
            assert "CREATE TABLE" in content

    def test_failed_dump_leaves_no_final_file(self, mock_connection):
        """If the dump dies mid-write, the final path must not exist; .partial remains."""
        mock_cursor = mock.MagicMock()
        mock_cursor.execute.side_effect = Exception("connection dropped")
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"

            stats = dumper.dump_table("users", output_path, DumpSettings(), OutputFormat.SQL)

            assert stats.success is False
            assert not output_path.exists()
            assert Path(str(output_path) + ".partial").exists()

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
        vars(dumper)["CSV_BATCH_SIZE"] = 3

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

    def test_where_clause_not_logged_at_info(self, mock_connection, caplog):
        """The WHERE predicate must not appear in INFO logs (possible PII)."""
        import logging
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([]))
        mock_connection.get_cursor.return_value = mock_cursor
        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir, caplog.at_level(logging.INFO):
            dumper.dump_table(
                "users", Path(tmpdir) / "t.sql",
                DumpSettings(where_clause="ssn = '123-45-6789'"),
                OutputFormat.SQL,
            )

        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert info_messages  # the "Dumping table" line is present
        assert not any("123-45-6789" in m for m in info_messages)

    def test_generated_columns_excluded(self, mock_connection):
        """Generated columns must be excluded from SELECT and INSERT (restore-breaking)."""
        mock_connection.get_table_columns.return_value = [
            ColumnInfo("id", "int", "NO", "PRI", None, "auto_increment"),
            ColumnInfo("full_name", "varchar(255)", "YES", "", None, "STORED GENERATED"),
            ColumnInfo("email", "varchar(255)", "YES", "", None, ""),
        ]
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "a@b.com")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "t.sql"
            stats = dumper.dump_table("users", output_path, DumpSettings(), OutputFormat.SQL)

            content = output_path.read_text()
            assert stats.success is True
            assert "`full_name`" not in content
            assert "`id`" in content
            assert "`email`" in content

    def test_dump_sql_escapes_table_and_column_identifiers(self):
        """SQL dump output escapes embedded backticks in identifiers."""
        mock_connection = mock.MagicMock()
        mock_connection.get_table_columns.return_value = [
            ColumnInfo("id", "int", "NO", "PRI", None, ""),
            ColumnInfo("we`ird", "varchar(255)", "YES", "", None, ""),
        ]
        mock_connection.get_create_table.return_value = (
            "CREATE TABLE `odd``table` (`id` int, `we``ird` varchar(255))"
        )
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "value")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "t.sql"
            stats = dumper.dump_table("odd`table", output_path, DumpSettings(), OutputFormat.SQL)

            content = output_path.read_text()
            assert stats.success is True
            assert "DROP TABLE IF EXISTS `odd``table`;" in content
            assert "INSERT INTO `odd``table` (`id`, `we``ird`) VALUES" in content

    def test_full_sql_dump_emits_fk_wrapper(self, mock_connection):
        """A full SQL dump wraps DDL+inserts in SET FOREIGN_KEY_CHECKS=0/1 by default."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            stats = dumper.dump_table(
                "users", output_path, DumpSettings(), OutputFormat.SQL
            )

            assert stats.success is True
            content = output_path.read_text()
            assert content.count("SET FOREIGN_KEY_CHECKS=0;") == 1
            assert content.count("SET FOREIGN_KEY_CHECKS=1;") == 1
            # =0 must come before CREATE; =1 must come after the last insert.
            assert content.index("SET FOREIGN_KEY_CHECKS=0;") < content.index("CREATE TABLE")
            assert content.index("INSERT INTO `users`") < content.index("SET FOREIGN_KEY_CHECKS=1;")

    def test_partial_row_limit_dump_omits_fk_wrapper(self, mock_connection):
        """A row_limited dump must NOT emit the FK wrapper (orphan-row risk)."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            stats = dumper.dump_table(
                "users", output_path, DumpSettings(row_limit=10), OutputFormat.SQL
            )

            content = output_path.read_text()
            assert stats.success is True
            assert "FOREIGN_KEY_CHECKS" not in content

    def test_partial_where_dump_omits_fk_wrapper(self, mock_connection):
        """A where_clause dump must NOT emit the FK wrapper."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            stats = dumper.dump_table(
                "users", output_path,
                DumpSettings(where_clause="active = 1"), OutputFormat.SQL
            )

            content = output_path.read_text()
            assert stats.success is True
            assert "FOREIGN_KEY_CHECKS" not in content

    def test_fk_wrapper_disabled_by_option(self, mock_connection):
        """disable_foreign_key_checks=False suppresses the wrapper on a full dump."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(
            mock_connection, {"compress": False, "disable_foreign_key_checks": False}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            stats = dumper.dump_table(
                "users", output_path, DumpSettings(), OutputFormat.SQL
            )

            content = output_path.read_text()
            assert stats.success is True
            assert "FOREIGN_KEY_CHECKS" not in content

    def test_csv_dump_never_emits_fk_statements(self, mock_connection):
        """CSV output carries no SQL FK statements regardless of the option."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(
            mock_connection, {"compress": False, "disable_foreign_key_checks": True}
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.csv"
            stats = dumper.dump_table(
                "users", output_path, DumpSettings(), OutputFormat.CSV
            )

            content = output_path.read_text()
            assert stats.success is True
            assert "FOREIGN_KEY_CHECKS" not in content

    def test_combined_mode_suppresses_per_file_wrapper(self, mock_connection):
        """emit_fk_wrapper=False (combined mode) suppresses the per-file wrapper."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            stats = dumper.dump_table(
                "users", output_path, DumpSettings(), OutputFormat.SQL,
                emit_fk_wrapper=False,
            )

            content = output_path.read_text()
            assert stats.success is True
            assert "FOREIGN_KEY_CHECKS" not in content


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

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_format_non_finite_float_via_method(self, dumper, value):
        """The public SQL value formatter rejects non-finite floats."""
        with pytest.raises(ValueError, match="non-finite float"):
            dumper._format_sql_value(value)

    def test_format_datetime_via_method(self, dumper):
        """Test datetime formatting via the method."""
        dt = datetime(2024, 6, 15, 12, 30, 0)
        result = dumper._format_sql_value(dt)
        assert result == "'2024-06-15 12:30:00'"

    def test_format_datetime_preserves_microseconds(self, dumper):
        """Microseconds must not be silently dropped."""
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        assert dumper._format_sql_value(dt) == "'2024-01-15 10:30:45.123456'"

    def test_format_string_with_null_byte(self, dumper):
        """NUL byte must be escaped as \\0, not emitted raw."""
        assert dumper._format_sql_value("a\x00b") == "'a\\0b'"

    def test_format_string_with_ctrl_z(self, dumper):
        """Ctrl-Z must be escaped as \\Z."""
        assert dumper._format_sql_value("a\x1ab") == "'a\\Zb'"

    def test_format_bytearray_as_hex(self, dumper):
        """bytearray must serialize as a hex literal like bytes."""
        assert dumper._format_sql_value(bytearray(b"\x00\xff")) == "X'00ff'"

    def test_format_decimal_unquoted(self, dumper):
        """Decimal must be an unquoted numeric literal."""
        from decimal import Decimal
        assert dumper._format_sql_value(Decimal("1.50")) == "1.50"

    def test_format_date(self, dumper):
        """date must serialize as a quoted ISO date."""
        from datetime import date
        assert dumper._format_sql_value(date(2024, 1, 15)) == "'2024-01-15'"

    def test_format_time(self, dumper):
        """time must serialize as a quoted ISO time."""
        from datetime import time
        assert dumper._format_sql_value(time(10, 30, 45)) == "'10:30:45'"

    def test_format_timedelta(self, dumper):
        """timedelta (MySQL TIME) must serialize as 'HH:MM:SS'."""
        from datetime import timedelta
        assert dumper._format_sql_value(timedelta(hours=10, minutes=30, seconds=45)) == "'10:30:45'"


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
