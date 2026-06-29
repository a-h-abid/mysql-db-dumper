"""
Unit tests for database_dumper.py
"""

import fnmatch
import re
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from src.database_dumper import DatabaseDumper
from src.models import ColumnInfo, DatabaseStats, DumpStats, OutputFormat, TableStats


class TestCompileExclusionPatterns:
    """Tests for _compile_exclusion_patterns method."""

    @pytest.fixture
    def dumper(self):
        """Create a DatabaseDumper with mocked config."""
        mock_config = mock.MagicMock()
        mock_config.get_output_settings.return_value = {}
        mock_config.get_defaults.return_value = {}
        return DatabaseDumper(mock_config)

    def test_compile_simple_pattern(self, dumper):
        """Test compiling simple pattern."""
        patterns = ["*_backup"]
        compiled = dumper._compile_exclusion_patterns(patterns)

        assert len(compiled) == 1
        assert isinstance(compiled[0], re.Pattern)

    def test_compiled_pattern_matches(self, dumper):
        """Test compiled patterns match correctly."""
        patterns = ["*_backup", "tmp_*"]
        compiled = dumper._compile_exclusion_patterns(patterns)

        # Test matches
        assert compiled[0].match("users_backup")
        assert compiled[1].match("tmp_data")

        # Test non-matches
        assert not compiled[0].match("users")
        assert not compiled[1].match("data_tmp")

    def test_empty_patterns(self, dumper):
        """Test compiling empty pattern list."""
        compiled = dumper._compile_exclusion_patterns([])
        assert compiled == []


class TestIsTableExcluded:
    """Tests for _is_table_excluded method."""

    @pytest.fixture
    def dumper(self):
        """Create a DatabaseDumper with mocked config."""
        mock_config = mock.MagicMock()
        mock_config.get_output_settings.return_value = {}
        mock_config.get_defaults.return_value = {}
        return DatabaseDumper(mock_config)

    def test_exact_match(self, dumper):
        """Test exact pattern match."""
        patterns = ["test_data"]
        assert dumper._is_table_excluded("test_data", patterns) is True
        assert dumper._is_table_excluded("test_data_2", patterns) is False

    def test_suffix_wildcard(self, dumper):
        """Test suffix wildcard pattern."""
        patterns = ["*_backup"]
        assert dumper._is_table_excluded("users_backup", patterns) is True
        assert dumper._is_table_excluded("orders_backup", patterns) is True
        assert dumper._is_table_excluded("backup_users", patterns) is False

    def test_prefix_wildcard(self, dumper):
        """Test prefix wildcard pattern."""
        patterns = ["tmp_*"]
        assert dumper._is_table_excluded("tmp_data", patterns) is True
        assert dumper._is_table_excluded("tmp_", patterns) is True
        assert dumper._is_table_excluded("data_tmp", patterns) is False

    def test_middle_wildcard(self, dumper):
        """Test middle wildcard pattern."""
        patterns = ["*_backup_*"]
        assert dumper._is_table_excluded("users_backup_2024", patterns) is True
        assert dumper._is_table_excluded("orders_backup_old", patterns) is True
        assert dumper._is_table_excluded("users_backup", patterns) is False

    def test_underscore_prefix(self, dumper):
        """Test underscore prefix pattern."""
        patterns = ["_*"]
        assert dumper._is_table_excluded("_hidden", patterns) is True
        assert dumper._is_table_excluded("_temp_data", patterns) is True
        assert dumper._is_table_excluded("users", patterns) is False

    def test_multiple_patterns(self, dumper):
        """Test multiple patterns."""
        patterns = ["*_backup", "tmp_*", "test_*"]
        assert dumper._is_table_excluded("users_backup", patterns) is True
        assert dumper._is_table_excluded("tmp_data", patterns) is True
        assert dumper._is_table_excluded("test_table", patterns) is True
        assert dumper._is_table_excluded("users", patterns) is False

    def test_with_compiled_patterns(self, dumper):
        """Test with pre-compiled patterns."""
        patterns = ["*_backup", "tmp_*"]
        compiled = dumper._compile_exclusion_patterns(patterns)

        assert dumper._is_table_excluded(
            "users_backup", patterns, compiled
        ) is True
        assert dumper._is_table_excluded(
            "tmp_data", patterns, compiled
        ) is True
        assert dumper._is_table_excluded(
            "users", patterns, compiled
        ) is False

    def test_empty_patterns(self, dumper):
        """Test with no patterns."""
        assert dumper._is_table_excluded("any_table", []) is False


class TestFilterDatabases:
    """Tests for _filter_databases method."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config with multiple databases."""
        config = mock.MagicMock()
        config.get_databases.return_value = [
            {"name": "db1", "instance": "primary"},
            {"name": "db2", "instance": "primary"},
            {"name": "db3", "instance": "secondary"},
        ]
        config.get_output_settings.return_value = {}
        config.get_defaults.return_value = {}
        return config

    def test_no_filters(self, mock_config):
        """Test with no filters returns all databases."""
        dumper = DatabaseDumper(mock_config)
        result = dumper._filter_databases(None, None)
        assert len(result) == 3

    def test_database_filter(self, mock_config):
        """Test filtering by database name."""
        dumper = DatabaseDumper(mock_config)
        result = dumper._filter_databases("db1", None)
        assert len(result) == 1
        assert result[0]["name"] == "db1"

    def test_instance_filter(self, mock_config):
        """Test filtering by instance."""
        dumper = DatabaseDumper(mock_config)
        result = dumper._filter_databases(None, "primary")
        assert len(result) == 2
        assert all(db["instance"] == "primary" for db in result)

    def test_both_filters(self, mock_config):
        """Test filtering by both database and instance."""
        dumper = DatabaseDumper(mock_config)
        result = dumper._filter_databases("db1", "primary")
        assert len(result) == 1
        assert result[0]["name"] == "db1"
        assert result[0]["instance"] == "primary"

    def test_database_not_found(self, mock_config):
        """Test filtering for non-existent database."""
        dumper = DatabaseDumper(mock_config)
        result = dumper._filter_databases("nonexistent", None)
        assert len(result) == 0

    def test_instance_not_found(self, mock_config):
        """Test filtering for non-existent instance."""
        dumper = DatabaseDumper(mock_config)
        result = dumper._filter_databases(None, "nonexistent")
        assert len(result) == 0

    def test_default_instance(self):
        """Test filtering uses 'primary' as default instance."""
        config = mock.MagicMock()
        config.get_databases.return_value = [
            {"name": "db1"},  # No instance specified
            {"name": "db2", "instance": "primary"},
        ]
        config.get_output_settings.return_value = {}
        config.get_defaults.return_value = {}

        dumper = DatabaseDumper(config)
        result = dumper._filter_databases(None, "primary")

        # Both should match since default is 'primary'
        assert len(result) == 2

    def test_nameless_entry_does_not_crash_database_filter(self):
        """A DB entry missing 'name' must not KeyError when --database filtering."""
        config = mock.MagicMock()
        config.get_databases.return_value = [
            {"instance": "primary"},            # nameless — must be tolerated
            {"name": "db1", "instance": "primary"},
        ]
        config.get_output_settings.return_value = {}
        config.get_defaults.return_value = {}
        dumper = DatabaseDumper(config)
        result = dumper._filter_databases("db1", None)
        assert len(result) == 1
        assert result[0]["name"] == "db1"


class TestDatabaseDumperInit:
    """Tests for DatabaseDumper initialization."""

    def test_init(self):
        """Test DatabaseDumper initialization."""
        mock_config = mock.MagicMock()
        mock_config.get_output_settings.return_value = {"directory": "./dumps"}
        mock_config.get_defaults.return_value = {"row_limit": 1000}

        dumper = DatabaseDumper(mock_config)

        assert dumper.config == mock_config
        assert dumper.output_settings == {"directory": "./dumps"}
        assert dumper.defaults == {"row_limit": 1000}
        assert isinstance(dumper.stats, DumpStats)

    def test_stats_initialized_empty(self):
        """Test stats are initialized as empty."""
        mock_config = mock.MagicMock()
        mock_config.get_output_settings.return_value = {}
        mock_config.get_defaults.return_value = {}

        dumper = DatabaseDumper(mock_config)

        assert dumper.stats.databases == []
        assert dumper.stats.total_tables == 0
        assert dumper.stats.total_rows == 0
        assert dumper.stats.errors == []


class TestRun:
    """Tests for run method."""

    @pytest.fixture
    def mock_config(self):
        """Create a comprehensive mock config."""
        config = mock.MagicMock()
        config.get_databases.return_value = [
            {"name": "testdb", "instance": "primary", "tables": "*"}
        ]
        config.get_output_settings.return_value = {"directory": "./dumps"}
        config.get_defaults.return_value = {}
        config.get_instance.return_value = {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "secret"
        }
        return config

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_run_creates_output_directory(self, mock_conn_class, mock_config):
        """Test that run creates the output directory."""
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "new_dumps"
            mock_config.get_output_settings.return_value = {
                "directory": str(output_dir)
            }

            dumper = DatabaseDumper(mock_config)
            dumper.run()

            assert output_dir.exists()

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_run_returns_stats(self, mock_conn_class, mock_config):
        """Test that run returns DumpStats."""
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_config.get_output_settings.return_value = {
                "directory": tmpdir
            }

            dumper = DatabaseDumper(mock_config)
            result = dumper.run()

            assert isinstance(result, DumpStats)

    def test_run_with_filters(self, mock_config):
        """Test run with database and instance filters."""
        mock_config.get_databases.return_value = []  # No matching databases

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_config.get_output_settings.return_value = {
                "directory": tmpdir
            }

            dumper = DatabaseDumper(mock_config)
            result = dumper.run(
                database_filter="nonexistent",
                instance_filter="nonexistent"
            )

            # Should return stats with empty databases
            assert result.databases == []


class TestDumpDatabase:
    """Tests for _dump_database method."""

    @pytest.fixture
    def mock_config(self):
        """Create a comprehensive mock config."""
        config = mock.MagicMock()
        config.get_output_settings.return_value = {
            "directory": "./dumps",
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": True,
        }
        config.get_defaults.return_value = {}
        config.get_instance.return_value = {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "secret",
        }
        return config

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_dump_database_connection_error(self, mock_conn_class, mock_config):
        """Test _dump_database handles connection errors."""
        mock_conn_class.side_effect = Exception("Connection refused")

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "instance": "primary", "tables": "*"}

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(db_config, Path(tmpdir), "20240101_120000")

        assert len(dumper.stats.errors) == 1
        assert dumper.stats.errors[0]["database"] == "testdb"
        assert dumper.stats.errors[0]["table"] is None
        assert len(dumper.stats.databases) == 1

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_consistent_snapshot_on_by_default(self, mock_conn_class, mock_config):
        """With no config flag, a consistent-snapshot txn is started."""
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"name": "testdb", "instance": "primary", "tables": "*"},
                Path(tmpdir), "20240101_120000"
            )

        mock_conn.start_consistent_snapshot.assert_called_once()

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_consistent_snapshot_skipped_when_disabled(self, mock_conn_class, mock_config):
        """consistent_snapshot: false skips the transaction (e.g. MyISAM)."""
        mock_config.get_instance.return_value = {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "secret", "consistent_snapshot": False,
        }
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"name": "testdb", "instance": "primary", "tables": "*"},
                Path(tmpdir), "20240101_120000"
            )

        mock_conn.start_consistent_snapshot.assert_not_called()

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_tls_options_passed_to_connection(self, mock_conn_class, mock_config):
        """Instance TLS settings are passed into DatabaseConnection."""
        mock_config.get_instance.return_value = {
            "host": "db.example.com",
            "port": 3306,
            "user": "root",
            "password": "secret",
            "ssl_ca": "/certs/ca.pem",
            "ssl_cert": "/certs/client-cert.pem",
            "ssl_key": "/certs/client-key.pem",
            "ssl_verify_cert": True,
            "consistent_snapshot": False,
        }
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"name": "testdb", "instance": "primary", "tables": "*"},
                Path(tmpdir), "20240101_120000"
            )

        mock_conn_class.assert_called_once_with(
            host="db.example.com",
            port=3306,
            user="root",
            password="secret",
            database="testdb",
            connect_timeout=30,
            ssl_ca="/certs/ca.pem",
            ssl_cert="/certs/client-cert.pem",
            ssl_key="/certs/client-key.pem",
            ssl_verify_cert=True,
        )

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_missing_name_records_error_and_continues(self, mock_conn_class, mock_config):
        """A database entry with no name is skipped (no connection), error recorded."""
        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"instance": "primary", "tables": "*"}, Path(tmpdir), "20240101_120000"
            )
        assert len(dumper.stats.errors) == 1
        assert "name" in dumper.stats.errors[0]["error"]
        mock_conn_class.assert_not_called()


class TestProcessDatabaseTables:
    """Tests for _process_database_tables method."""

    @pytest.fixture
    def mock_config(self):
        config = mock.MagicMock()
        config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": True,
        }
        config.get_defaults.return_value = {}
        return config

    @mock.patch('src.database_dumper.TableDumper')
    def test_process_tables_separate_files(self, mock_td_class, mock_config):
        """Test processing tables with separate files mode."""
        mock_td = mock.MagicMock()
        mock_td.dump_table.return_value = TableStats(
            table="users", rows_dumped=10, success=True
        )
        mock_td_class.return_value = mock_td

        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users"]

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                mock_conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )

        assert len(db_stats.tables) == 1
        assert db_stats.total_rows == 10
        assert dumper.stats.total_tables == 1

    @mock.patch('src.database_dumper.TableDumper')
    def test_process_tables_single_file(self, mock_td_class, mock_config):
        """Test processing tables with single file mode."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
        }

        mock_td = mock.MagicMock()
        mock_td._resolve_output_paths.side_effect = lambda output_path, append=False, defer_publish=False: (
            output_path,
            Path(str(output_path) + ".partial"),
        )

        def dump_side_effect(**kwargs):
            Path(str(kwargs["output_path"]) + ".partial").write_text("users dump")
            return TableStats(table="users", rows_dumped=5, success=True)

        mock_td.dump_table.side_effect = dump_side_effect
        mock_td_class.return_value = mock_td

        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users"]

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                mock_conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            published = Path(tmpdir) / "testdb_20240101_120000.sql"
            assert published.exists()

        assert len(db_stats.tables) == 1

    @mock.patch('src.database_dumper.TableDumper')
    def test_process_tables_no_timestamp_suffix(self, mock_td_class, mock_config):
        """Test processing tables without timestamp suffix."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": False,
        }

        mock_td = mock.MagicMock()
        mock_td.dump_table.return_value = TableStats(
            table="users", rows_dumped=3, success=True
        )
        mock_td_class.return_value = mock_td

        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users"]

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                mock_conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )

        assert len(db_stats.tables) == 1


class TestGetTablesToDump:
    """Tests for _get_tables_to_dump method."""

    @pytest.fixture
    def dumper(self):
        config = mock.MagicMock()
        config.get_output_settings.return_value = {}
        config.get_defaults.return_value = {}
        return DatabaseDumper(config)

    def test_all_tables_no_exclusions(self, dumper):
        """Test getting all tables without exclusions."""
        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users", "orders"]
        db_config = {"tables": "*"}

        result = dumper._get_tables_to_dump(mock_conn, db_config)
        assert result == [{"name": "users"}, {"name": "orders"}]

    def test_all_tables_with_exclusions(self, dumper):
        """Test getting all tables with exclusion patterns."""
        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users", "users_backup", "orders"]
        db_config = {"tables": "*", "exclude_tables": ["*_backup"]}

        result = dumper._get_tables_to_dump(mock_conn, db_config)
        assert result == [{"name": "users"}, {"name": "orders"}]

    def test_explicit_table_list(self, dumper):
        """Test with explicit table list (no exclusions)."""
        mock_conn = mock.MagicMock()
        db_config = {
            "tables": [
                {"name": "users", "row_limit": 100},
                {"name": "orders"},
            ]
        }

        result = dumper._get_tables_to_dump(mock_conn, db_config)
        assert len(result) == 2

    def test_explicit_table_list_with_exclusions(self, dumper):
        """Test explicit table list with exclusion patterns."""
        mock_conn = mock.MagicMock()
        db_config = {
            "tables": [
                {"name": "users"},
                {"name": "users_backup"},
                "orders",
            ],
            "exclude_tables": ["*_backup"],
        }

        result = dumper._get_tables_to_dump(mock_conn, db_config)
        assert len(result) == 2

    def test_explicit_string_table_with_exclusion(self, dumper):
        """Test explicit string table entries with exclusion."""
        mock_conn = mock.MagicMock()
        db_config = {
            "tables": ["users", "tmp_data", "orders"],
            "exclude_tables": ["tmp_*"],
        }

        result = dumper._get_tables_to_dump(mock_conn, db_config)
        assert len(result) == 2

    def test_tables_null_treated_as_all(self, dumper):
        """tables: null means 'all tables', not a crash."""
        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users", "orders"]
        result = dumper._get_tables_to_dump(mock_conn, {"tables": None})
        assert result == [{"name": "users"}, {"name": "orders"}]


class TestDumpSingleTable:
    """Tests for _dump_single_table method."""

    @pytest.fixture
    def mock_config(self):
        config = mock.MagicMock()
        config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": True,
        }
        config.get_defaults.return_value = {}
        return config

    def test_dump_single_table_separate_files(self, mock_config):
        """Test dumping single table with separate files."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="users", rows_dumped=10, success=True
        )

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb"}

        with tempfile.TemporaryDirectory() as tmpdir:
            result = dumper._dump_single_table(
                mock_dumper, {"name": "users"}, db_config,
                Path(tmpdir), OutputFormat.SQL, True, "20240101", is_first=True
            )

        assert result.table == "users"
        assert result.rows_dumped == 10

    def test_dump_single_table_string_config(self, mock_config):
        """Test dumping table when config is a string."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="users", rows_dumped=5, success=True
        )

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb"}

        with tempfile.TemporaryDirectory() as tmpdir:
            result = dumper._dump_single_table(
                mock_dumper, "users", db_config,
                Path(tmpdir), OutputFormat.SQL, True, "20240101", is_first=True
            )

        assert result.table == "users"

    def test_dump_single_table_single_file_mode(self, mock_config):
        """Test dumping single table in single-file mode."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="users", rows_dumped=5, success=True
        )

        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
        }
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb"}

        with tempfile.TemporaryDirectory() as tmpdir:
            result = dumper._dump_single_table(
                mock_dumper, {"name": "users"}, db_config,
                Path(tmpdir), OutputFormat.SQL, False, "20240101", is_first=True
            )

        assert result.table == "users"
        # Verify dump_table called with append=False for first table
        call_args = mock_dumper.dump_table.call_args
        assert call_args.kwargs.get('append', call_args[1].get('append')) is False

    def test_dump_single_table_single_file_append_deferred(self, mock_config):
        """Second single-file table appends to the deferred partial file."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="orders", rows_dumped=3, success=True
        )

        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
        }
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb"}

        with tempfile.TemporaryDirectory() as tmpdir:
            result = dumper._dump_single_table(
                mock_dumper, {"name": "orders"}, db_config,
                Path(tmpdir), OutputFormat.SQL, False, "20240101", is_first=False,
                defer_publish=True
            )

        # Verify append=True for non-first table
        call_args = mock_dumper.dump_table.call_args
        assert call_args.kwargs.get('append', call_args[1].get('append')) is True
        assert call_args.kwargs.get('defer_publish', call_args[1].get('defer_publish')) is True

    @mock.patch('src.database_dumper.TableDumper')
    def test_single_file_failure_does_not_publish_final_file(self, mock_td_class, mock_config):
        """A later table failure leaves only the partial single-file artifact."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
        }

        def dump_side_effect(**kwargs):
            output_path = Path(kwargs["output_path"])
            partial_path = Path(str(output_path) + ".partial")
            if kwargs["table"] == "users":
                partial_path.write_text("users dump")
                return TableStats(table="users", rows_dumped=1, success=True)
            with partial_path.open("a", encoding="utf-8") as handle:
                handle.write("\norders partial")
            return TableStats(table="orders", rows_dumped=0, success=False, error="boom")

        mock_td = mock.MagicMock()
        mock_td._resolve_output_paths.side_effect = lambda output_path, append=False, defer_publish=False: (
            output_path,
            Path(str(output_path) + ".partial"),
        )
        mock_td.dump_table.side_effect = dump_side_effect
        mock_td_class.return_value = mock_td

        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users", "orders", "ignored"]

        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                mock_conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            final_path = Path(tmpdir) / "testdb_20240101_120000.sql"
            partial_path = Path(str(final_path) + ".partial")

            assert not final_path.exists()
            assert partial_path.exists()

        assert [table.table for table in db_stats.tables] == ["users", "orders"]
        assert len(dumper.stats.errors) == 1

    def test_dump_single_table_no_timestamp_suffix(self, mock_config):
        """Test dumping table in single-file mode without timestamp suffix."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="users", rows_dumped=5, success=True
        )

        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": False,
        }
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb"}

        with tempfile.TemporaryDirectory() as tmpdir:
            result = dumper._dump_single_table(
                mock_dumper, {"name": "users"}, db_config,
                Path(tmpdir), OutputFormat.SQL, False, "20240101", is_first=True
            )

        assert result.table == "users"

    def test_table_name_path_traversal_sanitized(self, mock_config):
        """A table name with .. must not escape the output directory."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="x", rows_dumped=1, success=True
        )
        dumper = DatabaseDumper(mock_config)

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_single_table(
                mock_dumper, {"name": "../../evil"}, {"name": "db"},
                Path(tmpdir), OutputFormat.SQL, True, "20240101", is_first=True
            )

        out = Path(mock_dumper.dump_table.call_args.kwargs["output_path"])
        assert ".." not in out.parts
        assert out.name == "evil.sql"

    def test_db_name_path_traversal_sanitized_single_file(self, mock_config):
        """A db name with .. must not escape the output directory in single-file mode."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="x", rows_dumped=1, success=True
        )
        dumper = DatabaseDumper(mock_config)

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_single_table(
                mock_dumper, {"name": "users"}, {"name": "../../evil"},
                Path(tmpdir), OutputFormat.SQL, False, "20240101", is_first=True
            )

        out = Path(mock_dumper.dump_table.call_args.kwargs["output_path"])
        assert ".." not in out.parts
        assert out.name.startswith("evil")


class TestCombinedFkWrapper:
    """Combined single-file dumps emit exactly one FK-check wrapper pair."""

    @pytest.fixture
    def mock_config(self):
        config = mock.MagicMock()
        config.get_defaults.return_value = {}
        return config

    def _mock_conn(self, tables):
        """A connection mock whose cursor yields one row per table dump."""
        conn = mock.MagicMock()
        conn.get_tables.return_value = tables
        conn.get_table_columns.return_value = [
            ColumnInfo("id", "int(11)", "NO", "PRI", None, ""),
        ]
        conn.get_create_table.side_effect = lambda t: f"CREATE TABLE `{t}` (`id` int)"

        def fresh_cursor(*args, **kwargs):
            cur = mock.MagicMock()
            cur.__iter__ = mock.MagicMock(return_value=iter([(1,)]))
            return cur

        conn.get_cursor.side_effect = fresh_cursor
        return conn

    def test_combined_full_dump_one_wrapper_pair(self, mock_config):
        """Two-table combined dump: exactly one =0 at top, one =1 at end, none between."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn(["users", "orders"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            published = Path(tmpdir) / "testdb_20240101_120000.sql"
            content = published.read_text()

        assert content.count("SET FOREIGN_KEY_CHECKS=0;") == 1
        assert content.count("SET FOREIGN_KEY_CHECKS=1;") == 1
        # =0 before the first CREATE, =1 after the last CREATE, none between tables.
        assert content.index("SET FOREIGN_KEY_CHECKS=0;") < content.index("CREATE TABLE `users`")
        assert content.index("CREATE TABLE `orders`") < content.index("SET FOREIGN_KEY_CHECKS=1;")
        # No stray =1 between the two CREATE statements.
        between = content[content.index("CREATE TABLE `users`"):content.index("CREATE TABLE `orders`")]
        assert "FOREIGN_KEY_CHECKS=1" not in between

    def test_combined_with_partial_table_is_unwrapped(self, mock_config):
        """If any table in a combined dump is partial, the whole file is unwrapped."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn(["users", "orders"])
        dumper = DatabaseDumper(mock_config)
        # 'orders' is partial via a per-table row_limit.
        db_config = {
            "name": "testdb",
            "tables": [{"name": "users"}, {"name": "orders", "row_limit": 5}],
        }
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            published = Path(tmpdir) / "testdb_20240101_120000.sql"
            content = published.read_text()

        assert "FOREIGN_KEY_CHECKS" not in content

    def test_combined_dump_disabled_by_option(self, mock_config):
        """disable_foreign_key_checks=False suppresses the combined wrapper."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
            "compress": False,
            "disable_foreign_key_checks": False,
        }
        conn = self._mock_conn(["users", "orders"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            published = Path(tmpdir) / "testdb_20240101_120000.sql"
            content = published.read_text()

        assert "FOREIGN_KEY_CHECKS" not in content

    def test_combined_compressed_dump_is_unwrapped(self, mock_config):
        """compress=True forces the combined wrapper off to avoid corrupting the gzip stream."""
        import gzip

        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
            "compress": True,
        }
        conn = self._mock_conn(["users", "orders"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            published = Path(tmpdir) / "testdb_20240101_120000.sql.gz"
            assert published.exists()
            content = gzip.open(published, "rt", encoding="utf-8").read()

        assert "FOREIGN_KEY_CHECKS" not in content

    def test_combined_empty_table_set_publishes_nothing(self, mock_config):
        """Empty table set in combined mode must not publish any file (no spurious wrapper-only file)."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn([])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            published = Path(tmpdir) / "testdb_20240101_120000.sql"
            partial = Path(tmpdir) / "testdb_20240101_120000.sql.partial"
            assert not published.exists(), "No file should be published for an empty table set"
            assert not partial.exists(), "No .partial file should be left for an empty table set"


class TestLogTableResult:
    """Tests for _log_table_result method."""

    @pytest.fixture
    def dumper(self):
        config = mock.MagicMock()
        config.get_output_settings.return_value = {}
        config.get_defaults.return_value = {}
        return DatabaseDumper(config)

    def test_log_successful_table(self, dumper, caplog):
        """Test logging successful table dump."""
        import logging
        table_stats = TableStats(table="users", rows_dumped=100, success=True)

        with caplog.at_level(logging.INFO):
            dumper._log_table_result(table_stats, "testdb")

        assert "users" in caplog.text
        assert "100 rows" in caplog.text

    def test_log_failed_table(self, dumper, caplog):
        """Test logging failed table dump."""
        import logging
        table_stats = TableStats(
            table="orders", success=False, error="Connection lost"
        )

        with caplog.at_level(logging.ERROR):
            dumper._log_table_result(table_stats, "testdb")

        assert "orders" in caplog.text
        assert "Connection lost" in caplog.text
        assert len(dumper.stats.errors) == 1
        assert dumper.stats.errors[0]["database"] == "testdb"
        assert dumper.stats.errors[0]["table"] == "orders"


class TestRestoreHelper:
    """restore.sh generation for separate-files SQL dumps."""

    @pytest.fixture
    def mock_config(self):
        config = mock.MagicMock()
        config.get_defaults.return_value = {}
        return config

    def _mock_conn(self, tables):
        conn = mock.MagicMock()
        conn.get_tables.return_value = tables
        conn.get_table_columns.return_value = [
            ColumnInfo("id", "int(11)", "NO", "PRI", None, ""),
        ]
        conn.get_create_table.side_effect = lambda t: f"CREATE TABLE `{t}` (`id` int)"

        def fresh_cursor(*args, **kwargs):
            cur = mock.MagicMock()
            cur.__iter__ = mock.MagicMock(return_value=iter([(1,)]))
            return cur

        conn.get_cursor.side_effect = fresh_cursor
        return conn

    def test_restore_helper_written_for_separate_sql(self, mock_config):
        """Separate-files SQL dump writes an executable restore.sh that globs *.sql."""
        import os
        import stat
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn(["users", "orders"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            helper = Path(tmpdir) / "testdb_20240101_120000" / "restore.sh"
            assert helper.exists()
            content = helper.read_text()
            assert content.count("SET FOREIGN_KEY_CHECKS=0;") == 1
            assert content.count("SET FOREIGN_KEY_CHECKS=1;") == 1
            assert "*.sql" in content
            assert "zcat" not in content
            # Executable bit set.
            mode = os.stat(helper).st_mode
            assert mode & stat.S_IXUSR

    def test_restore_helper_uses_zcat_when_compressed(self, mock_config):
        """Compressed separate dump produces a helper that globs *.sql.gz via zcat."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": True,
            "compress": True,
        }
        conn = self._mock_conn(["users"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            helper = Path(tmpdir) / "testdb_20240101_120000" / "restore.sh"
            assert helper.exists()
            content = helper.read_text()
            assert "*.sql.gz" in content
            assert "zcat" in content

    def test_no_restore_helper_for_csv(self, mock_config):
        """CSV separate dump writes no restore.sh."""
        mock_config.get_output_settings.return_value = {
            "format": "csv",
            "separate_files": True,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn(["users"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            helper = Path(tmpdir) / "testdb_20240101_120000" / "restore.sh"
            assert not helper.exists()

    def test_no_restore_helper_for_combined(self, mock_config):
        """Combined single-file dump writes no restore.sh."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": False,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn(["users"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": "*"}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            helper = Path(tmpdir) / "restore.sh"
            assert not helper.exists()
            helper2 = Path(tmpdir) / "testdb_20240101_120000" / "restore.sh"
            assert not helper2.exists()

    def test_no_restore_helper_when_all_partial(self, mock_config):
        """If every separate table is partial (none wrapped), write no restore.sh."""
        mock_config.get_output_settings.return_value = {
            "format": "sql",
            "separate_files": True,
            "timestamp_suffix": True,
            "compress": False,
        }
        conn = self._mock_conn(["users"])
        dumper = DatabaseDumper(mock_config)
        db_config = {"name": "testdb", "tables": [{"name": "users", "row_limit": 5}]}
        db_stats = DatabaseStats(name="testdb", instance="primary")

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._process_database_tables(
                conn, db_config, db_stats, Path(tmpdir), "20240101_120000"
            )
            helper = Path(tmpdir) / "testdb_20240101_120000" / "restore.sh"
            assert not helper.exists()


class TestFkWrapperHelperBranches:
    """Unit tests for _table_was_fk_wrapped and _combined_dump_wrappable branches."""

    @pytest.fixture
    def dumper_fk_disabled(self):
        """DatabaseDumper with disable_foreign_key_checks=False (feature off)."""
        config = mock.MagicMock()
        config.get_output_settings.return_value = {'disable_foreign_key_checks': False}
        config.get_defaults.return_value = {}
        return DatabaseDumper(config)

    @pytest.fixture
    def dumper_fk_enabled(self):
        """DatabaseDumper with disable_foreign_key_checks=True (feature on)."""
        config = mock.MagicMock()
        config.get_output_settings.return_value = {'disable_foreign_key_checks': True}
        config.get_defaults.return_value = {}
        return DatabaseDumper(config)

    # --- line 333: _table_was_fk_wrapped returns False when option is off ---

    def test_table_was_fk_wrapped_returns_false_when_option_off(self, dumper_fk_disabled):
        """Line 333: early return False when disable_foreign_key_checks is False."""
        stats = TableStats(table='users', success=True)
        result = dumper_fk_disabled._table_was_fk_wrapped(stats, {'name': 'testdb', 'tables': '*'})
        assert result is False

    # --- line 399: _combined_dump_wrappable returns False for non-SQL format ---

    def test_combined_dump_wrappable_false_for_csv(self, dumper_fk_enabled):
        """Line 399: early return False when output_format is not SQL."""
        result = dumper_fk_enabled._combined_dump_wrappable(
            [{'name': 'users'}],
            {'name': 'testdb', 'tables': '*'},
            OutputFormat.CSV,
        )
        assert result is False

    # --- line 405: string table entries are normalised to dicts ---

    def test_combined_dump_wrappable_string_entries_normalised(self, dumper_fk_enabled):
        """Line 405: string table entries are coerced to {'name': ...} dicts."""
        result = dumper_fk_enabled._combined_dump_wrappable(
            ['users', 'orders'],
            {'name': 'testdb', 'tables': ['users', 'orders']},
            OutputFormat.SQL,
        )
        # Both tables are full dumps (no row_limit/where_clause), so wrappable.
        assert result is True
