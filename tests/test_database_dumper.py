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
from src.models import DatabaseStats, DumpStats


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
