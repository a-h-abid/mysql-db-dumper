"""
Unit tests for utils.py
"""

import logging
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from src.models import DumpSettings
from src.utils import setup_logging, format_settings_display, print_dry_run_info


class TestSetupLogging:
    """Tests for setup_logging function."""

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        """Reset logging configuration before each test."""
        root_logger = logging.getLogger()
        # Remove all handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        # Reset level to NOTSET so basicConfig will work
        root_logger.setLevel(logging.NOTSET)
        yield

    def test_default_log_level(self):
        """Test default log level is INFO."""
        setup_logging({})
        assert logging.getLogger().level == logging.INFO

    def test_custom_log_level(self):
        """Test setting custom log level."""
        setup_logging({"level": "DEBUG"})
        assert logging.getLogger().level == logging.DEBUG

    def test_log_level_case_insensitive(self):
        """Test log level is case insensitive."""
        setup_logging({"level": "warning"})
        assert logging.getLogger().level == logging.WARNING

    def test_log_to_file(self):
        """Test logging to a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "test.log"
            setup_logging({"file": str(log_file)})

            # Log a message
            logging.info("Test message")

            # Check the file was created
            assert log_file.exists()

    def test_creates_log_directory(self):
        """Test that log directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / "nested" / "dir" / "test.log"
            setup_logging({"file": str(log_file)})

            # Directory should be created
            assert log_file.parent.exists()


class TestFormatSettingsDisplay:
    """Tests for format_settings_display function."""

    def test_empty_settings(self):
        """Test formatting with no settings."""
        settings = DumpSettings()
        parts = format_settings_display(settings)
        assert parts == []

    def test_row_limit_only(self):
        """Test formatting with only row_limit."""
        settings = DumpSettings(row_limit=1000)
        parts = format_settings_display(settings)
        assert parts == ["limit=1000"]

    def test_order_by_with_direction(self):
        """Test formatting with order_by and direction."""
        settings = DumpSettings(order_by="created_at", order_direction="DESC")
        parts = format_settings_display(settings)
        assert parts == ["order=created_at DESC"]

    def test_where_clause(self):
        """Test formatting with where_clause."""
        settings = DumpSettings(where_clause="status = 'active'")
        parts = format_settings_display(settings)
        assert parts == ["where='status = 'active''"]

    def test_all_settings(self):
        """Test formatting with all settings."""
        settings = DumpSettings(
            row_limit=500,
            order_by="id",
            order_direction="ASC",
            where_clause="active = 1"
        )
        parts = format_settings_display(settings)
        assert "limit=500" in parts
        assert "order=id ASC" in parts
        assert "where='active = 1'" in parts
        assert len(parts) == 3

    def test_zero_row_limit(self):
        """Test formatting with zero row_limit (should still show)."""
        settings = DumpSettings(row_limit=0)
        parts = format_settings_display(settings)
        assert parts == ["limit=0"]


class TestPrintDryRunInfo:
    """Tests for print_dry_run_info function."""

    @pytest.fixture(autouse=True)
    def setup_logging(self):
        """Setup logging for tests."""
        logging.basicConfig(level=logging.INFO)

    def test_all_tables(self, caplog):
        """Test dry run info for all tables."""
        databases = [
            {"name": "testdb", "instance": "primary", "tables": "*"}
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "Would dump database: testdb from instance: primary" in caplog.text
        assert "All tables" in caplog.text

    def test_specific_tables_as_strings(self, caplog):
        """Test dry run info for specific tables as strings."""
        databases = [
            {
                "name": "testdb",
                "instance": "primary",
                "tables": ["users", "orders"]
            }
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "- users" in caplog.text
        assert "- orders" in caplog.text

    def test_tables_with_config(self, caplog):
        """Test dry run info for tables with configuration."""
        databases = [
            {
                "name": "testdb",
                "instance": "primary",
                "tables": [
                    {"name": "users", "row_limit": 1000},
                    {"name": "orders", "order_by": "id", "order_direction": "DESC"}
                ]
            }
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "users" in caplog.text
        assert "limit=1000" in caplog.text
        assert "orders" in caplog.text
        assert "order=id DESC" in caplog.text

    def test_tables_with_no_limits(self, caplog):
        """Test dry run info for tables without any limits."""
        databases = [
            {
                "name": "testdb",
                "instance": "primary",
                "tables": [{"name": "users"}]
            }
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "users (no limits)" in caplog.text

    def test_database_level_row_limit(self, caplog):
        """Test dry run info shows database-level row limit."""
        databases = [
            {
                "name": "testdb",
                "instance": "primary",
                "row_limit": 5000,
                "tables": "*"
            }
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "Database-level row_limit: 5000" in caplog.text

    def test_multiple_databases(self, caplog):
        """Test dry run info for multiple databases."""
        databases = [
            {"name": "db1", "instance": "primary", "tables": "*"},
            {"name": "db2", "instance": "secondary", "tables": "*"}
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "db1 from instance: primary" in caplog.text
        assert "db2 from instance: secondary" in caplog.text

    def test_default_instance(self, caplog):
        """Test dry run info uses 'primary' as default instance."""
        databases = [
            {"name": "testdb", "tables": "*"}  # No instance specified
        ]
        defaults = {}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "from instance: primary" in caplog.text

    def test_with_defaults_applied(self, caplog):
        """Test dry run info applies defaults to table configs."""
        databases = [
            {
                "name": "testdb",
                "instance": "primary",
                "tables": [{"name": "users"}]
            }
        ]
        defaults = {"row_limit": 1000}

        with caplog.at_level(logging.INFO):
            print_dry_run_info(databases, defaults)

        assert "users" in caplog.text
        assert "limit=1000" in caplog.text
