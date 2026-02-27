"""
Integration tests for MySQL Database Dumper.

These tests require a running MySQL instance. They are skipped
automatically when the MYSQL_HOST environment variable is not set.

Set the following environment variables to run:
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
"""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from src.config import ConfigLoader
from src.connection import DatabaseConnection
from src.database_dumper import DatabaseDumper
from src.models import OutputFormat


# Skip all tests in this module if MySQL is not available
pytestmark = pytest.mark.skipif(
    not os.environ.get("MYSQL_HOST"),
    reason="MYSQL_HOST not set; skipping integration tests",
)


@pytest.fixture
def mysql_config():
    """Get MySQL connection config from environment."""
    return {
        "host": os.environ.get("MYSQL_HOST", "127.0.0.1"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": os.environ.get("MYSQL_USER", "root"),
        "password": os.environ.get("MYSQL_PASSWORD", ""),
        "database": os.environ.get("MYSQL_DATABASE", "test_db"),
    }


class TestDatabaseConnectionIntegration:
    """Integration tests for DatabaseConnection."""

    def test_connect_and_disconnect(self, mysql_config):
        """Test connecting and disconnecting from a real MySQL instance."""
        conn = DatabaseConnection(**mysql_config)
        conn.connect()
        assert conn.connection is not None
        assert conn.connection.is_connected()
        conn.disconnect()

    def test_context_manager(self, mysql_config):
        """Test using DatabaseConnection as a context manager."""
        with DatabaseConnection(**mysql_config) as conn:
            assert conn.connection.is_connected()

    def test_get_tables(self, mysql_config):
        """Test listing tables from a real database."""
        with DatabaseConnection(**mysql_config) as conn:
            tables = conn.get_tables()
            assert "users" in tables
            assert "orders" in tables

    def test_get_table_columns(self, mysql_config):
        """Test getting column info from a real table."""
        with DatabaseConnection(**mysql_config) as conn:
            columns = conn.get_table_columns("users")
            column_names = [c.name for c in columns]
            assert "id" in column_names
            assert "name" in column_names

    def test_get_create_table(self, mysql_config):
        """Test getting CREATE TABLE statement."""
        with DatabaseConnection(**mysql_config) as conn:
            create_stmt = conn.get_create_table("users")
            assert "CREATE TABLE" in create_stmt
            assert "users" in create_stmt

    def test_get_row_count(self, mysql_config):
        """Test getting row count."""
        with DatabaseConnection(**mysql_config) as conn:
            count = conn.get_row_count("users")
            assert count == 3

    def test_get_row_count_with_where(self, mysql_config):
        """Test getting row count with WHERE clause."""
        with DatabaseConnection(**mysql_config) as conn:
            count = conn.get_row_count("users", where_clause="name = 'Alice'")
            assert count == 1

    def test_execute_query(self, mysql_config):
        """Test executing a query."""
        with DatabaseConnection(**mysql_config) as conn:
            results = conn.execute_query("SELECT name FROM users ORDER BY id")
            names = [row[0] for row in results]
            assert names == ["Alice", "Bob", "Charlie"]


class TestDatabaseDumperIntegration:
    """Integration tests for the full dump pipeline."""

    def test_dump_sql_format(self, mysql_config):
        """Test dumping a database in SQL format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_data = {
                "instances": {
                    "primary": {
                        "host": mysql_config["host"],
                        "port": mysql_config["port"],
                        "user": mysql_config["user"],
                        "password": mysql_config["password"],
                    }
                },
                "databases": [
                    {
                        "name": mysql_config["database"],
                        "instance": "primary",
                        "tables": "*",
                    }
                ],
                "defaults": {},
                "output": {
                    "directory": tmpdir,
                    "format": "sql",
                    "compress": False,
                    "separate_files": True,
                    "timestamp_suffix": False,
                },
                "logging": {"level": "DEBUG"},
            }

            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = ConfigLoader(config_path)
            dumper = DatabaseDumper(config)
            stats = dumper.run()

            assert len(stats.databases) == 1
            assert stats.total_tables >= 2
            assert stats.total_rows >= 6
            assert stats.errors == []

            # Verify SQL files were created
            db_dir = Path(tmpdir) / mysql_config["database"]
            assert db_dir.exists()
            assert (db_dir / "users.sql").exists()
            assert (db_dir / "orders.sql").exists()

            # Verify SQL content
            users_sql = (db_dir / "users.sql").read_text()
            assert "CREATE TABLE" in users_sql
            assert "INSERT INTO" in users_sql

    def test_dump_csv_format(self, mysql_config):
        """Test dumping a database in CSV format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_data = {
                "instances": {
                    "primary": {
                        "host": mysql_config["host"],
                        "port": mysql_config["port"],
                        "user": mysql_config["user"],
                        "password": mysql_config["password"],
                    }
                },
                "databases": [
                    {
                        "name": mysql_config["database"],
                        "instance": "primary",
                        "tables": [{"name": "users", "row_limit": 2}],
                    }
                ],
                "defaults": {},
                "output": {
                    "directory": tmpdir,
                    "format": "csv",
                    "compress": False,
                    "separate_files": True,
                    "timestamp_suffix": False,
                },
                "logging": {"level": "DEBUG"},
            }

            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = ConfigLoader(config_path)
            dumper = DatabaseDumper(config)
            stats = dumper.run()

            assert stats.total_rows == 2  # Limited to 2 rows
            assert stats.errors == []

    def test_dump_with_row_limit_and_order(self, mysql_config):
        """Test dumping with row limit and ordering."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_data = {
                "instances": {
                    "primary": {
                        "host": mysql_config["host"],
                        "port": mysql_config["port"],
                        "user": mysql_config["user"],
                        "password": mysql_config["password"],
                    }
                },
                "databases": [
                    {
                        "name": mysql_config["database"],
                        "instance": "primary",
                        "tables": [
                            {
                                "name": "users",
                                "row_limit": 1,
                                "order_by": "id",
                                "order_direction": "DESC",
                            }
                        ],
                    }
                ],
                "defaults": {},
                "output": {
                    "directory": tmpdir,
                    "format": "sql",
                    "compress": False,
                    "separate_files": True,
                    "timestamp_suffix": False,
                },
                "logging": {"level": "DEBUG"},
            }

            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = ConfigLoader(config_path)
            dumper = DatabaseDumper(config)
            stats = dumper.run()

            assert stats.total_rows == 1
            assert stats.errors == []

    def test_dump_compressed(self, mysql_config):
        """Test dumping with gzip compression."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_data = {
                "instances": {
                    "primary": {
                        "host": mysql_config["host"],
                        "port": mysql_config["port"],
                        "user": mysql_config["user"],
                        "password": mysql_config["password"],
                    }
                },
                "databases": [
                    {
                        "name": mysql_config["database"],
                        "instance": "primary",
                        "tables": [{"name": "users"}],
                    }
                ],
                "defaults": {},
                "output": {
                    "directory": tmpdir,
                    "format": "sql",
                    "compress": True,
                    "separate_files": True,
                    "timestamp_suffix": False,
                },
                "logging": {"level": "DEBUG"},
            }

            config_path = os.path.join(tmpdir, "config.yaml")
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            config = ConfigLoader(config_path)
            dumper = DatabaseDumper(config)
            stats = dumper.run()

            assert stats.errors == []

            # Verify compressed file exists
            db_dir = Path(tmpdir) / mysql_config["database"]
            gz_files = list(db_dir.glob("*.gz"))
            assert len(gz_files) >= 1
