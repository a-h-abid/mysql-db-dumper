"""
Unit tests for models.py
"""

import pytest
from src.models import (
    OutputFormat,
    OrderDirection,
    ColumnInfo,
    TableStats,
    DatabaseStats,
    DumpStats,
    DumpSettings,
)


class TestOutputFormat:
    """Tests for OutputFormat enum."""

    def test_sql_format(self):
        assert OutputFormat.SQL.value == "sql"
        assert OutputFormat.SQL.extension == "sql"

    def test_csv_format(self):
        assert OutputFormat.CSV.value == "csv"
        assert OutputFormat.CSV.extension == "csv"

    def test_format_from_string(self):
        assert OutputFormat("sql") == OutputFormat.SQL
        assert OutputFormat("csv") == OutputFormat.CSV

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            OutputFormat("xml")


class TestOrderDirection:
    """Tests for OrderDirection enum."""

    def test_asc_direction(self):
        assert OrderDirection.ASC.value == "ASC"

    def test_desc_direction(self):
        assert OrderDirection.DESC.value == "DESC"


class TestColumnInfo:
    """Tests for ColumnInfo dataclass."""

    def test_column_info_creation(self):
        col = ColumnInfo(
            name="id",
            type="int(11)",
            nullable="NO",
            key="PRI",
            default=None,
            extra="auto_increment"
        )
        assert col.name == "id"
        assert col.type == "int(11)"
        assert col.nullable == "NO"
        assert col.key == "PRI"
        assert col.default is None
        assert col.extra == "auto_increment"

    def test_column_info_with_default(self):
        col = ColumnInfo(
            name="status",
            type="varchar(20)",
            nullable="YES",
            key="",
            default="active",
            extra=""
        )
        assert col.default == "active"


class TestTableStats:
    """Tests for TableStats dataclass."""

    def test_default_values(self):
        stats = TableStats(table="users")
        assert stats.table == "users"
        assert stats.rows_dumped == 0
        assert stats.file_path == ""
        assert stats.success is False
        assert stats.error is None

    def test_successful_dump(self):
        stats = TableStats(
            table="orders",
            rows_dumped=1500,
            file_path="/dumps/orders.sql",
            success=True
        )
        assert stats.rows_dumped == 1500
        assert stats.success is True

    def test_failed_dump(self):
        stats = TableStats(
            table="products",
            error="Connection timeout"
        )
        assert stats.success is False
        assert stats.error == "Connection timeout"


class TestDatabaseStats:
    """Tests for DatabaseStats dataclass."""

    def test_default_values(self):
        stats = DatabaseStats(name="mydb", instance="primary")
        assert stats.name == "mydb"
        assert stats.instance == "primary"
        assert stats.tables == []
        assert stats.total_rows == 0

    def test_with_tables(self):
        table1 = TableStats(table="users", rows_dumped=100, success=True)
        table2 = TableStats(table="orders", rows_dumped=200, success=True)
        stats = DatabaseStats(
            name="ecommerce",
            instance="primary",
            tables=[table1, table2],
            total_rows=300
        )
        assert len(stats.tables) == 2
        assert stats.total_rows == 300


class TestDumpStats:
    """Tests for DumpStats dataclass."""

    def test_default_values(self):
        stats = DumpStats()
        assert stats.databases == []
        assert stats.total_tables == 0
        assert stats.total_rows == 0
        assert stats.errors == []

    def test_with_data(self):
        db_stats = DatabaseStats(name="testdb", instance="primary")
        stats = DumpStats(
            databases=[db_stats],
            total_tables=5,
            total_rows=1000,
            errors=[{"database": "faildb", "error": "Connection failed"}]
        )
        assert len(stats.databases) == 1
        assert stats.total_tables == 5
        assert stats.total_rows == 1000
        assert len(stats.errors) == 1


class TestDumpSettings:
    """Tests for DumpSettings dataclass."""

    def test_default_values(self):
        settings = DumpSettings()
        assert settings.row_limit is None
        assert settings.order_by is None
        assert settings.order_direction == "ASC"
        assert settings.where_clause is None

    def test_with_all_values(self):
        settings = DumpSettings(
            row_limit=1000,
            order_by="created_at",
            order_direction="DESC",
            where_clause="status = 'active'"
        )
        assert settings.row_limit == 1000
        assert settings.order_by == "created_at"
        assert settings.order_direction == "DESC"
        assert settings.where_clause == "status = 'active'"

    def test_from_configs_defaults_only(self):
        defaults = {
            "row_limit": 500,
            "order_by": "id",
            "order_direction": "ASC"
        }
        settings = DumpSettings.from_configs(defaults, {}, {})
        assert settings.row_limit == 500
        assert settings.order_by == "id"
        assert settings.order_direction == "ASC"
        assert settings.where_clause is None

    def test_from_configs_db_overrides_defaults(self):
        defaults = {"row_limit": 500, "order_by": "id"}
        db_config = {"row_limit": 1000}
        settings = DumpSettings.from_configs(defaults, db_config, {})
        assert settings.row_limit == 1000  # Overridden by db_config
        assert settings.order_by == "id"  # From defaults

    def test_from_configs_table_overrides_all(self):
        defaults = {"row_limit": 500, "order_by": "id", "order_direction": "ASC"}
        db_config = {"row_limit": 1000, "order_direction": "DESC"}
        table_config = {"row_limit": 100, "where_clause": "active = 1"}
        settings = DumpSettings.from_configs(defaults, db_config, table_config)
        assert settings.row_limit == 100  # From table_config
        assert settings.order_by == "id"  # From defaults
        assert settings.order_direction == "DESC"  # From db_config
        assert settings.where_clause == "active = 1"  # From table_config

    def test_from_configs_empty_configs(self):
        settings = DumpSettings.from_configs({}, {}, {})
        assert settings.row_limit is None
        assert settings.order_by is None
        assert settings.order_direction == "ASC"  # Default in dataclass
        assert settings.where_clause is None

    def test_from_configs_priority_chain(self):
        """Test the full priority chain: table > database > defaults."""
        defaults = {
            "row_limit": 100,
            "order_by": "default_col",
            "order_direction": "ASC",
            "where_clause": "default_where"
        }
        db_config = {
            "row_limit": 200,
            "order_by": "db_col",
        }
        table_config = {
            "row_limit": 300,
        }
        settings = DumpSettings.from_configs(defaults, db_config, table_config)
        assert settings.row_limit == 300  # table wins
        assert settings.order_by == "db_col"  # db wins over default
        assert settings.order_direction == "ASC"  # default
        assert settings.where_clause == "default_where"  # default

    def test_from_configs_table_order_direction_overrides(self):
        """Test that table-level order_direction overrides database-level."""
        defaults = {"order_direction": "ASC"}
        db_config = {"order_direction": "ASC", "order_by": "id"}
        table_config = {"order_direction": "DESC"}  # Should override!
        settings = DumpSettings.from_configs(defaults, db_config, table_config)
        assert settings.order_direction == "DESC"  # table wins
        assert settings.order_by == "id"  # from db_config
