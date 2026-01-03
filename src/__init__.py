"""
MySQL Database Dumper
=====================
A configurable tool to dump MySQL databases and tables with support for:
- Multiple database instances
- Row limits
- Custom ordering (ASC/DESC)
- WHERE clauses
- Multiple output formats (SQL, CSV)
- Compression support
"""

from .config import ConfigLoader
from .connection import DatabaseConnection
from .database_dumper import DatabaseDumper
from .main import main
from .models import (
    ColumnInfo,
    DatabaseStats,
    DumpSettings,
    DumpStats,
    OrderDirection,
    OutputFormat,
    TableStats,
)
from .table_dumper import TableDumper
from .utils import format_settings_display, print_dry_run_info, setup_logging

__version__ = "1.0.0"

__all__ = [
    # Main entry point
    "main",
    # Core classes
    "ConfigLoader",
    "DatabaseConnection",
    "DatabaseDumper",
    "TableDumper",
    # Models
    "ColumnInfo",
    "DatabaseStats",
    "DumpSettings",
    "DumpStats",
    "OrderDirection",
    "OutputFormat",
    "TableStats",
    # Utilities
    "format_settings_display",
    "print_dry_run_info",
    "setup_logging",
]
