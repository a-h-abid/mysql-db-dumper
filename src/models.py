"""
Data models and enums for MySQL Database Dumper.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any


def coerce_optional_int(value: Any, field_name: str) -> Optional[int]:
    """Coerce a config value to int (or None), accepting numeric strings.

    Environment-variable interpolation yields strings, so a config value like
    row_limit: "${LIMIT}" arrives as "5000" rather than 5000. Normalize such values
    and fail loudly on genuinely non-numeric input.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"'{field_name}' must be an integer, got boolean {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"'{field_name}' must be an integer, got {value!r}")


class OutputFormat(Enum):
    """Supported output formats for database dumps."""
    SQL = "sql"
    CSV = "csv"

    @property
    def extension(self) -> str:
        return self.value


class OrderDirection(Enum):
    """Sort order direction."""
    ASC = "ASC"
    DESC = "DESC"


@dataclass
class ColumnInfo:
    """Database column metadata."""
    name: str
    type: str
    nullable: str
    key: str
    default: Any
    extra: str


@dataclass
class TableStats:
    """Statistics for a single table dump."""
    table: str
    rows_dumped: int = 0
    file_path: str = ""
    success: bool = False
    error: Optional[str] = None


@dataclass
class DatabaseStats:
    """Statistics for a single database dump."""
    name: str
    instance: str
    tables: list[TableStats] = field(default_factory=list)
    total_rows: int = 0


@dataclass
class DumpStats:
    """Overall dump statistics."""
    databases: list[DatabaseStats] = field(default_factory=list)
    total_tables: int = 0
    total_rows: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DumpSettings:
    """Merged settings for dumping a table."""
    row_limit: Optional[int] = None
    order_by: Optional[str] = None
    order_direction: str = "ASC"
    where_clause: Optional[str] = None

    @classmethod
    def from_configs(
        cls,
        defaults: dict[str, Any],
        db_config: dict[str, Any],
        table_config: dict[str, Any]
    ) -> "DumpSettings":
        """
        Create DumpSettings by merging configs with priority: table > database > defaults.
        """
        settings = {}
        for key in ['row_limit', 'order_by', 'order_direction', 'where_clause']:
            if key in defaults:
                settings[key] = defaults[key]
            if key in db_config:
                settings[key] = db_config[key]
            if key in table_config:
                settings[key] = table_config[key]
        if 'row_limit' in settings:
            settings['row_limit'] = coerce_optional_int(settings['row_limit'], 'row_limit')
        return cls(**settings)
