#!/usr/bin/env python3
"""
MySQL Database Dumper
=====================
A configurable script to dump MySQL databases and tables with support for:
- Multiple database instances
- Row limits
- Custom ordering (ASC/DESC)
- WHERE clauses
- Multiple output formats (SQL, CSV)
- Compression support
"""

import os
import sys
import re
import gzip
import logging
import argparse
import fnmatch
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Any, TextIO

import yaml
import mysql.connector
from mysql.connector import Error as MySQLError


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
        return cls(**settings)


class ConfigLoader:
    """Loads and validates configuration from YAML file."""

    ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        return self._resolve_env_vars(config)

    def _resolve_env_vars(self, obj: Any) -> Any:
        """Recursively resolve environment variables in config."""
        if isinstance(obj, str):
            matches = self.ENV_VAR_PATTERN.findall(obj)
            for match in matches:
                env_value = os.environ.get(match, '')
                obj = obj.replace(f'${{{match}}}', env_value)
            return obj
        elif isinstance(obj, dict):
            return {k: self._resolve_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_env_vars(item) for item in obj]
        return obj

    def get_instance(self, instance_name: str) -> dict[str, Any]:
        """Get database instance configuration."""
        instances = self.config.get('instances', {})
        if instance_name not in instances:
            raise ValueError(f"Instance '{instance_name}' not found in configuration")
        return instances[instance_name]

    def get_databases(self) -> list[dict[str, Any]]:
        """Get list of databases to dump."""
        return self.config.get('databases', [])

    def get_defaults(self) -> dict[str, Any]:
        """Get default settings."""
        return self.config.get('defaults', {})

    def get_output_settings(self) -> dict[str, Any]:
        """Get output settings."""
        return self.config.get('output', {})

    def get_logging_settings(self) -> dict[str, Any]:
        """Get logging settings."""
        return self.config.get('logging', {})


class DatabaseConnection:
    """Manages MySQL database connections with context manager support."""

    DEFAULT_PORT = 3306
    DEFAULT_CHARSET = 'utf8mb4'

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def __enter__(self) -> "DatabaseConnection":
        """Context manager entry - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close connection."""
        self.disconnect()

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.DEFAULT_CHARSET,
                use_unicode=True
            )
            logging.info(f"Connected to {self.host}:{self.port}/{self.database or 'N/A'}")
        except MySQLError as e:
            logging.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.debug("Database connection closed")

    def execute_query(self, query: str, params: Optional[tuple] = None) -> list[tuple]:
        """Execute a query and return results."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def get_cursor(self, buffered: bool = False):
        """Get a cursor for streaming large results.

        Args:
            buffered: If False (default), uses server-side cursor for memory-efficient
                     streaming of large result sets. If True, uses buffered cursor.
        """
        return self.connection.cursor(buffered=buffered)

    def get_tables(self) -> list[str]:
        """Get list of all tables in the current database."""
        results = self.execute_query("SHOW TABLES")
        return [row[0] for row in results]

    def get_table_columns(self, table: str) -> list[ColumnInfo]:
        """Get column information for a table."""
        results = self.execute_query(f"DESCRIBE `{table}`")
        return [
            ColumnInfo(
                name=row[0],
                type=row[1],
                nullable=row[2],
                key=row[3],
                default=row[4],
                extra=row[5]
            )
            for row in results
        ]

    def get_create_table(self, table: str) -> str:
        """Get CREATE TABLE statement."""
        results = self.execute_query(f"SHOW CREATE TABLE `{table}`")
        return results[0][1]

    def get_row_count(self, table: str, where_clause: Optional[str] = None) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) FROM `{table}`"
        if where_clause:
            query += f" WHERE {where_clause}"
        results = self.execute_query(query)
        return results[0][0]


class TableDumper:
    """Handles dumping of individual tables."""

    DEFAULT_BATCH_SIZE = 1000
    CSV_BATCH_SIZE = 5000  # Larger batches for CSV as it's simpler

    def __init__(self, connection: DatabaseConnection, output_settings: dict[str, Any]):
        self.connection = connection
        self.output_settings = output_settings
        self.batch_size = output_settings.get('batch_size', self.DEFAULT_BATCH_SIZE)

        # Pre-build type formatters for faster dispatch
        self._type_formatters: dict[type, callable] = {
            type(None): lambda v: 'NULL',
            bool: lambda v: '1' if v else '0',
            int: str,
            float: str,
            bytes: lambda v: f"X'{v.hex()}'",
            datetime: lambda v: f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'",
        }

    def dump_table(
        self,
        table: str,
        output_path: Path,
        settings: DumpSettings,
        output_format: OutputFormat = OutputFormat.SQL,
        append: bool = False
    ) -> TableStats:
        """
        Dump a table to file.

        Args:
            table: Name of the table to dump.
            output_path: Path for the output file.
            settings: Dump settings (limits, ordering, filters).
            output_format: Output format (SQL or CSV).
            append: If True, append to existing file instead of overwriting.

        Returns:
            TableStats with dump statistics.
        """
        stats = TableStats(table=table, file_path=str(output_path))

        try:
            columns = self.connection.get_table_columns(table)
            column_names = [col.name for col in columns]

            query = self._build_select_query(table, column_names, settings)
            logging.info(f"Dumping table '{table}' with query: {query[:200]}...")

            output_path, file_handle = self._open_output_file(output_path, append)
            stats.file_path = str(output_path)

            try:
                if output_format == OutputFormat.SQL:
                    stats.rows_dumped = self._dump_as_sql(
                        file_handle, table, column_names, query
                    )
                elif output_format == OutputFormat.CSV:
                    stats.rows_dumped = self._dump_as_csv(
                        file_handle, table, column_names, query
                    )
                else:
                    raise ValueError(f"Unsupported output format: {output_format}")

                stats.success = True
            finally:
                file_handle.close()

        except Exception as e:
            stats.error = str(e)
            logging.error(f"Error dumping table '{table}': {e}")

        return stats

    def _open_output_file(self, output_path: Path, append: bool) -> tuple[Path, TextIO]:
        """Open output file with optional compression."""
        file_mode = 'at' if append else 'wt'

        if self.output_settings.get('compress', False):
            output_path = Path(str(output_path) + '.gz')
            file_handle = gzip.open(output_path, file_mode, encoding='utf-8')
        else:
            file_handle = open(output_path, file_mode[0], encoding='utf-8')

        return output_path, file_handle

    def _build_select_query(
        self,
        table: str,
        columns: list[str],
        settings: DumpSettings
    ) -> str:
        """Build SELECT query with options."""
        quoted_columns = ', '.join(f'`{col}`' for col in columns)
        query = f"SELECT {quoted_columns} FROM `{table}`"

        if settings.where_clause:
            query += f" WHERE {settings.where_clause}"

        if settings.order_by and settings.order_by in columns:
            direction = settings.order_direction.upper()
            query += f" ORDER BY `{settings.order_by}` {direction}"
        elif settings.order_by:
            logging.warning(f"Order column '{settings.order_by}' not found in table '{table}'")

        if settings.row_limit is not None and settings.row_limit >= 0:
            query += f" LIMIT {settings.row_limit}"

        return query

    def _dump_as_sql(
        self,
        file_handle: TextIO,
        table: str,
        columns: list[str],
        query: str
    ) -> int:
        """Dump table data as SQL INSERT statements."""
        # Write header
        file_handle.write(f"-- MySQL Dump\n")
        file_handle.write(f"-- Table: {table}\n")
        file_handle.write(f"-- Generated: {datetime.now().isoformat()}\n")
        file_handle.write(f"-- -------------------------------------------------\n\n")

        # Write CREATE TABLE statement
        create_statement = self.connection.get_create_table(table)
        file_handle.write(f"DROP TABLE IF EXISTS `{table}`;\n\n")
        file_handle.write(f"{create_statement};\n\n")

        # Write data
        cursor = self.connection.get_cursor()
        cursor.execute(query)

        rows_dumped = 0
        batch = []
        quoted_columns = ', '.join([f'`{col}`' for col in columns])

        for row in cursor:
            batch.append(row)
            rows_dumped += 1

            if len(batch) >= self.batch_size:
                self._write_insert_batch(file_handle, table, quoted_columns, batch)
                batch = []

        # Write remaining rows
        if batch:
            self._write_insert_batch(file_handle, table, quoted_columns, batch)

        cursor.close()

        file_handle.write(f"\n-- Dump complete. {rows_dumped} rows.\n")
        return rows_dumped

    def _write_insert_batch(
        self,
        file_handle: TextIO,
        table: str,
        columns: str,
        rows: list[tuple]
    ) -> None:
        """Write a batch of rows as INSERT statement."""
        if not rows:
            return

        file_handle.write(f"INSERT INTO `{table}` ({columns}) VALUES\n")

        value_lines = [
            f"  ({', '.join(self._format_sql_value(val) for val in row)})"
            for row in rows
        ]

        file_handle.write(',\n'.join(value_lines))
        file_handle.write(';\n\n')

    def _format_sql_value(self, value: Any) -> str:
        """Format a value for SQL INSERT statement.

        Uses type-based dispatch for common types to avoid isinstance() overhead.
        """
        # Fast path: direct type lookup
        formatter = self._type_formatters.get(type(value))
        if formatter:
            return formatter(value)

        # Slow path: string conversion with escaping
        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        escaped = escaped.replace("\n", "\\n").replace("\r", "\\r")
        return f"'{escaped}'"

    def _dump_as_csv(
        self,
        file_handle: TextIO,
        table: str,
        columns: list[str],
        query: str
    ) -> int:
        """Dump table data as CSV with batched writes for better performance."""
        import csv

        writer = csv.writer(file_handle, quoting=csv.QUOTE_MINIMAL)

        # Write header
        writer.writerow(columns)

        # Write data in batches for better I/O performance
        cursor = self.connection.get_cursor()
        cursor.execute(query)

        rows_dumped = 0
        batch = []

        for row in cursor:
            batch.append(row)
            rows_dumped += 1

            if len(batch) >= self.CSV_BATCH_SIZE:
                writer.writerows(batch)
                batch = []

        # Write remaining rows
        if batch:
            writer.writerows(batch)

        cursor.close()
        return rows_dumped


class DatabaseDumper:
    """Main class for database dumping operations."""

    def __init__(self, config: ConfigLoader):
        self.config = config
        self.output_settings = config.get_output_settings()
        self.defaults = config.get_defaults()
        self.stats = DumpStats()

    def _compile_exclusion_patterns(self, exclude_patterns: list[str]) -> list[re.Pattern]:
        """
        Pre-compile exclusion patterns to regex for faster matching.

        Converts fnmatch patterns to compiled regex patterns.
        """
        return [re.compile(fnmatch.translate(pattern)) for pattern in exclude_patterns]

    def _is_table_excluded(
        self,
        table_name: str,
        exclude_patterns: list[str],
        compiled_patterns: Optional[list[re.Pattern]] = None
    ) -> bool:
        """
        Check if a table should be excluded based on patterns.

        Supports:
        - Exact matches: 'users_backup'
        - Wildcard patterns: '*_old', 'tmp_*', '*_backup_*'

        Uses pre-compiled regex patterns for better performance when
        checking many tables against the same exclusion list.
        """
        if compiled_patterns:
            for i, compiled in enumerate(compiled_patterns):
                if compiled.match(table_name):
                    logging.debug(f"Table '{table_name}' excluded by pattern '{exclude_patterns[i]}'")
                    return True
        else:
            for pattern in exclude_patterns:
                if fnmatch.fnmatch(table_name, pattern):
                    logging.debug(f"Table '{table_name}' excluded by pattern '{pattern}'")
                    return True
        return False

    def run(
        self,
        database_filter: Optional[str] = None,
        instance_filter: Optional[str] = None
    ) -> DumpStats:
        """Run the dump process for all configured databases.

        Args:
            database_filter: If specified, only dump this database name
            instance_filter: If specified, only dump databases from this instance
        """
        output_dir = Path(self.output_settings.get('directory', './dumps'))
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        databases = self._filter_databases(database_filter, instance_filter)

        logging.info(f"Starting dump of {len(databases)} database(s)")

        for db_config in databases:
            self._dump_database(db_config, output_dir, timestamp)

        return self.stats

    def _filter_databases(
        self,
        database_filter: Optional[str],
        instance_filter: Optional[str]
    ) -> list[dict[str, Any]]:
        """Filter databases based on provided filters."""
        databases = self.config.get_databases()

        if database_filter:
            databases = [db for db in databases if db['name'] == database_filter]
            if not databases:
                logging.warning(f"No database named '{database_filter}' found in configuration")

        if instance_filter:
            databases = [db for db in databases if db.get('instance', 'primary') == instance_filter]
            if not databases:
                logging.warning(f"No databases found for instance '{instance_filter}'")

        return databases

    def _dump_database(
        self,
        db_config: dict[str, Any],
        output_dir: Path,
        timestamp: str
    ) -> None:
        """Dump a single database."""
        db_name = db_config['name']
        instance_name = db_config.get('instance', 'primary')

        db_stats = DatabaseStats(name=db_name, instance=instance_name)

        try:
            instance_config = self.config.get_instance(instance_name)

            with DatabaseConnection(
                host=instance_config['host'],
                port=instance_config.get('port', DatabaseConnection.DEFAULT_PORT),
                user=instance_config['user'],
                password=instance_config['password'],
                database=db_name
            ) as conn:
                self._process_database_tables(conn, db_config, db_stats, output_dir, timestamp)

        except Exception as e:
            logging.error(f"Error dumping database '{db_name}': {e}")
            self.stats.errors.append({
                'database': db_name,
                'table': None,
                'error': str(e)
            })

        self.stats.databases.append(db_stats)

    def _process_database_tables(
        self,
        conn: DatabaseConnection,
        db_config: dict[str, Any],
        db_stats: DatabaseStats,
        output_dir: Path,
        timestamp: str
    ) -> None:
        """Process and dump all tables for a database."""
        db_name = db_config['name']

        # Create output subdirectory
        if self.output_settings.get('timestamp_suffix', True):
            db_output_dir = output_dir / f"{db_name}_{timestamp}"
        else:
            db_output_dir = output_dir / db_name
        db_output_dir.mkdir(parents=True, exist_ok=True)

        # Get tables to dump
        tables_to_dump = self._get_tables_to_dump(conn, db_config)
        logging.info(f"Dumping {len(tables_to_dump)} table(s) from '{db_name}'")

        # Create dumper and process tables
        dumper = TableDumper(conn, self.output_settings)
        output_format = OutputFormat(self.output_settings.get('format', 'sql'))
        separate_files = self.output_settings.get('separate_files', True)

        for i, table_config in enumerate(tables_to_dump):
            table_stats = self._dump_single_table(
                dumper, table_config, db_config, db_output_dir,
                output_format, separate_files, is_first=(i == 0)
            )

            db_stats.tables.append(table_stats)
            db_stats.total_rows += table_stats.rows_dumped
            self.stats.total_tables += 1
            self.stats.total_rows += table_stats.rows_dumped

            self._log_table_result(table_stats, db_name)

    def _get_tables_to_dump(
        self,
        conn: DatabaseConnection,
        db_config: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get list of tables to dump, applying exclusion patterns."""
        tables_config = db_config.get('tables', '*')
        exclude_patterns = db_config.get('exclude_tables', [])
        compiled_patterns = self._compile_exclusion_patterns(exclude_patterns) if exclude_patterns else None

        if tables_config == '*':
            table_names = conn.get_tables()
            if exclude_patterns:
                original_count = len(table_names)
                table_names = [
                    t for t in table_names
                    if not self._is_table_excluded(t, exclude_patterns, compiled_patterns)
                ]
                excluded_count = original_count - len(table_names)
                if excluded_count > 0:
                    logging.info(f"Excluded {excluded_count} table(s) matching exclusion patterns")
            return [{'name': t} for t in table_names]

        # Explicit table list
        tables_to_dump = tables_config
        if exclude_patterns:
            tables_to_dump = [
                t for t in tables_to_dump
                if not self._is_table_excluded(
                    t['name'] if isinstance(t, dict) else t,
                    exclude_patterns,
                    compiled_patterns
                )
            ]
        return tables_to_dump

    def _dump_single_table(
        self,
        dumper: TableDumper,
        table_config: dict[str, Any] | str,
        db_config: dict[str, Any],
        db_output_dir: Path,
        output_format: OutputFormat,
        separate_files: bool,
        is_first: bool
    ) -> TableStats:
        """Dump a single table and return stats."""
        if isinstance(table_config, str):
            table_config = {'name': table_config}

        table_name = table_config['name']
        settings = DumpSettings.from_configs(self.defaults, db_config, table_config)

        logging.debug(
            f"Table '{table_name}' effective settings: "
            f"row_limit={settings.row_limit}, order_by={settings.order_by}, "
            f"order_direction={settings.order_direction}, where_clause={settings.where_clause}"
        )

        # Determine output file path
        if separate_files:
            output_path = db_output_dir / f"{table_name}.{output_format.extension}"
            append = False
        else:
            output_path = db_output_dir / f"{db_config['name']}.{output_format.extension}"
            append = not is_first

        return dumper.dump_table(
            table=table_name,
            output_path=output_path,
            settings=settings,
            output_format=output_format,
            append=append
        )

    def _log_table_result(self, table_stats: TableStats, db_name: str) -> None:
        """Log the result of a table dump."""
        if table_stats.success:
            logging.info(f"  ✓ {table_stats.table}: {table_stats.rows_dumped} rows")
        else:
            logging.error(f"  ✗ {table_stats.table}: {table_stats.error}")
            self.stats.errors.append({
                'database': db_name,
                'table': table_stats.table,
                'error': table_stats.error
            })


def setup_logging(log_settings: dict[str, Any]) -> None:
    """Setup logging configuration."""
    log_level = getattr(logging, log_settings.get('level', 'INFO').upper())
    log_file = log_settings.get('file')

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


def print_dry_run_info(databases: list[dict[str, Any]], defaults: dict[str, Any]) -> None:
    """Print information about what would be dumped in dry-run mode."""
    for db in databases:
        logging.info(f"Would dump database: {db['name']} from instance: {db.get('instance', 'primary')}")

        db_row_limit = db.get('row_limit')
        if db_row_limit is not None:
            logging.info(f"  Database-level row_limit: {db_row_limit}")

        tables = db.get('tables', '*')
        if tables == '*':
            logging.info("  - All tables (with database/default settings)")
        else:
            for t in tables:
                if isinstance(t, dict):
                    settings = DumpSettings.from_configs(defaults, db, t)
                    settings_parts = _format_settings_display(settings)

                    if settings_parts:
                        logging.info(f"  - {t['name']} ({', '.join(settings_parts)})")
                    else:
                        logging.info(f"  - {t['name']} (no limits)")
                else:
                    logging.info(f"  - {t}")


def _format_settings_display(settings: DumpSettings) -> list[str]:
    """Format settings for display in dry-run mode."""
    parts = []
    if settings.row_limit is not None:
        parts.append(f"limit={settings.row_limit}")
    if settings.order_by:
        parts.append(f"order={settings.order_by} {settings.order_direction}")
    if settings.where_clause:
        parts.append(f"where='{settings.where_clause}'")
    return parts


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='MySQL Database Dumper - Configurable database backup tool'
    )
    parser.add_argument(
        '-c', '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be dumped without actually dumping'
    )
    parser.add_argument(
        '-d', '--database',
        help='Dump only the specified database (must be defined in config)'
    )
    parser.add_argument(
        '-i', '--instance',
        help='Dump only databases from the specified instance'
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = ConfigLoader(args.config)
    except FileNotFoundError:
        print(f"Error: Configuration file '{args.config}' not found")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML in configuration file: {e}")
        sys.exit(1)

    # Setup logging
    log_settings = config.get_logging_settings()
    if args.verbose:
        log_settings['level'] = 'DEBUG'
    setup_logging(log_settings)

    # Dry run mode
    if args.dry_run:
        logging.info("DRY RUN MODE - No data will be dumped")
        databases = config.get_databases()
        defaults = config.get_defaults()

        # Apply filters for dry run as well
        if args.database:
            databases = [db for db in databases if db['name'] == args.database]
        if args.instance:
            databases = [db for db in databases if db.get('instance', 'primary') == args.instance]

        print_dry_run_info(databases, defaults)
        sys.exit(0)

    # Run dump
    try:
        dumper = DatabaseDumper(config)
        stats = dumper.run(
            database_filter=args.database,
            instance_filter=args.instance
        )

        # Print summary
        logging.info("=" * 50)
        logging.info("DUMP COMPLETE")
        logging.info(f"Databases: {len(stats.databases)}")
        logging.info(f"Tables: {stats.total_tables}")
        logging.info(f"Total Rows: {stats.total_rows}")

        if stats.errors:
            logging.warning(f"Errors: {len(stats.errors)}")
            for err in stats.errors:
                logging.warning(f"  - {err['database']}/{err['table']}: {err['error']}")
            sys.exit(1)

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
