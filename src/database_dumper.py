"""
Main database dumping orchestration for MySQL Database Dumper.
"""

import fnmatch
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .config import ConfigLoader
from .connection import DatabaseConnection
from .models import DatabaseStats, DumpSettings, DumpStats, OutputFormat, TableStats
from .table_dumper import TableDumper


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
        separate_files = self.output_settings.get('separate_files', True)

        # Create output subdirectory only for separate_files mode
        if separate_files:
            if self.output_settings.get('timestamp_suffix', True):
                db_output_dir = output_dir / f"{db_name}_{timestamp}"
            else:
                db_output_dir = output_dir / db_name
            db_output_dir.mkdir(parents=True, exist_ok=True)
        else:
            # Single file mode: use output_dir directly
            db_output_dir = output_dir
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
                output_format, separate_files, timestamp, is_first=(i == 0)
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
        timestamp: str,
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
            # Single file directly in output directory
            db_name = db_config['name']
            if self.output_settings.get('timestamp_suffix', True):
                output_path = db_output_dir / f"{db_name}_{timestamp}.{output_format.extension}"
            else:
                output_path = db_output_dir / f"{db_name}.{output_format.extension}"
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
