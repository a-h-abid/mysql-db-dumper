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


DEFAULT_CONNECT_TIMEOUT = DatabaseConnection.DEFAULT_CONNECT_TIMEOUT


def _safe_path_component(name: str) -> str:
    """Reduce a name to a single safe filesystem component (strips any path/traversal)."""
    return Path(str(name)).name


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
            databases = [db for db in databases if db.get('name') == database_filter]
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
        db_name = db_config.get('name')
        if not db_name:
            logging.error("Skipping database entry with no 'name' field")
            self.stats.errors.append({
                'database': None,
                'table': None,
                'error': "Database entry is missing the required 'name' field",
            })
            return
        instance_name = db_config.get('instance', 'primary')

        db_stats = DatabaseStats(name=db_name, instance=instance_name)

        try:
            instance_config = self.config.get_instance(instance_name)

            with DatabaseConnection(
                host=instance_config['host'],
                port=instance_config.get('port', DatabaseConnection.DEFAULT_PORT),
                user=instance_config['user'],
                password=instance_config['password'],
                database=db_name,
                connect_timeout=instance_config.get(
                    'connect_timeout', DEFAULT_CONNECT_TIMEOUT
                ),
                ssl_ca=instance_config.get('ssl_ca'),
                ssl_cert=instance_config.get('ssl_cert'),
                ssl_key=instance_config.get('ssl_key'),
                ssl_verify_cert=instance_config.get('ssl_verify_cert'),
            ) as conn:
                if instance_config.get('consistent_snapshot', True):
                    conn.start_consistent_snapshot()
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
        safe_db = _safe_path_component(db_name)
        if separate_files:
            if self.output_settings.get('timestamp_suffix', True):
                db_output_dir = output_dir / f"{safe_db}_{timestamp}"
            else:
                db_output_dir = output_dir / safe_db
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
        single_final_path = None
        single_write_path = None
        wrap_combined = False
        if not separate_files:
            single_output_path = self._single_file_output_path(
                db_config, db_output_dir, output_format, timestamp
            )
            single_final_path, single_write_path = dumper._resolve_output_paths(
                single_output_path, append=False, defer_publish=True
            )
            wrap_combined = self._combined_dump_wrappable(
                tables_to_dump, db_config, output_format
            )
            if not tables_to_dump:
                wrap_combined = False
            if wrap_combined and not self.output_settings.get('compress', False):
                # Seed the .partial once with the opening wrapper, then make EVERY
                # table append to it (see handle-clobber caveat). Without this, the
                # first table's truncating open would discard the =0 line.
                with open(single_write_path, 'wt', encoding='utf-8') as fh:
                    fh.write("SET FOREIGN_KEY_CHECKS=0;\n\n")
            elif wrap_combined:
                # Compressed combined dumps can't be safely seeded/appended as
                # plain text; leave unwrapped rather than corrupt the gzip stream.
                logging.info(
                    f"Combined compressed dump of '{db_config['name']}' left "
                    f"unwrapped (FK wrapper unsupported for compressed single files)."
                )
                wrap_combined = False

        for i, table_config in enumerate(tables_to_dump):
            table_stats = self._dump_single_table(
                dumper, table_config, db_config, db_output_dir,
                output_format, separate_files, timestamp, is_first=(i == 0),
                defer_publish=not separate_files,
                combined_wrapped=wrap_combined,
            )

            db_stats.tables.append(table_stats)
            db_stats.total_rows += table_stats.rows_dumped
            self.stats.total_tables += 1
            self.stats.total_rows += table_stats.rows_dumped

            self._log_table_result(table_stats, db_name)

            if not separate_files and not table_stats.success:
                break

        if (
            not separate_files
            and single_final_path is not None
            and single_write_path is not None
            and single_write_path.exists()
            and all(table.success for table in db_stats.tables)
        ):
            if wrap_combined:
                with open(single_write_path, 'at', encoding='utf-8') as fh:
                    fh.write("SET FOREIGN_KEY_CHECKS=1;\n")
            single_write_path.replace(single_final_path)

        # Separate-files SQL dumps get a restore.sh that disables FK checks once
        # for the whole session and sources every wrapped table file in any order.
        if (
            separate_files
            and output_format == OutputFormat.SQL
            and any(
                table.success and self._table_was_fk_wrapped(table, db_config)
                for table in db_stats.tables
            )
        ):
            self._write_restore_helper(
                db_output_dir, self.output_settings.get('compress', False)
            )

    def _single_file_output_path(
        self,
        db_config: dict[str, Any],
        db_output_dir: Path,
        output_format: OutputFormat,
        timestamp: str
    ) -> Path:
        """Build the logical final path for a single-file database dump."""
        safe_db = _safe_path_component(db_config['name'])
        if self.output_settings.get('timestamp_suffix', True):
            return db_output_dir / f"{safe_db}_{timestamp}.{output_format.extension}"
        return db_output_dir / f"{safe_db}.{output_format.extension}"

    def _write_restore_helper(self, db_output_dir: Path, compress: bool) -> None:
        """Write an executable restore.sh that sources every dumped .sql in any order.

        It disables FOREIGN_KEY_CHECKS once for the whole restore session, so table
        and row order (and cycles) become irrelevant. It streams SQL to stdout and
        never invokes mysql itself, so no credentials are ever written to disk.
        """
        import os
        import stat

        if compress:
            glob = "*.sql.gz"
            emit = 'zcat "$f"'
        else:
            glob = "*.sql"
            emit = 'cat "$f"'

        script = (
            "#!/bin/sh\n"
            "# Restore helper generated by mysql-db-dumper.\n"
            "# Disables FOREIGN_KEY_CHECKS once, then sources every table file in any\n"
            "# order, so dependency/cycle ordering does not matter. Restore in ONE\n"
            "# mysql session (FOREIGN_KEY_CHECKS is a session variable):\n"
            "#   ./restore.sh | mysql -u USER -p TARGET_DB\n"
            "#   ./restore.sh > restore.sql   # then import restore.sql\n"
            'dir="$(dirname "$0")"\n'
            'echo "SET FOREIGN_KEY_CHECKS=0;"\n'
            f'for f in "$dir"/{glob}; do\n'
            '  [ -e "$f" ] || continue\n'
            '  echo "-- >>> $f"\n'
            f"  {emit}\n"
            "done\n"
            'echo "SET FOREIGN_KEY_CHECKS=1;"\n'
        )

        helper_path = db_output_dir / "restore.sh"
        helper_path.write_text(script, encoding="utf-8")
        mode = os.stat(helper_path).st_mode
        os.chmod(helper_path, mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def _table_was_fk_wrapped(
        self, table_stats: TableStats, db_config: dict[str, Any]
    ) -> bool:
        """Whether this separate-files table file received its own FK wrapper.

        Mirrors the decision in TableDumper.dump_table: option on AND full dump.
        (Format is already known to be SQL by the caller.)
        """
        if not self.output_settings.get('disable_foreign_key_checks', True):
            return False
        # Find the table-level config (if any) from db_config['tables'].
        table_config: dict[str, Any] = {'name': table_stats.table}
        tables_config = db_config.get('tables', '*')
        if isinstance(tables_config, list):
            for entry in tables_config:
                if isinstance(entry, dict) and entry.get('name') == table_stats.table:
                    table_config = entry
                    break
        settings = DumpSettings.from_configs(
            self.defaults, db_config, table_config
        )
        return settings.row_limit is None and not settings.where_clause

    def _get_tables_to_dump(
        self,
        conn: DatabaseConnection,
        db_config: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Get list of tables to dump, applying exclusion patterns."""
        tables_config = db_config.get('tables', '*')
        if tables_config is None:
            tables_config = '*'
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

    def _combined_dump_wrappable(
        self,
        tables_to_dump: list[dict[str, Any] | str],
        db_config: dict[str, Any],
        output_format: OutputFormat,
    ) -> bool:
        """Whether a combined single-file SQL dump may be FK-wrapped.

        Wrappable only when the option is on, the format is SQL, and EVERY table
        is a full dump. Any partial table (row_limit/where_clause) makes the
        whole file unsafe to wrap, because FK checks off + a filtered subset can
        insert orphan child rows.
        """
        if output_format != OutputFormat.SQL:
            return False
        if not self.output_settings.get('disable_foreign_key_checks', True):
            return False

        for table_config in tables_to_dump:
            if isinstance(table_config, str):
                table_config = {'name': table_config}
            settings = DumpSettings.from_configs(self.defaults, db_config, table_config)
            if settings.row_limit is not None or settings.where_clause:
                logging.info(
                    f"Combined dump of '{db_config['name']}' left unwrapped: table "
                    f"'{table_config['name']}' is a partial dump (row_limit/where_clause)."
                )
                return False
        return True

    def _dump_single_table(
        self,
        dumper: TableDumper,
        table_config: dict[str, Any] | str,
        db_config: dict[str, Any],
        db_output_dir: Path,
        output_format: OutputFormat,
        separate_files: bool,
        timestamp: str,
        is_first: bool,
        defer_publish: bool = False,
        combined_wrapped: bool = False,
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
            output_path = db_output_dir / f"{_safe_path_component(table_name)}.{output_format.extension}"
            append = False
        else:
            # Single file directly in output directory
            output_path = self._single_file_output_path(
                db_config, db_output_dir, output_format, timestamp
            )
            # When the combined file is FK-wrapped, the .partial was pre-seeded
            # with =0, so EVERY table (including the first) must append to it.
            append = combined_wrapped or not is_first

        return dumper.dump_table(
            table=table_name,
            output_path=output_path,
            settings=settings,
            output_format=output_format,
            append=append,
            defer_publish=defer_publish,
            emit_fk_wrapper=False if not separate_files else None,
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
