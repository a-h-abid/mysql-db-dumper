"""
Table dumping functionality for MySQL Database Dumper.
"""

import csv
import gzip
import logging
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, TextIO

from .connection import DatabaseConnection
from .models import DumpSettings, OrderDirection, OutputFormat, TableStats, coerce_optional_int


def _format_timedelta(value: timedelta) -> str:
    """Format a timedelta as a MySQL TIME literal '[-]HH:MM:SS[.ffffff]'."""
    sign = '-' if value < timedelta(0) else ''
    value = abs(value)
    total_seconds = value.days * 86400 + value.seconds
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    out = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if value.microseconds:
        out += f".{value.microseconds:06d}"
    return f"'{out}'"


class TableDumper:
    """Handles dumping of individual tables."""

    DEFAULT_BATCH_SIZE = 1000
    CSV_BATCH_SIZE = 5000  # Larger batches for CSV as it's simpler

    def __init__(self, connection: DatabaseConnection, output_settings: dict[str, Any]):
        self.connection = connection
        self.output_settings = output_settings
        self.batch_size = coerce_optional_int(
            output_settings.get('batch_size'), 'batch_size'
        ) or self.DEFAULT_BATCH_SIZE

        # Pre-build type formatters for faster dispatch
        self._type_formatters: dict[type, callable] = {
            type(None): lambda v: 'NULL',
            bool: lambda v: '1' if v else '0',
            int: str,
            float: str,
            Decimal: str,
            bytes: lambda v: f"X'{v.hex()}'",
            bytearray: lambda v: f"X'{v.hex()}'",
            datetime: lambda v: f"'{v.isoformat(' ')}'",
            date: lambda v: f"'{v.isoformat()}'",
            time: lambda v: f"'{v.isoformat()}'",
            timedelta: _format_timedelta,
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
            # Generated columns cannot be inserted; including them breaks restore.
            column_names = [
                col.name for col in columns
                if 'GENERATED' not in (col.extra or '').upper()
            ]

            query = self._build_select_query(table, column_names, settings)
            logging.info(f"Dumping table '{table}'")
            logging.debug(f"Query for '{table}': {query}")

            # A dump that contains only a subset of rows must NOT recreate the table on
            # restore: doing so would DROP the live table and replace it with the subset.
            # Only a full, unfiltered dump may emit DROP TABLE.
            full_table = settings.row_limit is None and not settings.where_clause
            add_drop_table = self.output_settings.get('add_drop_table', full_table)
            if add_drop_table and not full_table:
                logging.warning(
                    f"Refusing to emit DROP TABLE for partial dump of '{table}' "
                    f"(row_limit/where_clause set); restoring it would destroy data."
                )
                add_drop_table = False

            final_path, write_path, file_handle = self._open_output_file(output_path, append)
            stats.file_path = str(final_path)

            try:
                if output_format == OutputFormat.SQL:
                    stats.rows_dumped = self._dump_as_sql(
                        file_handle, table, column_names, query, add_drop_table
                    )
                elif output_format == OutputFormat.CSV:
                    stats.rows_dumped = self._dump_as_csv(
                        file_handle, table, column_names, query
                    )
                else:
                    raise ValueError(f"Unsupported output format: {output_format}")
            finally:
                file_handle.close()

            # Atomic publish: only expose a fully-written dump at the final path.
            if write_path != final_path:
                Path(write_path).replace(final_path)

            stats.success = True

        except Exception as e:
            stats.error = str(e)
            logging.error(f"Error dumping table '{table}': {e}")

        return stats

    def _open_output_file(self, output_path: Path, append: bool) -> tuple[Path, Path, TextIO]:
        """Open output file with optional compression.

        Returns (final_path, write_path, handle). For non-append writes, write_path is a
        temporary '.partial' file the caller atomically renames to final_path on success,
        so an interrupted dump never leaves a truncated file at final_path. Append mode
        targets final_path directly (a shared single-file dump cannot be rebuilt per table).
        """
        compress = self.output_settings.get('compress', False)
        final_path = Path(str(output_path) + '.gz') if compress else output_path

        if append:
            write_path = final_path
            file_mode = 'at'
        else:
            write_path = Path(str(final_path) + '.partial')
            file_mode = 'wt'

        if compress:
            file_handle = gzip.open(write_path, file_mode, encoding='utf-8')
        else:
            file_handle = open(write_path, file_mode, encoding='utf-8')

        return final_path, write_path, file_handle

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
            try:
                direction = OrderDirection(settings.order_direction.upper()).value
            except ValueError:
                logging.warning(
                    f"Table '{table}': invalid order_direction "
                    f"'{settings.order_direction}', defaulting to ASC"
                )
                direction = OrderDirection.ASC.value
            query += f" ORDER BY `{settings.order_by}` {direction}"
        elif settings.order_by:
            logging.warning(f"Order column '{settings.order_by}' not found in table '{table}'")
        elif settings.order_direction != "ASC":
            # User set order_direction but not order_by - warn them
            logging.warning(
                f"Table '{table}': 'order_direction' is set to '{settings.order_direction}' "
                f"but 'order_by' is not specified. The order_direction setting will be ignored."
            )

        if settings.row_limit is not None:
            if settings.row_limit >= 0:
                query += f" LIMIT {settings.row_limit}"
            else:
                logging.warning(
                    f"Table '{table}': negative row_limit "
                    f"({settings.row_limit}) treated as unlimited"
                )

        return query

    def _dump_as_sql(
        self,
        file_handle: TextIO,
        table: str,
        columns: list[str],
        query: str,
        add_drop_table: bool = True
    ) -> int:
        """Dump table data as SQL INSERT statements."""
        # Write header
        file_handle.write(f"-- MySQL Dump\n")
        file_handle.write(f"-- Table: {table}\n")
        file_handle.write(f"-- Generated: {datetime.now().isoformat()}\n")
        file_handle.write(f"-- -------------------------------------------------\n\n")

        # Write CREATE TABLE statement. DROP is only safe for a full dump (see dump_table).
        create_statement = self.connection.get_create_table(table)
        if add_drop_table:
            file_handle.write(f"DROP TABLE IF EXISTS `{table}`;\n\n")
        file_handle.write(f"{create_statement};\n\n")

        # Write data
        cursor = self.connection.get_cursor()
        try:
            cursor.execute(query)

            rows_dumped = 0
            batch = []
            quoted_columns = ', '.join([f'`{col}`' for col in columns])

            for row in cursor:
                batch.append(row)
                rows_dumped += 1

                if len(batch) >= self.batch_size:
                    self._write_insert_batch(file_handle, table, quoted_columns, batch)
                    batch.clear()

            # Write remaining rows
            if batch:
                self._write_insert_batch(file_handle, table, quoted_columns, batch)
        finally:
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

        # Slow path: string conversion with escaping.
        # Backslash MUST be escaped first so the escape sequences we add below
        # are not themselves doubled. Covers the same specials as mysqldump.
        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        escaped = escaped.replace("\n", "\\n").replace("\r", "\\r")
        escaped = escaped.replace("\0", "\\0").replace("\x1a", "\\Z")
        return f"'{escaped}'"

    def _dump_as_csv(
        self,
        file_handle: TextIO,
        table: str,
        columns: list[str],
        query: str
    ) -> int:
        """Dump table data as CSV with batched writes for better performance."""
        writer = csv.writer(file_handle, quoting=csv.QUOTE_MINIMAL)

        # Write header
        writer.writerow(columns)

        # Write data in batches for better I/O performance
        cursor = self.connection.get_cursor()
        try:
            cursor.execute(query)

            rows_dumped = 0
            batch = []

            for row in cursor:
                batch.append(row)
                rows_dumped += 1

                if len(batch) >= self.CSV_BATCH_SIZE:
                    writer.writerows(batch)
                    batch.clear()

            # Write remaining rows
            if batch:
                writer.writerows(batch)
        finally:
            cursor.close()

        return rows_dumped
