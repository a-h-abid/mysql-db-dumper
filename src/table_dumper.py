"""
Table dumping functionality for MySQL Database Dumper.
"""

import csv
import gzip
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

from .connection import DatabaseConnection
from .models import DumpSettings, OutputFormat, TableStats


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
        elif settings.order_direction != "ASC":
            # User set order_direction but not order_by - warn them
            logging.warning(
                f"Table '{table}': 'order_direction' is set to '{settings.order_direction}' "
                f"but 'order_by' is not specified. The order_direction setting will be ignored."
            )

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
