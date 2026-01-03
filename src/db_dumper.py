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
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any, Union

import yaml
import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector.cursor import MySQLCursorBuffered


class ConfigLoader:
    """Loads and validates configuration from YAML file."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Resolve environment variables
        config = self._resolve_env_vars(config)
        return config

    def _resolve_env_vars(self, obj: Any) -> Any:
        """Recursively resolve environment variables in config."""
        if isinstance(obj, str):
            # Match ${VAR_NAME} pattern
            pattern = r'\$\{([^}]+)\}'
            matches = re.findall(pattern, obj)
            for match in matches:
                env_value = os.environ.get(match, '')
                obj = obj.replace(f'${{{match}}}', env_value)
            return obj
        elif isinstance(obj, dict):
            return {k: self._resolve_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._resolve_env_vars(item) for item in obj]
        return obj

    def get_instance(self, instance_name: str) -> Dict:
        """Get database instance configuration."""
        instances = self.config.get('instances', {})
        if instance_name not in instances:
            raise ValueError(f"Instance '{instance_name}' not found in configuration")
        return instances[instance_name]

    def get_databases(self) -> List[Dict]:
        """Get list of databases to dump."""
        return self.config.get('databases', [])

    def get_defaults(self) -> Dict:
        """Get default settings."""
        return self.config.get('defaults', {})

    def get_output_settings(self) -> Dict:
        """Get output settings."""
        return self.config.get('output', {})

    def get_logging_settings(self) -> Dict:
        """Get logging settings."""
        return self.config.get('logging', {})


class DatabaseConnection:
    """Manages MySQL database connections."""

    def __init__(self, host: str, port: int, user: str, password: str, database: str = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
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

    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
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
        if buffered:
            return self.connection.cursor(buffered=True)
        # Use unbuffered cursor for memory-efficient streaming
        return self.connection.cursor(buffered=False)

    def get_tables(self) -> List[str]:
        """Get list of all tables in the current database."""
        query = "SHOW TABLES"
        results = self.execute_query(query)
        return [row[0] for row in results]

    def get_table_columns(self, table: str) -> List[Dict]:
        """Get column information for a table."""
        query = f"DESCRIBE `{table}`"
        results = self.execute_query(query)
        columns = []
        for row in results:
            columns.append({
                'name': row[0],
                'type': row[1],
                'null': row[2],
                'key': row[3],
                'default': row[4],
                'extra': row[5]
            })
        return columns

    def get_create_table(self, table: str) -> str:
        """Get CREATE TABLE statement."""
        query = f"SHOW CREATE TABLE `{table}`"
        results = self.execute_query(query)
        return results[0][1]

    def get_row_count(self, table: str, where_clause: str = None) -> int:
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

    def __init__(self, connection: DatabaseConnection, output_settings: Dict):
        self.connection = connection
        self.output_settings = output_settings
        self.batch_size = output_settings.get('batch_size', self.DEFAULT_BATCH_SIZE)

        # Pre-build type formatters for faster dispatch
        self._type_formatters = {
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
        row_limit: Optional[int] = None,
        order_by: Optional[str] = None,
        order_direction: str = "ASC",
        where_clause: Optional[str] = None,
        output_format: str = "sql",
        append: bool = False
    ) -> Dict:
        """
        Dump a table to file.

        Args:
            append: If True, append to existing file instead of overwriting.
                   Used when separate_files is False to write multiple tables
                   to the same file.

        Returns:
            Dict with dump statistics
        """
        stats = {
            'table': table,
            'rows_dumped': 0,
            'file_path': str(output_path),
            'success': False,
            'error': None
        }

        try:
            # Get table structure
            columns = self.connection.get_table_columns(table)
            column_names = [col['name'] for col in columns]

            # Build query
            query = self._build_select_query(
                table, column_names, row_limit, order_by, order_direction, where_clause
            )

            logging.info(f"Dumping table '{table}' with query: {query[:200]}...")

            # Open output file
            file_mode = 'at' if append else 'wt'
            if self.output_settings.get('compress', False):
                output_path = Path(str(output_path) + '.gz')
                file_handle = gzip.open(output_path, file_mode, encoding='utf-8')
            else:
                file_handle = open(output_path, file_mode[0], encoding='utf-8')

            try:
                if output_format == 'sql':
                    stats['rows_dumped'] = self._dump_as_sql(
                        file_handle, table, column_names, query
                    )
                elif output_format == 'csv':
                    stats['rows_dumped'] = self._dump_as_csv(
                        file_handle, table, column_names, query
                    )
                else:
                    raise ValueError(f"Unsupported output format: {output_format}")

                stats['success'] = True
                stats['file_path'] = str(output_path)

            finally:
                file_handle.close()

        except Exception as e:
            stats['error'] = str(e)
            logging.error(f"Error dumping table '{table}': {e}")

        return stats

    def _build_select_query(
        self,
        table: str,
        columns: List[str],
        row_limit: Optional[int],
        order_by: Optional[str],
        order_direction: str,
        where_clause: Optional[str]
    ) -> str:
        """Build SELECT query with options."""
        quoted_columns = ', '.join([f'`{col}`' for col in columns])
        query = f"SELECT {quoted_columns} FROM `{table}`"

        if where_clause:
            query += f" WHERE {where_clause}"

        if order_by:
            # Validate order_by column exists
            if order_by in columns:
                query += f" ORDER BY `{order_by}` {order_direction.upper()}"
            else:
                logging.warning(f"Order column '{order_by}' not found in table '{table}'")

        if row_limit is not None and row_limit > 0:
            query += f" LIMIT {row_limit}"

        return query

    def _dump_as_sql(
        self,
        file_handle,
        table: str,
        columns: List[str],
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
        file_handle,
        table: str,
        columns: str,
        rows: List[tuple]
    ) -> None:
        """Write a batch of rows as INSERT statement."""
        if not rows:
            return

        file_handle.write(f"INSERT INTO `{table}` ({columns}) VALUES\n")

        value_lines = []
        for row in rows:
            values = []
            for val in row:
                values.append(self._format_sql_value(val))
            value_lines.append(f"  ({', '.join(values)})")

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
        file_handle,
        table: str,
        columns: List[str],
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
        self.stats = {
            'databases': [],
            'total_tables': 0,
            'total_rows': 0,
            'errors': []
        }

    def _compile_exclusion_patterns(self, exclude_patterns: List[str]) -> List[re.Pattern]:
        """
        Pre-compile exclusion patterns to regex for faster matching.

        Converts fnmatch patterns to compiled regex patterns.
        """
        compiled = []
        for pattern in exclude_patterns:
            # Convert fnmatch pattern to regex
            regex_pattern = fnmatch.translate(pattern)
            compiled.append(re.compile(regex_pattern))
        return compiled

    def _is_table_excluded(
        self,
        table_name: str,
        exclude_patterns: List[str],
        compiled_patterns: List[re.Pattern] = None
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
            # Fast path: use pre-compiled patterns
            for i, compiled in enumerate(compiled_patterns):
                if compiled.match(table_name):
                    logging.debug(f"Table '{table_name}' excluded by pattern '{exclude_patterns[i]}'")
                    return True
        else:
            # Fallback: use fnmatch directly
            for pattern in exclude_patterns:
                if fnmatch.fnmatch(table_name, pattern):
                    logging.debug(f"Table '{table_name}' excluded by pattern '{pattern}'")
                    return True
        return False

    def run(
        self,
        database_filter: Optional[str] = None,
        instance_filter: Optional[str] = None
    ) -> Dict:
        """Run the dump process for all configured databases.

        Args:
            database_filter: If specified, only dump this database name
            instance_filter: If specified, only dump databases from this instance
        """
        # Setup output directory
        output_dir = Path(self.output_settings.get('directory', './dumps'))
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        databases = self.config.get_databases()

        # Apply filters
        if database_filter:
            databases = [db for db in databases if db['name'] == database_filter]
            if not databases:
                logging.warning(f"No database named '{database_filter}' found in configuration")

        if instance_filter:
            databases = [db for db in databases if db.get('instance', 'primary') == instance_filter]
            if not databases:
                logging.warning(f"No databases found for instance '{instance_filter}'")

        logging.info(f"Starting dump of {len(databases)} database(s)")

        for db_config in databases:
            self._dump_database(db_config, output_dir, timestamp)

        return self.stats

    def _dump_database(
        self,
        db_config: Dict,
        output_dir: Path,
        timestamp: str
    ) -> None:
        """Dump a single database."""
        db_name = db_config['name']
        instance_name = db_config.get('instance', 'primary')

        db_stats = {
            'name': db_name,
            'instance': instance_name,
            'tables': [],
            'total_rows': 0
        }

        try:
            # Get instance configuration
            instance_config = self.config.get_instance(instance_name)

            # Connect to database
            conn = DatabaseConnection(
                host=instance_config['host'],
                port=instance_config.get('port', 3306),
                user=instance_config['user'],
                password=instance_config['password'],
                database=db_name
            )
            conn.connect()

            try:
                # Create output subdirectory for database
                if self.output_settings.get('timestamp_suffix', True):
                    db_output_dir = output_dir / f"{db_name}_{timestamp}"
                else:
                    db_output_dir = output_dir / db_name
                db_output_dir.mkdir(parents=True, exist_ok=True)

                # Get tables to dump
                tables_config = db_config.get('tables', '*')
                exclude_patterns = db_config.get('exclude_tables', [])

                # Pre-compile exclusion patterns for faster matching
                compiled_patterns = self._compile_exclusion_patterns(exclude_patterns) if exclude_patterns else None

                if tables_config == '*':
                    # Dump all tables
                    table_names = conn.get_tables()
                    # Apply exclusion patterns
                    if exclude_patterns:
                        original_count = len(table_names)
                        table_names = [
                            t for t in table_names
                            if not self._is_table_excluded(t, exclude_patterns, compiled_patterns)
                        ]
                        excluded_count = original_count - len(table_names)
                        if excluded_count > 0:
                            logging.info(f"Excluded {excluded_count} table(s) matching exclusion patterns")
                    tables_to_dump = [{'name': t} for t in table_names]
                else:
                    tables_to_dump = tables_config
                    # Apply exclusion patterns to explicitly listed tables as well
                    if exclude_patterns:
                        tables_to_dump = [
                            t for t in tables_to_dump
                            if not self._is_table_excluded(
                                t['name'] if isinstance(t, dict) else t,
                                exclude_patterns,
                                compiled_patterns
                            )
                        ]

                logging.info(f"Dumping {len(tables_to_dump)} table(s) from '{db_name}'")

                # Create dumper
                dumper = TableDumper(conn, self.output_settings)
                output_format = self.output_settings.get('format', 'sql')

                # Track if we need to append when using single file mode
                separate_files = self.output_settings.get('separate_files', True)
                is_first_table = True

                for table_config in tables_to_dump:
                    if isinstance(table_config, str):
                        table_config = {'name': table_config}

                    table_name = table_config['name']

                    # Merge settings: defaults < database < table
                    settings = {**self.defaults}
                    for key in ['row_limit', 'order_by', 'order_direction', 'where_clause']:
                        if key in db_config:
                            settings[key] = db_config[key]
                        if key in table_config:
                            settings[key] = table_config[key]

                    # Determine output file
                    if separate_files:
                        ext = 'csv' if output_format == 'csv' else 'sql'
                        output_path = db_output_dir / f"{table_name}.{ext}"
                        append = False
                    else:
                        ext = 'csv' if output_format == 'csv' else 'sql'
                        output_path = db_output_dir / f"{db_name}.{ext}"
                        # Append to file for all tables after the first one
                        append = not is_first_table
                        is_first_table = False

                    # Dump table
                    table_stats = dumper.dump_table(
                        table=table_name,
                        output_path=output_path,
                        row_limit=settings.get('row_limit'),
                        order_by=settings.get('order_by'),
                        order_direction=settings.get('order_direction', 'ASC'),
                        where_clause=settings.get('where_clause'),
                        output_format=output_format,
                        append=append
                    )

                    db_stats['tables'].append(table_stats)
                    db_stats['total_rows'] += table_stats['rows_dumped']
                    self.stats['total_tables'] += 1
                    self.stats['total_rows'] += table_stats['rows_dumped']

                    if table_stats['success']:
                        logging.info(
                            f"  ✓ {table_name}: {table_stats['rows_dumped']} rows"
                        )
                    else:
                        logging.error(
                            f"  ✗ {table_name}: {table_stats['error']}"
                        )
                        self.stats['errors'].append({
                            'database': db_name,
                            'table': table_name,
                            'error': table_stats['error']
                        })

            finally:
                conn.disconnect()

        except Exception as e:
            logging.error(f"Error dumping database '{db_name}': {e}")
            self.stats['errors'].append({
                'database': db_name,
                'table': None,
                'error': str(e)
            })

        self.stats['databases'].append(db_stats)


def setup_logging(log_settings: Dict) -> None:
    """Setup logging configuration."""
    log_level = getattr(logging, log_settings.get('level', 'INFO').upper())
    log_file = log_settings.get('file')

    handlers = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


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

        # Apply filters for dry run as well
        if args.database:
            databases = [db for db in databases if db['name'] == args.database]
        if args.instance:
            databases = [db for db in databases if db.get('instance', 'primary') == args.instance]

        for db in databases:
            logging.info(f"Would dump database: {db['name']} from instance: {db.get('instance', 'primary')}")
            tables = db.get('tables', '*')
            if tables == '*':
                logging.info("  - All tables")
            else:
                for t in tables:
                    if isinstance(t, dict):
                        logging.info(f"  - {t['name']}")
                    else:
                        logging.info(f"  - {t}")
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
        logging.info(f"Databases: {len(stats['databases'])}")
        logging.info(f"Tables: {stats['total_tables']}")
        logging.info(f"Total Rows: {stats['total_rows']}")

        if stats['errors']:
            logging.warning(f"Errors: {len(stats['errors'])}")
            for err in stats['errors']:
                logging.warning(f"  - {err['database']}/{err['table']}: {err['error']}")
            sys.exit(1)

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
