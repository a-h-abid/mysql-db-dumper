#!/usr/bin/env python3
"""
MySQL Database Dumper - CLI Entry Point
=======================================
A configurable script to dump MySQL databases and tables with support for:
- Multiple database instances
- Row limits
- Custom ordering (ASC/DESC)
- WHERE clauses
- Multiple output formats (SQL, CSV)
- Compression support
"""

import argparse
import logging
import sys

import yaml

from .config import ConfigLoader
from .database_dumper import DatabaseDumper
from .incremental import IncrementalTracker
from .utils import print_dry_run_info, setup_logging


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
    parser.add_argument(
        '--since',
        metavar='COLUMN',
        help='Enable incremental dump based on timestamp column (e.g., "updated_at")'
    )
    parser.add_argument(
        '--clear-metadata',
        action='store_true',
        help='Clear incremental dump metadata and start fresh'
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

    # Handle incremental dump metadata
    tracker = None
    if args.since or args.clear_metadata:
        metadata_file = config.get_output_settings().get('metadata_file', '.dump_metadata.json')
        tracker = IncrementalTracker(metadata_file)

        if args.clear_metadata:
            tracker.clear_metadata(
                database=args.database,
                instance=args.instance
            )
            logging.info("Cleared incremental dump metadata")
            if not args.since:
                sys.exit(0)

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
        dumper = DatabaseDumper(config, incremental_tracker=tracker, timestamp_column=args.since)
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
