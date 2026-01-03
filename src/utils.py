"""
Utility functions for MySQL Database Dumper.
"""

import logging
import sys
from pathlib import Path
from typing import Any

from .models import DumpSettings


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
                    settings_parts = format_settings_display(settings)

                    if settings_parts:
                        logging.info(f"  - {t['name']} ({', '.join(settings_parts)})")
                    else:
                        logging.info(f"  - {t['name']} (no limits)")
                else:
                    logging.info(f"  - {t}")


def format_settings_display(settings: DumpSettings) -> list[str]:
    """Format settings for display in dry-run mode."""
    parts = []
    if settings.row_limit is not None:
        parts.append(f"limit={settings.row_limit}")
    if settings.order_by:
        parts.append(f"order={settings.order_by} {settings.order_direction}")
    if settings.where_clause:
        parts.append(f"where='{settings.where_clause}'")
    return parts
