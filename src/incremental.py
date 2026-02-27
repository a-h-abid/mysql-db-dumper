"""
Incremental dump support for MySQL Database Dumper.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class IncrementalTracker:
    """Tracks last dump times for incremental/differential dumps."""

    def __init__(self, metadata_file: str = ".dump_metadata.json"):
        self.metadata_file = Path(metadata_file)
        self.metadata: dict[str, dict[str, Any]] = {}
        self._load_metadata()

    def _load_metadata(self) -> None:
        """Load metadata from file."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    self.metadata = json.load(f)
                logging.debug(f"Loaded metadata from {self.metadata_file}")
            except (json.JSONDecodeError, IOError) as e:
                logging.warning(f"Failed to load metadata: {e}. Starting fresh.")
                self.metadata = {}
        else:
            logging.debug("No existing metadata file found. Starting fresh.")
            self.metadata = {}

    def _save_metadata(self) -> None:
        """Save metadata to file."""
        try:
            self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f, indent=2, default=str)
            logging.debug(f"Saved metadata to {self.metadata_file}")
        except IOError as e:
            logging.error(f"Failed to save metadata: {e}")

    def get_last_dump_time(
        self,
        database: str,
        table: str,
        instance: str = "primary"
    ) -> Optional[datetime]:
        """Get the last dump time for a specific table."""
        key = f"{instance}:{database}:{table}"
        if key in self.metadata:
            timestamp_str = self.metadata[key].get('last_dump')
            if timestamp_str:
                try:
                    return datetime.fromisoformat(timestamp_str)
                except ValueError:
                    logging.warning(f"Invalid timestamp in metadata for {key}")
        return None

    def set_last_dump_time(
        self,
        database: str,
        table: str,
        timestamp: datetime,
        instance: str = "primary",
        rows_dumped: int = 0
    ) -> None:
        """Set the last dump time for a specific table."""
        key = f"{instance}:{database}:{table}"
        self.metadata[key] = {
            'last_dump': timestamp.isoformat(),
            'rows_dumped': rows_dumped
        }
        self._save_metadata()

    def generate_incremental_where_clause(
        self,
        database: str,
        table: str,
        timestamp_column: str,
        instance: str = "primary",
        existing_where: Optional[str] = None
    ) -> Optional[str]:
        """Generate WHERE clause for incremental dump based on last dump time."""
        last_dump = self.get_last_dump_time(database, table, instance)

        if not last_dump:
            logging.info(
                f"No previous dump found for {database}.{table}. "
                f"Performing full dump."
            )
            return existing_where

        # Format timestamp for MySQL
        timestamp_str = last_dump.strftime('%Y-%m-%d %H:%M:%S')
        incremental_clause = f"`{timestamp_column}` > '{timestamp_str}'"

        if existing_where:
            return f"({existing_where}) AND {incremental_clause}"
        return incremental_clause

    def clear_metadata(
        self,
        database: Optional[str] = None,
        table: Optional[str] = None,
        instance: Optional[str] = None
    ) -> None:
        """Clear metadata for specific or all tables."""
        if database is None and table is None and instance is None:
            # Clear all
            self.metadata = {}
            logging.info("Cleared all dump metadata")
        else:
            # Clear specific entries
            keys_to_remove = []
            prefix = ""
            if instance:
                prefix += f"{instance}:"
            if database:
                prefix += f"{database}:"
            if table:
                prefix += table

            for key in self.metadata:
                if key.startswith(prefix):
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                del self.metadata[key]

            logging.info(f"Cleared metadata for {len(keys_to_remove)} table(s)")

        self._save_metadata()
