"""
Configuration loading and validation for MySQL Database Dumper.
"""

import logging
import os
import re
from typing import Any

import yaml


class ConfigLoader:
    """Loads and validates configuration from YAML file."""

    ENV_VAR_PATTERN = re.compile(r'\$\{([^}]+)\}')

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()

    def _load_config(self) -> dict[str, Any]:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        return self._resolve_env_vars(config)

    def _validate_config(self) -> None:
        """Validate configuration for security and correctness."""
        # Validate instances
        instances = self.config.get('instances', {})
        for instance_name, instance_config in instances.items():
            # Check for empty passwords (security risk from missing env vars)
            password = instance_config.get('password', '')
            if not password or password.strip() == '':
                raise ValueError(
                    f"Instance '{instance_name}' has empty password. "
                    f"Check that environment variables are properly set."
                )

            # Validate port number
            port = instance_config.get('port', 3306)
            if not isinstance(port, int) or port < 1 or port > 65535:
                raise ValueError(
                    f"Instance '{instance_name}' has invalid port: {port}. "
                    f"Must be an integer between 1 and 65535."
                )

        # Validate output settings
        output = self.config.get('output', {})
        batch_size = output.get('batch_size')
        if batch_size is not None:
            if not isinstance(batch_size, int) or batch_size < 1:
                raise ValueError(
                    f"Invalid batch_size: {batch_size}. Must be a positive integer."
                )

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
