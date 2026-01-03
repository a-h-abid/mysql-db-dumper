"""
Unit tests for config.py
"""

import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import yaml

from src.config import ConfigLoader


class TestConfigLoader:
    """Tests for ConfigLoader class."""

    @pytest.fixture
    def sample_config(self):
        """Sample configuration dictionary."""
        return {
            "instances": {
                "primary": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "secret"
                },
                "secondary": {
                    "host": "192.168.1.100",
                    "port": 3307,
                    "user": "admin",
                    "password": "admin_pass"
                }
            },
            "databases": [
                {
                    "name": "testdb",
                    "instance": "primary",
                    "tables": "*"
                },
                {
                    "name": "analytics",
                    "instance": "secondary",
                    "tables": [
                        {"name": "events", "row_limit": 1000}
                    ]
                }
            ],
            "defaults": {
                "row_limit": 500,
                "order_direction": "ASC"
            },
            "output": {
                "directory": "./dumps",
                "format": "sql",
                "compress": False
            },
            "logging": {
                "level": "INFO",
                "file": "./dumps/dump.log"
            }
        }

    @pytest.fixture
    def config_file(self, sample_config):
        """Create a temporary config file."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(sample_config, f)
            f.flush()
            yield f.name
        os.unlink(f.name)

    def test_load_config(self, config_file):
        """Test loading a valid config file."""
        loader = ConfigLoader(config_file)
        assert loader.config is not None

    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader("/nonexistent/path/config.yaml")

    def test_get_instance(self, config_file):
        """Test getting a specific instance configuration."""
        loader = ConfigLoader(config_file)
        instance = loader.get_instance("primary")
        assert instance["host"] == "localhost"
        assert instance["port"] == 3306
        assert instance["user"] == "root"

    def test_get_instance_not_found(self, config_file):
        """Test getting a non-existent instance raises error."""
        loader = ConfigLoader(config_file)
        with pytest.raises(ValueError) as exc_info:
            loader.get_instance("nonexistent")
        assert "not found in configuration" in str(exc_info.value)

    def test_get_databases(self, config_file):
        """Test getting list of databases."""
        loader = ConfigLoader(config_file)
        databases = loader.get_databases()
        assert len(databases) == 2
        assert databases[0]["name"] == "testdb"
        assert databases[1]["name"] == "analytics"

    def test_get_defaults(self, config_file):
        """Test getting default settings."""
        loader = ConfigLoader(config_file)
        defaults = loader.get_defaults()
        assert defaults["row_limit"] == 500
        assert defaults["order_direction"] == "ASC"

    def test_get_output_settings(self, config_file):
        """Test getting output settings."""
        loader = ConfigLoader(config_file)
        output = loader.get_output_settings()
        assert output["directory"] == "./dumps"
        assert output["format"] == "sql"
        assert output["compress"] is False

    def test_get_logging_settings(self, config_file):
        """Test getting logging settings."""
        loader = ConfigLoader(config_file)
        logging = loader.get_logging_settings()
        assert logging["level"] == "INFO"
        assert logging["file"] == "./dumps/dump.log"

    def test_empty_sections(self):
        """Test handling of missing config sections."""
        config = {"instances": {"primary": {"host": "localhost"}}}
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(config, f)
            f.flush()
            loader = ConfigLoader(f.name)
        os.unlink(f.name)

        assert loader.get_databases() == []
        assert loader.get_defaults() == {}
        assert loader.get_output_settings() == {}
        assert loader.get_logging_settings() == {}


class TestEnvironmentVariables:
    """Tests for environment variable resolution."""

    @pytest.fixture
    def env_config(self):
        """Configuration with environment variables."""
        return {
            "instances": {
                "primary": {
                    "host": "${DB_HOST}",
                    "port": 3306,
                    "user": "${DB_USER}",
                    "password": "${DB_PASSWORD}"
                }
            },
            "output": {
                "directory": "${OUTPUT_DIR}/dumps"
            }
        }

    @pytest.fixture
    def env_config_file(self, env_config):
        """Create a temporary config file with env vars."""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(env_config, f)
            f.flush()
            yield f.name
        os.unlink(f.name)

    def test_resolve_env_vars(self, env_config_file):
        """Test environment variables are resolved."""
        with mock.patch.dict(os.environ, {
            "DB_HOST": "db.example.com",
            "DB_USER": "myuser",
            "DB_PASSWORD": "mypassword",
            "OUTPUT_DIR": "/var/backups"
        }):
            loader = ConfigLoader(env_config_file)
            instance = loader.get_instance("primary")
            assert instance["host"] == "db.example.com"
            assert instance["user"] == "myuser"
            assert instance["password"] == "mypassword"

            output = loader.get_output_settings()
            assert output["directory"] == "/var/backups/dumps"

    def test_missing_env_var_becomes_empty(self, env_config_file):
        """Test missing environment variables become empty strings."""
        with mock.patch.dict(os.environ, {}, clear=True):
            # Clear any existing env vars that might match
            for key in ["DB_HOST", "DB_USER", "DB_PASSWORD", "OUTPUT_DIR"]:
                os.environ.pop(key, None)

            loader = ConfigLoader(env_config_file)
            instance = loader.get_instance("primary")
            assert instance["host"] == ""
            assert instance["user"] == ""
            assert instance["password"] == ""

    def test_partial_env_var_resolution(self, env_config_file):
        """Test partial environment variable resolution."""
        with mock.patch.dict(os.environ, {
            "DB_HOST": "localhost",
            "OUTPUT_DIR": "/data"
        }, clear=True):
            loader = ConfigLoader(env_config_file)
            instance = loader.get_instance("primary")
            assert instance["host"] == "localhost"
            assert instance["user"] == ""  # Not set

    def test_env_var_in_nested_list(self):
        """Test env var resolution in nested lists."""
        config = {
            "databases": [
                {"name": "${DB_NAME}", "tables": ["${TABLE_1}", "${TABLE_2}"]}
            ]
        }
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(config, f)
            f.flush()
            config_path = f.name

        with mock.patch.dict(os.environ, {
            "DB_NAME": "production",
            "TABLE_1": "users",
            "TABLE_2": "orders"
        }):
            loader = ConfigLoader(config_path)
            databases = loader.get_databases()
            assert databases[0]["name"] == "production"
            assert databases[0]["tables"] == ["users", "orders"]

        os.unlink(config_path)

    def test_non_string_values_unchanged(self):
        """Test that non-string values are not modified."""
        config = {
            "instances": {
                "primary": {
                    "host": "localhost",
                    "port": 3306,
                    "ssl": True,
                    "timeout": None
                }
            }
        }
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(config, f)
            f.flush()
            loader = ConfigLoader(f.name)
        os.unlink(f.name)

        instance = loader.get_instance("primary")
        assert instance["port"] == 3306
        assert instance["ssl"] is True
        assert instance["timeout"] is None
