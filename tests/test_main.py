"""
Additional unit tests for main.py to improve coverage
"""

import sys
import tempfile
from io import StringIO
from unittest import mock

import pytest
import yaml

from src.main import main


class TestMainFunction:
    """Tests for main() entry point."""

    @pytest.fixture
    def valid_config(self):
        """Create a valid config file."""
        config = {
            "instances": {
                "primary": {
                    "host": "localhost",
                    "port": 3306,
                    "user": "root",
                    "password": "testpass"
                }
            },
            "databases": [
                {
                    "name": "testdb",
                    "instance": "primary",
                    "tables": "*"
                }
            ],
            "defaults": {},
            "output": {
                "directory": "./dumps",
                "format": "sql"
            },
            "logging": {
                "level": "INFO"
            }
        }
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.yaml', delete=False
        ) as f:
            yaml.dump(config, f)
            f.flush()
            yield f.name

    def test_config_file_not_found(self, capsys):
        """Test handling of missing config file."""
        with mock.patch.object(sys, 'argv', ['prog', '-c', '/nonexistent/config.yaml']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
            captured = capsys.readouterr()
            assert "not found" in captured.out.lower()

    def test_invalid_yaml_config(self, capsys):
        """Test handling of invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [[[")
            f.flush()
            config_path = f.name

        with mock.patch.object(sys, 'argv', ['prog', '-c', config_path]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
            captured = capsys.readouterr()
            assert "invalid yaml" in captured.out.lower() or "error" in captured.out.lower()

    def test_dry_run_mode(self, valid_config, capsys):
        """Test dry-run mode."""
        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config, '--dry-run']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    def test_verbose_flag(self, valid_config):
        """Test verbose flag sets debug logging."""
        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config, '-v', '--dry-run']):
            with mock.patch('src.main.setup_logging') as mock_setup:
                with pytest.raises(SystemExit):
                    main()
                # Check that logging was called with DEBUG level
                call_args = mock_setup.call_args
                assert call_args[0][0]['level'] == 'DEBUG'

    def test_database_filter(self, valid_config):
        """Test database filter."""
        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config, '-d', 'testdb', '--dry-run']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    def test_instance_filter(self, valid_config):
        """Test instance filter."""
        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config, '-i', 'primary', '--dry-run']):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0

    @mock.patch('src.main.DatabaseDumper')
    def test_successful_dump(self, mock_dumper_class, valid_config):
        """Test successful dump execution."""
        # Mock the dumper
        mock_dumper = mock.MagicMock()
        mock_stats = mock.MagicMock()
        mock_stats.databases = [mock.MagicMock()]
        mock_stats.total_tables = 5
        mock_stats.total_rows = 1000
        mock_stats.errors = []
        mock_dumper.run.return_value = mock_stats
        mock_dumper_class.return_value = mock_dumper

        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config]):
            main()

        mock_dumper.run.assert_called_once()

    @mock.patch('src.main.DatabaseDumper')
    def test_dump_with_errors(self, mock_dumper_class, valid_config):
        """Test dump execution with errors."""
        # Mock the dumper with errors
        mock_dumper = mock.MagicMock()
        mock_stats = mock.MagicMock()
        mock_stats.databases = [mock.MagicMock()]
        mock_stats.total_tables = 5
        mock_stats.total_rows = 1000
        mock_stats.errors = [
            {'database': 'testdb', 'table': 'users', 'error': 'Connection lost'}
        ]
        mock_dumper.run.return_value = mock_stats
        mock_dumper_class.return_value = mock_dumper

        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @mock.patch('src.main.DatabaseDumper')
    def test_fatal_error(self, mock_dumper_class, valid_config):
        """Test handling of fatal errors during dump."""
        # Mock the dumper to raise an exception
        mock_dumper = mock.MagicMock()
        mock_dumper.run.side_effect = Exception("Fatal error")
        mock_dumper_class.return_value = mock_dumper

        with mock.patch.object(sys, 'argv', ['prog', '-c', valid_config]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1
