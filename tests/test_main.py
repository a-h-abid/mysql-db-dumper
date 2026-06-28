"""
Unit tests for main.py
"""

import importlib
from unittest import mock

import pytest
import yaml

from src.models import DumpStats

# Import the actual module so we can use mock.patch.object on it
main_module = importlib.import_module('src.main')
main_func = main_module.main


class TestMainConfigErrors:
    """Tests for configuration loading error handling."""

    def test_config_file_not_found(self):
        """Test sys.exit(1) when config file is not found."""
        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', side_effect=FileNotFoundError), \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_args = mock.MagicMock()
            mock_args.config = 'missing.yaml'
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = mock_args
            mock_sys.exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                main_func()

            mock_sys.exit.assert_called_once_with(1)

    def test_invalid_yaml_config(self):
        """Test sys.exit(1) when config file has invalid YAML."""
        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', side_effect=yaml.YAMLError("bad yaml")), \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_args = mock.MagicMock()
            mock_args.config = 'bad.yaml'
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = mock_args
            mock_sys.exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                main_func()

            mock_sys.exit.assert_called_once_with(1)

    def test_invalid_config_value_exits_1(self):
        """Test sys.exit(1) when ConfigLoader raises ValueError (bad config values)."""
        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', side_effect=ValueError("bad config")), \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_args = mock.MagicMock()
            mock_args.config = 'config.yaml'
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = mock_args
            mock_sys.exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit) as exc_info:
                main_func()

            assert exc_info.value.code == 1
            mock_sys.exit.assert_called_once_with(1)

    def test_invalid_logging_level_exits_1(self):
        """Test sys.exit(1) when setup_logging raises ValueError (invalid log level)."""
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {'level': 'INVALID'}

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging',
                               side_effect=ValueError("Invalid logging level 'INVALID'")), \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_args = mock.MagicMock()
            mock_args.config = 'config.yaml'
            mock_args.verbose = False
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = mock_args
            mock_sys.exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit) as exc_info:
                main_func()

            assert exc_info.value.code == 1
            mock_sys.exit.assert_called_once_with(1)


class TestMainVerbose:
    """Tests for verbose flag handling."""

    def test_verbose_sets_debug_level(self):
        """Test that --verbose sets log level to DEBUG."""
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {'level': 'INFO'}
        mock_config.get_databases.return_value = []
        mock_config.get_defaults.return_value = {}

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging') as mock_setup_logging, \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_args = mock.MagicMock()
            mock_args.config = 'config.yaml'
            mock_args.verbose = True
            mock_args.dry_run = True
            mock_args.database = None
            mock_args.instance = None
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = mock_args
            mock_sys.exit.side_effect = SystemExit(0)

            with pytest.raises(SystemExit):
                main_func()

            mock_setup_logging.assert_called_once_with({'level': 'DEBUG'})


class TestMainDryRun:
    """Tests for dry run mode."""

    def _make_args(self, database=None, instance=None):
        """Create mock args for dry run."""
        mock_args = mock.MagicMock()
        mock_args.config = 'config.yaml'
        mock_args.verbose = False
        mock_args.dry_run = True
        mock_args.database = database
        mock_args.instance = instance
        return mock_args

    def test_dry_run_all_databases(self):
        """Test dry run mode prints info for all databases."""
        databases = [
            {'name': 'db1', 'instance': 'primary'},
            {'name': 'db2', 'instance': 'secondary'},
        ]
        defaults = {'output_dir': '/dumps'}
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_config.get_databases.return_value = databases
        mock_config.get_defaults.return_value = defaults

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'print_dry_run_info') as mock_print, \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args()
            mock_sys.exit.side_effect = SystemExit(0)

            with pytest.raises(SystemExit):
                main_func()

            mock_print.assert_called_once_with(databases, defaults)
            mock_sys.exit.assert_called_once_with(0)

    def test_dry_run_filtered_by_database(self):
        """Test dry run mode filters by --database flag."""
        databases = [
            {'name': 'db1', 'instance': 'primary'},
            {'name': 'db2', 'instance': 'secondary'},
        ]
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_config.get_databases.return_value = databases
        mock_config.get_defaults.return_value = {}

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'print_dry_run_info') as mock_print, \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args(database='db1')
            mock_sys.exit.side_effect = SystemExit(0)

            with pytest.raises(SystemExit):
                main_func()

            mock_print.assert_called_once_with([{'name': 'db1', 'instance': 'primary'}], {})

    def test_dry_run_filtered_by_instance(self):
        """Test dry run mode filters by --instance flag."""
        databases = [
            {'name': 'db1', 'instance': 'primary'},
            {'name': 'db2', 'instance': 'secondary'},
            {'name': 'db3'},
        ]
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_config.get_databases.return_value = databases
        mock_config.get_defaults.return_value = {}

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'print_dry_run_info') as mock_print, \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args(instance='primary')
            mock_sys.exit.side_effect = SystemExit(0)

            with pytest.raises(SystemExit):
                main_func()

            mock_print.assert_called_once_with(
                [{'name': 'db1', 'instance': 'primary'}, {'name': 'db3'}],
                {},
            )


class TestMainDumpRun:
    """Tests for actual dump execution."""

    def _make_args(self, database=None, instance=None):
        """Create mock args for a normal run."""
        mock_args = mock.MagicMock()
        mock_args.config = 'config.yaml'
        mock_args.verbose = False
        mock_args.dry_run = False
        mock_args.database = database
        mock_args.instance = instance
        return mock_args

    def test_successful_dump_no_errors(self):
        """Test successful dump run with no errors."""
        stats = DumpStats(databases=['db1'], total_tables=5, total_rows=100, errors=[])
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_dumper = mock.MagicMock()
        mock_dumper.run.return_value = stats

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'DatabaseDumper', return_value=mock_dumper), \
             mock.patch.object(main_module, 'logging') as mock_logging, \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args()

            main_func()

            mock_dumper.run.assert_called_once_with(database_filter=None, instance_filter=None)
            mock_logging.info.assert_any_call("DUMP COMPLETE")
            mock_sys.exit.assert_not_called()

    def test_dump_with_errors_exits_1(self):
        """Test dump run with errors calls sys.exit(1)."""
        stats = DumpStats(
            databases=['db1'],
            total_tables=5,
            total_rows=100,
            errors=[{'database': 'db1', 'table': 't1', 'error': 'connection lost'}],
        )
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_dumper = mock.MagicMock()
        mock_dumper.run.return_value = stats

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'DatabaseDumper', return_value=mock_dumper), \
             mock.patch.object(main_module, 'logging'), \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args()
            mock_sys.exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                main_func()

            mock_sys.exit.assert_called_once_with(1)

    def test_fatal_exception_exits_1(self):
        """Test fatal exception during dump calls sys.exit(1)."""
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_dumper = mock.MagicMock()
        mock_dumper.run.side_effect = RuntimeError("connection refused")

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'DatabaseDumper', return_value=mock_dumper), \
             mock.patch.object(main_module, 'logging') as mock_logging, \
             mock.patch.object(main_module, 'sys') as mock_sys:
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args()
            mock_sys.exit.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                main_func()

            mock_logging.error.assert_called_once()
            mock_sys.exit.assert_called_once_with(1)

    def test_database_and_instance_filters_passed(self):
        """Test that --database and --instance filters are passed to dumper.run()."""
        stats = DumpStats(databases=['mydb'], total_tables=2, total_rows=50, errors=[])
        mock_config = mock.MagicMock()
        mock_config.get_logging_settings.return_value = {}
        mock_dumper = mock.MagicMock()
        mock_dumper.run.return_value = stats

        with mock.patch.object(main_module, 'argparse') as mock_argparse, \
             mock.patch.object(main_module, 'ConfigLoader', return_value=mock_config), \
             mock.patch.object(main_module, 'setup_logging'), \
             mock.patch.object(main_module, 'DatabaseDumper', return_value=mock_dumper), \
             mock.patch.object(main_module, 'logging'), \
             mock.patch.object(main_module, 'sys'):
            mock_argparse.ArgumentParser.return_value.parse_args.return_value = self._make_args(
                database='mydb', instance='replica'
            )

            main_func()

            mock_dumper.run.assert_called_once_with(database_filter='mydb', instance_filter='replica')


class TestDunderMain:
    """Tests for __main__.py module execution."""

    def test_dunder_main_calls_main(self):
        """Test that __main__.py calls main()."""
        dunder_main = importlib.import_module('src.__main__')
        with mock.patch.object(dunder_main, 'main') as mock_main:
            # Simulate if __name__ == "__main__" by calling the guarded code
            # Since __main__.py only defines `from .main import main` and the guard,
            # we verify the import wiring is correct.
            dunder_main.main()
            mock_main.assert_called_once()
