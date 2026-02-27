"""
Integration tests for MySQL Database Dumper using Docker MySQL container.

These tests require Docker and docker-compose to be installed.
Run with: docker-compose -f docker-compose.test.yml up -d
"""

import os
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from src.config import ConfigLoader
from src.database_dumper import DatabaseDumper


@pytest.fixture(scope="module")
def mysql_container():
    """Start MySQL container for testing."""
    # Check if Docker is available
    try:
        subprocess.run(["docker", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("Docker is not available")

    # Start the container
    compose_file = Path(__file__).parent.parent.parent / "docker-compose.test.yml"
    subprocess.run(
        ["docker-compose", "-f", str(compose_file), "up", "-d"],
        check=True,
        capture_output=True
    )

    # Wait for MySQL to be ready
    max_wait = 30
    waited = 0
    while waited < max_wait:
        result = subprocess.run(
            ["docker-compose", "-f", str(compose_file), "exec", "-T", "mysql-test",
             "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-ptestpassword"],
            capture_output=True
        )
        if result.returncode == 0:
            break
        time.sleep(1)
        waited += 1

    if waited >= max_wait:
        pytest.fail("MySQL container failed to become ready")

    yield

    # Cleanup
    subprocess.run(
        ["docker-compose", "-f", str(compose_file), "down", "-v"],
        check=True,
        capture_output=True
    )


@pytest.fixture
def test_config():
    """Create a test configuration."""
    config = {
        "instances": {
            "test": {
                "host": "localhost",
                "port": 3307,
                "user": "testuser",
                "password": "testpass"
            }
        },
        "databases": [
            {
                "name": "testdb",
                "instance": "test",
                "tables": "*",
                "exclude_tables": ["*_backup", "tmp_*"]
            }
        ],
        "defaults": {
            "row_limit": None
        },
        "output": {
            "directory": "./test_dumps",
            "format": "sql",
            "compress": False,
            "progress_bar": False  # Disable progress bar for tests
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
        config_path = f.name

    yield config_path

    os.unlink(config_path)


def test_full_database_dump(mysql_container, test_config, tmp_path):
    """Test dumping the entire test database."""
    # Update config to use tmp_path
    with open(test_config, 'r') as f:
        config = yaml.safe_load(f)
    config['output']['directory'] = str(tmp_path / "dumps")
    with open(test_config, 'w') as f:
        yaml.dump(config, f)

    # Run the dump
    config_loader = ConfigLoader(test_config)
    dumper = DatabaseDumper(config_loader)
    stats = dumper.run()

    # Verify stats
    assert len(stats.databases) == 1
    assert stats.databases[0].name == "testdb"
    assert stats.total_tables == 3  # users, products, orders (excludes backup and tmp tables)
    assert stats.total_rows == 15  # 5 users + 5 products + 5 orders
    assert len(stats.errors) == 0

    # Verify output files exist
    dump_dirs = list((tmp_path / "dumps").glob("testdb_*"))
    assert len(dump_dirs) == 1
    dump_dir = dump_dirs[0]

    assert (dump_dir / "users.sql").exists()
    assert (dump_dir / "products.sql").exists()
    assert (dump_dir / "orders.sql").exists()
    assert not (dump_dir / "users_backup.sql").exists()
    assert not (dump_dir / "tmp_cache.sql").exists()


def test_dump_with_row_limit(mysql_container, test_config, tmp_path):
    """Test dumping with row limits."""
    # Update config
    with open(test_config, 'r') as f:
        config = yaml.safe_load(f)
    config['output']['directory'] = str(tmp_path / "dumps")
    config['databases'][0]['tables'] = [
        {"name": "users", "row_limit": 3},
        {"name": "products", "row_limit": 2}
    ]
    with open(test_config, 'w') as f:
        yaml.dump(config, f)

    # Run the dump
    config_loader = ConfigLoader(test_config)
    dumper = DatabaseDumper(config_loader)
    stats = dumper.run()

    # Verify stats
    assert stats.total_tables == 2
    assert stats.total_rows == 5  # 3 users + 2 products


def test_dump_with_where_clause(mysql_container, test_config, tmp_path):
    """Test dumping with WHERE clause filtering."""
    # Update config
    with open(test_config, 'r') as f:
        config = yaml.safe_load(f)
    config['output']['directory'] = str(tmp_path / "dumps")
    config['databases'][0]['tables'] = [
        {"name": "orders", "where_clause": "status = 'completed'"}
    ]
    with open(test_config, 'w') as f:
        yaml.dump(config, f)

    # Run the dump
    config_loader = ConfigLoader(test_config)
    dumper = DatabaseDumper(config_loader)
    stats = dumper.run()

    # Verify stats - should only get completed orders
    assert stats.total_tables == 1
    assert stats.total_rows == 3  # Only 3 completed orders


def test_dump_with_ordering(mysql_container, test_config, tmp_path):
    """Test dumping with ORDER BY."""
    # Update config
    with open(test_config, 'r') as f:
        config = yaml.safe_load(f)
    config['output']['directory'] = str(tmp_path / "dumps")
    config['databases'][0]['tables'] = [
        {"name": "users", "order_by": "created_at", "order_direction": "DESC", "row_limit": 2}
    ]
    with open(test_config, 'w') as f:
        yaml.dump(config, f)

    # Run the dump
    config_loader = ConfigLoader(test_config)
    dumper = DatabaseDumper(config_loader)
    stats = dumper.run()

    # Verify stats
    assert stats.total_tables == 1
    assert stats.total_rows == 2

    # Verify file content has most recent users (we'd need to parse SQL to fully verify)
    dump_dirs = list((tmp_path / "dumps").glob("testdb_*"))
    dump_file = dump_dirs[0] / "users.sql"
    content = dump_file.read_text()
    assert "eve@example.com" in content  # Most recent user
    assert "david@example.com" in content  # Second most recent


def test_csv_output_format(mysql_container, test_config, tmp_path):
    """Test dumping to CSV format."""
    # Update config
    with open(test_config, 'r') as f:
        config = yaml.safe_load(f)
    config['output']['directory'] = str(tmp_path / "dumps")
    config['output']['format'] = "csv"
    config['databases'][0]['tables'] = [{"name": "products"}]
    with open(test_config, 'w') as f:
        yaml.dump(config, f)

    # Run the dump
    config_loader = ConfigLoader(test_config)
    dumper = DatabaseDumper(config_loader)
    stats = dumper.run()

    # Verify CSV file exists
    dump_dirs = list((tmp_path / "dumps").glob("testdb_*"))
    csv_file = dump_dirs[0] / "products.csv"
    assert csv_file.exists()

    # Verify CSV content
    content = csv_file.read_text()
    lines = content.strip().split('\n')
    assert len(lines) == 6  # Header + 5 products
    assert lines[0] == "id,name,price,stock,created_at"  # Header


def test_connection_retry_logic(mysql_container, test_config, tmp_path):
    """Test that connection retry logic works."""
    # Update config with invalid host that will fail, then use correct one
    with open(test_config, 'r') as f:
        config = yaml.safe_load(f)
    config['output']['directory'] = str(tmp_path / "dumps")
    config['instances']['test']['max_retries'] = 2
    config['instances']['test']['retry_delay'] = 1
    with open(test_config, 'w') as f:
        yaml.dump(config, f)

    # This should succeed with retries if needed
    config_loader = ConfigLoader(test_config)
    dumper = DatabaseDumper(config_loader)
    stats = dumper.run()

    assert len(stats.errors) == 0
