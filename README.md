# MySQL Database Dumper

A configurable Python script to dump MySQL databases with support for multiple instances, row limits, custom ordering, and more.

## Features

- **Multiple Database Instances**: Connect to different MySQL servers
- **Configurable Row Limits**: Dump all rows or specify a limit
- **Custom Ordering**: Sort by any column in ASC or DESC order
- **WHERE Clauses**: Filter data with custom conditions
- **Multiple Output Formats**: SQL or CSV
- **Compression**: Optional gzip compression
- **Environment Variables**: Secure password management via env vars
- **Flexible Configuration**: YAML-based configuration file

## Installation

```bash
# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Project Structure

```
src/
├── __init__.py        # Package exports
├── __main__.py        # Entry point for `python -m src`
├── main.py            # CLI argument parsing
├── config.py          # Configuration loading
├── connection.py      # Database connection management
├── database_dumper.py # Main orchestration
├── table_dumper.py    # Table dump logic
├── models.py          # Data models and enums
└── utils.py           # Utility functions
```

## Configuration

Edit `config.yaml` to configure your dump settings:

### Database Instances

```yaml
instances:
  primary:
    host: "localhost"
    port: 3306
    user: "root"
    password: "your_password"

  secondary:
    host: "192.168.1.100"
    port: 3306
    user: "admin"
    password: "${MYSQL_SECONDARY_PASSWORD}"  # Environment variable
```

### Databases and Tables

```yaml
databases:
  # Dump all tables from a database
  - name: "my_database"
    instance: "primary"
    tables: "*"

  # Dump specific tables with custom settings
  - name: "analytics_db"
    instance: "primary"
    tables:
      - name: "events"
        row_limit: 10000
        order_by: "created_at"
        order_direction: "DESC"
        where_clause: "status = 'active'"
```

### Excluding Tables

You can exclude tables using exact names or wildcard patterns:

```yaml
databases:
  - name: "my_database"
    instance: "primary"
    tables: "*"
    exclude_tables:
      - "users_backup"    # Exact match
      - "*_old"           # Tables ending with _old
      - "tmp_*"           # Tables starting with tmp_
      - "*_backup_*"      # Tables containing _backup_
      - "temp_??"         # temp_ followed by exactly 2 characters
```

**Supported wildcards:**
- `*` - matches any number of characters
- `?` - matches any single character
- `[seq]` - matches any character in seq
- `[!seq]` - matches any character not in seq

### Configuration Options

| Option | Level | Description |
|--------|-------|-------------|
| `row_limit` | default/database/table | Number of rows to dump (null = unlimited) |
| `order_by` | default/database/table | Column to sort by |
| `order_direction` | default/database/table | ASC or DESC |
| `where_clause` | default/database/table | SQL WHERE condition |
| `exclude_tables` | database | List of table patterns to exclude (supports wildcards) |

### Logging Configuration

You can configure logging behavior in the `logging` section of `config.yaml`.

```yaml
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
  file: "./dumps/dump.log"
```

**Docker / Stdout Only:**
To write logs only to stdout (useful for Docker containers), comment out or remove the `file` option:

```yaml
logging:
  level: "INFO"
  # file: "./dumps/dump.log"  <-- Comment out to disable file logging
```

Settings cascade: `defaults` → `database` → `table` (most specific wins)

## Usage

### Basic Usage

```bash
python -m src
```

### With Custom Config File

```bash
python -m src -c /path/to/config.yaml
```

### Dry Run (Preview)

```bash
python -m src --dry-run
```

### Verbose Output

```bash
python -m src -v
```

### Dump Specific Database

Dump only a specific database from your configuration:

```bash
python -m src --database my_database
# or
python -m src -d my_database
```

### Dump from Specific Instance

Dump only databases configured for a specific instance:

```bash
python -m src --instance primary
# or
python -m src -i secondary
```

### Combine Filters

You can combine filters to dump a specific database from a specific instance:

```bash
python -m src --database users --instance us_east
```

### Command Line Options

| Option | Short | Description |
|--------|-------|-------------|
| `--config` | `-c` | Path to configuration file (default: config.yaml) |
| `--verbose` | `-v` | Enable verbose/debug output |
| `--dry-run` | | Preview what would be dumped without dumping |
| `--database` | `-d` | Dump only the specified database |
| `--instance` | `-i` | Dump only databases from the specified instance |

## Output Structure

```
dumps/
├── ecommerce_db_20241229_143022/
│   ├── users.sql
│   ├── orders.sql
│   └── products.sql
├── analytics_db_20241229_143022/
│   ├── user_events.sql
│   └── page_views.sql
└── dump.log
```

## Environment Variables

For security, you can use environment variables for passwords:

```yaml
instances:
  production:
    password: "${MYSQL_PROD_PASSWORD}"
```

Then set the environment variable:

```bash
export MYSQL_PROD_PASSWORD="your_secure_password"
python -m src
```

## Examples

### Dump Last 1000 Orders

```yaml
databases:
  - name: "shop_db"
    instance: "primary"
    tables:
      - name: "orders"
        row_limit: 1000
        order_by: "order_date"
        order_direction: "DESC"
```

### Dump Active Users Only

```yaml
databases:
  - name: "app_db"
    instance: "primary"
    tables:
      - name: "users"
        where_clause: "status = 'active' AND last_login > '2024-01-01'"
        order_by: "last_login"
        order_direction: "DESC"
```

### Dump Table Structure Only (No Data)

To export only the table schema without any data, set `row_limit: 0`:

```yaml
# For all tables in a database
databases:
  - name: "my_database"
    instance: "primary"
    row_limit: 0
    tables: "*"
```

```yaml
# For specific tables
databases:
  - name: "my_database"
    instance: "primary"
    tables:
      - name: "users"
        row_limit: 0
      - name: "orders"
        row_limit: 0
```

```yaml
# As a global default for all databases
defaults:
  row_limit: 0
```

This will generate SQL files containing only `DROP TABLE` and `CREATE TABLE` statements without any `INSERT` statements.

### Dump to CSV with Compression

```yaml
output:
  directory: "./exports"
  format: "csv"
  compress: true
```

### Multiple Instances

```yaml
instances:
  us_east:
    host: "db-us-east.example.com"
    port: 3306
    user: "reader"
    password: "${US_EAST_DB_PASSWORD}"

  eu_west:
    host: "db-eu-west.example.com"
    port: 3306
    user: "reader"
    password: "${EU_WEST_DB_PASSWORD}"

databases:
  - name: "users"
    instance: "us_east"
    tables: "*"

  - name: "users"
    instance: "eu_west"
    tables: "*"
```

## License

MIT License
