# New Features Documentation

This document describes the new features implemented for the MySQL Database Dumper.

## 1. Visual Progress Bars with tqdm

### Overview
Replaced text-based logging progress indicators with visual progress bars showing real-time progress, speed, and ETA.

### Usage
```bash
# Progress bars are enabled by default
python -m src

# Disable progress bars (use logging instead)
# Set in config.yaml:
output:
  progress_bar: false
```

### Benefits
- Visual feedback during long-running dumps
- Shows rows/second processing speed
- Displays estimated time remaining
- Non-intrusive (clears after completion)

## 2. Integration Tests with MySQL Docker

### Overview
Comprehensive integration test suite using real MySQL database in Docker container.

### Running Integration Tests
```bash
# Start MySQL test container
docker-compose -f docker-compose.test.yml up -d

# Wait for MySQL to be ready (healthcheck)
sleep 10

# Run integration tests
python -m pytest tests/integration/ -v

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

### Test Coverage
- Full database dumps
- Row limits
- WHERE clause filtering
- ORDER BY clauses
- CSV format exports
- Table exclusion patterns
- Connection retry logic

## 3. Incremental/Differential Dumps

### Overview
Dump only rows that have changed since the last dump, based on a timestamp column.

### Usage
```bash
# First dump (full dump)
python -m src --since updated_at

# Subsequent dumps (only new/updated rows)
python -m src --since updated_at

# Clear metadata and start fresh
python -m src --clear-metadata

# Clear metadata for specific database
python -m src --clear-metadata --database mydb
```

### How It Works
1. Tracks last dump time in `.dump_metadata.json` file
2. Generates WHERE clause: `updated_at > 'last_dump_time'`
3. Combines with existing WHERE clauses if present
4. Updates metadata after successful dump

### Configuration
```yaml
# Optional: specify metadata file location
output:
  metadata_file: ".dump_metadata.json"
```

### Example
```bash
# Dump all orders created/updated since last dump
python -m src --database orders --since updated_at

# Output:
# First run: Dumps all rows (no previous metadata)
# Second run: Only dumps rows where updated_at > last_dump_time
```

## 4. Parallel Table Dumping

### Overview
Dump multiple tables simultaneously using parallel threads for faster completion.

### Usage
```bash
# Enable parallel dumping with default 4 workers
python -m src --parallel

# Specify number of workers
python -m src --parallel --max-workers 8

# Combine with other options
python -m src --parallel --max-workers 4 --database mydb
```

### How It Works
1. Creates thread pool with N workers
2. Each worker gets its own database connection
3. Tables dumped concurrently (order not guaranteed)
4. Thread-safe statistics aggregation
5. Automatic fallback to sequential for single-file mode

### Performance
- **Best for**: Many small-medium tables
- **Speedup**: Up to Nx faster (N = number of tables or workers, whichever is smaller)
- **Trade-offs**: Increased database load, more connections

### Limitations
- Only works with `separate_files: true` mode
- Falls back to sequential for single-file dumps
- Progress bars show individual table progress

### Example Performance
```bash
# Sequential: 5 tables @ 30s each = 150s total
python -m src

# Parallel: 5 tables @ 30s each with 4 workers = ~45s total
python -m src --parallel --max-workers 4
```

## 5. Connection Management

### Overview
Parallel execution naturally implements connection pooling - each worker thread maintains its own connection.

### Details
- **Sequential mode**: Single connection reused
- **Parallel mode**: One connection per worker
- **Auto-cleanup**: Connections closed after each table
- **Retry logic**: Applied to each connection independently

## Combined Usage Examples

### Example 1: Fast Incremental Backup
```bash
# Fast parallel incremental dump
python -m src \
  --since updated_at \
  --parallel \
  --max-workers 8
```

### Example 2: Specific Database with Progress
```bash
# Single database, parallel, with progress bars
python -m src \
  --database production \
  --parallel \
  --max-workers 4
```

### Example 3: Full Backup Reset
```bash
# Clear metadata and do fresh parallel dump
python -m src \
  --clear-metadata \
  --parallel \
  --max-workers 6
```

## Configuration Reference

### New Configuration Options

```yaml
# config.yaml

output:
  # Enable/disable progress bars (default: true)
  progress_bar: true

  # Metadata file for incremental dumps (default: .dump_metadata.json)
  metadata_file: ".dump_metadata.json"

  # Other existing options...
  directory: "./dumps"
  format: "sql"
  separate_files: true
```

### Command Line Options

| Option | Short | Description |
|--------|-------|-------------|
| `--since COLUMN` | | Enable incremental dump based on timestamp column |
| `--clear-metadata` | | Clear incremental metadata and start fresh |
| `--parallel` | | Enable parallel table dumping |
| `--max-workers N` | | Number of parallel workers (default: 4) |

## Best Practices

### When to Use Parallel Dumping
- ✅ Multiple tables of similar size
- ✅ Separate files mode enabled
- ✅ Database server can handle multiple connections
- ✅ Network bandwidth is not the bottleneck
- ❌ Single very large table
- ❌ Single file output mode
- ❌ Restricted connection limits

### When to Use Incremental Dumps
- ✅ Tables have `updated_at` or `modified_at` column
- ✅ Regular periodic backups needed
- ✅ Want to minimize data transfer
- ❌ No timestamp column available
- ❌ Need complete historical dumps

### Performance Tuning
```bash
# For many small tables
python -m src --parallel --max-workers 8

# For few large tables
python -m src --parallel --max-workers 2

# For incremental only recent data
python -m src --since updated_at

# For maximum performance
python -m src --since updated_at --parallel --max-workers 8
```

## Troubleshooting

### Parallel Dumps Not Working
- Check: `separate_files: true` in config
- Check: More than 1 table to dump
- Check: Database connection limits

### Incremental Dumps Missing Data
- Verify timestamp column exists in all tables
- Check `.dump_metadata.json` for corrupt data
- Use `--clear-metadata` to reset

### Progress Bars Not Showing
- Check: `progress_bar: true` in config (default)
- Check: Not running in non-TTY environment
- Set `progress_bar: false` for CI/CD

## Migration from Previous Version

All new features are opt-in and backward compatible:

```bash
# Old command still works exactly the same
python -m src

# New features are optional
python -m src --parallel --since updated_at
```

No configuration changes required unless you want to use the new features.
