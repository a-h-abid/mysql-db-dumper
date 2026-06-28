# MySQL Dumper Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the correctness, data-loss, and reliability findings from the code review so that dumps are safe to restore, faithful to the source data, and robust against bad config.

**Architecture:** The codebase is a single-process Python CLI (`src/`) with focused modules: `config.py` (YAML + env-var loading), `connection.py` (mysql.connector wrapper), `table_dumper.py` (per-table serialization), `database_dumper.py` (orchestration), `models.py` (dataclasses/enums), `utils.py` (logging + dry-run). Each fix is a localized change to one or two of these modules plus its unit test. No new runtime dependencies are introduced.

**Tech Stack:** Python 3.10–3.12, `mysql-connector-python>=8.0.0`, `PyYAML>=6.0`, `pytest>=7.0.0`, `pytest-cov>=4.0.0`.

## Global Constraints

- **No new runtime dependencies.** Keep `requirements.txt` to `mysql-connector-python` + `PyYAML`. Fixes use only the standard library + existing deps.
- **Python floor is 3.10** — built-in generics (`list[...]`, `dict[...]`, `X | Y`) are already used and remain fine.
- **Tests must keep passing under the CI gate**, which runs:
  `python -m pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=95 -v`
  (defined in `.github/workflows/test.yml`). Each task that changes behavior MUST update any existing test that asserted the old behavior — this plan calls those out explicitly.
- **Single-test command pattern:** `python -m pytest tests/<file>::<Class>::<test> -v`.
- **Behavior-change config keys** added by this plan: `output.add_drop_table` (Task 2) and per-instance `consistent_snapshot` (Task 6). Both default to the safe behavior when absent; document them in `config.example.yaml` as part of the task that adds them.
- **Style:** match the existing file — f-strings for logging, `logging` module (never `print` except the pre-logging config-error path in `main.py`), type hints on new functions, module-level helper functions where shared.

### Prerequisites (run once before Task 1)

- [ ] **Set up a virtualenv with deps so the test suite can import `mysql.connector`:**

```bash
cd /home/abid/dev-projects/abd/mysql-db-dumper
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
python -m pytest tests/ -q   # baseline: confirm the suite is green before changing anything
```

Expected: all tests pass (baseline green). Keep this venv active for every task's test step.

---

## File Structure

| File | Responsibility | Touched by tasks |
|---|---|---|
| `src/table_dumper.py` | Per-table query build + SQL/CSV serialization + file IO | 1, 2, 5, 7, 11, 12 |
| `src/models.py` | Dataclasses, enums, `coerce_optional_int` helper | 3 |
| `src/connection.py` | Connection wrapper; numeric coercion; snapshot txn | 3, 6 |
| `src/config.py` | YAML load + env-var resolution (fail-fast) | 4 |
| `src/main.py` | CLI entry; config/logging error handling | 4, 8 |
| `src/utils.py` | Logging setup (level validation) | 8 |
| `src/database_dumper.py` | Orchestration; snapshot gate; config resilience; path safety | 6, 9, 10 |
| `config.example.yaml` | Documented sample config | 2, 6 |
| `tests/test_table_dumper.py` | Unit tests for serialization | 1, 2, 5, 7, 11, 12 |
| `tests/test_models.py` | Unit tests for models/helpers | 3 |
| `tests/test_config.py` | Unit tests for config loader | 4 |
| `tests/test_utils.py` | Unit tests for logging setup | 8 |
| `tests/test_connection.py` | Unit tests for connection | 6 |
| `tests/test_database_dumper.py` | Unit tests for orchestration | 6, 9, 10 |

Tasks are designed to be applied **in order**; later tasks assume the edits from earlier ones (e.g. Tasks 5/7/11 edit the `dump_table`/`_build_select_query` shape produced by Task 2).

---

### Task 1: Faithful value serialization (escaping + type formatters)

Fixes review #2 (escaper misses `\0`, `\x1a`, `bytearray`) and #5 (`Decimal`/`date`/`time`/`timedelta` mis-serialized, datetime microseconds dropped). Also adds the adversarial-value tests the suite currently lacks (NEW-4).

**Files:**
- Modify: `src/table_dumper.py` (imports, `_type_formatters`, `_format_sql_value`, new `_format_timedelta`)
- Test: `tests/test_table_dumper.py` (add cases to `TestFormatSqlValue`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `_format_sql_value(value)` now correctly serializes `bytearray`, `Decimal`, `date`, `time`, `timedelta`, microsecond-precision `datetime`, and strings containing NUL / Ctrl-Z. `_format_timedelta(v: timedelta) -> str` module-level helper returning a quoted `'[-]HH:MM:SS[.ffffff]'` literal.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_table_dumper.py` inside `class TestFormatSqlValue` (after `test_format_datetime_via_method`):

```python
    def test_format_datetime_preserves_microseconds(self, dumper):
        """Microseconds must not be silently dropped."""
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        assert dumper._format_sql_value(dt) == "'2024-01-15 10:30:45.123456'"

    def test_format_string_with_null_byte(self, dumper):
        """NUL byte must be escaped as \\0, not emitted raw."""
        assert dumper._format_sql_value("a\x00b") == "'a\\0b'"

    def test_format_string_with_ctrl_z(self, dumper):
        """Ctrl-Z must be escaped as \\Z."""
        assert dumper._format_sql_value("a\x1ab") == "'a\\Zb'"

    def test_format_bytearray_as_hex(self, dumper):
        """bytearray must serialize as a hex literal like bytes."""
        assert dumper._format_sql_value(bytearray(b"\x00\xff")) == "X'00ff'"

    def test_format_decimal_unquoted(self, dumper):
        """Decimal must be an unquoted numeric literal."""
        from decimal import Decimal
        assert dumper._format_sql_value(Decimal("1.50")) == "1.50"

    def test_format_date(self, dumper):
        """date must serialize as a quoted ISO date."""
        from datetime import date
        assert dumper._format_sql_value(date(2024, 1, 15)) == "'2024-01-15'"

    def test_format_time(self, dumper):
        """time must serialize as a quoted ISO time."""
        from datetime import time
        assert dumper._format_sql_value(time(10, 30, 45)) == "'10:30:45'"

    def test_format_timedelta(self, dumper):
        """timedelta (MySQL TIME) must serialize as 'HH:MM:SS'."""
        from datetime import timedelta
        assert dumper._format_sql_value(timedelta(hours=10, minutes=30, seconds=45)) == "'10:30:45'"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_table_dumper.py::TestFormatSqlValue -v`
Expected: the 8 new tests FAIL (e.g. `test_format_bytearray_as_hex` produces `"'bytearray(...)'"`; `test_format_string_with_null_byte` leaves the raw NUL).

- [ ] **Step 3: Update imports in `src/table_dumper.py`**

Replace:

```python
from datetime import datetime
```

with:

```python
from datetime import date, datetime, time, timedelta
from decimal import Decimal
```

- [ ] **Step 4: Add the `_format_timedelta` helper at module level**

Insert immediately after the imports block (before `class TableDumper:`):

```python
def _format_timedelta(value: timedelta) -> str:
    """Format a timedelta as a MySQL TIME literal '[-]HH:MM:SS[.ffffff]'."""
    sign = '-' if value < timedelta(0) else ''
    value = abs(value)
    total_seconds = value.days * 86400 + value.seconds
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    out = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if value.microseconds:
        out += f".{value.microseconds:06d}"
    return f"'{out}'"
```

- [ ] **Step 5: Extend `_type_formatters` and fix the slow-path escaping**

Replace the `_type_formatters` dict (currently lines ~28-35):

```python
        self._type_formatters: dict[type, callable] = {
            type(None): lambda v: 'NULL',
            bool: lambda v: '1' if v else '0',
            int: str,
            float: str,
            bytes: lambda v: f"X'{v.hex()}'",
            datetime: lambda v: f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'",
        }
```

with:

```python
        self._type_formatters: dict[type, callable] = {
            type(None): lambda v: 'NULL',
            bool: lambda v: '1' if v else '0',
            int: str,
            float: str,
            Decimal: str,
            bytes: lambda v: f"X'{v.hex()}'",
            bytearray: lambda v: f"X'{v.hex()}'",
            datetime: lambda v: f"'{v.isoformat(' ')}'",
            date: lambda v: f"'{v.isoformat()}'",
            time: lambda v: f"'{v.isoformat()}'",
            timedelta: _format_timedelta,
        }
```

Then replace the slow path in `_format_sql_value` (currently lines ~210-213):

```python
        # Slow path: string conversion with escaping
        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        escaped = escaped.replace("\n", "\\n").replace("\r", "\\r")
        return f"'{escaped}'"
```

with:

```python
        # Slow path: string conversion with escaping.
        # Backslash MUST be escaped first so the escape sequences we add below
        # are not themselves doubled. Covers the same specials as mysqldump.
        escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
        escaped = escaped.replace("\n", "\\n").replace("\r", "\\r")
        escaped = escaped.replace("\0", "\\0").replace("\x1a", "\\Z")
        return f"'{escaped}'"
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/test_table_dumper.py::TestFormatSqlValue tests/test_table_dumper.py::TestTypeFormatters -v`
Expected: all PASS (existing `test_format_datetime` still passes because `isoformat(' ')` omits microseconds when they are zero).

- [ ] **Step 7: Commit**

```bash
git add src/table_dumper.py tests/test_table_dumper.py
git commit -m "fix: faithful SQL value serialization (escaping, bytearray, Decimal, date/time/timedelta, datetime µs)"
```

---

### Task 2: Restore-safety — guard DROP TABLE on partial dumps + atomic file writes

Fixes review #1 (every dump emits `DROP TABLE`, so restoring a `row_limit`/`where_clause` dump destroys the live table) and NEW-2 (interrupted dumps leave a truncated file that looks complete). Approach: never emit `DROP TABLE` for a filtered/limited dump; always emit `CREATE TABLE` so a restore onto an existing table fails loudly instead of destroying data; write to a `.partial` file and atomically rename only on success.

**Files:**
- Modify: `src/table_dumper.py` (`dump_table`, `_open_output_file`, `_dump_as_sql`)
- Modify: `config.example.yaml` (document `add_drop_table`)
- Test: `tests/test_table_dumper.py` (update `TestOpenOutputFile`; add partial-dump + atomic-write tests)

**Interfaces:**
- Consumes: `DumpSettings.row_limit`, `DumpSettings.where_clause` (from `models.py`, unchanged).
- Produces:
  - `_open_output_file(output_path, append) -> tuple[Path, Path, TextIO]` returning `(final_path, write_path, handle)`. **Signature changed from a 2-tuple to a 3-tuple.**
  - `_dump_as_sql(file_handle, table, columns, query, add_drop_table: bool = True) -> int` — new trailing param.
  - New optional config key `output.add_drop_table` (bool). Absent → DROP only on full dumps. Even if `true`, DROP is still suppressed for partial dumps.

- [ ] **Step 1: Write the failing tests**

In `tests/test_table_dumper.py`, **replace** the three methods in `class TestOpenOutputFile` (`test_open_without_compression`, `test_open_with_compression`, `test_open_append_mode`) with:

```python
    def test_open_without_compression(self, dumper_no_compress):
        """Non-append writes go to a .partial temp file; final is the plain path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            final, write_path, handle = dumper_no_compress._open_output_file(
                output_path, append=False
            )
            handle.close()

            assert final == output_path
            assert write_path == Path(str(output_path) + ".partial")
            assert not str(final).endswith(".gz")

    def test_open_with_compression(self, dumper_with_compress):
        """Compression adds .gz to final, and the temp file is .gz.partial."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            final, write_path, handle = dumper_with_compress._open_output_file(
                output_path, append=False
            )
            handle.close()

            assert str(final).endswith(".gz")
            assert final == Path(str(output_path) + ".gz")
            assert write_path == Path(str(output_path) + ".gz.partial")

    def test_open_append_mode(self, dumper_no_compress):
        """Append mode targets the final file directly (no .partial)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            output_path.write_text("initial")  # simulate an existing single-file dump

            final, write_path, handle = dumper_no_compress._open_output_file(
                output_path, append=True
            )
            handle.write("appended")
            handle.close()

            assert write_path == final == output_path
            content = output_path.read_text()
            assert "initial" in content
            assert "appended" in content
```

Then add to `class TestDumpTable` (after `test_dump_table_sql_with_data`):

```python
    def test_partial_dump_omits_drop_table(self, mock_connection):
        """A row_limited dump must NOT emit DROP TABLE (restore would destroy data)."""
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "Alice")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"
            settings = DumpSettings(row_limit=10)  # partial -> unsafe to recreate

            stats = dumper.dump_table("users", output_path, settings, OutputFormat.SQL)

            content = output_path.read_text()
            assert stats.success is True
            assert "DROP TABLE" not in content
            assert "CREATE TABLE" in content

    def test_failed_dump_leaves_no_final_file(self, mock_connection):
        """If the dump dies mid-write, the final path must not exist; .partial remains."""
        mock_cursor = mock.MagicMock()
        mock_cursor.execute.side_effect = Exception("connection dropped")
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.sql"

            stats = dumper.dump_table("users", output_path, DumpSettings(), OutputFormat.SQL)

            assert stats.success is False
            assert not output_path.exists()
            assert Path(str(output_path) + ".partial").exists()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_table_dumper.py::TestOpenOutputFile tests/test_table_dumper.py::TestDumpTable -v`
Expected: the `TestOpenOutputFile` tests FAIL (current `_open_output_file` returns a 2-tuple → unpack error), and `test_partial_dump_omits_drop_table` / `test_failed_dump_leaves_no_final_file` FAIL.

- [ ] **Step 3: Rewrite `_open_output_file` for atomic temp writes**

Replace the whole method (currently lines ~92-102):

```python
    def _open_output_file(self, output_path: Path, append: bool) -> tuple[Path, Path, TextIO]:
        """Open output file with optional compression.

        Returns (final_path, write_path, handle). For non-append writes, write_path is a
        temporary '.partial' file the caller atomically renames to final_path on success,
        so an interrupted dump never leaves a truncated file at final_path. Append mode
        targets final_path directly (a shared single-file dump cannot be rebuilt per table).
        """
        compress = self.output_settings.get('compress', False)
        final_path = Path(str(output_path) + '.gz') if compress else output_path

        if append:
            write_path = final_path
            file_mode = 'at'
        else:
            write_path = Path(str(final_path) + '.partial')
            file_mode = 'wt'

        if compress:
            file_handle = gzip.open(write_path, file_mode, encoding='utf-8')
        else:
            file_handle = open(write_path, file_mode[0], encoding='utf-8')

        return final_path, write_path, file_handle
```

- [ ] **Step 4: Rewrite `dump_table` to compute the DROP guard and rename on success**

Replace the body of `dump_table` from `stats = TableStats(...)` through `return stats` (currently lines ~58-90) with:

```python
        stats = TableStats(table=table, file_path=str(output_path))

        try:
            columns = self.connection.get_table_columns(table)
            column_names = [col.name for col in columns]

            query = self._build_select_query(table, column_names, settings)
            logging.info(f"Dumping table '{table}' with query: {query[:200]}...")

            # A dump that contains only a subset of rows must NOT recreate the table on
            # restore: doing so would DROP the live table and replace it with the subset.
            # Only a full, unfiltered dump may emit DROP TABLE.
            full_table = settings.row_limit is None and not settings.where_clause
            add_drop_table = self.output_settings.get('add_drop_table', full_table)
            if add_drop_table and not full_table:
                logging.warning(
                    f"Refusing to emit DROP TABLE for partial dump of '{table}' "
                    f"(row_limit/where_clause set); restoring it would destroy data."
                )
                add_drop_table = False

            final_path, write_path, file_handle = self._open_output_file(output_path, append)
            stats.file_path = str(final_path)

            try:
                if output_format == OutputFormat.SQL:
                    stats.rows_dumped = self._dump_as_sql(
                        file_handle, table, column_names, query, add_drop_table
                    )
                elif output_format == OutputFormat.CSV:
                    stats.rows_dumped = self._dump_as_csv(
                        file_handle, table, column_names, query
                    )
                else:
                    raise ValueError(f"Unsupported output format: {output_format}")
            finally:
                file_handle.close()

            # Atomic publish: only expose a fully-written dump at the final path.
            if write_path != final_path:
                Path(write_path).replace(final_path)

            stats.success = True

        except Exception as e:
            stats.error = str(e)
            logging.error(f"Error dumping table '{table}': {e}")

        return stats
```

- [ ] **Step 5: Guard the DROP in `_dump_as_sql`**

Change the signature (currently line ~134-140) to add the parameter:

```python
    def _dump_as_sql(
        self,
        file_handle: TextIO,
        table: str,
        columns: list[str],
        query: str,
        add_drop_table: bool = True
    ) -> int:
```

and replace the DROP/CREATE write block (currently lines ~148-151):

```python
        # Write CREATE TABLE statement
        create_statement = self.connection.get_create_table(table)
        file_handle.write(f"DROP TABLE IF EXISTS `{table}`;\n\n")
        file_handle.write(f"{create_statement};\n\n")
```

with:

```python
        # Write CREATE TABLE statement. DROP is only safe for a full dump (see dump_table).
        create_statement = self.connection.get_create_table(table)
        if add_drop_table:
            file_handle.write(f"DROP TABLE IF EXISTS `{table}`;\n\n")
        file_handle.write(f"{create_statement};\n\n")
```

- [ ] **Step 6: Document the new flag in `config.example.yaml`**

In the `output:` block, after the `batch_size:` line, add:

```yaml
  # add_drop_table: false  # force-disable DROP TABLE. Default: DROP only on full
  #                        # (unfiltered) dumps; partial dumps never DROP, to keep restores safe.
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `python -m pytest tests/test_table_dumper.py -v`
Expected: all PASS, including the existing `test_dump_table_sql_with_data` (full dump → DROP still present) and `test_dump_table_compressed` (`stats.file_path` ends with `.gz`).

- [ ] **Step 8: Commit**

```bash
git add src/table_dumper.py config.example.yaml tests/test_table_dumper.py
git commit -m "fix: never DROP TABLE on partial dumps; write dumps atomically via .partial rename"
```

---

### Task 3: Coerce env-sourced numeric config to int

Fixes NEW-1: env-var interpolation always yields strings, so `row_limit: "${LIMIT}"` arrives as `"5000"` and crashes the `row_limit >= 0` comparison; `batch_size`/`port`/`connect_timeout` have the same flaw.

**Files:**
- Modify: `src/models.py` (add `coerce_optional_int`; use it in `DumpSettings.from_configs`)
- Modify: `src/connection.py` (coerce `port`, `connect_timeout` in `__init__`)
- Modify: `src/table_dumper.py` (coerce `batch_size` in `__init__`)
- Test: `tests/test_models.py` (new `TestCoerceOptionalInt` class)

**Interfaces:**
- Produces: `coerce_optional_int(value: Any, field_name: str) -> Optional[int]` in `src/models.py` — returns `None` for `None`, the `int` for ints / numeric strings, and raises `ValueError` for booleans or non-numeric values. Imported by `connection.py` and `table_dumper.py`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_models.py`:

```python
class TestCoerceOptionalInt:
    """Tests for coerce_optional_int and its use in DumpSettings."""

    def test_passthrough_int(self):
        from src.models import coerce_optional_int
        assert coerce_optional_int(5000, "row_limit") == 5000

    def test_none_stays_none(self):
        from src.models import coerce_optional_int
        assert coerce_optional_int(None, "row_limit") is None

    def test_numeric_string_coerced(self):
        from src.models import coerce_optional_int
        assert coerce_optional_int("5000", "row_limit") == 5000

    def test_garbage_string_raises(self):
        import pytest
        from src.models import coerce_optional_int
        with pytest.raises(ValueError):
            coerce_optional_int("not-a-number", "row_limit")

    def test_bool_rejected(self):
        import pytest
        from src.models import coerce_optional_int
        with pytest.raises(ValueError):
            coerce_optional_int(True, "row_limit")

    def test_from_configs_coerces_string_row_limit(self):
        from src.models import DumpSettings
        settings = DumpSettings.from_configs({}, {"row_limit": "5000"}, {})
        assert settings.row_limit == 5000
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_models.py::TestCoerceOptionalInt -v`
Expected: FAIL with `ImportError: cannot import name 'coerce_optional_int'`.

- [ ] **Step 3: Add the helper and use it in `DumpSettings.from_configs`**

In `src/models.py`, add this function immediately after the imports (before `class OutputFormat`):

```python
def coerce_optional_int(value: Any, field_name: str) -> Optional[int]:
    """Coerce a config value to int (or None), accepting numeric strings.

    Environment-variable interpolation yields strings, so a config value like
    row_limit: "${LIMIT}" arrives as "5000" rather than 5000. Normalize such values
    and fail loudly on genuinely non-numeric input.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"'{field_name}' must be an integer, got boolean {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"'{field_name}' must be an integer, got {value!r}")
```

Then in `DumpSettings.from_configs`, replace the final `return cls(**settings)` with:

```python
        if 'row_limit' in settings:
            settings['row_limit'] = coerce_optional_int(settings['row_limit'], 'row_limit')
        return cls(**settings)
```

- [ ] **Step 4: Coerce `port` and `connect_timeout` in `src/connection.py`**

Change the import line:

```python
from .models import ColumnInfo
```

to:

```python
from .models import ColumnInfo, coerce_optional_int
```

Then in `DatabaseConnection.__init__`, replace:

```python
        self.port = port
```

with:

```python
        self.port = coerce_optional_int(port, 'port')
```

and replace:

```python
        self.connect_timeout = connect_timeout
```

with:

```python
        self.connect_timeout = coerce_optional_int(connect_timeout, 'connect_timeout')
```

- [ ] **Step 5: Coerce `batch_size` in `src/table_dumper.py`**

Change the import line:

```python
from .models import DumpSettings, OutputFormat, TableStats
```

to:

```python
from .models import DumpSettings, OutputFormat, TableStats, coerce_optional_int
```

Then in `TableDumper.__init__`, replace:

```python
        self.batch_size = output_settings.get('batch_size', self.DEFAULT_BATCH_SIZE)
```

with:

```python
        self.batch_size = coerce_optional_int(
            output_settings.get('batch_size'), 'batch_size'
        ) or self.DEFAULT_BATCH_SIZE
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/test_models.py tests/test_connection.py tests/test_table_dumper.py -v`
Expected: all PASS (existing `test_connect` still asserts `port=3306`; `test_init` still asserts `batch_size == 1000`).

- [ ] **Step 7: Commit**

```bash
git add src/models.py src/connection.py src/table_dumper.py tests/test_models.py
git commit -m "fix: coerce env-sourced numeric config (row_limit, port, connect_timeout, batch_size) to int"
```

---

### Task 4: Fail fast on missing environment variables

Fixes review #6: an unset `${VAR}` currently resolves to `""` (silent empty host/password). Make it raise a clear error at config load.

**Files:**
- Modify: `src/config.py` (`_resolve_env_vars`)
- Modify: `src/main.py` (catch `ValueError` from config load)
- Test: `tests/test_config.py` (update two tests that assert the old empty-string behavior)

**Interfaces:**
- Consumes: nothing new.
- Produces: `ConfigLoader(path)` raises `ValueError` (message contains `"not set"`) when a referenced env var is unset. `main()` prints `Error: Invalid configuration: ...` and exits 1.

- [ ] **Step 1: Update the existing tests to expect fail-fast**

In `tests/test_config.py`, **replace** `test_missing_env_var_becomes_empty` with:

```python
    def test_missing_env_var_raises(self, env_config_file):
        """Missing env vars fail fast instead of silently becoming empty strings."""
        with mock.patch.dict(os.environ, {}, clear=True):
            for key in ["DB_HOST", "DB_USER", "DB_PASSWORD", "OUTPUT_DIR"]:
                os.environ.pop(key, None)
            with pytest.raises(ValueError) as exc_info:
                ConfigLoader(env_config_file)
            assert "not set" in str(exc_info.value)
```

and **replace** `test_partial_env_var_resolution` with:

```python
    def test_partial_env_var_raises_on_missing(self, env_config_file):
        """If any referenced env var is unset, loading fails fast."""
        with mock.patch.dict(os.environ, {"DB_HOST": "localhost", "OUTPUT_DIR": "/data"}, clear=True):
            with pytest.raises(ValueError):
                ConfigLoader(env_config_file)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_config.py::TestEnvironmentVariables -v`
Expected: `test_missing_env_var_raises` and `test_partial_env_var_raises_on_missing` FAIL (current code returns `""`, does not raise).

- [ ] **Step 3: Make `_resolve_env_vars` fail fast**

In `src/config.py`, replace the string branch of `_resolve_env_vars` (currently lines ~30-35):

```python
        if isinstance(obj, str):
            matches = self.ENV_VAR_PATTERN.findall(obj)
            for match in matches:
                env_value = os.environ.get(match, '')
                obj = obj.replace(f'${{{match}}}', env_value)
            return obj
```

with:

```python
        if isinstance(obj, str):
            matches = self.ENV_VAR_PATTERN.findall(obj)
            for match in matches:
                if match not in os.environ:
                    raise ValueError(
                        f"Environment variable '{match}' referenced in "
                        f"configuration is not set"
                    )
                obj = obj.replace(f'${{{match}}}', os.environ[match])
            return obj
```

- [ ] **Step 4: Catch the new error in `main.py`**

In `src/main.py`, in the `try`/`except` around `config = ConfigLoader(args.config)`, add a third `except` after the `yaml.YAMLError` handler:

```python
    except ValueError as e:
        print(f"Error: Invalid configuration: {e}")
        sys.exit(1)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python -m pytest tests/test_config.py -v`
Expected: all PASS (including the unchanged `test_resolve_env_vars` and `test_env_var_in_nested_list`, which set every referenced var).

- [ ] **Step 6: Commit**

```bash
git add src/config.py src/main.py tests/test_config.py
git commit -m "fix: fail fast when a referenced environment variable is unset"
```

---

### Task 5: Exclude generated columns from dumps

Fixes review #4: `GENERATED ALWAYS` columns are emitted in `INSERT`, which MySQL rejects on restore.

**Files:**
- Modify: `src/table_dumper.py` (`dump_table` column selection)
- Test: `tests/test_table_dumper.py` (add to `TestDumpTable`)

**Interfaces:**
- Consumes: `ColumnInfo.extra` (already populated by `connection.get_table_columns`).
- Produces: `column_names` excludes any column whose `extra` contains `"GENERATED"`, so both the SELECT and the INSERT skip generated columns.

- [ ] **Step 1: Write the failing test**

Add to `class TestDumpTable` in `tests/test_table_dumper.py`:

```python
    def test_generated_columns_excluded(self, mock_connection):
        """Generated columns must be excluded from SELECT and INSERT (restore-breaking)."""
        mock_connection.get_table_columns.return_value = [
            ColumnInfo("id", "int", "NO", "PRI", None, "auto_increment"),
            ColumnInfo("full_name", "varchar(255)", "YES", "", None, "STORED GENERATED"),
            ColumnInfo("email", "varchar(255)", "YES", "", None, ""),
        ]
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([(1, "a@b.com")]))
        mock_connection.get_cursor.return_value = mock_cursor

        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "t.sql"
            stats = dumper.dump_table("users", output_path, DumpSettings(), OutputFormat.SQL)

            content = output_path.read_text()
            assert stats.success is True
            assert "`full_name`" not in content
            assert "`id`" in content
            assert "`email`" in content
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_table_dumper.py::TestDumpTable::test_generated_columns_excluded -v`
Expected: FAIL — ```assert "`full_name`" not in content``` fails because the generated column is included.

- [ ] **Step 3: Filter generated columns in `dump_table`**

In `src/table_dumper.py`, in `dump_table`, replace:

```python
            columns = self.connection.get_table_columns(table)
            column_names = [col.name for col in columns]
```

with:

```python
            columns = self.connection.get_table_columns(table)
            # Generated columns cannot be inserted; including them breaks restore.
            column_names = [
                col.name for col in columns
                if 'GENERATED' not in (col.extra or '').upper()
            ]
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_table_dumper.py::TestDumpTable -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/table_dumper.py tests/test_table_dumper.py
git commit -m "fix: exclude generated columns from dumps so restores succeed"
```

---

### Task 6: Consistent-snapshot transaction (default on)

Fixes review #3: tables are read with autocommit, so a multi-table write mid-backup yields an inconsistent dump. Add a `REPEATABLE READ` + `START TRANSACTION WITH CONSISTENT SNAPSHOT` per connection, gated by per-instance `consistent_snapshot` (default `true`).

**Files:**
- Modify: `src/connection.py` (new `start_consistent_snapshot`)
- Modify: `src/database_dumper.py` (`_dump_database` calls it)
- Modify: `config.example.yaml` (document the flag)
- Test: `tests/test_connection.py` (method test), `tests/test_database_dumper.py` (gate tests)

**Interfaces:**
- Produces: `DatabaseConnection.start_consistent_snapshot() -> None` issuing the two session statements. `_dump_database` calls it after connecting and before reading tables when `instance_config.get('consistent_snapshot', True)` is truthy.

- [ ] **Step 1: Write the failing tests**

Add to `class TestDatabaseConnection` in `tests/test_connection.py`:

```python
    @mock.patch('src.connection.mysql.connector.connect')
    def test_start_consistent_snapshot(self, mock_connect):
        """Snapshot helper issues REPEATABLE READ + START TRANSACTION."""
        mock_cursor = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        conn = DatabaseConnection(
            host="localhost", port=3306, user="root", password="secret"
        )
        conn.connect()
        conn.start_consistent_snapshot()

        executed = [call.args[0] for call in mock_cursor.execute.call_args_list]
        assert "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ" in executed
        assert "START TRANSACTION WITH CONSISTENT SNAPSHOT" in executed
        mock_cursor.close.assert_called_once()
```

Add to `class TestDumpDatabase` in `tests/test_database_dumper.py`:

```python
    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_consistent_snapshot_on_by_default(self, mock_conn_class, mock_config):
        """With no config flag, a consistent-snapshot txn is started."""
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"name": "testdb", "instance": "primary", "tables": "*"},
                Path(tmpdir), "20240101_120000"
            )

        mock_conn.start_consistent_snapshot.assert_called_once()

    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_consistent_snapshot_skipped_when_disabled(self, mock_conn_class, mock_config):
        """consistent_snapshot: false skips the transaction (e.g. MyISAM)."""
        mock_config.get_instance.return_value = {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "secret", "consistent_snapshot": False,
        }
        mock_conn = mock.MagicMock()
        mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = mock.MagicMock(return_value=False)
        mock_conn.get_tables.return_value = []
        mock_conn_class.return_value = mock_conn

        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"name": "testdb", "instance": "primary", "tables": "*"},
                Path(tmpdir), "20240101_120000"
            )

        mock_conn.start_consistent_snapshot.assert_not_called()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest tests/test_connection.py::TestDatabaseConnection::test_start_consistent_snapshot "tests/test_database_dumper.py::TestDumpDatabase" -v`
Expected: FAIL — `start_consistent_snapshot` does not exist; `_dump_database` does not call it.

- [ ] **Step 3: Add `start_consistent_snapshot` to `DatabaseConnection`**

In `src/connection.py`, add this method after `disconnect` (before `execute_query`):

```python
    def start_consistent_snapshot(self) -> None:
        """Begin a REPEATABLE READ transaction with a consistent snapshot.

        Gives every table in the dump the same point-in-time view, matching
        `mysqldump --single-transaction`. InnoDB only.
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            logging.debug("Started consistent-snapshot transaction")
        finally:
            cursor.close()
```

- [ ] **Step 4: Call it from `_dump_database`**

In `src/database_dumper.py`, in `_dump_database`, replace:

```python
            ) as conn:
                self._process_database_tables(conn, db_config, db_stats, output_dir, timestamp)
```

with:

```python
            ) as conn:
                if instance_config.get('consistent_snapshot', True):
                    conn.start_consistent_snapshot()
                self._process_database_tables(conn, db_config, db_stats, output_dir, timestamp)
```

- [ ] **Step 5: Document the flag in `config.example.yaml`**

Under each instance (e.g. after the `primary:` instance's `password:` line), add a comment line; for the `primary` instance:

```yaml
    # consistent_snapshot: true  # default true (InnoDB). Set false for MyISAM-only instances.
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/test_connection.py tests/test_database_dumper.py -v`
Expected: all PASS (existing `TestRun` tests still pass — `start_consistent_snapshot` is a no-op on the MagicMock connection).

- [ ] **Step 7: Commit**

```bash
git add src/connection.py src/database_dumper.py config.example.yaml tests/test_connection.py tests/test_database_dumper.py
git commit -m "feat: consistent-snapshot transaction per connection (default on, opt-out per instance)"
```

---

### Task 7: Validate `order_direction`

Fixes review #7: `order_direction` is interpolated raw. Validate it against the existing `OrderDirection` enum and default to `ASC` with a warning on bad input.

**Files:**
- Modify: `src/table_dumper.py` (import `OrderDirection`; `_build_select_query`)
- Test: `tests/test_table_dumper.py` (add to `TestBuildSelectQuery`)

**Interfaces:**
- Consumes: `OrderDirection` from `models.py` (already defined).
- Produces: `_build_select_query` emits only `ASC`/`DESC`; invalid values log a warning and fall back to `ASC`.

- [ ] **Step 1: Write the failing test**

Add to `class TestBuildSelectQuery` in `tests/test_table_dumper.py`:

```python
    def test_invalid_order_direction_defaults_to_asc(self, dumper, caplog):
        """An invalid order_direction is rejected and falls back to ASC."""
        import logging
        caplog.set_level(logging.WARNING)
        settings = DumpSettings(order_by="id", order_direction="SIDEWAYS")
        query = dumper._build_select_query("users", ["id", "name"], settings)
        assert "ORDER BY `id` ASC" in query
        assert "invalid order_direction" in caplog.text
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_table_dumper.py::TestBuildSelectQuery::test_invalid_order_direction_defaults_to_asc -v`
Expected: FAIL — current code emits `ORDER BY \`id\` SIDEWAYS`.

- [ ] **Step 3: Import the enum and validate**

In `src/table_dumper.py`, change:

```python
from .models import DumpSettings, OutputFormat, TableStats, coerce_optional_int
```

to:

```python
from .models import DumpSettings, OrderDirection, OutputFormat, TableStats, coerce_optional_int
```

Then in `_build_select_query`, replace:

```python
        if settings.order_by and settings.order_by in columns:
            direction = settings.order_direction.upper()
            query += f" ORDER BY `{settings.order_by}` {direction}"
```

with:

```python
        if settings.order_by and settings.order_by in columns:
            try:
                direction = OrderDirection(settings.order_direction.upper()).value
            except ValueError:
                logging.warning(
                    f"Table '{table}': invalid order_direction "
                    f"'{settings.order_direction}', defaulting to ASC"
                )
                direction = OrderDirection.ASC.value
            query += f" ORDER BY `{settings.order_by}` {direction}"
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_table_dumper.py::TestBuildSelectQuery -v`
Expected: all PASS (existing `test_query_with_order_by` → `ASC`, `test_query_with_order_desc` → `DESC`).

- [ ] **Step 5: Commit**

```bash
git add src/table_dumper.py tests/test_table_dumper.py
git commit -m "fix: validate order_direction against ASC/DESC, default to ASC on bad input"
```

---

### Task 8: Fail cleanly on an invalid log level

Fixes NEW-3: `getattr(logging, level.upper())` raises a raw `AttributeError` for a bad level, before logging is configured.

**Files:**
- Modify: `src/utils.py` (`setup_logging`)
- Modify: `src/main.py` (wrap `setup_logging`)
- Test: `tests/test_utils.py` (add to the logging test class)

**Interfaces:**
- Produces: `setup_logging` raises `ValueError` (message contains `"Invalid logging level"`) on an unknown level. `main()` prints `Error: ...` and exits 1.

- [ ] **Step 1: Write the failing test**

Add to the `setup_logging` test class in `tests/test_utils.py` (the class starting around line 17):

```python
    def test_invalid_log_level_raises(self):
        import pytest
        from src.utils import setup_logging
        with pytest.raises(ValueError) as exc_info:
            setup_logging({"level": "TRACE"})
        assert "Invalid logging level" in str(exc_info.value)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_utils.py -k invalid_log_level -v`
Expected: FAIL — `setup_logging` raises `AttributeError`, not `ValueError`.

- [ ] **Step 3: Validate the level in `setup_logging`**

In `src/utils.py`, replace:

```python
    log_level = getattr(logging, log_settings.get('level', 'INFO').upper())
```

with:

```python
    level_name = log_settings.get('level', 'INFO').upper()
    log_level = getattr(logging, level_name, None)
    if not isinstance(log_level, int):
        raise ValueError(
            f"Invalid logging level '{level_name}'. "
            f"Use one of DEBUG, INFO, WARNING, ERROR, CRITICAL."
        )
```

- [ ] **Step 4: Wrap the call in `main.py`**

In `src/main.py`, replace:

```python
    setup_logging(log_settings)
```

with:

```python
    try:
        setup_logging(log_settings)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python -m pytest tests/test_utils.py -v`
Expected: all PASS (existing `test_default_log_level`, `test_custom_log_level`, `test_log_level_case_insensitive` still pass — valid levels resolve to ints).

- [ ] **Step 6: Commit**

```bash
git add src/utils.py src/main.py tests/test_utils.py
git commit -m "fix: validate logging level and fail cleanly instead of an AttributeError traceback"
```

---

### Task 9: Config resilience — missing `name` and null `tables`

Fixes the two "Worth considering" config crashes: a database entry missing `name` aborts the whole run; `tables:` present-but-null crashes that database with a `TypeError`.

**Files:**
- Modify: `src/database_dumper.py` (`_dump_database`, `_get_tables_to_dump`)
- Test: `tests/test_database_dumper.py` (add to `TestDumpDatabase` and `TestGetTablesToDump`)

**Interfaces:**
- Produces: a nameless DB entry is skipped with an error recorded in `stats.errors` (no connection attempt, run continues). `tables: null` is treated as `'*'` (all tables); `tables: []` still means "no tables".

- [ ] **Step 1: Write the failing tests**

Add to `class TestDumpDatabase` in `tests/test_database_dumper.py`:

```python
    @mock.patch('src.database_dumper.DatabaseConnection')
    def test_missing_name_records_error_and_continues(self, mock_conn_class, mock_config):
        """A database entry with no name is skipped (no connection), error recorded."""
        dumper = DatabaseDumper(mock_config)
        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_database(
                {"instance": "primary", "tables": "*"}, Path(tmpdir), "20240101_120000"
            )
        assert len(dumper.stats.errors) == 1
        assert "name" in dumper.stats.errors[0]["error"]
        mock_conn_class.assert_not_called()
```

Add to `class TestGetTablesToDump` in `tests/test_database_dumper.py`:

```python
    def test_tables_null_treated_as_all(self, dumper):
        """tables: null means 'all tables', not a crash."""
        mock_conn = mock.MagicMock()
        mock_conn.get_tables.return_value = ["users", "orders"]
        result = dumper._get_tables_to_dump(mock_conn, {"tables": None})
        assert result == [{"name": "users"}, {"name": "orders"}]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest "tests/test_database_dumper.py::TestDumpDatabase::test_missing_name_records_error_and_continues" "tests/test_database_dumper.py::TestGetTablesToDump::test_tables_null_treated_as_all" -v`
Expected: FAIL — missing name raises `KeyError`; `tables: None` raises `TypeError` on `len(None)`.

- [ ] **Step 3: Guard the missing name in `_dump_database`**

In `src/database_dumper.py`, replace the first two lines of `_dump_database`:

```python
        db_name = db_config['name']
        instance_name = db_config.get('instance', 'primary')
```

with:

```python
        db_name = db_config.get('name')
        if not db_name:
            logging.error("Skipping database entry with no 'name' field")
            self.stats.errors.append({
                'database': None,
                'table': None,
                'error': "Database entry is missing the required 'name' field",
            })
            return
        instance_name = db_config.get('instance', 'primary')
```

- [ ] **Step 4: Treat null `tables` as `'*'` in `_get_tables_to_dump`**

In `src/database_dumper.py`, replace the first line of `_get_tables_to_dump`:

```python
        tables_config = db_config.get('tables', '*')
```

with:

```python
        tables_config = db_config.get('tables', '*')
        if tables_config is None:
            tables_config = '*'
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python -m pytest tests/test_database_dumper.py -v`
Expected: all PASS (existing tests, which all supply `name`, are unaffected).

- [ ] **Step 6: Commit**

```bash
git add src/database_dumper.py tests/test_database_dumper.py
git commit -m "fix: skip nameless DB entries gracefully; treat tables:null as all tables"
```

---

### Task 10: Sanitize filesystem path components

Fixes the "Worth considering" path-traversal: `db_name`/`table_name` are interpolated straight into output paths. Reduce each to a safe single component.

**Files:**
- Modify: `src/database_dumper.py` (new `_safe_path_component`; use in `_process_database_tables` and `_dump_single_table`)
- Test: `tests/test_database_dumper.py` (add to `TestDumpSingleTable`)

**Interfaces:**
- Produces: `_safe_path_component(name: str) -> str` returning `Path(str(name)).name`. Used wherever a db/table name becomes a path segment. SQL identifiers passed to the dumper are unchanged — only filesystem paths are sanitized.

- [ ] **Step 1: Write the failing test**

Add to `class TestDumpSingleTable` in `tests/test_database_dumper.py`:

```python
    def test_table_name_path_traversal_sanitized(self, mock_config):
        """A table name with .. must not escape the output directory."""
        mock_dumper = mock.MagicMock()
        mock_dumper.dump_table.return_value = TableStats(
            table="x", rows_dumped=1, success=True
        )
        dumper = DatabaseDumper(mock_config)

        with tempfile.TemporaryDirectory() as tmpdir:
            dumper._dump_single_table(
                mock_dumper, {"name": "../../evil"}, {"name": "db"},
                Path(tmpdir), OutputFormat.SQL, True, "20240101", is_first=True
            )

        out = Path(mock_dumper.dump_table.call_args.kwargs["output_path"])
        assert ".." not in out.parts
        assert out.name == "evil.sql"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest "tests/test_database_dumper.py::TestDumpSingleTable::test_table_name_path_traversal_sanitized" -v`
Expected: FAIL — `out.parts` contains `".."` and `out.name` is `evil.sql` under an escaped path.

- [ ] **Step 3: Add the helper**

In `src/database_dumper.py`, add at module level (after the imports, before `class DatabaseDumper`):

```python
def _safe_path_component(name: str) -> str:
    """Reduce a name to a single safe filesystem component (strips any path/traversal)."""
    return Path(str(name)).name
```

- [ ] **Step 4: Sanitize the directory name in `_process_database_tables`**

In `_process_database_tables`, replace:

```python
        if separate_files:
            if self.output_settings.get('timestamp_suffix', True):
                db_output_dir = output_dir / f"{db_name}_{timestamp}"
            else:
                db_output_dir = output_dir / db_name
```

with:

```python
        safe_db = _safe_path_component(db_name)
        if separate_files:
            if self.output_settings.get('timestamp_suffix', True):
                db_output_dir = output_dir / f"{safe_db}_{timestamp}"
            else:
                db_output_dir = output_dir / safe_db
```

- [ ] **Step 5: Sanitize the file name in `_dump_single_table`**

In `_dump_single_table`, replace:

```python
        if separate_files:
            output_path = db_output_dir / f"{table_name}.{output_format.extension}"
            append = False
        else:
            # Single file directly in output directory
            db_name = db_config['name']
            if self.output_settings.get('timestamp_suffix', True):
                output_path = db_output_dir / f"{db_name}_{timestamp}.{output_format.extension}"
            else:
                output_path = db_output_dir / f"{db_name}.{output_format.extension}"
```

with:

```python
        if separate_files:
            output_path = db_output_dir / f"{_safe_path_component(table_name)}.{output_format.extension}"
            append = False
        else:
            # Single file directly in output directory
            safe_db = _safe_path_component(db_config['name'])
            if self.output_settings.get('timestamp_suffix', True):
                output_path = db_output_dir / f"{safe_db}_{timestamp}.{output_format.extension}"
            else:
                output_path = db_output_dir / f"{safe_db}.{output_format.extension}"
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python -m pytest tests/test_database_dumper.py -v`
Expected: all PASS (existing `TestDumpSingleTable` tests use plain names, unaffected by sanitization).

- [ ] **Step 7: Commit**

```bash
git add src/database_dumper.py tests/test_database_dumper.py
git commit -m "fix: sanitize db/table names used as filesystem paths to prevent traversal"
```

---

### Task 11: Keep `where_clause` out of INFO logs

Fixes the "Worth considering" log-leak: the full query (including `where_clause`, which may carry PII) is logged at INFO. Log the table at INFO, the full query at DEBUG.

**Files:**
- Modify: `src/table_dumper.py` (`dump_table`)
- Test: `tests/test_table_dumper.py` (add to `TestDumpTable`)

**Interfaces:**
- Produces: at INFO, only `Dumping table '<name>'`; the full query (with predicate) goes to DEBUG.

- [ ] **Step 1: Write the failing test**

Add to `class TestDumpTable` in `tests/test_table_dumper.py`:

```python
    def test_where_clause_not_logged_at_info(self, mock_connection, caplog):
        """The WHERE predicate must not appear in INFO logs (possible PII)."""
        import logging
        mock_cursor = mock.MagicMock()
        mock_cursor.__iter__ = mock.MagicMock(return_value=iter([]))
        mock_connection.get_cursor.return_value = mock_cursor
        dumper = TableDumper(mock_connection, {"compress": False})

        with tempfile.TemporaryDirectory() as tmpdir, caplog.at_level(logging.INFO):
            dumper.dump_table(
                "users", Path(tmpdir) / "t.sql",
                DumpSettings(where_clause="ssn = '123-45-6789'"),
                OutputFormat.SQL,
            )

        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert info_messages  # the "Dumping table" line is present
        assert not any("123-45-6789" in m for m in info_messages)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest "tests/test_table_dumper.py::TestDumpTable::test_where_clause_not_logged_at_info" -v`
Expected: FAIL — the INFO line currently includes the query with the predicate.

- [ ] **Step 3: Split the log lines in `dump_table`**

In `src/table_dumper.py`, in `dump_table`, replace:

```python
            logging.info(f"Dumping table '{table}' with query: {query[:200]}...")
```

with:

```python
            logging.info(f"Dumping table '{table}'")
            logging.debug(f"Query for '{table}': {query}")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_table_dumper.py::TestDumpTable -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/table_dumper.py tests/test_table_dumper.py
git commit -m "fix: log table name at INFO and full query (with predicate) at DEBUG only"
```

---

### Task 12: Warn on negative `row_limit`

Fixes the Nit: a negative `row_limit` silently means "unlimited". Keep that behavior but log a warning so it isn't a surprise.

**Files:**
- Modify: `src/table_dumper.py` (`_build_select_query`)
- Test: `tests/test_table_dumper.py` (add to `TestBuildSelectQuery`)

**Interfaces:**
- Produces: a negative `row_limit` emits no `LIMIT` clause and logs a warning; `0` still emits `LIMIT 0`; positive values unchanged.

- [ ] **Step 1: Write the failing test**

Add to `class TestBuildSelectQuery` in `tests/test_table_dumper.py`:

```python
    def test_negative_row_limit_warns_and_is_unlimited(self, dumper, caplog):
        """A negative row_limit emits no LIMIT and warns the user."""
        import logging
        caplog.set_level(logging.WARNING)
        settings = DumpSettings(row_limit=-5)
        query = dumper._build_select_query("users", ["id"], settings)
        assert "LIMIT" not in query
        assert "negative row_limit" in caplog.text
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest "tests/test_table_dumper.py::TestBuildSelectQuery::test_negative_row_limit_warns_and_is_unlimited" -v`
Expected: FAIL — no warning is logged (the current code silently skips the LIMIT).

- [ ] **Step 3: Add the warning in `_build_select_query`**

In `src/table_dumper.py`, replace:

```python
        if settings.row_limit is not None and settings.row_limit >= 0:
            query += f" LIMIT {settings.row_limit}"

        return query
```

with:

```python
        if settings.row_limit is not None:
            if settings.row_limit >= 0:
                query += f" LIMIT {settings.row_limit}"
            else:
                logging.warning(
                    f"Table '{table}': negative row_limit "
                    f"({settings.row_limit}) treated as unlimited"
                )

        return query
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_table_dumper.py::TestBuildSelectQuery -v`
Expected: all PASS (existing `test_query_with_limit` and `test_query_with_zero_limit` still pass).

- [ ] **Step 5: Commit**

```bash
git add src/table_dumper.py tests/test_table_dumper.py
git commit -m "fix: warn when a negative row_limit is treated as unlimited"
```

---

## Final verification

- [ ] **Run the full suite under the CI coverage gate:**

Run:
```bash
python -m pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=95 -v
```
Expected: all tests PASS and coverage ≥ 95% (the CI gate). If coverage dipped below 95% on a touched module, add a focused test for the uncovered new branch before considering the work done.

- [ ] **Sanity-check the dry-run path still works end-to-end:**

Run:
```bash
python -m src.main --config config.example.yaml --dry-run
```
Expected: it prints the dry-run plan and exits 0 (this exercises config load + logging setup with the documented sample config).

---

## Self-Review

**1. Spec coverage** — every review finding maps to a task:

| Finding | Severity | Task |
|---|---|---|
| #2 escaping (`\0`, `\x1a`, `bytearray`) | 🔴 | 1 |
| #5 type formatters / datetime µs | 🟠 | 1 |
| NEW-4 tests enshrine lossy behavior | 🟠 | 1 (adversarial-value tests added) |
| #1 DROP TABLE on partial dumps | 🔴 | 2 |
| NEW-2 atomic writes | 🔴 | 2 |
| NEW-1 env-var numeric typing | 🔴 | 3 |
| #6 missing env var → empty string | 🟠 | 4 |
| #4 generated columns | 🟠 | 5 |
| #3 consistent snapshot | 🟠 | 6 |
| #7 order_direction validation | 🟠 | 7 |
| NEW-3 invalid log level | 🟠 | 8 |
| missing `name` aborts run | 🟡 | 9 |
| `tables: null` crash | 🟡 | 9 |
| path traversal via names | 🟡 | 10 |
| `where_clause` logged at INFO | 🟡 | 11 |
| negative `row_limit` semantics | 🟢 | 12 |

**2. Placeholder scan** — no `TODO`/`TBD`/"handle edge cases"/"similar to Task N"; every code step shows the full replacement text and every test step shows the full test.

**3. Type/name consistency** — `coerce_optional_int` (Task 3) is defined in `models.py` and imported by `connection.py` and `table_dumper.py` (Tasks 3, 7 share the same import line, which Task 7 extends). `_open_output_file`'s 3-tuple return (Task 2) is consumed only by `dump_table` (same task) and the updated `TestOpenOutputFile` (same task). `_dump_as_sql`'s new `add_drop_table` param (Task 2) is passed only from `dump_table` (same task). `start_consistent_snapshot` (Task 6) is defined in `connection.py` and called in `database_dumper.py` (same task). `_safe_path_component` (Task 10) is defined and used within `database_dumper.py`. No mismatches.
