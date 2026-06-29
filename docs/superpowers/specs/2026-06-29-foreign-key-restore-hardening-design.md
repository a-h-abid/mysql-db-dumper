# Foreign Key Restore Hardening — Design

## Problem

The dumper preserves foreign keys correctly — they live inside MySQL's
`SHOW CREATE TABLE` output, which the dumper writes verbatim. The problem is
purely **restore ordering**, not FK loss:

- A child table's `CREATE TABLE` (with an inline `FOREIGN KEY ... REFERENCES`)
  fails if the referenced parent table does not exist yet.
- A child row `INSERT` fails if the parent row is not present yet, because FK
  checks are enabled during restore.
- Circular references (`a` → `b` → `a`) make plain sequential restore
  impossible regardless of order.

Two dump shapes are affected differently:

- **Combined single-file dump** (`separate_files: false`): all tables in one
  `.sql`, restored in one `mysql` session. Ordering matters most here and is
  most fixable.
- **Separate-files dump** (`separate_files: true`, the default): one `.sql`
  per table. The user restores them by hand or via a loop, with no built-in
  ordering. Each file's inline FKs can fail in isolation.

CSV output is unaffected — it carries no DDL or FK semantics.

## Chosen Approach

Wrap full SQL dumps with `SET FOREIGN_KEY_CHECKS=0` / `SET FOREIGN_KEY_CHECKS=1`,
exactly as `mysqldump` does. Disabling FK checks for the restore session makes
table-create order and row-insert order irrelevant, and handles cycles, which a
topological sort cannot do cleanly.

Rejected alternatives:

- **Topological sort** of tables via `information_schema` (parent-first). It is
  the plan's own "larger fix," needs cycle handling, and — critically — buys
  nothing once FK checks are disabled, since ordering becomes irrelevant.
- **Strip FKs from `CREATE TABLE`, re-add via `ALTER TABLE`.** Requires parsing
  and transforming MySQL DDL, which is fragile and far larger than the problem.

### Key correctness fact

`FOREIGN_KEY_CHECKS` is a **session variable**. The wrapper only protects a
restore that imports the whole file (or the whole helper stream) in **one
`mysql` session** — e.g. `mysql db < dump.sql`. Restoring fragments separately
defeats it. This is the normal case, so the wrapper is effective in practice;
it is documented as a constraint, not relied upon silently.

## Configuration

New optional output setting:

```yaml
output:
  disable_foreign_key_checks: true   # default: true
```

Read from `output_settings` via the existing `get_output_settings()`, the same
way `add_drop_table`, `compress`, and `batch_size` are read. **No `models.py`
change is required.**

Resolution rules (mirroring the existing `add_drop_table` safety logic in
`table_dumper.py`):

| Condition | Wrapper emitted? |
|-----------|------------------|
| Full dump (no `row_limit`, no `where_clause`), option on (default) | Yes |
| Partial dump (`row_limit` or `where_clause` set) | **Never**, even if option on |
| CSV format | **Never** (SQL-only) |
| `disable_foreign_key_checks: false` | Never |

**Partial-dump rationale:** disabling FK checks while restoring a filtered
subset can insert orphan child rows whose parents were never dumped — silent
referential corruption. Partial dumps are exempt by the same reasoning the code
already uses to refuse `DROP TABLE` on partial dumps. When a partial dump
suppresses the wrapper, emit one info-level log, consistent with the existing
DROP TABLE refusal.

## Component Design

### A. `_dump_as_sql` — per-file wrapper (separate-files mode)

`_dump_as_sql` gains an `emit_fk_wrapper: bool` parameter.

- **True** only in separate-files mode when: option on, full dump, SQL format.
- **False** in combined mode (DatabaseDumper owns the wrapper there — see B).

When true, the written file is:

```sql
-- MySQL Dump
-- Table: orders
-- Generated: ...
-- -------------------------------------------------

SET FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS `orders`;

CREATE TABLE `orders` (... FOREIGN KEY ...);

INSERT INTO `orders` (...) VALUES
  (...);

-- Dump complete. N rows.
SET FOREIGN_KEY_CHECKS=1;
```

Each file restores independently regardless of order. A per-table partial dump
skips its own wrapper without affecting sibling files (they are separate files).

### B. `DatabaseDumper` — file-level wrapper (combined mode)

In combined mode the wrapper must be emitted **exactly once**: `=0` before the
first table, `=1` after the last — never between tables. `_dump_as_sql` does not
know table boundaries; `DatabaseDumper` does. So the file-level wrapper is owned
by `DatabaseDumper._process_database_tables`, and `_dump_as_sql` is called with
`emit_fk_wrapper=False` for every combined-mode table.

The combined wrapper is emitted only if **the option is on AND no table in the
set is partial**. `DatabaseDumper` already merges `DumpSettings` per table, so it
can decide "are all tables full?" up front. If any table is partial, the whole
combined file is left unwrapped and one info log explains why.

Flow in `_process_database_tables` when `not separate_files`, SQL, and wrappable:

**Handle-clobber caveat.** Today the first combined table opens the file with
`append=False` (mode `wt`, truncating) and later tables use `append=True`
(mode `at`). If we seed the `.partial` file with `=0` before the loop, the first
table's `wt` open would truncate it and discard the header. Fix: seed the
`.partial` file once with the `=0` header, then make **every** combined table
(including the first) open in append mode. So:

1. **Before the loop:** create the resolved `single_write_path` (`.partial`) and
   write `SET FOREIGN_KEY_CHECKS=0;\n\n`.
2. **Per-table writes:** every table opens the `.partial` in append mode and is
   called with `emit_fk_wrapper=False`. (This makes `is_first`/`append=False`
   irrelevant for combined SQL when wrapping — every table appends.)
3. **After the loop, only if all tables succeeded** (the existing publish guard):
   append `SET FOREIGN_KEY_CHECKS=1;\n` to the `.partial` file, then perform the
   atomic `.partial → final` rename.

When the combined dump is *not* wrapped (option off, or any partial table),
behavior is exactly as today — first table `append=False`, no FK lines.

### C. Restore helper (separate-files mode)

After all tables of a database dump, `DatabaseDumper` writes `restore.sh` into
the per-database output dir (`<db>_<timestamp>/`). It disables FK checks **once
for the whole session** and sources every `.sql` in any order, so cross-file
ordering and cycles are irrelevant:

```sh
#!/bin/sh
# Restore helper for <db> dumped <timestamp>.
# Usage: ./restore.sh | mysql -u USER -p TARGET_DB
#   or:  ./restore.sh > restore.sql   (then import restore.sql)
echo "SET FOREIGN_KEY_CHECKS=0;"
for f in "$(dirname "$0")"/*.sql; do
  echo "-- >>> $f"
  cat "$f"
done
echo "SET FOREIGN_KEY_CHECKS=1;"
```

Decisions:

- **Streams SQL to stdout**, never invokes `mysql` directly — credential-free,
  inspectable, user pipes it where they want. No host/user/password on disk.
- **Globs `*.sql`** so it picks up exactly the dumped tables. When
  `compress: true`, files are `.sql.gz`; the helper globs and `zcat`s those
  instead (helper adapts to the compress setting).
- **Generated only when** SQL + separate_files + at least one wrapped (full)
  table. No helper for CSV or combined mode.
- Written `restore.sh` (POSIX `sh`), made executable (mode `0755`).
- The per-file wrappers (A) are redundant *inside* the helper session but
  harmless, and they are what makes each file independently restorable — the
  point of separate files.

## Error Handling & Edge Cases

- **Combined dump, table fails mid-way:** existing guard breaks the loop and
  publishes only if all tables succeed. The closing `=1` is hooked into that
  same success path, so a failed combined dump never publishes a file with an
  unbalanced (open `=0`, no closing `=1`) wrapper. The `.partial` is discarded.
- **Separate file fails:** each file is wrapped and atomically published on its
  own, so a failure leaves no half-written wrapped file. `restore.sh` globs
  whatever `.sql` exists; a missing table is simply not sourced.
- **Mixed full/partial in combined mode:** any partial table ⇒ whole combined
  file unwrapped, with one info log.
- **`restore.sh` collisions:** written fresh into each timestamped dir; no
  cross-run collision.
- **Empty database (no wrappable tables):** no wrapper, no `restore.sh`.

## Testing

Extend `tests/test_table_dumper.py` and `tests/test_database_dumper.py`:

1. Full separate SQL file emits `SET FOREIGN_KEY_CHECKS=0` before `CREATE` and
   `=1` after the completion comment.
2. Partial separate dump (`row_limit`) → no wrapper.
3. Partial separate dump (`where_clause`) → no wrapper.
4. CSV dump → no FK statements at all.
5. Combined single-file SQL → exactly one `=0` near top, exactly one `=1` at
   end; assert no `=1` appears between two tables' `CREATE` statements.
6. Combined dump where one table is partial → whole file unwrapped.
7. `disable_foreign_key_checks: false` → no wrapper in any mode.
8. `restore.sh` generated in separate-files SQL mode, executable, contains one
   `=0`/`=1` pair, and globs `*.sql` (or `*.sql.gz` when compressed).
9. No `restore.sh` for CSV or combined mode.

**Test environment:** no system `pytest`; use `uv` to create `.venv` and run via
`.venv/bin/python -m pytest`.

## Documentation

Document `disable_foreign_key_checks` (default `true`, full-SQL-only,
partial-exempt) and the generated `restore.sh` in both `config.example.yaml`
and `README.md`.

## Files to Touch

- `src/table_dumper.py` — `emit_fk_wrapper` parameter on `_dump_as_sql`; thread
  the per-file decision from `dump_table`.
- `src/database_dumper.py` — combined-mode file-level wrapper in
  `_process_database_tables`; `restore.sh` generation for separate-files mode;
  the "all tables full?" combined-wrapper decision.
- `config.example.yaml` — document the new setting.
- `README.md` — document the setting and `restore.sh`.
- `tests/test_table_dumper.py`, `tests/test_database_dumper.py` — the tests above.

No change needed to `src/models.py` or `src/config.py` (the setting is read
through the existing `output_settings` dict).
