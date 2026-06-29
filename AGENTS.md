# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project Overview

This is a Python 3.10+ MySQL database dumper. The package entry point is `python -m src`, with CLI handling in `src/main.py`, configuration loading in `src/config.py`, connection code in `src/connection.py`, database orchestration in `src/database_dumper.py`, and table-level dump logic in `src/table_dumper.py`.

Primary tests live in `tests/`. Integration tests live in `tests/integration/` and require a real MySQL instance configured through environment variables.

## Setup

Install runtime dependencies:

```bash
pip install -r requirements.txt
```

Install development dependencies:

```bash
pip install -r requirements-dev.txt
```

Using a virtual environment is recommended:

```bash
python3 -m venv venv
source venv/bin/activate
```

## Common Commands

Run the application:

```bash
python -m src
```

Run with a custom config file:

```bash
python -m src -c config.yaml
```

Preview work without dumping:

```bash
python -m src --dry-run
```

Run unit tests:

```bash
python -m pytest tests/ -v
```

Run unit tests with coverage threshold:

```bash
python -m pytest tests/ --cov=src --cov-report=term-missing --cov-fail-under=98 -v
```

Run integration tests only when MySQL test environment variables are configured:

```bash
python -m pytest tests/integration/ -v
```

## Coding Guidelines

- Keep changes focused and consistent with the existing module boundaries.
- Prefer the existing dataclasses and enums in `src/models.py` for typed configuration behavior.
- Preserve the current YAML configuration style and update `config.example.yaml` and `README.md` when adding user-facing config options.
- Avoid broad refactors while fixing a targeted bug or implementing a narrow feature.
- Add or update focused tests for behavior changes, especially around dump output formatting and config parsing.
- Do not introduce logging or output changes unless they are part of the requested behavior.

## Dump Behavior Notes

- SQL table dumping is primarily controlled by `src/table_dumper.py`.
- Multi-table/database orchestration is controlled by `src/database_dumper.py`.
- MySQL schema DDL comes from `SHOW CREATE TABLE`; avoid manually reconstructing table definitions unless explicitly required.
- Partial dumps using `row_limit` or `where_clause` are treated more cautiously than full dumps, especially for destructive restore behavior such as `DROP TABLE`.

## Secrets and Local Files

- Do not expose or hard-code credentials from `config.yaml` or environment variables.
- Prefer `config.example.yaml` for documentation examples.
- Treat files under `dumps/` as generated output unless the user specifically asks to inspect or modify them.
