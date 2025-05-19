# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Oracle Migration Data Validator - a configuration-driven data validation program for Oracle database migrations. The project reduces the effort of writing data validation scripts by automating comparisons between Oracle databases using database links.

### Key Requirements
- Compare tables in two Oracle databases connected via database link
- Configuration-driven SQL generation based on table names and natural key columns
- Support for full and incremental comparison of changed data
- Progress tracking with resumable capability
- Results tracking in a database table
- Email notifications for validation results
- Concurrent execution support (configurable)
- Configurable run window to limit execution times

### Technical Constraints
- Python-based implementation (requires Python >=3.11)
- Oracle PL/SQL for data comparisons to avoid data transfer
- Must handle tables with billions of rows in chunks

## Development Commands

Run the application:
```bash
python main.py config/sample_config.json
```

Run with specific tables:
```bash
python main.py config/sample_config.json --tables CUSTOMERS ORDERS
```

Resume failed validations:
```bash
python main.py config/sample_config.json --resume
```

Install dependencies:
```bash
pip install -e .
```

Install development dependencies:
```bash
pip install -e .[dev]
```

Run tests:
```bash
pytest tests/
```

Format code:
```bash
black src/ tests/
isort src/ tests/
```

Type checking:
```bash
mypy src/
```

## Architecture Notes

- Main entry point is `main.py` with CLI argument parsing
- Configuration loaded from JSON files (see `config/sample_config.json`)
- Key modules:
  - `orchestrator.py`: Manages overall validation process
  - `validators/table_validator.py`: Performs table-level validations
  - `db/connection.py`: Oracle database connection management
  - `db/repository.py`: Progress and result persistence
  - `utils/email_sender.py`: Email notifications
  - `utils/window_checker.py`: Run window enforcement
- Validation uses PL/SQL blocks executed over database links
- Progress tracking enables resumable validations
- Concurrent execution via ThreadPoolExecutor

## Implementation Focus

When implementing features, consider:
1. Database connection management to both Oracle instances
2. Configuration structure for table mappings and validation rules
3. Chunk-based processing for large tables
4. Progress tracking and resumable operations
5. Concurrent execution framework
6. Result storage and reporting mechanisms