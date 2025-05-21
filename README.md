# Oracle Migration Data Validator

A configuration-driven data validation program for Oracle database migrations. This tool automates the comparison between Oracle databases using database links, reducing the effort of writing manual validation scripts.

## Features

- **Database Link Optimization**: Leverages Oracle database links for efficient data comparison
- **Configuration-Driven**: JSON-based configuration for table mappings and validation rules
- **Chunk Processing**: Handles tables with billions of rows by processing in configurable chunks
- **Progress Tracking**: Resumable validations with detailed progress tracking
- **Concurrent Execution**: Configurable parallelism for faster validation
- **Connection Pooling**: Efficient Oracle connection management for optimal performance
- **Run Window Controls**: Schedule validations to run only during specified time windows (e.g., off-peak hours)
- **Incremental Validation**: Ability to validate only rows changed since the last run
- **Result Storage**: Detailed validation results stored in database tables

## Requirements

- Python >=3.11
- Oracle Database with database link created on the target database pointing to the source database
- Oracle Instant Client or full Oracle Client

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd data-validator-dblink
```

2. Install dependencies:
```bash
pip install -e .
```

For development:
```bash
pip install -e .[dev]
```

## Configuration

Create a JSON configuration file (see `config/sample_config.json` for example):

```json
{
    "target_db": {
        "username": "target_user",
        "password": "target_password",
        "host": "target-db-host",
        "port": 1521,
        "service_name": "target_service",
        "pool_min": 2,
        "pool_max": 10,
        "pool_increment": 1
    },
    "db_link_name": "SOURCE_DB_LINK",
    "db_link_notes": "This DB link must be created on the target database and point to the source database",
    "table_mappings": [
        {
            "source_table": "CUSTOMERS",
            "target_table": "CUSTOMERS",
            "natural_keys": ["CUSTOMER_ID"],
            "exclude_columns": ["CREATED_DATE"],
            "chunk_size": 10000,
            "incremental_mode": true,
            "incremental_column": "LAST_MODIFIED_DATE"
        }
    ]
}
```

> **Note:** The `source_db` configuration is no longer required, as all operations use the database link from the target database to access the source database.

### Environment Variables

The configuration supports environment variable substitution for sensitive values. Create a `.env` file (see `.env.example`):

```bash
TARGET_DB_USERNAME=target_user
TARGET_DB_PASSWORD=target_password
TARGET_DB_HOST=target-db-host
TARGET_DB_SERVICE=target_service

```

Then reference them in your config using `${VARIABLE_NAME}`:

```json
{
    "target_db": {
        "username": "${TARGET_DB_USERNAME}",
        "password": "${TARGET_DB_PASSWORD}",
        "host": "${TARGET_DB_HOST}",
        "service_name": "${TARGET_DB_SERVICE}"
    }
}
```

### Connection Pooling Configuration

The tool uses Oracle connection pooling to efficiently manage database connections. You can configure the following parameters in your database configuration:

```json
"target_db": {
    "username": "target_user",
    "password": "target_password",
    "host": "target-db-host",
    "port": 1521,
    "service_name": "target_service",
    "pool_min": 2,        // Minimum number of connections in the pool
    "pool_max": 10,       // Maximum number of connections in the pool
    "pool_increment": 1   // Number of connections to add when more are needed
}
```

Connection pooling parameters:
- `pool_min`: Minimum number of connections to maintain in the pool (default: 2)
- `pool_max`: Maximum number of connections allowed in the pool (default: 10)
- `pool_increment`: Number of connections to create at once when more are needed (default: 1)

Choose these values based on your workload:
- For small/medium validations: Use defaults (`pool_min=2`, `pool_max=10`)
- For large validations with high concurrency: Consider increasing (`pool_min=5`, `pool_max=20`)

## Usage

### Run validation for all configured tables:
```bash
python main.py config/your_config.json
```

### Validate specific tables:
```bash
python main.py config/your_config.json --tables CUSTOMERS ORDERS
```

### Resume failed or paused validations:
```bash
python main.py config/your_config.json --resume
```

### Test connection pooling:
```bash
python test_connection_pool.py
```
Note: Update the database credentials in test_connection_pool.py before running.

## How It Works

1. **Connection Setup**: Establishes connection pools to both source and target databases
2. **Table Initialization**: Creates progress and results tracking tables in the target database if needed
3. **Chunk-Based Validation**: For each table:
   - Processes data in chunks using natural keys for ordering
   - Compares data using PL/SQL blocks executed on the target database using a database link to the source
   - Tracks progress for resumability
   - Records detailed results
4. **Connection Management**: 
   - Reuses database connections from the pools for optimal performance
   - Automatically manages connection lifecycle and cleanup
   - Scales connections based on concurrent validation workload

## Incremental Validation

Incremental validation allows you to only validate rows that have changed since the last successful validation:

- Configure incremental validation per table using `incremental_mode` and `incremental_column`
- When enabled, only rows that have changed since the last successful validation will be processed
- The tool looks for the most recent successful validation timestamp in the results table
- Useful for large tables that change infrequently or for periodic validation runs

Example configuration:
```json
{
    "source_table": "CUSTOMERS",
    "target_table": "CUSTOMERS",
    "natural_keys": ["CUSTOMER_ID"],
    "incremental_mode": true,
    "incremental_column": "LAST_MODIFIED_DATE"
}
```

In this example, only customer records with a `LAST_MODIFIED_DATE` more recent than the last successful validation will be validated.

## Run Window Configuration

The run window feature allows you to specify when validations are allowed to run, which is useful for scheduling validations during off-peak hours or maintenance windows.

### Configuration Options

```json
"run_window": {
    "start_time": "22:00:00",   // When the window opens (HH:MM:SS in 24-hour format)
    "end_time": "06:00:00",     // When the window closes (HH:MM:SS in 24-hour format)
    "days_of_week": [0, 1, 2, 3, 4]  // Days when validation can run (0=Monday, 6=Sunday)
}
```

### Key Features

- **Time Window**: Define start and end times using 24-hour format (HH:MM:SS)
- **Cross-Midnight Windows**: If end_time is earlier than start_time, the window is assumed to cross midnight
- **Day Selection**: Specify which days of the week validations are allowed to run
- **Automatic Waiting**: If a validation is attempted outside the window, it will report how long until the next window opens

### Examples

#### Night-time Window (Monday-Friday)
```json
"run_window": {
    "start_time": "20:00:00",   // 8:00 PM
    "end_time": "06:00:00",     // 6:00 AM the next day
    "days_of_week": [0, 1, 2, 3, 4]  // Monday through Friday
}
```

#### Weekend Window
```json
"run_window": {
    "start_time": "00:00:00",   // Midnight
    "end_time": "23:59:59",     // 11:59 PM
    "days_of_week": [5, 6]      // Saturday and Sunday only
}
```

#### No Window (Run Anytime)
Simply omit the `run_window` section from your configuration to allow validations to run at any time.

## Architecture

- **Main Components**:
  - `ValidationOrchestrator`: Manages the overall validation process
  - `TableValidator`: Handles individual table validations
  - `OracleConnectionManager`: Manages database connections and connection pools
  - `OracleConnectionPool`: Handles connection pooling for efficient database access
  - `ValidationRepository`: Handles progress and result persistence
  - `WindowChecker`: Enforces run window constraints

## Progress Tracking

The tool creates two database tables in the target database:
- `DATA_VALIDATION_PROGRESS`: Tracks validation progress for resumability
- `DATA_VALIDATION_RESULTS`: Stores final validation results
- `DATA_VALIDATION_MISMATCH_DETAILS`: Stores detailed information about mismatched rows