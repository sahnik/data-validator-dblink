# Oracle Migration Data Validator

A configuration-driven data validation program for Oracle database migrations. This tool automates the comparison between Oracle databases using database links, reducing the effort of writing manual validation scripts.

## Features

- **Database Link Optimization**: Leverages Oracle database links for efficient data comparison
- **Configuration-Driven**: JSON-based configuration for table mappings and validation rules
- **Chunk Processing**: Handles tables with billions of rows by processing in configurable chunks
- **Progress Tracking**: Resumable validations with detailed progress tracking
- **Concurrent Execution**: Configurable parallelism for faster validation
- **Email Notifications**: Automated reports sent via email
- **Run Window Support**: Configure specific times when validations can run
- **Result Storage**: Detailed validation results stored in database tables

## Requirements

- Python >=3.11
- Oracle Database with database link between source and target
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
    "source_db": {
        "username": "source_user",
        "password": "source_password",
        "host": "source-db-host",
        "port": 1521,
        "service_name": "source_service"
    },
    "target_db": {
        "username": "target_user",
        "password": "target_password",
        "host": "target-db-host",
        "port": 1521,
        "service_name": "target_service"
    },
    "db_link_name": "TARGET_DB_LINK",
    "table_mappings": [
        {
            "source_table": "CUSTOMERS",
            "target_table": "CUSTOMERS",
            "natural_keys": ["CUSTOMER_ID"],
            "exclude_columns": ["CREATED_DATE"],
            "chunk_size": 10000
        }
    ]
}
```

### Environment Variables

The configuration supports environment variable substitution for sensitive values. Create a `.env` file (see `.env.example`):

```bash
SOURCE_DB_USERNAME=source_user
SOURCE_DB_PASSWORD=source_password
SOURCE_DB_HOST=source-db-host
SOURCE_DB_SERVICE=source_service

TARGET_DB_USERNAME=target_user
TARGET_DB_PASSWORD=target_password
TARGET_DB_HOST=target-db-host
TARGET_DB_SERVICE=target_service

SMTP_USERNAME=notifications@example.com
SMTP_PASSWORD=smtp_password
```

Then reference them in your config using `${VARIABLE_NAME}`:

```json
{
    "source_db": {
        "username": "${SOURCE_DB_USERNAME}",
        "password": "${SOURCE_DB_PASSWORD}",
        "host": "${SOURCE_DB_HOST}",
        "service_name": "${SOURCE_DB_SERVICE}"
    },
    "email_config": {
        "smtp_username": "${SMTP_USERNAME}",
        "smtp_password": "${SMTP_PASSWORD}"
    }
}
```

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

## How It Works

1. **Connection Setup**: Establishes connections to both source and target databases
2. **Table Initialization**: Creates progress and results tracking tables if needed
3. **Chunk-Based Validation**: For each table:
   - Processes data in chunks using natural keys for ordering
   - Compares data using PL/SQL blocks executed over the database link
   - Tracks progress for resumability
   - Records detailed results
4. **Reporting**: Sends email notifications with validation summary

## Architecture

- **Main Components**:
  - `ValidationOrchestrator`: Manages the overall validation process
  - `TableValidator`: Handles individual table validations
  - `OracleConnectionManager`: Manages database connections
  - `ValidationRepository`: Handles progress and result persistence
  - `EmailSender`: Sends notification emails
  - `WindowChecker`: Enforces run window constraints

## Progress Tracking

The tool creates two database tables:
- `DATA_VALIDATION_PROGRESS`: Tracks validation progress for resumability
- `DATA_VALIDATION_RESULTS`: Stores final validation results

## Email Reports

When enabled, the tool sends HTML email reports containing:
- Summary of validated tables
- Match/mismatch statistics
- Validation duration
- Error details for failed validations
