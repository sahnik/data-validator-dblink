import argparse
import json
import logging
import sys
import atexit
from pathlib import Path

from src.data_validator.config import ValidationConfig
from src.data_validator.orchestrator import ValidationOrchestrator
from src.data_validator.db.connection import OracleConnectionPool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_validator.log')
    ]
)

logger = logging.getLogger(__name__)


def load_config(config_file: Path) -> ValidationConfig:
    """Load configuration from JSON file."""
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
        
        return ValidationConfig(**config_data)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise


def cleanup_resources():
    """Clean up resources on program exit."""
    logger.info("Cleaning up database connection pools...")
    try:
        OracleConnectionPool.close_all_pools()
        logger.info("All connection pools closed successfully")
    except Exception as e:
        logger.error(f"Error closing connection pools: {e}")


def main():
    # Register cleanup handler to ensure pools are closed on exit
    atexit.register(cleanup_resources)
    
    parser = argparse.ArgumentParser(description='Oracle Migration Data Validator')
    parser.add_argument(
        'config',
        type=Path,
        help='Path to configuration file'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume any paused or failed validations'
    )
    parser.add_argument(
        '--tables',
        nargs='+',
        help='Specific tables to validate (overrides config)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Filter tables if specified
        if args.tables:
            config.table_mappings = [
                mapping for mapping in config.table_mappings
                if mapping.source_table in args.tables
            ]
        
        # Create and run orchestrator
        orchestrator = ValidationOrchestrator(config)
        
        if args.resume:
            results = orchestrator.resume_validations()
        else:
            results = orchestrator.run_validation()
        
        # Print summary
        print(f"\nValidation completed. Processed {len(results)} tables.")
        
        for result in results:
            print(f"\n{result.table_name}:")
            print(f"  Status: {result.status}")
            print(f"  Total rows: {result.total_rows:,}")
            print(f"  Matched: {result.matched_rows:,}")
            print(f"  Mismatched: {result.mismatched_rows:,}")
            print(f"  Missing in target: {result.missing_in_target:,}")
            print(f"  Extra in target: {result.extra_in_target:,}")
            print(f"  Duration: {result.validation_duration_seconds:.2f} seconds")
        
        # Exit with error if any validations failed
        failed_count = len([r for r in results if r.status == 'FAILED'])
        if failed_count > 0:
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)
    finally:
        # Ensure connection pools are closed even if we exit due to an error
        cleanup_resources()


if __name__ == "__main__":
    main()