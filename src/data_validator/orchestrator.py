import asyncio
import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List

from .config import ValidationConfig, ValidationResult
from .db.connection import OracleConnectionManager
from .db.repository import ValidationRepository
from .validators.table_validator import TableValidator
from .utils.email_sender import EmailSender
from .utils.window_checker import WindowChecker

logger = logging.getLogger(__name__)


class ValidationOrchestrator:
    def __init__(self, config: ValidationConfig):
        self.config = config
        
        # Initialize database connections
        self.source_db = OracleConnectionManager(config.source_db)
        self.target_db = OracleConnectionManager(config.target_db)
        
        # Initialize repository
        self.repository = ValidationRepository(
            self.source_db,
            config.progress_table_name,
            config.results_table_name
        )
        
        # Initialize utilities
        self.email_sender = EmailSender(config.email_config)
        self.window_checker = WindowChecker(config.run_window)
        
        # Initialize validator
        self.table_validator = TableValidator(
            self.source_db,
            self.target_db,
            config.db_link_name,
            self.repository
        )
    
    def initialize(self):
        """Initialize the validation system."""
        logger.info("Initializing validation system...")
        
        # Test database connections
        if not self.source_db.test_connection():
            raise Exception("Failed to connect to source database")
        
        if not self.target_db.test_connection():
            raise Exception("Failed to connect to target database")
        
        # Initialize repository tables
        self.repository.initialize_tables()
        
        logger.info("Validation system initialized successfully")
    
    def run_validation(self) -> List[ValidationResult]:
        """Run the complete validation process."""
        logger.info("Starting validation process...")
        
        # Check if we're within the run window
        if not self.window_checker.is_within_window():
            seconds_until_window = self.window_checker.seconds_until_window_opens()
            logger.info(f"Outside run window. Next window opens in {seconds_until_window} seconds")
            return []
        
        # Initialize if needed
        self.initialize()
        
        # Run validations
        results = self._run_concurrent_validations()
        
        # Send email report
        if results and self.config.email_config.enabled:
            self.email_sender.send_validation_report(results)
        
        logger.info(f"Validation process completed. Processed {len(results)} tables")
        return results
    
    def _run_concurrent_validations(self) -> List[ValidationResult]:
        """Run table validations concurrently."""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent_validations) as executor:
            # Submit all validation tasks
            future_to_mapping = {
                executor.submit(self._validate_table_with_window_check, mapping): mapping
                for mapping in self.config.table_mappings
            }
            
            # Process completed validations
            for future in as_completed(future_to_mapping):
                mapping = future_to_mapping[future]
                
                try:
                    result = future.result()
                    if result:  # None if outside window
                        results.append(result)
                        logger.info(f"Completed validation for {mapping.source_table}")
                except Exception as e:
                    logger.error(f"Error validating {mapping.source_table}: {e}")
        
        return results
    
    def _validate_table_with_window_check(self, mapping) -> ValidationResult:
        """Validate a table with window check."""
        # Check if we're still within the run window
        if not self.window_checker.is_within_window():
            logger.info(f"Skipping {mapping.source_table} - outside run window")
            return None
        
        return self.table_validator.validate_table(mapping)
    
    def resume_validations(self) -> List[ValidationResult]:
        """Resume any paused or failed validations."""
        logger.info("Checking for validations to resume...")
        
        # Find tables with incomplete validations
        resumable_tables = []
        for mapping in self.config.table_mappings:
            progress = self.repository.get_latest_progress(mapping.source_table)
            
            if progress and progress.status in ['IN_PROGRESS', 'PAUSED', 'FAILED']:
                logger.info(f"Found resumable validation for {mapping.source_table}")
                resumable_tables.append(mapping)
        
        if not resumable_tables:
            logger.info("No validations to resume")
            return []
        
        # Update config to only validate resumable tables
        self.config.table_mappings = resumable_tables
        
        # Run validation
        return self.run_validation()