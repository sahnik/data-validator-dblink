import logging
from datetime import datetime
from typing import List, Optional

from ..config import ValidationProgress, ValidationResult, MismatchDetail, DatabaseConfig
from .connection import OracleConnectionManager

logger = logging.getLogger(__name__)


class ValidationRepository:
    """Repository for storing validation progress and results.
    
    All results and progress tracking are stored in the target database.
    """
    def __init__(self, target_db_manager: OracleConnectionManager, progress_table: str, 
                 results_table: str, mismatch_details_table: str = "DATA_VALIDATION_MISMATCH_DETAILS"):
        self.db_manager = target_db_manager
        self.progress_table = progress_table
        self.results_table = results_table
        self.mismatch_details_table = mismatch_details_table
        
    def initialize_tables(self):
        """Create progress, results, and mismatch details tables if they don't exist."""
        progress_ddl = f"""
        CREATE TABLE {self.progress_table} (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            table_name VARCHAR2(100) NOT NULL,
            status VARCHAR2(20) NOT NULL,
            total_rows NUMBER,
            processed_rows NUMBER DEFAULT 0,
            last_processed_key VARCHAR2(1000),
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            error_message VARCHAR2(4000),
            CONSTRAINT unique_table_progress UNIQUE (table_name, started_at)
        )
        """
        
        results_ddl = f"""
        CREATE TABLE {self.results_table} (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            table_name VARCHAR2(100) NOT NULL,
            total_rows NUMBER,
            matched_rows NUMBER,
            mismatched_rows NUMBER,
            missing_in_target NUMBER,
            extra_in_target NUMBER,
            validation_duration_seconds NUMBER,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            status VARCHAR2(20) NOT NULL,
            error_message VARCHAR2(4000)
        )
        """
        
        mismatch_details_ddl = f"""
        CREATE TABLE {self.mismatch_details_table} (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            validation_id NUMBER NOT NULL,
            table_name VARCHAR2(100) NOT NULL,
            mismatch_type VARCHAR2(20) NOT NULL,
            key_values CLOB,
            column_name VARCHAR2(100),
            source_value CLOB,
            target_value CLOB,
            capture_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_mismatch_validation FOREIGN KEY (validation_id) 
                REFERENCES {self.results_table}(id) ON DELETE CASCADE
        )
        """
        
        # Check if tables exist
        tables_ddl = [
            (self.progress_table, progress_ddl),
            (self.results_table, results_ddl),
            (self.mismatch_details_table, mismatch_details_ddl)
        ]
        
        for table_name, ddl in tables_ddl:
            check_query = f"""
            SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(:table_name)
            """
            result = self.db_manager.execute_query(check_query, {"table_name": table_name})
            
            if result[0]['COUNT(*)'] == 0:
                logger.info(f"Creating table {table_name}")
                self.db_manager.execute_ddl(ddl)
            else:
                logger.info(f"Table {table_name} already exists")
    
    def create_progress_entry(self, table_name: str, total_rows: int = 0) -> int:
        """Create a new progress entry and return its ID."""
        insert_query = f"""
        INSERT INTO {self.progress_table} 
        (table_name, status, total_rows, processed_rows, started_at, updated_at)
        VALUES (:table_name, 'IN_PROGRESS', :total_rows, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING id INTO :id
        """
        
        with self.db_manager.get_connection() as connection:
            with self.db_manager.get_cursor(connection) as cursor:
                id_var = cursor.var(int)
                cursor.execute(insert_query, {
                    "table_name": table_name,
                    "total_rows": total_rows,
                    "id": id_var
                })
                connection.commit()
                return id_var.getvalue()[0]
    
    def update_progress(self, progress_id: int, processed_rows: int, 
                       last_processed_key: Optional[str] = None,
                       status: str = "IN_PROGRESS"):
        """Update progress for a validation."""
        update_query = f"""
        UPDATE {self.progress_table}
        SET processed_rows = :processed_rows,
            last_processed_key = :last_processed_key,
            status = :status,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
        """
        
        with self.db_manager.get_connection() as connection:
            with self.db_manager.get_cursor(connection) as cursor:
                cursor.execute(update_query, {
                    "processed_rows": processed_rows,
                    "last_processed_key": last_processed_key,
                    "status": status,
                    "id": progress_id
                })
                connection.commit()
    
    def complete_progress(self, progress_id: int, status: str = "COMPLETED", 
                         error_message: Optional[str] = None):
        """Mark a progress entry as completed."""
        update_query = f"""
        UPDATE {self.progress_table}
        SET status = :status,
            completed_at = CURRENT_TIMESTAMP,
            error_message = :error_message,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
        """
        
        with self.db_manager.get_connection() as connection:
            with self.db_manager.get_cursor(connection) as cursor:
                cursor.execute(update_query, {
                    "status": status,
                    "error_message": error_message,
                    "id": progress_id
                })
                connection.commit()
    
    def get_latest_progress(self, table_name: str) -> Optional[ValidationProgress]:
        """Get the latest progress entry for a table."""
        query = f"""
        SELECT * FROM {self.progress_table}
        WHERE table_name = :table_name
        ORDER BY started_at DESC
        FETCH FIRST 1 ROW ONLY
        """
        
        results = self.db_manager.execute_query(query, {"table_name": table_name})
        if results:
            row = results[0]
            return ValidationProgress(
                id=row['ID'],
                table_name=row['TABLE_NAME'],
                status=row['STATUS'],
                total_rows=row['TOTAL_ROWS'] or 0,
                processed_rows=row['PROCESSED_ROWS'] or 0,
                last_processed_key=row['LAST_PROCESSED_KEY'],
                started_at=row['STARTED_AT'],
                updated_at=row['UPDATED_AT'],
                completed_at=row['COMPLETED_AT'],
                error_message=row['ERROR_MESSAGE']
            )
        return None
    
    def save_result(self, result: ValidationResult) -> int:
        """Save validation result and return its ID."""
        insert_query = f"""
        INSERT INTO {self.results_table}
        (table_name, total_rows, matched_rows, mismatched_rows, missing_in_target,
         extra_in_target, validation_duration_seconds, started_at, completed_at,
         status, error_message)
        VALUES (:table_name, :total_rows, :matched_rows, :mismatched_rows,
                :missing_in_target, :extra_in_target, :validation_duration_seconds,
                :started_at, :completed_at, :status, :error_message)
        RETURNING id INTO :id
        """
        
        with self.db_manager.get_connection() as connection:
            with self.db_manager.get_cursor(connection) as cursor:
                id_var = cursor.var(int)
                cursor.execute(insert_query, {
                    "table_name": result.table_name,
                    "total_rows": result.total_rows,
                    "matched_rows": result.matched_rows,
                    "mismatched_rows": result.mismatched_rows,
                    "missing_in_target": result.missing_in_target,
                    "extra_in_target": result.extra_in_target,
                    "validation_duration_seconds": result.validation_duration_seconds,
                    "started_at": result.started_at,
                    "completed_at": result.completed_at,
                    "status": result.status,
                    "error_message": result.error_message,
                    "id": id_var
                })
                
                # Get the ID from the returned value
                validation_id = id_var.getvalue()[0]
                
                # Save mismatch details if available
                if result.mismatch_details:
                    self.save_mismatch_details(validation_id, result.mismatch_details)
                
                connection.commit()
                return validation_id
    
    def save_mismatch_details(self, validation_id: int, details: List[MismatchDetail]):
        """Save multiple mismatch details for a validation."""
        if not details:
            return
        
        # Log before attempting to insert
        logger.debug(f"Preparing to save {len(details)} mismatch details for validation {validation_id}")
        
        try:
            # Create the insert statement with APPEND hint to improve performance
            insert_query = f"""
            INSERT /*+ APPEND */ INTO {self.mismatch_details_table}
            (validation_id, table_name, mismatch_type, key_values, column_name, 
             source_value, target_value)
            VALUES (:validation_id, :table_name, :mismatch_type, :key_values, 
                    :column_name, :source_value, :target_value)
            """
            
            # Process in smaller batches to reduce contention
            BATCH_SIZE = 50
            total_inserted = 0
            
            with self.db_manager.get_connection() as connection:
                # Set a longer timeout for the connection
                connection.callTimeout = 120000  # 2 minutes
                
                for i in range(0, len(details), BATCH_SIZE):
                    batch = details[i:i+BATCH_SIZE]
                    params_list = []
                    
                    for detail in batch:
                        params_list.append({
                            "validation_id": validation_id,
                            "table_name": detail.table_name,
                            "mismatch_type": detail.mismatch_type,
                            "key_values": detail.key_values,
                            "column_name": detail.column_name,
                            "source_value": detail.source_value,
                            "target_value": detail.target_value
                        })
                    
                    # Execute the batch
                    with self.db_manager.get_cursor(connection) as cursor:
                        cursor.executemany(insert_query, params_list)
                        connection.commit()
                        total_inserted += len(batch)
                        logger.debug(f"Saved batch of {len(batch)} mismatch details ({total_inserted}/{len(details)} total)")
                
            logger.info(f"Successfully saved all {total_inserted} mismatch details for validation {validation_id}")
            
        except Exception as e:
            logger.error(f"Error saving mismatch details: {e}")
            # Continue execution rather than failing the whole validation
            # Just log the error and return
            logger.warning("Continuing validation without mismatch details")
                
    def get_mismatch_details(self, validation_id: int) -> List[MismatchDetail]:
        """Get mismatch details for a specific validation."""
        query = f"""
        SELECT * FROM {self.mismatch_details_table}
        WHERE validation_id = :validation_id
        ORDER BY id
        """
        
        result = self.db_manager.execute_query(query, {"validation_id": validation_id})
        return [
            MismatchDetail(
                id=row['ID'],
                validation_id=row['VALIDATION_ID'],
                table_name=row['TABLE_NAME'],
                mismatch_type=row['MISMATCH_TYPE'],
                key_values=row['KEY_VALUES'],
                column_name=row['COLUMN_NAME'],
                source_value=row['SOURCE_VALUE'],
                target_value=row['TARGET_VALUE'],
                capture_time=row['CAPTURE_TIME']
            )
            for row in result
        ]
    
    def get_recent_results(self, limit: int = 10) -> List[ValidationResult]:
        """Get recent validation results."""
        query = f"""
        SELECT * FROM {self.results_table}
        ORDER BY completed_at DESC
        FETCH FIRST :limit ROWS ONLY
        """
        
        results = self.db_manager.execute_query(query, {"limit": limit})
        return [
            ValidationResult(
                id=row['ID'],
                table_name=row['TABLE_NAME'],
                total_rows=row['TOTAL_ROWS'] or 0,
                matched_rows=row['MATCHED_ROWS'] or 0,
                mismatched_rows=row['MISMATCHED_ROWS'] or 0,
                missing_in_target=row['MISSING_IN_TARGET'] or 0,
                extra_in_target=row['EXTRA_IN_TARGET'] or 0,
                validation_duration_seconds=row['VALIDATION_DURATION_SECONDS'] or 0,
                started_at=row['STARTED_AT'],
                completed_at=row['COMPLETED_AT'],
                status=row['STATUS'],
                error_message=row['ERROR_MESSAGE']
            )
            for row in results
        ]