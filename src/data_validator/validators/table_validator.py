import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..config import TableMapping, ValidationResult
from ..db.connection import OracleConnectionManager
from ..db.repository import ValidationRepository

logger = logging.getLogger(__name__)


class TableValidator:
    def __init__(self, target_db: OracleConnectionManager, db_link_name: str, repository: ValidationRepository):
        self.target_db = target_db
        self.db_link_name = db_link_name
        self.repository = repository
        # The DB link is created on the target database pointing to the source
    
    def validate_table(self, mapping: TableMapping) -> ValidationResult:
        """Validate a single table mapping."""
        logger.info(f"Starting validation for table {mapping.source_table}")
        if mapping.incremental_mode:
            logger.info(f"Using incremental validation mode with column: {mapping.incremental_column}")
        
        start_time = datetime.now()
        
        # Create progress entry
        progress_id = self.repository.create_progress_entry(mapping.source_table)
        
        try:
            # Get total row count - still using source_db since we want to count source rows
            total_rows = self._get_table_row_count(
                mapping.source_table, 
                mapping.where_clause,
                mapping.incremental_mode,
                mapping.incremental_column
            )
            self.repository.update_progress(progress_id, 0, status="IN_PROGRESS")
            
            # Perform validation in chunks
            result = self._validate_in_chunks(mapping, progress_id, total_rows)
            
            # Complete progress
            self.repository.complete_progress(progress_id, status="COMPLETED")
            
            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Save and return result
            validation_result = ValidationResult(
                id=0,  # Will be set by repository
                table_name=mapping.source_table,
                total_rows=total_rows,
                matched_rows=result['matched'],
                mismatched_rows=result['mismatched'],
                missing_in_target=result['missing_in_target'],
                extra_in_target=result['extra_in_target'],
                validation_duration_seconds=duration,
                started_at=start_time,
                completed_at=end_time,
                status="SUCCESS" if result['mismatched'] == 0 else "PARTIAL",
                error_message=None
            )
            
            self.repository.save_result(validation_result)
            return validation_result
            
        except Exception as e:
            logger.error(f"Error validating table {mapping.source_table}: {e}")
            self.repository.complete_progress(progress_id, status="FAILED", error_message=str(e))
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            validation_result = ValidationResult(
                id=0,
                table_name=mapping.source_table,
                total_rows=0,
                matched_rows=0,
                mismatched_rows=0,
                missing_in_target=0,
                extra_in_target=0,
                validation_duration_seconds=duration,
                started_at=start_time,
                completed_at=end_time,
                status="FAILED",
                error_message=str(e)
            )
            
            self.repository.save_result(validation_result)
            raise
    
    def _get_table_row_count(self, table_name: str, where_clause: Optional[str] = None, 
                         incremental_mode: bool = False, incremental_column: Optional[str] = None) -> int:
        """Get the total row count for a table."""
        query = f"SELECT COUNT(*) as cnt FROM {table_name}@{self.db_link_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        # Apply incremental filter if enabled
        if incremental_mode and incremental_column:
            # Get the last run time from previous validation results
            last_run_time = self._get_last_validation_time(table_name)
            if last_run_time:
                where_connector = "WHERE" if "WHERE" not in query else "AND"
                query += f" {where_connector} {incremental_column} > TO_TIMESTAMP('{last_run_time}', 'YYYY-MM-DD HH24:MI:SS.FF')"
        
        # Use target_db with database link to query the source
        result = self.target_db.execute_query(query)
        return result[0]['CNT']
    
    def _validate_in_chunks(self, mapping: TableMapping, progress_id: int, 
                           total_rows: int) -> Dict[str, int]:
        """Validate table data in chunks."""
        results = {
            'matched': 0,
            'mismatched': 0,
            'missing_in_target': 0,
            'extra_in_target': 0
        }
        
        # Get column info
        column_info = self._get_column_info(mapping.source_table)
        
        # Get columns to compare
        columns = self._get_table_columns(mapping.source_table)
        columns = [col for col in columns if col not in mapping.exclude_columns]
        
        # Build natural key condition - reversed s and t references since we're querying from target now
        key_conditions = " AND ".join([f"t.{key} = s.{key}" for key in mapping.natural_keys])
        
        # Execute validation in chunks
        processed_rows = 0
        last_key = None
        
        while processed_rows < total_rows:
            # Build PL/SQL block for this chunk with pagination
            plsql_block = self._build_chunk_plsql(
                mapping.source_table,
                mapping.target_table,
                columns,
                mapping.natural_keys,
                key_conditions,
                mapping.where_clause,
                mapping.chunk_size,
                column_info,
                last_key,  # Pass last_key for pagination
                mapping.incremental_mode,
                mapping.incremental_column
            )
            
            chunk_result = self._execute_chunk_validation(
                plsql_block, 
                last_key,
                mapping.chunk_size
            )
            
            results['matched'] += chunk_result['matched']
            results['mismatched'] += chunk_result['mismatched']
            results['missing_in_target'] += chunk_result['missing_in_target']
            
            processed_rows += chunk_result['processed']
            last_key = chunk_result['last_key']
            
            # Update progress
            self.repository.update_progress(progress_id, processed_rows, last_key)
            
            if chunk_result['processed'] < mapping.chunk_size:
                break
        
        # Check for extra rows in target
        results['extra_in_target'] = self._count_extra_in_target(
            mapping.source_table,
            mapping.target_table,
            mapping.natural_keys,
            mapping.incremental_mode,
            mapping.incremental_column
        )
        
        return results
    
    def _get_last_validation_time(self, table_name: str) -> Optional[str]:
        """Get the last validation time for a table."""
        query = f"""
        SELECT MAX(completed_at) as last_run
        FROM {self.repository.results_table}
        WHERE table_name = :table_name
        AND status = 'SUCCESS'
        """
        
        result = self.repository.db_manager.execute_query(query, {"table_name": table_name})
        if result and result[0]['LAST_RUN']:
            return result[0]['LAST_RUN'].strftime("%Y-%m-%d %H:%M:%S.%f")
        return None
    
    def _get_incremental_condition(self, table_name: str, incremental_mode: bool, 
                                 incremental_column: Optional[str], 
                                 for_extra_target: bool = False) -> str:
        """Build SQL condition for incremental validation."""
        if not (incremental_mode and incremental_column):
            return ""
            
        last_run_time = self._get_last_validation_time(table_name)
        if not last_run_time:
            return ""
            
        # For extra-in-target query, we apply the filter on target table
        table_alias = "t" if for_extra_target else "s"
        return f"AND {table_alias}.{incremental_column} > TO_TIMESTAMP('{last_run_time}', 'YYYY-MM-DD HH24:MI:SS.FF')"
    
    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get all columns for a table."""
        query = """
        SELECT column_name 
        FROM user_tab_columns@{0} 
        WHERE table_name = UPPER(:table_name)
        ORDER BY column_id
        """.format(self.db_link_name)
        
        # Use target_db with database link to get column metadata from source
        result = self.target_db.execute_query(query, {"table_name": table_name})
        return [row['COLUMN_NAME'] for row in result]
    
    def _get_column_info(self, table_name: str) -> Dict[str, str]:
        """Get column names and data types for a table."""
        query = """
        SELECT column_name, data_type
        FROM user_tab_columns@{0} 
        WHERE table_name = UPPER(:table_name)
        """.format(self.db_link_name)
        
        # Use target_db with database link to get column metadata from source
        result = self.target_db.execute_query(query, {"table_name": table_name})
        return {row['COLUMN_NAME']: row['DATA_TYPE'] for row in result}
    
    def _build_chunk_plsql(self, source_table: str, target_table: str,
                          columns: List[str], natural_keys: List[str],
                          key_conditions: str, where_clause: Optional[str],
                          chunk_size: int, column_info: Dict[str, str],
                          last_key: Optional[str] = None, 
                          incremental_mode: bool = False,
                          incremental_column: Optional[str] = None) -> str:
        """Build PL/SQL block for chunk-based comparison."""
        
        # Build column comparison conditions - reversed s and t references
        column_comparisons = []
        for col in columns:
            if col not in natural_keys:
                # Build the comparison based on data type
                if column_info.get(col) in ['VARCHAR2', 'CHAR', 'CLOB']:
                    # String comparison
                    comparison = f"""
                    (t.{col} IS NULL AND s.{col} IS NOT NULL) OR 
                    (t.{col} IS NOT NULL AND s.{col} IS NULL) OR 
                    (t.{col} IS NOT NULL AND s.{col} IS NOT NULL AND t.{col} != s.{col})
                    """.strip()
                else:
                    # Numeric/Date comparison  
                    comparison = f"""
                    (t.{col} IS NULL AND s.{col} IS NOT NULL) OR 
                    (t.{col} IS NOT NULL AND s.{col} IS NULL) OR 
                    (t.{col} != s.{col})
                    """.strip()
                column_comparisons.append(f"({comparison})")
        
        comparison_condition = " OR ".join(column_comparisons) if column_comparisons else "1=0"
        
        # Build key ordering for pagination - using target (t) columns now
        key_ordering = ", ".join([f"t.{key}" for key in natural_keys])
        key_values = ", ".join([f"t.{key}" for key in natural_keys])
        
        # Build pagination condition if last_key is provided
        pagination_condition = ""
        if last_key:
            # Parse last_key to build condition (using our delimiter)
            key_parts = last_key.split('~|~')
            if len(key_parts) == len(natural_keys):
                # Build proper pagination condition for composite keys
                pagination_conditions = []
                for i in range(len(natural_keys)):
                    conditions = []
                    # Add equality conditions for all previous keys
                    for j in range(i):
                        key_name = natural_keys[j]
                        key_value = key_parts[j].strip()
                        
                        # Format the value based on data type - using target (t) columns now
                        if column_info.get(key_name) in ['VARCHAR2', 'CHAR', 'CLOB']:
                            conditions.append(f"t.{key_name} = '{key_value}'")
                        elif column_info.get(key_name) == 'DATE':
                            conditions.append(f"t.{key_name} = TO_DATE('{key_value}', 'YYYY-MM-DD HH24:MI:SS')")
                        else:  # Numeric
                            conditions.append(f"t.{key_name} = {key_value}")
                    
                    # Add greater than condition for current key
                    key_name = natural_keys[i]
                    key_value = key_parts[i].strip()
                    
                    if column_info.get(key_name) in ['VARCHAR2', 'CHAR', 'CLOB']:
                        conditions.append(f"t.{key_name} > '{key_value}'")
                    elif column_info.get(key_name) == 'DATE':
                        conditions.append(f"t.{key_name} > TO_DATE('{key_value}', 'YYYY-MM-DD HH24:MI:SS')")
                    else:  # Numeric
                        conditions.append(f"t.{key_name} > {key_value}")
                    
                    if conditions:
                        pagination_conditions.append(f"({' AND '.join(conditions)})")
                
                if pagination_conditions:
                    pagination_condition = f"AND ({' OR '.join(pagination_conditions)})"
        
        # Build key value formatting for last_key
        key_formatting = []
        for key in natural_keys:
            if column_info.get(key) == 'DATE':
                key_formatting.append(f"TO_CHAR(rec.{key}, 'YYYY-MM-DD HH24:MI:SS')")
            else:
                key_formatting.append(f"TO_CHAR(rec.{key})")
        # Note: rec contains target table columns since we're querying from target
        
        last_key_concat = " || '~|~' || ".join(key_formatting)
        
        plsql = f"""
        DECLARE
            v_matched NUMBER := 0;
            v_mismatched NUMBER := 0;
            v_missing NUMBER := 0;
            v_processed NUMBER := 0;
            v_last_key VARCHAR2(4000);
            
            CURSOR c_data IS
                SELECT {key_values}, 
                       CASE WHEN s.{natural_keys[0]} IS NULL THEN 'MISSING'
                            WHEN {comparison_condition} THEN 'MISMATCH'
                            ELSE 'MATCH'
                       END as status
                FROM {target_table} t
                LEFT JOIN {source_table}@{self.db_link_name} s
                    ON {key_conditions}
                WHERE 1=1
                {f"AND {where_clause}" if where_clause else ""}
                {self._get_incremental_condition(source_table, incremental_mode, incremental_column)}
                {pagination_condition}
                ORDER BY {key_ordering}
                FETCH FIRST {chunk_size} ROWS ONLY;
        BEGIN
            FOR rec IN c_data LOOP
                v_processed := v_processed + 1;
                v_last_key := {last_key_concat};
                
                IF rec.status = 'MATCH' THEN
                    v_matched := v_matched + 1;
                ELSIF rec.status = 'MISMATCH' THEN
                    v_mismatched := v_mismatched + 1;
                ELSIF rec.status = 'MISSING' THEN
                    v_missing := v_missing + 1;
                END IF;
            END LOOP;
            
            :matched := v_matched;
            :mismatched := v_mismatched;
            :missing := v_missing;
            :processed := v_processed;
            :last_key := v_last_key;
        END;
        """
        
        return plsql
    
    def _execute_chunk_validation(self, plsql_block: str, last_key: Optional[str],
                                 chunk_size: int) -> Dict[str, any]:
        """Execute validation for a single chunk."""
        # Changed to use target_db for executing validation since the DB link is on target
        with self.target_db.get_connection() as connection:
            with self.target_db.get_cursor(connection) as cursor:
                # Prepare output variables
                matched = cursor.var(int)
                mismatched = cursor.var(int)
                missing = cursor.var(int)
                processed = cursor.var(int)
                last_key_var = cursor.var(str)
                
                # Build parameters - no need for last_key_param as it's built into the query
                params = {
                    "matched": matched,
                    "mismatched": mismatched,
                    "missing": missing,
                    "processed": processed,
                    "last_key": last_key_var
                }
                
                # Execute PL/SQL block
                cursor.execute(plsql_block, params)
                
                return {
                    "matched": matched.getvalue(),
                    "mismatched": mismatched.getvalue(),
                    "missing_in_target": missing.getvalue(),
                    "processed": processed.getvalue(),
                    "last_key": last_key_var.getvalue()
                }
    
    def _count_extra_in_target(self, source_table: str, target_table: str,
                              natural_keys: List[str],
                              incremental_mode: bool = False,
                              incremental_column: Optional[str] = None) -> int:
        """Count rows that exist in target but not in source."""
        # Since we're now querying from target, we need to change the approach
        # The "extra in target" are now rows in target that don't exist in source@dblink
        key_conditions = " AND ".join([f"t.{key} = s.{key}" for key in natural_keys])
        
        # Apply incremental filter if enabled
        incremental_condition = self._get_incremental_condition(source_table, incremental_mode, incremental_column, for_extra_target=True)
        
        query = f"""
        SELECT COUNT(*) as cnt
        FROM {target_table} t
        WHERE NOT EXISTS (
            SELECT 1 FROM {source_table}@{self.db_link_name} s
            WHERE {key_conditions}
        )
        {incremental_condition}
        """
        
        # Execute from target database
        result = self.target_db.execute_query(query)
        return result[0]['CNT']