import logging
import time
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from ..config import TableMapping, ValidationResult, MismatchDetail
from ..db.connection import OracleConnectionManager
from ..db.repository import ValidationRepository

logger = logging.getLogger(__name__)


class TableValidator:
    def __init__(self, target_db: OracleConnectionManager, db_link_name: str, repository: ValidationRepository, config=None):
        self.target_db = target_db
        self.db_link_name = db_link_name
        self.repository = repository
        self.config = config  # Store the config for access to mismatch details settings
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
            
            # Process any mismatch details
            mismatch_details = []
            if 'mismatch_details' in result and self.config and self.config.store_mismatch_details:
                # Convert raw mismatch details to MismatchDetail objects
                for detail in result['mismatch_details']:
                    mismatch_details.append(MismatchDetail(
                        validation_id=0,  # Will be set when saving
                        table_name=mapping.source_table,
                        mismatch_type=detail['mismatch_type'],
                        key_values=detail['key_values'],
                        column_name=detail['column_name'],
                        source_value=detail['source_value'],
                        target_value=detail['target_value']
                    ))
            
            # First save the validation result to get an ID
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
                error_message=None,
                mismatch_details=[]  # Don't pass mismatch_details here, we'll handle them separately
            )
            
            # Save the result and get the assigned ID
            validation_id = self.repository.save_result(validation_result)
            validation_result.id = validation_id
            
            # Now save the mismatch details with the correct validation_id
            if mismatch_details and hasattr(self, 'config') and self.config.store_mismatch_details:
                logger.info(f"Saving {len(mismatch_details)} mismatch details for validation {validation_id}")
                # Update each mismatch detail with the correct validation_id
                for detail in mismatch_details:
                    detail.validation_id = validation_id
                # Save the details
                self.repository.save_mismatch_details(validation_id, mismatch_details)
            
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
                           total_rows: int) -> Dict[str, any]:
        """Validate table data in chunks."""
        results = {
            'matched': 0,
            'mismatched': 0,
            'missing_in_target': 0,
            'extra_in_target': 0,
            'mismatch_details': []
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
            
            # Collect mismatch details if available and store_mismatch_details is enabled
            if 'mismatch_details' in chunk_result and self.config and self.config.store_mismatch_details:
                # Only collect up to the maximum allowed
                remaining_capacity = self.config.max_mismatch_details - len(results['mismatch_details'])
                if remaining_capacity > 0:
                    details_to_add = chunk_result['mismatch_details'][:remaining_capacity]
                    results['mismatch_details'].extend(details_to_add)
                    
                    # Log if we're reaching the limit
                    if len(results['mismatch_details']) >= self.config.max_mismatch_details:
                        logger.info(f"Reached maximum mismatch details limit ({self.config.max_mismatch_details})" +
                                  f" for table {mapping.source_table}")
            
            processed_rows += chunk_result['processed']
            last_key = chunk_result['last_key']
            
            # Update progress
            self.repository.update_progress(progress_id, processed_rows, last_key)
            
            if chunk_result['processed'] < mapping.chunk_size:
                break
        
        # Check for extra rows in target
        extra_result = self._count_extra_in_target(
            mapping.source_table,
            mapping.target_table,
            mapping.natural_keys,
            mapping.incremental_mode,
            mapping.incremental_column
        )
        
        # If we received a tuple with count and details, unpack it
        if isinstance(extra_result, tuple) and len(extra_result) == 2:
            count, details = extra_result
            results['extra_in_target'] = count
            
            # Add details if we have capacity and details collection is enabled
            if self.config and self.config.store_mismatch_details:
                remaining_capacity = self.config.max_mismatch_details - len(results['mismatch_details'])
                if remaining_capacity > 0 and details:
                    details_to_add = details[:remaining_capacity]
                    results['mismatch_details'].extend(details_to_add)
        else:
            # Simple count was returned
            results['extra_in_target'] = extra_result
        
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
    
    def _generate_column_checks(self, columns: List[str], natural_keys: List[str], 
                              target_table: str, source_table: str, db_link_name: str) -> str:
        """Generate PL/SQL code for checking which columns are mismatched."""
        # This function creates a cursor to check each column for mismatches
        # and returns a PL/SQL block that will execute this check
        
        cols_to_check = [col for col in columns if col not in natural_keys]
        if not cols_to_check:
            return "-- No columns to check\n"
            
        # Create a comma-separated list of columns for the query
        col_list = ', '.join([f"'{col}'" for col in cols_to_check])
        
        # Build natural key conditions for the WHERE clause
        # We need to join the records using natural keys
        key_conditions_parts = []
        for key in natural_keys:
            key_conditions_parts.append(f"t.{key} = s.{key}")
        key_conditions = " AND ".join(key_conditions_parts)
        
        # Build a WHERE clause for the natural key conditions against the current record
        record_key_conditions_parts = []
        for key in natural_keys:
            record_key_conditions_parts.append(f"t.{key} = rec.{key}")
        record_key_conditions = " AND ".join(record_key_conditions_parts)
            
        # Create a PL/SQL block that checks each column
        # Instead of using ROWTYPE, we directly compare columns between tables
        plsql = f"""
        -- For each non-key column, check if it's mismatched
        FOR check_col IN (
            SELECT column_name 
            FROM user_tab_columns 
            WHERE table_name = UPPER('{target_table}')
            AND column_name IN ({col_list})
        ) LOOP
            -- Check if this column has a mismatch between source and target
            -- using dynamic SQL since we need to reference columns by name dynamically
            DECLARE
                v_is_mismatched NUMBER := 0;
                v_sql VARCHAR2(4000);
            BEGIN
                -- Build dynamic SQL to check for mismatches
                v_sql := 'SELECT COUNT(*) FROM ' || '{target_table}' || ' t, ' ||
                         '{source_table}@' || '{db_link_name}' || ' s ' ||
                         'WHERE ' || '{key_conditions}' || ' AND ' || '{record_key_conditions}' || ' AND ' ||
                         '(t.' || check_col.column_name || ' != s.' || check_col.column_name || 
                         ' OR (t.' || check_col.column_name || ' IS NULL AND s.' || check_col.column_name || ' IS NOT NULL)' ||
                         ' OR (t.' || check_col.column_name || ' IS NOT NULL AND s.' || check_col.column_name || ' IS NULL))';
                
                -- Execute the dynamic SQL
                EXECUTE IMMEDIATE v_sql INTO v_is_mismatched;
                
                -- If mismatched, add to our collection
                IF v_is_mismatched > 0 THEN
                    -- Add the column to our mismatched columns collection
                    v_columns(v_detail_count) := check_col.column_name;
                    EXIT; -- We only need one representative column for the detail
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- Log the error but continue with other columns
                    v_columns(v_detail_count) := 'ERROR: ' || SQLERRM;
                    EXIT;
            END;
        END LOOP;
        """
        
        return plsql

    def _build_chunk_plsql(self, source_table: str, target_table: str,
                          columns: List[str], natural_keys: List[str],
                          key_conditions: str, where_clause: Optional[str],
                          chunk_size: int, column_info: Dict[str, str],
                          last_key: Optional[str] = None, 
                          incremental_mode: bool = False,
                          incremental_column: Optional[str] = None) -> str:
        """Build PL/SQL block for chunk-based comparison.
        
        Note: When using database links in Oracle, avoid using %ROWTYPE references
        as they can cause type mismatch errors (DPY-2007). Instead, use direct column
        references or dynamic SQL for column comparisons.
        """
        
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
        
        # Add the column-specific mismatches for detailed reporting
        column_mismatch_clauses = []
        for col in columns:
            if col not in natural_keys:
                # Don't include comparison in the main SELECT to avoid cartesian product
                column_mismatch_clauses.append(f"""
                WHEN '{col}' THEN
                    CASE 
                        WHEN t.{col} IS NULL AND s.{col} IS NOT NULL THEN 'Target NULL, Source: ' || s.{col}
                        WHEN t.{col} IS NOT NULL AND s.{col} IS NULL THEN 'Target: ' || t.{col} || ', Source NULL'
                        WHEN t.{col} != s.{col} THEN 'Target: ' || t.{col} || ', Source: ' || s.{col}
                        ELSE NULL
                    END
                """)
        
        column_mismatch_case = "NULL" if not column_mismatch_clauses else f"""
        CASE c.col_name
        {' '.join(column_mismatch_clauses)}
        ELSE NULL
        END
        """
        
        # Aggregate the natural key values as a JSON-like string
        key_json_elements = []
        for key in natural_keys:
            key_json_elements.append(f"'{key}:' || t.{key}")
        
        key_json = " || ',' || ".join(key_json_elements)
        
        # Generate column check code
        column_checks = self._generate_column_checks(columns, natural_keys, target_table, source_table, self.db_link_name)
        max_details = self.config.max_mismatch_details if hasattr(self, 'config') and self.config else 1000
        
        # Generate the join conditions for key checks
        join_conditions = ' '.join([f"AND t.{key} = rec.{key}" for key in natural_keys])
        
        # Format the JSON key string differently to avoid f-string nesting issues
        key_json_expr = " || ',' || ".join([f"'{key}:' || t.{key}" for key in natural_keys])
        
        # Log the plsql generation for debugging
        logger.debug(f"Generating simplified PL/SQL for {target_table} validation with {source_table}@{self.db_link_name}")
        
        # Create PL/SQL that avoids using ROWTYPE with database links
        plsql = f"""
        DECLARE
            v_matched NUMBER := 0;
            v_mismatched NUMBER := 0;
            v_missing NUMBER := 0;
            v_processed NUMBER := 0;
            v_last_key VARCHAR2(4000);
            v_detail_count NUMBER := 0;
            v_key_value VARCHAR2(4000);
            
            -- Simple cursor that directly checks for mismatches between source and target
            -- We need to specify column aliases to avoid duplicate column names
            CURSOR c_data IS
                SELECT 
                    {", ".join([f"t.{col} as t_{col}" for col in columns])},
                    {", ".join([f"s.{col} as s_{col}" for col in columns])},
                    CASE 
                        WHEN s.{natural_keys[0]} IS NULL THEN 'MISSING'
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
            -- Process each row using direct SQL comparison
            FOR rec IN c_data LOOP
                v_processed := v_processed + 1;
                
                -- Construct the last key value for pagination
                v_key_value := NULL;
                {" || '~|~' || ".join([f"v_key_value := COALESCE(v_key_value || '~|~', '') || TO_CHAR(rec.t_{key})" for key in natural_keys])};
                v_last_key := v_key_value;
                
                IF rec.status = 'MATCH' THEN
                    v_matched := v_matched + 1;
                ELSIF rec.status = 'MISMATCH' THEN
                    v_mismatched := v_mismatched + 1;
                ELSIF rec.status = 'MISSING' THEN
                    v_missing := v_missing + 1;
                END IF;
            END LOOP;
            
            -- Return results via OUT parameters
            :matched := v_matched;
            :mismatched := v_mismatched;
            :missing := v_missing;
            :processed := v_processed;
            :last_key := v_last_key;
            :detail_count := v_detail_count;
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
                
                # Variables for mismatch details - simplified for testing
                # Just use a count variable, no arrays needed for simplified test
                detail_count = cursor.var(int)
                
                # Build parameters - simplified for testing
                params = {
                    "matched": matched,
                    "mismatched": mismatched,
                    "missing": missing,
                    "processed": processed,
                    "last_key": last_key_var,
                    "detail_count": detail_count
                }
                
                # Log the first 500 characters of the PL/SQL block for debugging
                logger.debug(f"Executing chunk validation with plsql_block preview: {plsql_block[:500]}...")
                
                try:
                    # Execute simple test before the actual PL/SQL
                    logger.debug("Executing simple test before main PL/SQL")
                    cursor.execute("""
                    BEGIN
                       :out_var := 42;
                    END;
                    """, {"out_var": detail_count})
                    test_result = detail_count.getvalue()
                    logger.debug(f"Simple test result: {test_result}")
                    
                    # Now execute the actual PL/SQL block
                    logger.debug("Executing main PL/SQL block")
                    cursor.execute(plsql_block, params)
                    logger.debug("PL/SQL execution completed successfully")
                    
                except Exception as e:
                    # Log the error with specific details
                    logger.error(f"Error executing PL/SQL: {str(e)}")
                    # Log a larger portion of the PL/SQL if there's an error
                    error_context = plsql_block[:min(len(plsql_block), 5000)]
                    logger.error(f"PL/SQL causing error (fragment): {error_context}")
                    raise
                
                # Log all variable values
                logger.debug("Getting variable values from PL/SQL execution")
                matched_val = matched.getvalue()
                mismatched_val = mismatched.getvalue()
                missing_val = missing.getvalue()
                processed_val = processed.getvalue()
                last_key_val = last_key_var.getvalue()
                detail_count_val = detail_count.getvalue() or 0
                
                logger.debug(f"Variable values - matched: {matched_val}, mismatched: {mismatched_val}, " +
                           f"missing: {missing_val}, processed: {processed_val}, last_key: {last_key_val}")
                
                # Get mismatch details using a separate query instead of PL/SQL arrays
                # This avoids the ORA-06513 error completely
                mismatch_details = []
                
                # Only get details if there are mismatches and we're configured to store them
                if (mismatched_val > 0 or missing_val > 0) and hasattr(self, 'config') and self.config and self.config.store_mismatch_details:
                    # Get a few sample mismatched and missing rows for detail
                    max_details_to_get = min(20, self.config.max_mismatch_details)  # Limit to reasonable number
                    try:
                        # Extract table and key information from the PL/SQL block
                        source_table = self._extract_table_from_plsql(plsql_block, "source_table")
                        target_table = self._extract_table_from_plsql(plsql_block, "target_table")
                        natural_keys = self._extract_natural_keys_from_plsql(plsql_block)
                        
                        # Check if we have enough information to proceed
                        if (source_table.startswith("UNKNOWN") or 
                            target_table.startswith("UNKNOWN") or 
                            not natural_keys):
                            # If we couldn't extract the necessary information, create a generic mismatch detail
                            logger.warning("Could not extract table information from PL/SQL block")
                            if mismatched_val > 0:
                                mismatch_details.append({
                                    "key_values": "{}",
                                    "mismatch_type": "COLUMN_MISMATCH",
                                    "column_name": "UNKNOWN",
                                    "source_value": f"Mismatched rows: {mismatched_val}",
                                    "target_value": "Could not extract detailed information"
                                })
                            if missing_val > 0:
                                mismatch_details.append({
                                    "key_values": "{}",
                                    "mismatch_type": "MISSING_IN_TARGET",
                                    "column_name": None,
                                    "source_value": f"Missing rows: {missing_val}",
                                    "target_value": None
                                })
                        else:
                            # Collect column mismatch details with direct SQL
                            if mismatched_val > 0:
                                mismatch_details.extend(
                                    self._get_column_mismatch_details(
                                        mapping_source_table=source_table,
                                        mapping_target_table=target_table,
                                        natural_keys=natural_keys,
                                        limit=max_details_to_get // 2  # Split the limit between different types
                                    )
                                )
                            
                            # Collect missing row details
                            if missing_val > 0:
                                mismatch_details.extend(
                                    self._get_missing_row_details(
                                        mapping_source_table=source_table,
                                        mapping_target_table=target_table,
                                        natural_keys=natural_keys,
                                        limit=max_details_to_get // 2
                                    )
                                )
                    except Exception as e:
                        # If detail collection fails, log but continue
                        logger.warning(f"Error collecting column mismatch details: {e}")
                        logger.info("Continuing validation without column mismatch details")
                
                logger.debug("Returning results from chunk validation")
                return {
                    "matched": matched_val,
                    "mismatched": mismatched_val,
                    "missing_in_target": missing_val,
                    "processed": processed_val,
                    "last_key": last_key_val,
                    "mismatch_details": mismatch_details
                }
    
    def _extract_table_from_plsql(self, plsql_block: str, table_var_name: str) -> str:
        """Extract a table name from the PL/SQL block."""
        import re
        
        # Try to find table names in the FROM clause which is the most reliable
        if table_var_name == "source_table":
            # Look for pattern like: JOIN table_name@db_link
            pattern = r"JOIN\s+(\w+)@"
            match = re.search(pattern, plsql_block)
            if match:
                return match.group(1)
                
            # Alternative pattern: FROM table_name@db_link
            pattern = r"FROM\s+(\w+)@"
            match = re.search(pattern, plsql_block)
            if match:
                return match.group(1)
        elif table_var_name == "target_table":
            # Look for pattern like: FROM table_name t
            pattern = r"FROM\s+(\w+)\s+t"
            match = re.search(pattern, plsql_block)
            if match:
                return match.group(1)
                
        # If we get here, try more generic approach
        # Look for variable definitions
        pattern = rf"{table_var_name}\s*=\s*([^\s,;]+)"
        match = re.search(pattern, plsql_block)
        if match:
            return match.group(1)
            
        # If all else fails, try to find any reference with the table alias
        if table_var_name == "target_table":
            # Find any reference to t.something
            pattern = r"t\.(\w+)"
            match = re.search(pattern, plsql_block)
            if match:
                # We found a column name, now try to find which table it belongs to
                return "UNKNOWN_TARGET_TABLE"
        
        return "UNKNOWN_TABLE"
        
    def _extract_natural_keys_from_plsql(self, plsql_block: str) -> List[str]:
        """Extract natural keys from the PL/SQL block."""
        # Search for key conditions in the ON clause 
        import re
        keys = []
        # Look for patterns like "t.KEY = s.KEY"
        key_pattern = r"t\.(\w+)\s*=\s*s\.\1"
        matches = re.findall(key_pattern, plsql_block)
        if matches:
            return matches
        return []
        
    def _parse_key_json(self, key_json: str) -> dict:
        """Parse a JSON-like string of key values."""
        if not key_json or not key_json.startswith('{') or not key_json.endswith('}'):
            return {}
            
        try:
            # For proper JSON strings
            import json
            return json.loads(key_json)
        except json.JSONDecodeError:
            # For semi-JSON strings we generate (they might not be proper JSON)
            try:
                # Remove the braces
                key_str = key_json[1:-1]
                # Split by commas
                pairs = key_str.split(',')
                
                result = {}
                for pair in pairs:
                    if ':' not in pair:
                        continue
                        
                    key, value = pair.split(':', 1)
                    
                    # Clean up the key and value
                    key = key.strip().strip('"\'')
                    value = value.strip().strip('"\'')
                    
                    # Try to convert numeric values
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except ValueError:
                        # Keep as string if not numeric
                        pass
                        
                    result[key] = value
                
                return result
            except Exception as e:
                logger.warning(f"Error parsing key JSON: {e}")
                return {}
    
    def _get_column_mismatch_details(self, mapping_source_table: str, mapping_target_table: str, 
                                   natural_keys: List[str], limit: int = 20) -> List[Dict]:
        """Get column mismatch details using direct SQL rather than PL/SQL arrays."""
        logger.debug(f"Getting column mismatch details for {mapping_target_table} vs {mapping_source_table}")
        
        if not natural_keys:
            logger.warning("No natural keys found for mismatch details query")
            return []
            
        # Get all non-key columns
        all_columns = self._get_table_columns(mapping_source_table)
        non_key_columns = [col for col in all_columns if col not in natural_keys]
        
        if not non_key_columns:
            # If all columns are part of the natural key, we won't find mismatches
            # Just return an empty list since there are no columns to compare
            logger.warning("No non-key columns found for mismatch details query")
            return []
            
        # Ensure we have at least one column to compare - safety check
        if not all_columns:
            logger.warning("No columns found for the table")
            return []
            
        # Build JSON format for key values
        key_json_expr = "'{' || " + " || ',' || ".join([f"'\"" + key + "\":\"' || t." + key + " || '\"'" for key in natural_keys]) + " || '}'"
        
        # Build comparison conditions for all columns 
        # We'll use a CASE expression to identify which column is mismatched
        comparison_conditions = []
        for col in non_key_columns:
            comparison_conditions.append(
                f"""WHEN ((t.{col} IS NULL AND s.{col} IS NOT NULL) OR 
                       (t.{col} IS NOT NULL AND s.{col} IS NULL) OR 
                       (t.{col} IS NOT NULL AND s.{col} IS NOT NULL AND t.{col} != s.{col}))
                    THEN '{col}'"""
            )
        
        column_case = "NULL" if not comparison_conditions else f"""
        CASE
            {' '.join(comparison_conditions)}
            ELSE NULL
        END as column_name
        """
        
        # Build all the conditions outside of f-strings to avoid nesting
        join_condition = " AND ".join([f"t.{key} = s.{key}" for key in natural_keys])
        
        # Build comparison conditions for WHERE clause
        where_conditions = []
        for col in non_key_columns:
            where_conditions.append(f"""(
                (t.{col} IS NULL AND s.{col} IS NOT NULL) OR 
                (t.{col} IS NOT NULL AND s.{col} IS NULL) OR 
                (t.{col} IS NOT NULL AND s.{col} IS NOT NULL AND t.{col} != s.{col})
            )""")
            
        # Handle the case where we have no conditions (should never happen)
        if not where_conditions:
            where_clause = "1=0"  # No conditions, no mismatches possible
        else:
            where_clause = " OR ".join(where_conditions)
        
        # Instead of trying to get the values in the SQL query, let's just get the column names
        # and then execute specific queries for each mismatched row to get the exact column values
        query = f"""
        SELECT DISTINCT
            {key_json_expr} as key_values,
            CASE 
                {' '.join(comparison_conditions)}
                ELSE NULL
            END as column_name,
            'COLUMN_MISMATCH' as mismatch_type
        FROM {mapping_target_table} t
        JOIN {mapping_source_table}@{self.db_link_name} s
        ON {join_condition}
        WHERE ({where_clause})
        AND ROWNUM <= {limit}
        """
        
        try:
            # Execute the query to get mismatch details
            logger.debug(f"Executing mismatch details query: {query[:500]}...")
            results = self.target_db.execute_query(query)
            
            # Process the initial results
            details = []
            
            # Parse the key values and get the column-specific values for each mismatch
            for row in results:
                if not row['COLUMN_NAME']:
                    continue  # Skip if no column name identified
                    
                column_name = row['COLUMN_NAME']
                key_values = row['KEY_VALUES']
                
                # Extract natural key values from the JSON-like string
                key_dict = self._parse_key_json(key_values)
                if not key_dict:
                    continue  # Skip if we can't parse the key values
                    
                # Get the actual source and target values for the specific mismatched column
                try:
                    # Build conditions for the WHERE clause to find this specific row
                    key_conditions = []
                    for key, value in key_dict.items():
                        # Properly quote string values
                        if isinstance(value, str):
                            key_conditions.append(f"t.{key} = '{value}'")
                        else:
                            key_conditions.append(f"t.{key} = {value}")
                            
                    # Execute a query to get the specific column values
                    value_query = f"""
                    SELECT 
                        TO_CHAR(t.{column_name}) as target_value,
                        TO_CHAR(s.{column_name}) as source_value
                    FROM {mapping_target_table} t
                    JOIN {mapping_source_table}@{self.db_link_name} s
                    ON {join_condition}
                    WHERE {" AND ".join(key_conditions)}
                    """
                    
                    # Execute the query to get the column values
                    value_results = self.target_db.execute_query(value_query)
                    
                    # Add the detail with the correct column values
                    if value_results:
                        source_value = value_results[0]['SOURCE_VALUE']
                        target_value = value_results[0]['TARGET_VALUE']
                        
                        detail = {
                            "key_values": key_values,
                            "mismatch_type": row['MISMATCH_TYPE'],
                            "column_name": column_name,
                            "source_value": source_value,
                            "target_value": target_value
                        }
                        details.append(detail)
                    else:
                        # If we can't get the values, add a detail with generic values
                        detail = {
                            "key_values": key_values,
                            "mismatch_type": row['MISMATCH_TYPE'],
                            "column_name": column_name,
                            "source_value": "Could not retrieve value",
                            "target_value": "Could not retrieve value"
                        }
                        details.append(detail)
                except Exception as e:
                    # If any error occurs, log and continue with the next mismatch
                    logger.warning(f"Error getting column values for {column_name}: {e}")
                    # Add a detail with the error
                    detail = {
                        "key_values": key_values,
                        "mismatch_type": row['MISMATCH_TYPE'],
                        "column_name": column_name,
                        "source_value": f"Error: {str(e)[:50]}",
                        "target_value": f"Error: {str(e)[:50]}"
                    }
                    details.append(detail)
                    
            logger.debug(f"Found {len(details)} column mismatch details")
            return details
            
        except Exception as e:
            logger.error(f"Error executing column mismatch details query: {e}")
            return []
    
    def _get_missing_row_details(self, mapping_source_table: str, mapping_target_table: str,
                               natural_keys: List[str], limit: int = 10) -> List[Dict]:
        """Get details of rows that exist in source but are missing from target."""
        logger.debug(f"Getting missing row details for {mapping_source_table} rows missing from {mapping_target_table}")
        
        if not natural_keys:
            logger.warning("No natural keys found for missing row details query")
            return []
        
        # Build a JSON string representation of the natural keys
        key_json_expr = "'{' || " + " || ',' || ".join([f"'\"" + key + "\":\"' || s." + key + " || '\"'" for key in natural_keys]) + " || '}'"
        
        # Build the join condition for natural keys
        join_condition = " AND ".join([f"t.{key} = s.{key}" for key in natural_keys])
        
        # Simple query to find rows in source that don't exist in target
        query = f"""
        SELECT 
            {key_json_expr} as key_values,
            'MISSING_IN_TARGET' as mismatch_type,
            NULL as column_name,
            NULL as target_value,
            NULL as source_value
        FROM {mapping_source_table}@{self.db_link_name} s
        WHERE NOT EXISTS (
            SELECT 1 FROM {mapping_target_table} t
            WHERE {join_condition}
        )
        AND ROWNUM <= {limit}
        """
        
        try:
            # Execute the query
            logger.debug(f"Executing missing row details query: {query[:500]}...")
            results = self.target_db.execute_query(query)
            
            # Format the results
            details = []
            for row in results:
                detail = {
                    "key_values": row['KEY_VALUES'],
                    "mismatch_type": row['MISMATCH_TYPE'],
                    "column_name": None,
                    "source_value": None,
                    "target_value": None
                }
                details.append(detail)
                
            logger.debug(f"Found {len(details)} missing row details")
            return details
            
        except Exception as e:
            logger.error(f"Error executing missing row details query: {e}")
            return []
    
    def _count_extra_in_target(self, source_table: str, target_table: str,
                              natural_keys: List[str],
                              incremental_mode: bool = False,
                              incremental_column: Optional[str] = None) -> Union[int, Tuple[int, List[Dict]]]:
        """Count rows that exist in target but not in source and optionally return details."""
        logger.debug(f"Counting extra rows in target table {target_table} not in source {source_table}")
        
        # Since we're now querying from target, we need to change the approach
        # The "extra in target" are now rows in target that don't exist in source@dblink
        key_conditions = " AND ".join([f"t.{key} = s.{key}" for key in natural_keys])
        
        # Apply incremental filter if enabled
        incremental_condition = self._get_incremental_condition(source_table, incremental_mode, incremental_column, for_extra_target=True)
        
        # If we don't need details, just return the count
        if not (hasattr(self, 'config') and self.config.store_mismatch_details):
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
            
        # We need both count and details
        # First, get the count
        count_query = f"""
        SELECT COUNT(*) as cnt
        FROM {target_table} t
        WHERE NOT EXISTS (
            SELECT 1 FROM {source_table}@{self.db_link_name} s
            WHERE {key_conditions}
        )
        {incremental_condition}
        """
        
        count_result = self.target_db.execute_query(count_query)
        count = count_result[0]['CNT']
        
        # If there are no extra rows or we don't need to capture details, return just the count
        if count == 0 or not self.config.store_mismatch_details:
            return count
        
        # Now get the details up to the maximum limit
        # Format the natural keys as a JSON-like string
        # Create a JSON-like representation of keys
        key_expr = " || ',' || ".join([f"'{key}:' || t.{key}" for key in natural_keys])
        
        detail_query = f"""
        SELECT {", ".join([f"t.{key}" for key in natural_keys])},
               '{{' || {key_expr} || '}}' as key_json
        FROM {target_table} t
        WHERE NOT EXISTS (
            SELECT 1 FROM {source_table}@{self.db_link_name} s
            WHERE {key_conditions}
        )
        {incremental_condition}
        FETCH FIRST {self.config.max_mismatch_details} ROWS ONLY
        """
        
        # Execute from target database to get details
        detail_results = self.target_db.execute_query(detail_query)
        
        # Format the details
        details = []
        for row in detail_results:
            details.append({
                "key_values": row['KEY_JSON'],
                "mismatch_type": "EXTRA_IN_TARGET",
                "column_name": None,
                "source_value": None,
                "target_value": None
            })
        
        return count, details