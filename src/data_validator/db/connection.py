import oracledb
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager

from ..config import DatabaseConfig

logger = logging.getLogger(__name__)

# Initialize Oracle client if needed
try:
    oracledb.init_oracle_client()
    logger.info("Oracle client initialized")
except Exception as e:
    logger.info(f"Oracle client initialization skipped: {e}")


class OracleConnectionManager:
    """Manages connections to Oracle databases for validation.
    
    When using database links for validation:
    - The DB link should be created on the target database
    - The DB link should point to the source database
    - Validation queries are executed from the target database
    """
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection: Optional[oracledb.Connection] = None
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            # First try with direct DSN string
            dsn = f"{self.config.host}:{self.config.port}/{self.config.service_name}"
            logger.info(f"Attempting to connect to Oracle database with DSN: {dsn}")
            logger.debug(f"Username: {self.config.username}")
            
            try:
                connection = oracledb.connect(
                    user=self.config.username,
                    password=self.config.password,
                    dsn=dsn
                )
            except Exception as e:
                logger.warning(f"Direct DSN connection failed: {e}, trying makedsn...")
                # Try with makedsn
                dsn = oracledb.makedsn(
                    self.config.host, 
                    self.config.port, 
                    service_name=self.config.service_name
                )
                connection = oracledb.connect(
                    user=self.config.username,
                    password=self.config.password,
                    dsn=dsn
                )
                
            logger.info(f"Connected to Oracle database at {self.config.host}:{self.config.port}")
            yield connection
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")
    
    @contextmanager
    def get_cursor(self, connection=None):
        """Get a cursor, either from provided connection or new connection."""
        if connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()
        else:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    yield cursor
    
    def execute_query(self, query: str, params: Dict[str, Any] = None, fetch_all: bool = True):
        """Execute a query and return results."""
        with self.get_connection() as connection:
            with self.get_cursor(connection) as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if fetch_all:
                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    return cursor
    
    def execute_ddl(self, statement: str):
        """Execute DDL statement."""
        with self.get_connection() as connection:
            with self.get_cursor(connection) as cursor:
                cursor.execute(statement)
                connection.commit()
                logger.info(f"Executed DDL: {statement[:100]}...")
    
    def test_connection(self) -> bool:
        """Test if database connection is working."""
        try:
            with self.get_connection() as connection:
                with self.get_cursor(connection) as cursor:
                    cursor.execute("SELECT 1 FROM DUAL")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False