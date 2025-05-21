import oracledb
import logging
import time
from typing import Optional, Dict, Any, Tuple
from contextlib import contextmanager

from ..config import DatabaseConfig

logger = logging.getLogger(__name__)

# Initialize Oracle client if needed
try:
    oracledb.init_oracle_client()
    logger.info("Oracle client initialized")
except Exception as e:
    logger.info(f"Oracle client initialization skipped: {e}")


class OracleConnectionPool:
    """Manages Oracle connection pools for both source and target databases."""
    _pools: Dict[str, oracledb.ConnectionPool] = {}
    
    @classmethod
    def get_pool(cls, db_config: DatabaseConfig, pool_min: int = 2, pool_max: int = 10, 
                 pool_increment: int = 1, timeout: int = 60) -> oracledb.ConnectionPool:
        """
        Get or create a connection pool for the specified database.
        
        Args:
            db_config: Database configuration
            pool_min: Minimum number of connections in the pool
            pool_max: Maximum number of connections in the pool
            pool_increment: Number of connections to create when more are needed
            timeout: Connection timeout in seconds
            
        Returns:
            Connection pool for the database
        """
        # Create a unique key for this database configuration
        pool_key = f"{db_config.username}@{db_config.host}:{db_config.port}/{db_config.service_name}"
        
        # Return existing pool if it exists and is still open
        if pool_key in cls._pools:
            pool = cls._pools[pool_key]
            try:
                # Test if pool is still valid
                with pool.acquire() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1 FROM DUAL")
                return pool
            except Exception as e:
                logger.warning(f"Existing pool {pool_key} is invalid, creating new one: {e}")
                # If we get here, the pool is invalid, so we'll create a new one
                if pool_key in cls._pools:
                    try:
                        cls._pools[pool_key].close(force=True)
                    except Exception as e2:
                        logger.warning(f"Error closing pool {pool_key}: {e2}")
                    del cls._pools[pool_key]
        
        # Create a new pool
        # First try with direct DSN string
        dsn = f"{db_config.host}:{db_config.port}/{db_config.service_name}"
        logger.info(f"Creating connection pool for {dsn} with min={pool_min}, max={pool_max}")
        
        try:
            pool = oracledb.create_pool(
                user=db_config.username,
                password=db_config.password,
                dsn=dsn,
                min=pool_min,
                max=pool_max,
                increment=pool_increment,
                getmode=oracledb.POOL_GETMODE_WAIT,
                wait_timeout=timeout,
                session_callback=cls._configure_session
            )
        except Exception as e:
            logger.warning(f"Direct DSN connection pool failed: {e}, trying makedsn...")
            # Try with makedsn
            dsn = oracledb.makedsn(
                db_config.host, 
                db_config.port, 
                service_name=db_config.service_name
            )
            pool = oracledb.create_pool(
                user=db_config.username,
                password=db_config.password,
                dsn=dsn,
                min=pool_min,
                max=pool_max,
                increment=pool_increment,
                getmode=oracledb.POOL_GETMODE_WAIT,
                wait_timeout=timeout,
                session_callback=cls._configure_session
            )
        
        # Store pool for future use
        cls._pools[pool_key] = pool
        logger.info(f"Created connection pool for {pool_key} with min={pool_min}, max={pool_max}")
        return pool
    
    @staticmethod
    def _configure_session(connection, requested_tag=None, actual_tag=None):
        """Configure session settings for connections from the pool."""
        with connection.cursor() as cursor:
            # Configure session settings
            cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
            cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")
            # Add other session settings as needed
    
    @classmethod
    def close_all_pools(cls):
        """Close all connection pools."""
        for pool_key, pool in list(cls._pools.items()):
            try:
                logger.info(f"Closing connection pool for {pool_key}")
                pool.close(force=False)  # Wait for connections to be returned
                del cls._pools[pool_key]
            except Exception as e:
                logger.warning(f"Error closing pool {pool_key}: {e}")
                # Try force close if normal close fails
                try:
                    pool.close(force=True)
                    del cls._pools[pool_key]
                except Exception as e2:
                    logger.error(f"Error force closing pool {pool_key}: {e2}")


class OracleConnectionManager:
    """Manages connections to Oracle databases for validation.
    
    When using database links for validation:
    - The DB link should be created on the target database
    - The DB link should point to the source database
    - Validation queries are executed from the target database
    """
    def __init__(self, config: DatabaseConfig, pool_min: int = 2, pool_max: int = 10, pool_increment: int = 1):
        self.config = config
        self.pool_min = pool_min
        self.pool_max = pool_max
        self.pool_increment = pool_increment
        self._pool = None
        
    @property
    def pool(self) -> oracledb.ConnectionPool:
        """Get the connection pool for this connection manager."""
        if not self._pool:
            self._pool = OracleConnectionPool.get_pool(
                self.config, 
                self.pool_min, 
                self.pool_max, 
                self.pool_increment
            )
        return self._pool
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections from the pool."""
        connection = None
        start_time = time.time()
        
        try:
            connection = self.pool.acquire()
            acquisition_time = time.time() - start_time
            if acquisition_time > 1.0:  # Log if it took more than 1 second to get a connection
                logger.warning(f"Slow connection acquisition: {acquisition_time:.2f}s")
            logger.debug(f"Acquired connection from pool in {acquisition_time:.4f}s")
            yield connection
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise
        finally:
            if connection:
                try:
                    # Return connection to the pool
                    self.pool.release(connection)
                    logger.debug(f"Released connection back to pool, took {time.time() - start_time:.4f}s total")
                except Exception as e:
                    logger.warning(f"Error returning connection to pool: {e}")
                    # If we can't return it to the pool, try to close it
                    try:
                        connection.close()
                    except:
                        pass
    
    @contextmanager
    def get_cursor(self, connection=None):
        """Get a cursor, either from provided connection or new connection from pool."""
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