import logging
import time
import concurrent.futures
from src.data_validator.config import DatabaseConfig
from src.data_validator.db.connection import OracleConnectionManager, OracleConnectionPool

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('connection_pool_test.log')
    ]
)

logger = logging.getLogger(__name__)

def test_query(connection_manager, query_id):
    """Execute a test query using the connection manager."""
    start_time = time.time()
    try:
        result = connection_manager.execute_query("SELECT 1 AS test_value FROM DUAL")
        logger.info(f"Query {query_id} executed successfully in {time.time() - start_time:.4f}s: {result}")
        return True
    except Exception as e:
        logger.error(f"Query {query_id} failed: {e}")
        return False

def run_concurrent_queries(connection_manager, num_queries, max_workers):
    """Run multiple queries concurrently using ThreadPoolExecutor."""
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all queries
        futures = [
            executor.submit(test_query, connection_manager, i)
            for i in range(num_queries)
        ]
        
        # Wait for all to complete
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    # Calculate stats
    success_count = sum(results)
    fail_count = len(results) - success_count
    total_time = time.time() - start_time
    
    logger.info(f"Completed {num_queries} queries in {total_time:.4f}s")
    logger.info(f"Success: {success_count}, Failed: {fail_count}")
    logger.info(f"Average time per query: {total_time/num_queries:.4f}s")
    
    return success_count, fail_count, total_time

def main():
    """Main test function."""
    try:
        # Create test configuration
        # IMPORTANT: Update these values with your actual Oracle database credentials
        db_config = DatabaseConfig(
            username="your_username",
            password="your_password",
            host="your_host",
            port=1521,
            service_name="your_service",
            pool_min=2,
            pool_max=10,
            pool_increment=1
        )
        
        # Create connection manager with pooling
        connection_manager = OracleConnectionManager(db_config)
        
        # Test if connection works
        logger.info("Testing initial connection...")
        if not connection_manager.test_connection():
            logger.error("Initial connection test failed. Please check your database credentials.")
            return
        
        logger.info("Connection test successful!")
        
        # Run tests with different concurrency levels
        for num_concurrent in [1, 5, 10, 20]:
            logger.info(f"\n=== Testing with {num_concurrent} concurrent connections ===")
            total_queries = num_concurrent * 5  # 5 queries per worker
            
            # Run test
            success, failures, duration = run_concurrent_queries(
                connection_manager, 
                total_queries, 
                num_concurrent
            )
            
            # Wait a bit between tests
            time.sleep(1)
        
        # Test pool reuse (should use existing pool)
        logger.info("\n=== Testing pool reuse ===")
        new_manager = OracleConnectionManager(db_config)
        success, failures, duration = run_concurrent_queries(new_manager, 10, 5)
        
        # Clean up resources
        logger.info("\nCleaning up connection pools...")
        OracleConnectionPool.close_all_pools()
        logger.info("Test completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
    finally:
        # Make sure to close pools
        OracleConnectionPool.close_all_pools()

if __name__ == "__main__":
    main()