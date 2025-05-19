#!/usr/bin/env python3
"""Test script to debug Oracle connection issues."""

import oracledb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_connection():
    """Test Oracle database connection."""
    
    # Get connection parameters from environment
    username = os.getenv("SOURCE_DB_USERNAME", "source_user")
    password = os.getenv("SOURCE_DB_PASSWORD", "source_password")
    host = os.getenv("SOURCE_DB_HOST", "source-db-host")
    port = os.getenv("SOURCE_DB_PORT", "1521")
    service = os.getenv("SOURCE_DB_SERVICE", "source_service")
    
    print(f"Connection parameters:")
    print(f"  Username: {username}")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Service: {service}")
    
    # Try different connection methods
    dsn = f"{host}:{port}/{service}"
    print(f"\nDSN: {dsn}")
    
    try:
        # Method 1: Basic connection
        print("\nTrying basic connection...")
        connection = oracledb.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        print("Success! Connection established.")
        
        # Test query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        print(f"Test query result: {result}")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"Failed: {e}")
        
        # Try with different parameters
        try:
            print("\nTrying with makedsn...")
            dsn = oracledb.makedsn(host, port, service_name=service)
            connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn
            )
            print("Success with makedsn!")
            connection.close()
        except Exception as e2:
            print(f"Also failed with makedsn: {e2}")


if __name__ == "__main__":
    test_connection()