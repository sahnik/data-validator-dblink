#!/usr/bin/env python3
"""Test script to debug Oracle connection issues."""

import oracledb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_target_connection():
    """Test connection to the target Oracle database."""
    
    username = os.getenv("TARGET_DB_USERNAME", "target_user")
    password = os.getenv("TARGET_DB_PASSWORD", "target_password")
    host = os.getenv("TARGET_DB_HOST", "target-db-host")
    port = os.getenv("TARGET_DB_PORT", "1521")
    service = os.getenv("TARGET_DB_SERVICE", "target_service")
    
    print(f"\nTARGET CONNECTION PARAMETERS:")
    print(f"  Username: {username}")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Service: {service}")
    
    # Try different connection methods
    dsn = f"{host}:{port}/{service}"
    print(f"\nDSN: {dsn}")
    
    try:
        # Method 1: Basic connection
        print("\nTrying basic connection to TARGET...")
        connection = oracledb.connect(
            user=username,
            password=password,
            dsn=dsn
        )
        print("SUCCESS! Target connection established.")
        
        # Test query
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        print(f"Test query result: {result}")
        
        cursor.close()
        connection.close()
        return connection, True
        
    except Exception as e:
        print(f"FAILED to connect to target: {e}")
        
        # Try with different parameters
        try:
            print("\nTrying TARGET with makedsn...")
            dsn = oracledb.makedsn(host, port, service_name=service)
            connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn
            )
            print("SUCCESS with makedsn!")
            connection.close()
            return None, True
        except Exception as e2:
            print(f"All connection attempts to target failed: {e2}")
            return None, False


def test_db_link():
    """Test database link from target to source."""
    connection, success = test_target_connection()
    if not success:
        print("\nCannot test DB link: Target connection failed")
        return False
        
    db_link_name = os.getenv("DB_LINK_NAME", "SOURCE_DB_LINK")
    
    # Connect to target database again if needed
    if not connection:
        username = os.getenv("TARGET_DB_USERNAME", "target_user")
        password = os.getenv("TARGET_DB_PASSWORD", "target_password")
        host = os.getenv("TARGET_DB_HOST", "target-db-host")
        port = os.getenv("TARGET_DB_PORT", "1521")
        service = os.getenv("TARGET_DB_SERVICE", "target_service")
        
        try:
            dsn = oracledb.makedsn(host, port, service_name=service)
            connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn
            )
        except Exception as e:
            print(f"\nFailed to reconnect to target: {e}")
            return False
    
    # Test database link
    try:
        print(f"\nTesting database link '{db_link_name}' from TARGET to SOURCE...")
        cursor = connection.cursor()
        
        # Simple test query
        cursor.execute(f"SELECT 1 FROM DUAL@{db_link_name}")
        result = cursor.fetchone()
        print(f"Simple DB Link test query result: {result}")
        
        # Test metadata query 
        cursor.execute(f"SELECT owner, table_name FROM all_tables@{db_link_name} WHERE ROWNUM <= 5")
        tables = cursor.fetchall()
        print(f"\nSample tables in source database:")
        for owner, table in tables:
            print(f"  {owner}.{table}")
            
        print("\nSUCCESS! Database link is working correctly.")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"\nDatabase link test FAILED: {e}")
        print("Make sure the database link is correctly created on the target database and points to the source.")
        if connection:
            connection.close()
        return False


if __name__ == "__main__":
    print("===== Oracle Database Validator Connection Test =====")
    print("This script tests the target database connection and the database link to the source.")
    
    if test_db_link():
        print("\n✅ All connections successful! Your configuration appears correct.")
    else:
        print("\n❌ Connection testing failed. Please check your configuration and database link setup.")
        print("\nTIPS:")
        print("1. Ensure the target database is accessible")
        print("2. Verify the database link is created on the target database and points to the source")
        print("3. Check permissions for the database link user")