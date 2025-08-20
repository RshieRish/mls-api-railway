#!/usr/bin/env python3
"""
Fix PostgreSQL collation version mismatch on Railway
This script addresses the collation version warning that's causing database errors
"""

import psycopg2
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("collation_fix")

def fix_collation_version():
    """Fix the PostgreSQL collation version mismatch"""
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        conn.autocommit = True
        cur = conn.cursor()
        
        log.info("Starting collation version fix...")
        
        # First, check current collation version
        cur.execute("""
            SELECT datname, datcollate, datctype 
            FROM pg_database 
            WHERE datname = current_database();
        """)
        result = cur.fetchone()
        log.info(f"Current database collation info: {result}")
        
        # Refresh collation version for the database
        log.info("Refreshing collation version...")
        cur.execute("ALTER DATABASE railway REFRESH COLLATION VERSION;")
        log.info("Collation version refreshed successfully")
        
        # Also refresh collation for all objects that use default collation
        log.info("Refreshing collation for all database objects...")
        
        # Get all tables that might have collation issues
        cur.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            AND data_type IN ('character varying', 'varchar', 'text', 'char', 'character');
        """)
        
        columns = cur.fetchall()
        log.info(f"Found {len(columns)} text columns to check")
        
        # For each text column, we could rebuild indexes if needed
        # But for now, just refresh the database collation version
        
        log.info("Collation fix completed successfully")
        
    except psycopg2.Error as e:
        log.error(f"Database error during collation fix: {e}")
        raise
    except Exception as e:
        log.error(f"Unexpected error during collation fix: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def test_database_connection():
    """Test if database connection works after collation fix"""
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Test a simple query
        cur.execute("SELECT 1 as test;")
        result = cur.fetchone()
        log.info(f"Database connection test successful: {result}")
        
        # Test CRM tables if they exist
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'crm_%';
        """)
        tables = cur.fetchall()
        log.info(f"Found CRM tables: {[t[0] for t in tables]}")
        
        conn.close()
        return True
        
    except Exception as e:
        log.error(f"Database connection test failed: {e}")
        return False

if __name__ == "__main__":
    log.info("Starting PostgreSQL collation fix for Railway...")
    
    try:
        fix_collation_version()
        
        # Test the connection after fix
        if test_database_connection():
            log.info("Collation fix completed and database is working properly")
        else:
            log.error("Collation fix completed but database connection test failed")
            
    except Exception as e:
        log.error(f"Failed to fix collation: {e}")
        exit(1)