#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_database_schema():
    """Check the complete database schema for the listings table"""
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment variables")
        return False
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get table schema information
        cur.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = 'listings'
            ORDER BY ordinal_position
        """)
        
        columns = cur.fetchall()
        
        if not columns:
            print("No columns found for 'listings' table. Table may not exist.")
            return False
        
        print(f"Found {len(columns)} columns in the 'listings' table:\n")
        print(f"{'Column Name':<30} {'Data Type':<20} {'Nullable':<10} {'Max Length':<12} {'Default':<20}")
        print("-" * 100)
        
        for col in columns:
            max_len = str(col['character_maximum_length']) if col['character_maximum_length'] else 'N/A'
            default = str(col['column_default'])[:18] + '...' if col['column_default'] and len(str(col['column_default'])) > 18 else str(col['column_default']) or 'None'
            
            print(f"{col['column_name']:<30} {col['data_type']:<20} {col['is_nullable']:<10} {max_len:<12} {default:<20}")
        
        # Also get a sample row to see actual data
        print("\n" + "="*100)
        print("SAMPLE DATA (first row):")
        print("="*100)
        
        cur.execute("SELECT * FROM listings LIMIT 1")
        sample_row = cur.fetchone()
        
        if sample_row:
            for key, value in sample_row.items():
                value_str = str(value)[:50] + '...' if value and len(str(value)) > 50 else str(value)
                print(f"{key:<30}: {value_str}")
        else:
            print("No data found in listings table")
        
        # Check for potential unique identifier columns
        print("\n" + "="*100)
        print("POTENTIAL UNIQUE IDENTIFIER COLUMNS:")
        print("="*100)
        
        identifier_candidates = []
        for col in columns:
            col_name = col['column_name'].lower()
            if any(keyword in col_name for keyword in ['id', 'mls', 'number', 'key', 'unique']):
                identifier_candidates.append(col['column_name'])
        
        if identifier_candidates:
            print("Found potential identifier columns:")
            for candidate in identifier_candidates:
                print(f"  - {candidate}")
        else:
            print("No obvious identifier columns found")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error checking schema: {e}")
        if 'conn' in locals():
            conn.close()
        return False

if __name__ == "__main__":
    print("Checking database schema...\n")
    success = check_database_schema()
    if not success:
        print("\nSchema check failed!")