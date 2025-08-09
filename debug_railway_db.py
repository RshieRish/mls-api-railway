#!/usr/bin/env python3
"""
Debug database connection issues in Railway
"""

import os
import psycopg2
import psycopg2.extras

# Database configuration
DB_DSN = os.getenv("DATABASE_URL")

def debug_connection():
    """Debug database connection and query results"""
    print("=== Railway Database Debug ===")
    
    if not DB_DSN:
        print("ERROR: DATABASE_URL not found")
        return False
        
    print(f"DATABASE_URL exists: {DB_DSN[:30]}...")
    
    try:
        # Test basic connection
        print("\n1. Testing basic connection...")
        conn = psycopg2.connect(dsn=DB_DSN)
        print("✅ Basic connection successful")
        
        # Test with RealDictCursor
        print("\n2. Testing RealDictCursor connection...")
        conn_dict = psycopg2.connect(dsn=DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)
        print("✅ RealDictCursor connection successful")
        
        # Test simple query with basic cursor
        print("\n3. Testing simple query with basic cursor...")
        with conn.cursor() as cur:
            cur.execute("SELECT 1 as test")
            result = cur.fetchone()
            print(f"Basic cursor result: {result}")
            print(f"Result type: {type(result)}")
            if result:
                print(f"Result[0]: {result[0]}")
        
        # Test simple query with RealDictCursor
        print("\n4. Testing simple query with RealDictCursor...")
        with conn_dict.cursor() as cur:
            cur.execute("SELECT 1 as test")
            result = cur.fetchone()
            print(f"RealDictCursor result: {result}")
            print(f"Result type: {type(result)}")
            if result:
                print(f"Result['test']: {result['test']}")
                print(f"Result[0]: {result[0]}")
        
        # Check if listings table exists
        print("\n5. Checking if listings table exists...")
        with conn_dict.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'listings'
                )
            """)
            result = cur.fetchone()
            print(f"Table exists query result: {result}")
            table_exists = result[0] if result else False
            print(f"Listings table exists: {table_exists}")
        
        # If table doesn't exist, create it
        if not table_exists:
            print("\n6. Creating listings table...")
            with conn_dict.cursor() as cur:
                cur.execute("""
                    CREATE TABLE listings (
                        id SERIAL PRIMARY KEY,
                        listing_key TEXT UNIQUE,
                        data JSONB NOT NULL DEFAULT '{}',
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
                conn_dict.commit()
                print("✅ Listings table created")
        
        # Test COUNT query on listings table
        print("\n7. Testing COUNT query on listings table...")
        with conn_dict.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM listings")
            result = cur.fetchone()
            print(f"COUNT query result: {result}")
            print(f"Result type: {type(result)}")
            if result:
                print(f"Result[0]: {result[0]}")
                count = result[0]
                print(f"Listings count: {count}")
        
        conn.close()
        conn_dict.close()
        print("\n✅ All tests completed successfully")
        return True
        
    except Exception as e:
        print(f"\n❌ Error during debug: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_connection()