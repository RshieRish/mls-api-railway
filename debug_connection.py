#!/usr/bin/env python3
"""
Debug database connection for Railway deployment
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_DSN = os.getenv("DATABASE_URL")

print(f"DATABASE_URL exists: {DB_DSN is not None}")
if DB_DSN:
    print(f"DATABASE_URL starts with: {DB_DSN[:20]}...")
else:
    print("DATABASE_URL is None or empty!")
    exit(1)

try:
    print("Attempting database connection...")
    conn = psycopg2.connect(dsn=DB_DSN, cursor_factory=RealDictCursor)
    print("✅ Connection successful!")
    
    with conn.cursor() as cur:
        print("Testing simple query...")
        cur.execute("SELECT 1 as test")
        result = cur.fetchone()
        print(f"✅ Query result: {result}")
        
        print("Checking if listings table exists...")
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'listings'
            )
        """)
        table_exists = cur.fetchone()['exists']
        print(f"Listings table exists: {table_exists}")
        
        if table_exists:
            cur.execute("SELECT COUNT(*) FROM listings")
            count = cur.fetchone()['count']
            print(f"Listings count: {count}")
        else:
            print("Creating listings table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS listings (
                    id SERIAL PRIMARY KEY,
                    listing_key TEXT UNIQUE,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
            """)
            print("✅ Table created")
    
    conn.close()
    print("✅ Connection closed successfully")
    
except Exception as e:
    print(f"❌ Error: {e}")
    print(f"Error type: {type(e)}")
    print(f"Error str: '{str(e)}'")
    import traceback
    traceback.print_exc()