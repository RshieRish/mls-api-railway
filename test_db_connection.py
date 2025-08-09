#!/usr/bin/env python3
"""
Test script to verify Neon database connection
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_DSN = os.getenv("DATABASE_URL")

if not DB_DSN:
    print("ERROR: DATABASE_URL not found in environment variables")
    exit(1)

print(f"Testing connection to: {DB_DSN[:50]}...")

try:
    # Test connection
    conn = psycopg2.connect(dsn=DB_DSN, cursor_factory=RealDictCursor)
    print("✅ Database connection successful!")
    
    with conn.cursor() as cur:
        # Check if listings table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'listings'
            )
        """)
        result = cur.fetchone()
        table_exists = result['exists']
        
        if table_exists:
            print("✅ Listings table exists")
            
            # Count listings
            cur.execute("SELECT COUNT(*) FROM listings")
            result = cur.fetchone()
            count = result['count']
            print(f"📊 Total listings in database: {count}")
            
            if count > 0:
                # Show sample listing
                cur.execute("SELECT listing_key, data FROM listings LIMIT 1")
                sample = cur.fetchone()
                print(f"📋 Sample listing key: {sample['listing_key']}")
                print(f"📋 Sample data keys: {list(sample['data'].keys())[:10]}...")
            else:
                print("⚠️  No listings found in database")
                print("🔧 This explains why Railway health check is failing - empty database!")
        else:
            print("❌ Listings table does not exist")
            print("🔧 Need to create table and populate with data")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

print("\n🎉 Database test completed successfully!")