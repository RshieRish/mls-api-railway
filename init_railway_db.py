#!/usr/bin/env python3
"""
Initialize database for Railway deployment
"""

import os
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json

# Database configuration
DB_DSN = os.getenv("DATABASE_URL")

# Field mapping from test.py
FIELD_MAP = [
    "ListingID",
    "ListingKey",
    "ListPrice",
    "StreetName",
    "City",
    "StateOrProvince",
    "PostalCode",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "LivingArea",
    "Latitude",
    "Longitude",
    "ListingStatus",
    "ModificationTimestamp",
]

def get_conn():
    return psycopg2.connect(dsn=DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    """Initialize database with listings table"""
    print("Initializing database...")
    
    if not DB_DSN:
        print("ERROR: DATABASE_URL not found")
        return False
        
    print(f"DATABASE_URL exists: {DB_DSN[:20]}...")
    
    try:
        cols = ",\n    ".join(f"{f.lower()} TEXT" for f in FIELD_MAP)
        
        with get_conn() as conn, conn.cursor() as cur:
            # Create table if it doesn't exist
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS listings (
                    id SERIAL PRIMARY KEY,
                    listing_key TEXT UNIQUE,
                    {cols},
                    data JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS listings_gin ON listings USING GIN (data);
                """
            )
            
            # Check if table exists and get count
            cur.execute("SELECT COUNT(*) FROM listings")
            result = cur.fetchone()
            count = result[0] if result and len(result) > 0 else 0
            print(f"✅ Database initialized successfully")
            print(f"✅ Listings table exists with {count} records")
            
            # Insert a test record if table is empty
            if count == 0:
                print("Inserting test record...")
                test_data = {
                    "ListingKey": "TEST001",
                    "ListingID": "TEST001",
                    "ListPrice": "500000",
                    "City": "Test City",
                    "StateOrProvince": "MA"
                }
                
                record = ["TEST001"]
                record += [test_data.get(f, None) for f in FIELD_MAP]
                record.append(Json(test_data))
                
                cols_list = ["listing_key"] + [f.lower() for f in FIELD_MAP] + ["data"]
                placeholders = ",".join(["%s"] * len(cols_list))
                
                cur.execute(
                    f"INSERT INTO listings ({', '.join(cols_list)}) VALUES ({placeholders})",
                    record
                )
                print("✅ Test record inserted")
            
        return True
        
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

if __name__ == "__main__":
    success = init_db()
    if success:
        print("Database initialization completed successfully!")
    else:
        print("Database initialization failed!")
        exit(1)