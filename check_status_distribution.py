#!/usr/bin/env python3
import psycopg2
import json
from collections import Counter

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== CHECKING LISTING STATUS DISTRIBUTION ===\n")

# Get total count
cur.execute("SELECT COUNT(*) FROM listings")
total_count = cur.fetchone()[0]
print(f"Total listings in database: {total_count}")

# Check status distribution from the data JSONB field
print("\n1. Checking STATUS field in raw data...")
cur.execute("""
    SELECT data->>'STATUS' as status, COUNT(*) as count
    FROM listings 
    WHERE data->>'STATUS' IS NOT NULL
    GROUP BY data->>'STATUS'
    ORDER BY count DESC
""")

status_results = cur.fetchall()
if status_results:
    print("Status distribution from STATUS field:")
    for status, count in status_results:
        print(f"  {status}: {count} listings")
else:
    print("  No STATUS field found in data")

# Check LISTING_STATUS field
print("\n2. Checking LISTING_STATUS field...")
cur.execute("""
    SELECT data->>'LISTING_STATUS' as status, COUNT(*) as count
    FROM listings 
    WHERE data->>'LISTING_STATUS' IS NOT NULL
    GROUP BY data->>'LISTING_STATUS'
    ORDER BY count DESC
""")

listing_status_results = cur.fetchall()
if listing_status_results:
    print("Status distribution from LISTING_STATUS field:")
    for status, count in listing_status_results:
        print(f"  {status}: {count} listings")
else:
    print("  No LISTING_STATUS field found in data")

# Check ListingStatus field (camelCase)
print("\n3. Checking ListingStatus field...")
cur.execute("""
    SELECT data->>'ListingStatus' as status, COUNT(*) as count
    FROM listings 
    WHERE data->>'ListingStatus' IS NOT NULL
    GROUP BY data->>'ListingStatus'
    ORDER BY count DESC
""")

camel_status_results = cur.fetchall()
if camel_status_results:
    print("Status distribution from ListingStatus field:")
    for status, count in camel_status_results:
        print(f"  {status}: {count} listings")
else:
    print("  No ListingStatus field found in data")

# Check what fields are available in a sample listing
print("\n4. Sample listing fields...")
cur.execute("SELECT data FROM listings LIMIT 1")
sample = cur.fetchone()
if sample and sample[0]:
    sample_data = sample[0]
    print("Available fields in sample listing:")
    status_related_fields = [key for key in sample_data.keys() if 'status' in key.lower()]
    if status_related_fields:
        print(f"  Status-related fields: {status_related_fields}")
        for field in status_related_fields:
            print(f"    {field}: {sample_data[field]}")
    else:
        print("  No status-related fields found")
        print(f"  All fields: {list(sample_data.keys())[:20]}...")  # Show first 20 fields

# Check if there's a separate status column
print("\n5. Checking table schema for status columns...")
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'listings' AND column_name ILIKE '%status%'
""")

status_columns = cur.fetchall()
if status_columns:
    print("Status columns in table:")
    for col_name, data_type in status_columns:
        print(f"  {col_name}: {data_type}")
        # Get distribution for this column
        cur.execute(f"SELECT {col_name}, COUNT(*) FROM listings WHERE {col_name} IS NOT NULL GROUP BY {col_name}")
        col_results = cur.fetchall()
        for value, count in col_results:
            print(f"    {value}: {count}")
else:
    print("  No status columns found in table schema")

cur.close()
conn.close()

print("\n=== STATUS CHECK COMPLETE ===")