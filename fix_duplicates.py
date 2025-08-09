import psycopg2
import json
from datetime import datetime

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== FIXING DUPLICATE LISTINGS ===")

# First, let's see Brandon's listings before cleanup
print("\n=== BRANDON'S LISTINGS BEFORE CLEANUP ===")
cur.execute("""
    SELECT listing_key, data, updated_at 
    FROM listings 
    WHERE data::text LIKE '%CN222505%' 
    ORDER BY listing_key, updated_at DESC
""")

brandon_before = cur.fetchall()
print(f"Brandon's listings before: {len(brandon_before)}")

for i, (listing_key, data, updated_at) in enumerate(brandon_before):
    if data and '_raw_data' in data:
        raw_data = data['_raw_data']
        street_name = raw_data.get('STREET_NAME', 'Unknown Street')
        city = raw_data.get('CITY', data.get('City', 'Unknown City'))
        list_price = raw_data.get('LIST_PRICE', data.get('ListPrice', 'Unknown Price'))
        status = raw_data.get('STATUS', 'Unknown Status')
        
        print(f"{i+1}. MLS: {listing_key} - {street_name}, {city} - ${list_price} ({status}) - {updated_at}")

# Count total duplicates
cur.execute("""
    SELECT COUNT(*) as total_duplicates
    FROM (
        SELECT listing_key, COUNT(*) as count
        FROM listings 
        GROUP BY listing_key
        HAVING COUNT(*) > 1
    ) duplicates
""")

total_duplicate_keys = cur.fetchone()[0]
print(f"\nTotal listing_keys with duplicates: {total_duplicate_keys}")

# Count total duplicate records
cur.execute("""
    SELECT SUM(count - 1) as total_duplicate_records
    FROM (
        SELECT listing_key, COUNT(*) as count
        FROM listings 
        GROUP BY listing_key
        HAVING COUNT(*) > 1
    ) duplicates
""")

total_duplicate_records = cur.fetchone()[0] or 0
print(f"Total duplicate records to remove: {total_duplicate_records}")

print("\n=== REMOVING DUPLICATES (keeping most recent) ===")

# Remove duplicates - keep only the most recent record for each listing_key
cur.execute("""
    DELETE FROM listings 
    WHERE ctid NOT IN (
        SELECT DISTINCT ON (listing_key) ctid
        FROM listings 
        ORDER BY listing_key, updated_at DESC
    )
""")

removed_count = cur.rowcount
print(f"Removed {removed_count} duplicate records")

# Commit the changes
conn.commit()

# Verify Brandon's listings after cleanup
print("\n=== BRANDON'S LISTINGS AFTER CLEANUP ===")
cur.execute("""
    SELECT listing_key, data, updated_at 
    FROM listings 
    WHERE data::text LIKE '%CN222505%' 
    ORDER BY listing_key
""")

brandon_after = cur.fetchall()
print(f"Brandon's listings after: {len(brandon_after)}")

for i, (listing_key, data, updated_at) in enumerate(brandon_after):
    if data and '_raw_data' in data:
        raw_data = data['_raw_data']
        street_name = raw_data.get('STREET_NAME', 'Unknown Street')
        city = raw_data.get('CITY', data.get('City', 'Unknown City'))
        list_price = raw_data.get('LIST_PRICE', data.get('ListPrice', 'Unknown Price'))
        status = raw_data.get('STATUS', 'Unknown Status')
        
        print(f"{i+1}. MLS: {listing_key} - {street_name}, {city} - ${list_price} ({status})")

# Verify no more duplicates exist
cur.execute("""
    SELECT COUNT(*) as remaining_duplicates
    FROM (
        SELECT listing_key, COUNT(*) as count
        FROM listings 
        GROUP BY listing_key
        HAVING COUNT(*) > 1
    ) duplicates
""")

remaining_duplicates = cur.fetchone()[0]
print(f"\nRemaining duplicates: {remaining_duplicates}")

# Get total listings count
cur.execute("SELECT COUNT(*) FROM listings")
total_listings = cur.fetchone()[0]
print(f"Total listings in database: {total_listings}")

cur.close()
conn.close()

print("\n=== SUMMARY ===")
print(f"Brandon's listings before: {len(brandon_before)}")
print(f"Brandon's listings after: {len(brandon_after)}")
print(f"Duplicate records removed: {removed_count}")
print(f"Remaining duplicates: {remaining_duplicates}")
print(f"Total listings: {total_listings}")

if len(brandon_after) < 3:
    print(f"\n⚠️  WARNING: Brandon should have 3 distinct listings, but only {len(brandon_after)} found.")
    print("This suggests the automation may not be refreshing properly or some listings are missing.")
else:
    print(f"\n✅ SUCCESS: Brandon has {len(brandon_after)} listings (expected 3 or more)")