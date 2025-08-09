import psycopg2
import json

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== DEBUGGING DATA STRUCTURE ===")

# Get one Brandon listing to see the actual JSON structure
cur.execute("""
    SELECT listing_key, data, updated_at 
    FROM listings 
    WHERE data::text LIKE '%CN222505%' 
    LIMIT 1
""")

result = cur.fetchone()
if result:
    listing_key, data, updated_at = result
    print(f"Listing Key: {listing_key}")
    print(f"Updated At: {updated_at}")
    print("\nRaw JSON Data:")
    print(json.dumps(data, indent=2))
    
    print("\n=== AVAILABLE KEYS IN DATA ===")
    if isinstance(data, dict):
        for key in sorted(data.keys()):
            print(f"{key}: {data[key]}")
else:
    print("No Brandon listings found")

print("\n=== CHECKING LISTING_KEY AS PRIMARY KEY ===")
# Check if listing_key is the MLS number
cur.execute("""
    SELECT listing_key, COUNT(*) as count
    FROM listings 
    GROUP BY listing_key
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 10
""")

duplicates_by_key = cur.fetchall()
print(f"Duplicate listing_keys found: {len(duplicates_by_key)}")
for listing_key, count in duplicates_by_key:
    print(f"Listing Key {listing_key}: {count} duplicates")

cur.close()
conn.close()