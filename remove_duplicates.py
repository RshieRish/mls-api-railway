import psycopg2
import json
from datetime import datetime

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== ANALYZING DUPLICATES BEFORE REMOVAL ===")

# First, let's see all Brandon's listings with proper data extraction
cur.execute("""
    SELECT listing_key, data, updated_at 
    FROM listings 
    WHERE data::text LIKE '%CN222505%' 
    ORDER BY listing_key, updated_at DESC
""")

brandon_results = cur.fetchall()
print(f"Total Brandon listings found: {len(brandon_results)}")
print("="*80)

for i, (listing_key, data, updated_at) in enumerate(brandon_results):
    if data and '_raw_data' in data:
        raw_data = data['_raw_data']
        street_num = raw_data.get('STREET_NUMBER', '')
        street_name = raw_data.get('STREET_NAME', 'Unknown Street')
        city = raw_data.get('CITY', data.get('City', 'Unknown City'))
        state = raw_data.get('STATE', 'Unknown State')
        zip_code = raw_data.get('ZIP_CODE', 'Unknown Zip')
        agent_id = raw_data.get('LIST_AGENT', 'Unknown Agent')
        list_price = raw_data.get('LIST_PRICE', data.get('ListPrice', 'Unknown Price'))
        status = raw_data.get('STATUS', 'Unknown Status')
        
        full_address = f"{street_num} {street_name}, {city}, {state} {zip_code}".strip()
        
        print(f"{i+1}. MLS: {listing_key}")
        print(f"   Address: {full_address}")
        print(f"   Agent: {agent_id}")
        print(f"   Price: ${list_price}")
        print(f"   Status: {status}")
        print(f"   Updated: {updated_at}")
        print()

print("\n=== FINDING ALL DUPLICATES IN DATABASE ===")

# Find all duplicates by listing_key
cur.execute("""
    SELECT listing_key, COUNT(*) as count, 
           MIN(updated_at) as first_seen,
           MAX(updated_at) as last_seen
    FROM listings 
    GROUP BY listing_key
    HAVING COUNT(*) > 1
    ORDER BY count DESC, listing_key
""")

all_duplicates = cur.fetchall()
print(f"Total listing_keys with duplicates: {len(all_duplicates)}")

total_duplicate_records = 0
for listing_key, count, first_seen, last_seen in all_duplicates:
    total_duplicate_records += (count - 1)  # Subtract 1 to keep one copy
    print(f"MLS {listing_key}: {count} copies (first: {first_seen}, last: {last_seen})")

print(f"\nTotal duplicate records to remove: {total_duplicate_records}")

# Ask for confirmation before removing
response = input(f"\nDo you want to remove {total_duplicate_records} duplicate records? (yes/no): ")

if response.lower() == 'yes':
    print("\n=== REMOVING DUPLICATES ===")
    
    removed_count = 0
    for listing_key, count, first_seen, last_seen in all_duplicates:
        if count > 1:
            # Keep the most recent record, remove older ones
            cur.execute("""
                DELETE FROM listings 
                WHERE listing_key = %s 
                AND updated_at < (
                    SELECT MAX(updated_at) 
                    FROM listings 
                    WHERE listing_key = %s
                )
            """, (listing_key, listing_key))
            
            deleted = cur.rowcount
            removed_count += deleted
            print(f"Removed {deleted} duplicate(s) for MLS {listing_key}")
    
    # Commit the changes
    conn.commit()
    print(f"\nTotal duplicates removed: {removed_count}")
    
    # Verify Brandon's listings after cleanup
    print("\n=== BRANDON'S LISTINGS AFTER CLEANUP ===")
    cur.execute("""
        SELECT listing_key, data, updated_at 
        FROM listings 
        WHERE data::text LIKE '%CN222505%' 
        ORDER BY listing_key
    """)
    
    brandon_after = cur.fetchall()
    print(f"Brandon's listings after cleanup: {len(brandon_after)}")
    
    for i, (listing_key, data, updated_at) in enumerate(brandon_after):
        if data and '_raw_data' in data:
            raw_data = data['_raw_data']
            street_name = raw_data.get('STREET_NAME', 'Unknown Street')
            city = raw_data.get('CITY', data.get('City', 'Unknown City'))
            list_price = raw_data.get('LIST_PRICE', data.get('ListPrice', 'Unknown Price'))
            status = raw_data.get('STATUS', 'Unknown Status')
            
            print(f"{i+1}. MLS: {listing_key} - {street_name}, {city} - ${list_price} ({status})")
    
else:
    print("Duplicate removal cancelled.")

cur.close()
conn.close()

print("\n=== SUMMARY ===")
print(f"Initial Brandon listings: {len(brandon_results)}")
print(f"Total duplicates found: {len(all_duplicates)}")
print(f"Total duplicate records: {total_duplicate_records}")