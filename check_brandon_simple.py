import psycopg2
import json
from datetime import datetime

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== FINAL CHECK OF BRANDON'S LISTINGS ===")

# Get all Brandon's listings with full details
cur.execute("""
    SELECT listing_key, data, updated_at 
    FROM listings 
    WHERE data::text LIKE '%CN222505%' 
    ORDER BY listing_key
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

# Check for listings with Chipmunk or Wood in the name
print("\n=== CHECKING FOR CHIPMUNK/WOOD STREET LISTINGS ===")
cur.execute("""
    SELECT listing_key, data 
    FROM listings 
    WHERE data::text ILIKE '%chipmunk%' OR data::text ILIKE '%wood%'
    ORDER BY listing_key
""")

street_listings = cur.fetchall()
print(f"Listings with Chipmunk/Wood in data: {len(street_listings)}")

for listing_key, data in street_listings:
    if data and '_raw_data' in data:
        raw_data = data['_raw_data']
        street_name = raw_data.get('STREET_NAME', 'Unknown Street')
        agent_id = raw_data.get('LIST_AGENT', 'Unknown Agent')
        list_price = raw_data.get('LIST_PRICE', data.get('ListPrice', 'Unknown Price'))
        print(f"MLS: {listing_key}, Agent: {agent_id}, Street: {street_name}, Price: ${list_price}")

# Check total listings count
cur.execute("SELECT COUNT(*) FROM listings")
total_listings = cur.fetchone()[0]
print(f"\nTotal listings in database: {total_listings}")

# Check if there are any duplicates remaining
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
print(f"Remaining duplicates: {remaining_duplicates}")

cur.close()
conn.close()

print("\n=== SUMMARY ===")
print(f"Brandon's confirmed listings: {len(brandon_results)}")
print(f"Expected listings: 3")
print(f"Missing listings: {3 - len(brandon_results)}")
print(f"Total database listings: {total_listings}")
print(f"Remaining duplicates: {remaining_duplicates}")

if len(brandon_results) < 3:
    print("\n🔍 INVESTIGATION NEEDED:")
    print("1. Brandon should have 3 distinct listings")
    print("2. Only found", len(brandon_results), "listings")
    print("3. Need to check if automation is missing some property types")
    print("4. May need to run automation manually to refresh data")
else:
    print("\n✅ SUCCESS: Brandon has the expected number of listings")