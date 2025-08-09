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

# Check if there are any other listings that might be Brandon's but not showing up
print("\n=== CHECKING FOR OTHER POTENTIAL BRANDON LISTINGS ===")

# Search for listings with similar patterns that might be Brandon's
cur.execute("""
    SELECT listing_key, data->>'_raw_data'->>'LIST_AGENT' as agent, 
           data->>'_raw_data'->>'STREET_NAME' as street,
           data->>'_raw_data'->>'LIST_PRICE' as price
    FROM listings 
    WHERE data->>'_raw_data'->>'STREET_NAME' ILIKE '%chipmunk%'
       OR data->>'_raw_data'->>'STREET_NAME' ILIKE '%wood%'
       OR data->>'_raw_data'->>'LIST_AGENT' = 'CN222505'
    ORDER BY listing_key
""")

potential_brandon = cur.fetchall()
print(f"Potential Brandon listings (by street name or agent): {len(potential_brandon)}")

for listing_key, agent, street, price in potential_brandon:
    print(f"MLS: {listing_key}, Agent: {agent}, Street: {street}, Price: ${price}")

# Check the automation logs to see what might be causing duplicates
print("\n=== CHECKING RECENT AUTOMATION ACTIVITY ===")

# Check if there are any automation-related tables or logs
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE '%log%' OR table_name LIKE '%automation%'
""")

log_tables = cur.fetchall()
if log_tables:
    print("Found log tables:")
    for table in log_tables:
        print(f"  - {table[0]}")
else:
    print("No log tables found")

# Check the most recent listings to see when they were added
print("\n=== RECENT LISTING ACTIVITY ===")
cur.execute("""
    SELECT listing_key, updated_at, created_at,
           data->>'_raw_data'->>'LIST_AGENT' as agent,
           data->>'_raw_data'->>'STREET_NAME' as street
    FROM listings 
    ORDER BY updated_at DESC 
    LIMIT 10
""")

recent_listings = cur.fetchall()
print("Most recent 10 listings:")
for listing_key, updated_at, created_at, agent, street in recent_listings:
    print(f"MLS: {listing_key}, Agent: {agent}, Street: {street}, Updated: {updated_at}")

cur.close()
conn.close()

print("\n=== SUMMARY ===")
print(f"Brandon's confirmed listings: {len(brandon_results)}")
print(f"Expected listings: 3")
print(f"Missing listings: {3 - len(brandon_results)}")

if len(brandon_results) < 3:
    print("\n🔍 NEXT STEPS TO INVESTIGATE:")
    print("1. Check if Brandon's other listings are in different property types (CC, RN, BU, LD, CI, MH)")
    print("2. Verify Brandon's agent ID is correct (CN222505)")
    print("3. Check if listings are in different status (SOLD, PENDING, etc.)")
    print("4. Run the automation manually to see if it picks up missing listings")