import psycopg2
import json
from collections import defaultdict

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== CHECKING BRANDON'S LISTINGS (CN222505) ===")

# Get all Brandon's listings with full details
cur.execute("""
    SELECT listing_key, data, updated_at 
    FROM listings 
    WHERE data::text LIKE '%CN222505%' 
    ORDER BY updated_at DESC
""")

brandon_results = cur.fetchall()
print(f"Total Brandon listings found: {len(brandon_results)}")
print("="*80)

# Track duplicates by MLS number
mls_tracker = defaultdict(list)

for i, (listing_key, data, updated_at) in enumerate(brandon_results):
    if data:
        mls_number = data.get('MLS_NUMBER', 'Unknown MLS')
        address = data.get('STREET_NAME', 'Unknown Address')
        street_num = data.get('STREET_NUMBER', '')
        city = data.get('CITY', 'Unknown City')
        state = data.get('STATE_PROVINCE', 'Unknown State')
        zip_code = data.get('POSTAL_CODE', 'Unknown Zip')
        agent_id = data.get('LIST_AGENT', 'Unknown Agent')
        list_price = data.get('LIST_PRICE', 'Unknown Price')
        status = data.get('STATUS', 'Unknown Status')
        
        full_address = f"{street_num} {address}, {city}, {state} {zip_code}".strip()
        
        print(f"{i+1}. MLS: {mls_number}")
        print(f"   Address: {full_address}")
        print(f"   Agent: {agent_id}")
        print(f"   Price: ${list_price}")
        print(f"   Status: {status}")
        print(f"   Listing Key: {listing_key}")
        print(f"   Updated: {updated_at}")
        print()
        
        # Track by MLS number for duplicate detection
        mls_tracker[mls_number].append({
            'listing_key': listing_key,
            'address': full_address,
            'updated_at': updated_at
        })

print("\n=== DUPLICATE ANALYSIS ===")
duplicates_found = False
for mls_number, listings in mls_tracker.items():
    if len(listings) > 1:
        duplicates_found = True
        print(f"\nDUPLICATE MLS {mls_number}: {len(listings)} entries")
        for listing in listings:
            print(f"  - Key: {listing['listing_key']}, Updated: {listing['updated_at']}")
            print(f"    Address: {listing['address']}")

if not duplicates_found:
    print("No duplicates found in Brandon's listings.")

print("\n=== CHECKING ALL DUPLICATES IN DATABASE ===")

# Check for duplicates across all listings by MLS number
cur.execute("""
    SELECT data->>'MLS_NUMBER' as mls_number, COUNT(*) as count
    FROM listings 
    WHERE data->>'MLS_NUMBER' IS NOT NULL
    GROUP BY data->>'MLS_NUMBER'
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 10
""")

all_duplicates = cur.fetchall()
print(f"Total MLS numbers with duplicates: {len(all_duplicates)}")

for mls_number, count in all_duplicates:
    print(f"MLS {mls_number}: {count} duplicates")
    
    # Get details for this MLS
    cur.execute("""
        SELECT listing_key, data->>'STREET_NAME' as address, 
               data->>'LIST_AGENT' as agent, updated_at
        FROM listings 
        WHERE data->>'MLS_NUMBER' = %s
        ORDER BY updated_at DESC
    """, (mls_number,))
    
    duplicate_details = cur.fetchall()
    for listing_key, address, agent, updated_at in duplicate_details:
        print(f"  - Key: {listing_key}, Agent: {agent}, Address: {address}, Updated: {updated_at}")
    print()

cur.close()
conn.close()

print("\n=== SUMMARY ===")
print(f"Brandon's total listings: {len(brandon_results)}")
print(f"Unique MLS numbers for Brandon: {len(mls_tracker)}")
print(f"Database-wide duplicates found: {len(all_duplicates)}")