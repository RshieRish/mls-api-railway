#!/usr/bin/env python3
import psycopg2

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== ACTUAL FILTER COUNTS FROM DATABASE ===\n")

# Define the correct MLS status mappings based on our analysis
status_mappings = {
    'For Sale': ['ACT', 'CTG', 'NEW', 'PCG', 'BOM', 'EXT'],
    'For Rent': ['RAC'],
    'Sold': ['SOLD']
}

print("MLS Status Code Mappings:")
for category, codes in status_mappings.items():
    print(f"  {category}: {', '.join(codes)}")
print()

# Get counts for each category using the listingstatus column (most reliable)
print("ACTUAL COUNTS (using listingstatus column):")
for category, mls_codes in status_mappings.items():
    placeholders = ', '.join(['%s'] * len(mls_codes))
    query = f"""
        SELECT COUNT(*) 
        FROM listings 
        WHERE listingstatus IN ({placeholders})
    """
    cur.execute(query, mls_codes)
    count = cur.fetchone()[0]
    print(f"  {category}: {count:,} listings")

print()

# Also show the breakdown by individual status codes
print("BREAKDOWN BY INDIVIDUAL STATUS CODES:")
cur.execute("""
    SELECT listingstatus, COUNT(*) as count
    FROM listings 
    GROUP BY listingstatus
    ORDER BY count DESC
""")

all_statuses = cur.fetchall()
for status, count in all_statuses:
    # Determine which category this status belongs to
    category = 'Unknown'
    for cat, codes in status_mappings.items():
        if status in codes:
            category = cat
            break
    print(f"  {status}: {count:,} listings ({category})")

print()

# Verify total
total_mapped = sum(count for _, count in all_statuses if any(status in codes for codes in status_mappings.values() for status in codes))
total_all = sum(count for _, count in all_statuses)
print(f"Total mapped listings: {total_mapped:,}")
print(f"Total all listings: {total_all:,}")
print(f"Unmapped listings: {total_all - total_mapped:,}")

cur.close()
conn.close()

print("\n=== QUERY COMPLETE ===")