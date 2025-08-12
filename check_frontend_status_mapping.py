#!/usr/bin/env python3
import psycopg2

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== FRONTEND STATUS MAPPING ANALYSIS ===\n")

# MLS Status codes and their meanings:
# ACT = Active (For Sale)
# CTG = Contingent (For Sale but under contract)
# NEW = New (For Sale)
# PCG = Pending (For Sale but pending)
# BOM = Back on Market (For Sale)
# EXT = Extended (For Sale)
# RAC = Rental Active (For Rent)
# SOLD = Sold

# Map MLS codes to frontend categories
status_mapping = {
    'sale': ['ACT', 'CTG', 'NEW', 'PCG', 'BOM', 'EXT'],  # For Sale
    'rent': ['RAC'],  # For Rent
    'sold': ['SOLD']  # Sold
}

print("Status code meanings:")
print("  ACT = Active (For Sale)")
print("  CTG = Contingent (For Sale but under contract)")
print("  NEW = New (For Sale)")
print("  PCG = Pending (For Sale but pending)")
print("  BOM = Back on Market (For Sale)")
print("  EXT = Extended (For Sale)")
print("  RAC = Rental Active (For Rent)")
print("  SOLD = Sold")
print()

# Get counts for each frontend category using the table column
print("FRONTEND FILTER COUNTS (using listingstatus column):")
for frontend_status, mls_codes in status_mapping.items():
    codes_str = "', '".join(mls_codes)
    cur.execute(f"""
        SELECT COUNT(*) 
        FROM listings 
        WHERE listingstatus IN ('{codes_str}')
    """)
    count = cur.fetchone()[0]
    print(f"  {frontend_status.upper()}: {count} listings")

print()

# Also check using the JSONB ListingStatus field
print("FRONTEND FILTER COUNTS (using data->>'ListingStatus' field):")
for frontend_status, mls_codes in status_mapping.items():
    codes_str = "', '".join(mls_codes)
    cur.execute(f"""
        SELECT COUNT(*) 
        FROM listings 
        WHERE data->>'ListingStatus' IN ('{codes_str}')
    """)
    count = cur.fetchone()[0]
    print(f"  {frontend_status.upper()}: {count} listings")

print()

# Check what the API is actually returning for status field
print("SAMPLE LISTINGS WITH STATUS MAPPING:")
cur.execute("""
    SELECT listingstatus, data->>'ListingStatus' as json_status, COUNT(*) as count
    FROM listings 
    GROUP BY listingstatus, data->>'ListingStatus'
    ORDER BY count DESC
    LIMIT 10
""")

results = cur.fetchall()
for table_status, json_status, count in results:
    print(f"  Table: {table_status}, JSON: {json_status}, Count: {count}")

cur.close()
conn.close()

print("\n=== ANALYSIS COMPLETE ===")
print("\nRECOMMENDATION:")
print("The frontend should map MLS status codes as follows:")
print("- For Sale: ACT, CTG, NEW, PCG, BOM, EXT")
print("- For Rent: RAC")
print("- Sold: SOLD")
print("\nThe API needs to transform the raw MLS status codes to these frontend categories.")