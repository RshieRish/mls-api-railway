import psycopg2

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== DATABASE SCHEMA ANALYSIS ===")

# Check table structure
print("\n1. LISTINGS TABLE STRUCTURE:")
cur.execute("""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = 'listings' 
    ORDER BY ordinal_position
""")

columns = cur.fetchall()
for column_name, data_type, is_nullable, column_default in columns:
    print(f"  {column_name}: {data_type} (nullable: {is_nullable}, default: {column_default})")

# Check constraints and indexes
print("\n2. CONSTRAINTS AND INDEXES:")
cur.execute("""
    SELECT conname, contype, pg_get_constraintdef(oid) as definition
    FROM pg_constraint 
    WHERE conrelid = 'listings'::regclass
""")

constraints = cur.fetchall()
if constraints:
    for conname, contype, definition in constraints:
        constraint_type = {
            'p': 'PRIMARY KEY',
            'u': 'UNIQUE',
            'f': 'FOREIGN KEY',
            'c': 'CHECK'
        }.get(contype, contype)
        print(f"  {conname}: {constraint_type} - {definition}")
else:
    print("  No constraints found")

# Check indexes
print("\n3. INDEXES:")
cur.execute("""
    SELECT indexname, indexdef
    FROM pg_indexes 
    WHERE tablename = 'listings'
""")

indexes = cur.fetchall()
for indexname, indexdef in indexes:
    print(f"  {indexname}: {indexdef}")

# Check if listing_key has unique constraint
print("\n4. LISTING_KEY UNIQUENESS CHECK:")
cur.execute("""
    SELECT listing_key, COUNT(*) as count
    FROM listings 
    GROUP BY listing_key
    HAVING COUNT(*) > 1
    LIMIT 5
""")

duplicates = cur.fetchall()
if duplicates:
    print("  Found duplicate listing_keys:")
    for listing_key, count in duplicates:
        print(f"    {listing_key}: {count} occurrences")
else:
    print("  ✅ No duplicate listing_keys found")

# Check total counts
print("\n5. DATABASE STATISTICS:")
cur.execute("SELECT COUNT(*) FROM listings")
total_listings = cur.fetchone()[0]
print(f"  Total listings: {total_listings}")

cur.execute("SELECT COUNT(DISTINCT listing_key) FROM listings")
unique_keys = cur.fetchone()[0]
print(f"  Unique listing keys: {unique_keys}")

if total_listings != unique_keys:
    print(f"  ⚠️  WARNING: {total_listings - unique_keys} duplicate records exist!")
else:
    print(f"  ✅ All listing keys are unique")

# Check Brandon's listings across different property types
print("\n6. BRANDON'S LISTINGS BY PROPERTY TYPE:")
cur.execute("""
    SELECT 
        data->>'_raw_data'->>'PROPERTY_TYPE' as prop_type,
        COUNT(*) as count,
        array_agg(listing_key) as listing_keys
    FROM listings 
    WHERE data::text LIKE '%CN222505%'
    GROUP BY data->>'_raw_data'->>'PROPERTY_TYPE'
""")

brandon_by_type = cur.fetchall()
if brandon_by_type:
    for prop_type, count, listing_keys in brandon_by_type:
        print(f"  {prop_type or 'Unknown'}: {count} listings - {listing_keys}")
else:
    print("  No property type data found for Brandon")

cur.close()
conn.close()

print("\n=== ANALYSIS COMPLETE ===")