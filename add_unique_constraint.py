import psycopg2

# Connect to Railway database
conn = psycopg2.connect('postgresql://postgres:yNcutIEgiapitqBwXksznJqdBjYAkoSV@metro.proxy.rlwy.net:51796/railway')
cur = conn.cursor()

print("=== ADDING UNIQUE CONSTRAINT TO PREVENT DUPLICATES ===")

try:
    # First, let's check if there are any remaining duplicates
    print("\n1. Checking for existing duplicates...")
    cur.execute("""
        SELECT listing_key, COUNT(*) as count
        FROM listings 
        GROUP BY listing_key
        HAVING COUNT(*) > 1
        LIMIT 5
    """)
    
    duplicates = cur.fetchall()
    if duplicates:
        print(f"  ⚠️  Found {len(duplicates)} duplicate listing_keys:")
        for listing_key, count in duplicates:
            print(f"    {listing_key}: {count} occurrences")
        print("  Cannot add unique constraint with existing duplicates!")
    else:
        print("  ✅ No duplicates found, safe to add unique constraint")
        
        # Add unique constraint on listing_key
        print("\n2. Adding unique constraint on listing_key...")
        cur.execute("""
            ALTER TABLE listings 
            ADD CONSTRAINT listings_listing_key_unique 
            UNIQUE (listing_key)
        """)
        
        conn.commit()
        print("  ✅ Unique constraint added successfully!")
        
        # Verify the constraint was added
        print("\n3. Verifying constraint...")
        cur.execute("""
            SELECT conname, pg_get_constraintdef(oid) as definition
            FROM pg_constraint 
            WHERE conrelid = 'listings'::regclass
            AND conname = 'listings_listing_key_unique'
        """)
        
        constraint = cur.fetchone()
        if constraint:
            print(f"  ✅ Constraint verified: {constraint[0]} - {constraint[1]}")
        else:
            print("  ❌ Constraint not found")
            
except psycopg2.Error as e:
    print(f"  ❌ Error: {e}")
    conn.rollback()

finally:
    cur.close()
    conn.close()

print("\n=== CONSTRAINT ADDITION COMPLETE ===")
print("\nThis will prevent future duplicate insertions.")
print("The automation will now get 'duplicate key value violates unique constraint' errors")
print("instead of creating duplicates, which is the correct behavior.")