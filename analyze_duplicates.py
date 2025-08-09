#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def analyze_potential_duplicates():
    """Analyze potential duplicates in the database using different criteria"""
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment variables")
        return False
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=" * 80)
        print("ANALYZING POTENTIAL DUPLICATES")
        print("=" * 80)
        
        # Check total count
        cur.execute("SELECT COUNT(*) as count FROM listings")
        total_count = cur.fetchone()['count']
        print(f"Total listings: {total_count}")
        
        # Check duplicates by listing_key
        print("\n1. Checking duplicates by listing_key:")
        cur.execute("""
            SELECT listing_key, COUNT(*) as count 
            FROM listings 
            GROUP BY listing_key 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        listing_key_dups = cur.fetchall()
        if listing_key_dups:
            print(f"   Found {len(listing_key_dups)} listing_key duplicates:")
            for dup in listing_key_dups:
                print(f"     {dup['listing_key']}: {dup['count']} copies")
        else:
            print("   No duplicates found by listing_key")
        
        # Check duplicates by listingid
        print("\n2. Checking duplicates by listingid:")
        cur.execute("""
            SELECT listingid, COUNT(*) as count 
            FROM listings 
            GROUP BY listingid 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        listingid_dups = cur.fetchall()
        if listingid_dups:
            print(f"   Found {len(listingid_dups)} listingid duplicates:")
            for dup in listingid_dups:
                print(f"     {dup['listingid']}: {dup['count']} copies")
        else:
            print("   No duplicates found by listingid")
        
        # Check duplicates by listingkey
        print("\n3. Checking duplicates by listingkey:")
        cur.execute("""
            SELECT listingkey, COUNT(*) as count 
            FROM listings 
            GROUP BY listingkey 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        listingkey_dups = cur.fetchall()
        if listingkey_dups:
            print(f"   Found {len(listingkey_dups)} listingkey duplicates:")
            for dup in listingkey_dups:
                print(f"     {dup['listingkey']}: {dup['count']} copies")
        else:
            print("   No duplicates found by listingkey")
        
        # Check for potential duplicates by address + price
        print("\n4. Checking duplicates by address + price:")
        cur.execute("""
            SELECT streetname, city, listprice, COUNT(*) as count 
            FROM listings 
            WHERE streetname IS NOT NULL AND city IS NOT NULL
            GROUP BY streetname, city, listprice 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 10
        """)
        address_dups = cur.fetchall()
        if address_dups:
            print(f"   Found {len(address_dups)} address+price duplicates:")
            for dup in address_dups:
                print(f"     {dup['streetname']}, {dup['city']} @ ${dup['listprice']}: {dup['count']} copies")
        else:
            print("   No duplicates found by address+price")
        
        # Check for exact duplicate rows (excluding id and updated_at)
        print("\n5. Checking for exact duplicate rows (excluding id, updated_at):")
        cur.execute("""
            SELECT 
                listing_key, listingid, listingkey, listprice, streetname, city, 
                stateorprovince, postalcode, bedroomstotal, bathroomstotalinteger, 
                livingarea, latitude, longitude, listingstatus, modificationtimestamp,
                COUNT(*) as count
            FROM listings 
            GROUP BY 
                listing_key, listingid, listingkey, listprice, streetname, city, 
                stateorprovince, postalcode, bedroomstotal, bathroomstotalinteger, 
                livingarea, latitude, longitude, listingstatus, modificationtimestamp
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 5
        """)
        exact_dups = cur.fetchall()
        if exact_dups:
            print(f"   Found {len(exact_dups)} exact duplicate rows:")
            for dup in exact_dups:
                print(f"     Listing {dup['listing_key']} ({dup['city']}): {dup['count']} copies")
        else:
            print("   No exact duplicate rows found")
        
        # Show some sample data to understand the structure
        print("\n6. Sample of first 5 records:")
        cur.execute("""
            SELECT id, listing_key, listingid, listingkey, city, listprice, updated_at
            FROM listings 
            ORDER BY id 
            LIMIT 5
        """)
        samples = cur.fetchall()
        print(f"   {'ID':<5} {'Listing Key':<15} {'Listing ID':<12} {'City':<15} {'Price':<12} {'Updated':<20}")
        print("   " + "-" * 85)
        for sample in samples:
            updated_str = str(sample['updated_at'])[:19] if sample['updated_at'] else 'None'
            print(f"   {sample['id']:<5} {str(sample['listing_key']):<15} {str(sample['listingid']):<12} {str(sample['city']):<15} {str(sample['listprice']):<12} {updated_str:<20}")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error analyzing duplicates: {e}")
        if 'conn' in locals():
            conn.close()
        return False

if __name__ == "__main__":
    success = analyze_potential_duplicates()
    if not success:
        print("\nAnalysis failed!")