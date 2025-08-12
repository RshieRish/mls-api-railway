import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')

# Get database URL from environment
DB_DSN = os.getenv('DATABASE_URL')

if not DB_DSN:
    print("ERROR: DATABASE_URL not found in environment variables")
    exit(1)

def get_conn():
    """Get database connection"""
    return psycopg2.connect(DB_DSN, cursor_factory=RealDictCursor)

def check_listing_status_distribution():
    """Check the distribution of listing statuses in the database"""
    with get_conn() as conn, conn.cursor() as cur:
        # Check raw status distribution
        print("=== Raw ListingStatus Distribution ===")
        cur.execute("""
            SELECT 
                data->>'ListingStatus' as status,
                COUNT(*) as count
            FROM listings 
            GROUP BY data->>'ListingStatus'
            ORDER BY count DESC
        """)
        
        results = cur.fetchall()
        total = sum(row['count'] for row in results)
        
        for row in results:
            status = row['status'] or 'NULL'
            count = row['count']
            percentage = (count / total) * 100
            print(f"{status:15} {count:8,} ({percentage:5.1f}%)")
        
        print(f"\nTotal listings: {total:,}")
        
        # Check for SOLD specifically
        print("\n=== Checking for SOLD listings ===")
        cur.execute("""
            SELECT COUNT(*) as sold_count
            FROM listings 
            WHERE data->>'ListingStatus' = 'SOLD'
        """)
        
        sold_count = cur.fetchone()['sold_count']
        print(f"SOLD listings: {sold_count:,}")
        
        # Check for any status containing 'SLD' or 'SOLD'
        cur.execute("""
            SELECT 
                data->>'ListingStatus' as status,
                COUNT(*) as count
            FROM listings 
            WHERE UPPER(data->>'ListingStatus') LIKE '%SLD%' 
               OR UPPER(data->>'ListingStatus') LIKE '%SOLD%'
            GROUP BY data->>'ListingStatus'
            ORDER BY count DESC
        """)
        
        sold_like = cur.fetchall()
        if sold_like:
            print("\n=== Statuses containing 'SLD' or 'SOLD' ===")
            for row in sold_like:
                print(f"{row['status']:15} {row['count']:8,}")
        else:
            print("\nNo statuses containing 'SLD' or 'SOLD' found")
        
        # Sample some listings to see the data structure
        print("\n=== Sample Listings ===")
        cur.execute("""
            SELECT 
                listing_key,
                data->>'ListingStatus' as status,
                data->>'ListPrice' as price,
                data->>'StreetName' as street,
                data->>'City' as city
            FROM listings 
            ORDER BY updated_at DESC
            LIMIT 5
        """)
        
        samples = cur.fetchall()
        for sample in samples:
            print(f"Key: {sample['listing_key']}, Status: {sample['status']}, Price: {sample['price']}, Address: {sample['street']}, {sample['city']}")

if __name__ == "__main__":
    try:
        check_listing_status_distribution()
    except Exception as e:
        print(f"Error: {e}")