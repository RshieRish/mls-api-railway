#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def deduplicate_database():
    """Remove duplicate listings from the database"""
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment variables")
        return False
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # First, check current count
        cur.execute("SELECT COUNT(*) as count FROM listings")
        initial_count = cur.fetchone()['count']
        print(f"Initial listings count: {initial_count}")
        
        # Find duplicates based on listing_key (the unique identifier)
        cur.execute("""
            SELECT listing_key, COUNT(*) as duplicate_count 
            FROM listings 
            GROUP BY listing_key 
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        """)
        
        duplicates = cur.fetchall()
        print(f"Found {len(duplicates)} MLS numbers with duplicates")
        
        if duplicates:
            total_duplicates = sum(dup['duplicate_count'] - 1 for dup in duplicates)
            print(f"Total duplicate records to remove: {total_duplicates}")
            
            # Show some examples
            print("\nTop 5 duplicates:")
            for i, dup in enumerate(duplicates[:5]):
                print(f"  Listing {dup['listing_key']}: {dup['duplicate_count']} copies")
        
        # Remove duplicates, keeping only the most recent entry (highest id)
        print("\nRemoving duplicates...")
        cur.execute("""
            DELETE FROM listings 
            WHERE id NOT IN (
                SELECT MAX(id) 
                FROM listings 
                GROUP BY listing_key
            )
        """)
        
        deleted_count = cur.rowcount
        print(f"Deleted {deleted_count} duplicate records")
        
        # Check final count
        cur.execute("SELECT COUNT(*) as count FROM listings")
        final_count = cur.fetchone()['count']
        print(f"Final listings count: {final_count}")
        
        # Commit changes
        conn.commit()
        print("\nDeduplication completed successfully!")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error during deduplication: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False

if __name__ == "__main__":
    print("Starting database deduplication...")
    success = deduplicate_database()
    if success:
        print("\nDatabase deduplication completed successfully!")
    else:
        print("\nDatabase deduplication failed!")