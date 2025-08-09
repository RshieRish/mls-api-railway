#!/usr/bin/env python3

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_timestamp_duplicates():
    """Check for duplicates based on insertion timestamps and data patterns"""
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment variables")
        return False
    
    try:
        # Connect to database
        conn = psycopg2.connect(database_url)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=" * 80)
        print("CHECKING TIMESTAMP-BASED DUPLICATES")
        print("=" * 80)
        
        # Check insertion timestamps
        print("1. Analyzing insertion timestamps:")
        cur.execute("""
            SELECT 
                DATE_TRUNC('minute', updated_at) as minute_group,
                COUNT(*) as count
            FROM listings 
            GROUP BY DATE_TRUNC('minute', updated_at)
            ORDER BY count DESC
            LIMIT 10
        """)
        timestamp_groups = cur.fetchall()
        
        print("   Top insertion time groups:")
        for group in timestamp_groups:
            print(f"     {group['minute_group']}: {group['count']} records")
        
        # Check if there are two distinct insertion times (indicating double loading)
        print("\n2. Checking distinct insertion timestamps:")
        cur.execute("""
            SELECT 
                DATE_TRUNC('second', updated_at) as timestamp_group,
                COUNT(*) as count
            FROM listings 
            GROUP BY DATE_TRUNC('second', updated_at)
            ORDER BY timestamp_group
        """)
        distinct_timestamps = cur.fetchall()
        
        print(f"   Found {len(distinct_timestamps)} distinct insertion timestamp groups:")
        for ts in distinct_timestamps:
            print(f"     {ts['timestamp_group']}: {ts['count']} records")
        
        # Check if we have sample data mixed with real data
        print("\n3. Checking for sample vs real data:")
        cur.execute("""
            SELECT 
                CASE 
                    WHEN listing_key LIKE 'TEST%' THEN 'Sample Data'
                    WHEN listing_key LIKE 'MA%' THEN 'Real Data'
                    ELSE 'Other'
                END as data_type,
                COUNT(*) as count
            FROM listings 
            GROUP BY 
                CASE 
                    WHEN listing_key LIKE 'TEST%' THEN 'Sample Data'
                    WHEN listing_key LIKE 'MA%' THEN 'Real Data'
                    ELSE 'Other'
                END
            ORDER BY count DESC
        """)
        data_types = cur.fetchall()
        
        for dtype in data_types:
            print(f"     {dtype['data_type']}: {dtype['count']} records")
        
        # Check for potential duplicates in real data only
        print("\n4. Checking duplicates in real data only (excluding TEST records):")
        cur.execute("""
            SELECT COUNT(*) as total_real_data
            FROM listings 
            WHERE listing_key NOT LIKE 'TEST%'
        """)
        real_data_count = cur.fetchone()['total_real_data']
        print(f"   Total real data records: {real_data_count}")
        
        # Check if real data has any patterns suggesting duplication
        cur.execute("""
            SELECT 
                city, 
                stateorprovince,
                COUNT(*) as count
            FROM listings 
            WHERE listing_key NOT LIKE 'TEST%'
            GROUP BY city, stateorprovince
            ORDER BY count DESC
            LIMIT 10
        """)
        city_counts = cur.fetchall()
        
        print("   Top cities in real data:")
        for city in city_counts:
            print(f"     {city['city']}, {city['stateorprovince']}: {city['count']} listings")
        
        # Check the data field for patterns
        print("\n5. Checking data field patterns:")
        cur.execute("""
            SELECT 
                jsonb_object_keys(data) as key,
                COUNT(*) as count
            FROM listings 
            WHERE listing_key NOT LIKE 'TEST%'
            GROUP BY jsonb_object_keys(data)
            ORDER BY count DESC
            LIMIT 15
        """)
        data_keys = cur.fetchall()
        
        print("   Most common data keys in real listings:")
        for key in data_keys:
            print(f"     {key['key']}: appears in {key['count']} records")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error checking timestamp duplicates: {e}")
        if 'conn' in locals():
            conn.close()
        return False

if __name__ == "__main__":
    success = check_timestamp_duplicates()
    if not success:
        print("\nTimestamp analysis failed!")