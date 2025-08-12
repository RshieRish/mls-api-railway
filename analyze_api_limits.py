#!/usr/bin/env python3
"""
Analyze the API limit issue and sold listings filtering.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    """Get database connection"""
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        cursor_factory=RealDictCursor
    )

def main():
    print("=== API LIMITS AND SOLD LISTINGS ANALYSIS ===\n")
    
    with get_conn() as conn, conn.cursor() as cur:
        # 1. Total listings in database
        cur.execute("SELECT COUNT(*) as total FROM listings")
        total_db = cur.fetchone()['total']
        print(f"Total listings in database: {total_db:,}")
        
        # 2. Check what the API query returns (first 25,000 by updated_at DESC)
        print("\n=== API Query Analysis (first 25,000 by updated_at DESC) ===")
        cur.execute("""
            SELECT 
                listingstatus,
                data->>'ListingStatus' as json_status,
                COUNT(*) as count
            FROM (
                SELECT listing_key, data, updated_at, listingstatus
                FROM listings
                ORDER BY 
                    CASE WHEN data->>'LIST_AGENT_ID' = 'CN222505' THEN 0 ELSE 1 END,
                    updated_at DESC
                LIMIT 25000
            ) api_results
            GROUP BY listingstatus, data->>'ListingStatus'
            ORDER BY count DESC
        """)
        
        api_results = cur.fetchall()
        api_total = sum(row['count'] for row in api_results)
        print(f"Total in API results (first 25,000): {api_total:,}")
        print("Status distribution in API results:")
        for row in api_results:
            status = row['listingstatus'] or 'NULL'
            json_status = row['json_status'] or 'NULL'
            count = row['count']
            print(f"  {status:10} / {json_status:10} : {count:8,}")
        
        # 3. Check what's in the remaining listings (beyond 25,000)
        print("\n=== Remaining Listings Analysis (beyond first 25,000) ===")
        cur.execute("""
            SELECT 
                listingstatus,
                data->>'ListingStatus' as json_status,
                COUNT(*) as count
            FROM (
                SELECT listing_key, data, updated_at, listingstatus
                FROM listings
                ORDER BY 
                    CASE WHEN data->>'LIST_AGENT_ID' = 'CN222505' THEN 0 ELSE 1 END,
                    updated_at DESC
                OFFSET 25000
            ) remaining_results
            GROUP BY listingstatus, data->>'ListingStatus'
            ORDER BY count DESC
        """)
        
        remaining_results = cur.fetchall()
        remaining_total = sum(row['count'] for row in remaining_results)
        print(f"Total in remaining listings: {remaining_total:,}")
        
        if remaining_results:
            print("Status distribution in remaining listings:")
            for row in remaining_results:
                status = row['listingstatus'] or 'NULL'
                json_status = row['json_status'] or 'NULL'
                count = row['count']
                print(f"  {status:10} / {json_status:10} : {count:8,}")
        else:
            print("No remaining listings found")
        
        # 4. Check for any SOLD listings specifically
        print("\n=== SOLD Listings Check ===")
        cur.execute("""
            SELECT COUNT(*) as sold_count
            FROM listings 
            WHERE 
                listingstatus = 'SOLD' OR
                listingstatus = 'SLD' OR
                data->>'ListingStatus' = 'SOLD' OR
                data->>'ListingStatus' = 'SLD'
        """)
        
        sold_count = cur.fetchone()['sold_count']
        print(f"Total SOLD/SLD listings in database: {sold_count:,}")
        
        # 5. Check if SOLD listings are in the API results or remaining
        if sold_count > 0:
            print("\n=== SOLD Listings Position Analysis ===")
            cur.execute("""
                WITH ranked_listings AS (
                    SELECT 
                        listing_key,
                        listingstatus,
                        data->>'ListingStatus' as json_status,
                        ROW_NUMBER() OVER (
                            ORDER BY 
                                CASE WHEN data->>'LIST_AGENT_ID' = 'CN222505' THEN 0 ELSE 1 END,
                                updated_at DESC
                        ) as rank
                    FROM listings
                    WHERE 
                        listingstatus = 'SOLD' OR
                        listingstatus = 'SLD' OR
                        data->>'ListingStatus' = 'SOLD' OR
                        data->>'ListingStatus' = 'SLD'
                )
                SELECT 
                    CASE 
                        WHEN rank <= 25000 THEN 'In API Results'
                        ELSE 'Beyond API Limit'
                    END as position,
                    COUNT(*) as count
                FROM ranked_listings
                GROUP BY 
                    CASE 
                        WHEN rank <= 25000 THEN 'In API Results'
                        ELSE 'Beyond API Limit'
                    END
            """)
            
            position_results = cur.fetchall()
            for row in position_results:
                print(f"  {row['position']}: {row['count']:,} SOLD listings")
        
        # 6. Summary
        print("\n=== SUMMARY ===")
        print(f"• Database contains {total_db:,} total listings")
        print(f"• API returns first {api_total:,} listings (25,000 limit)")
        print(f"• {remaining_total:,} listings are beyond the API limit")
        print(f"• {sold_count:,} SOLD listings exist in database")
        
        if sold_count == 0:
            print("\n🔍 ROOT CAUSE: No SOLD listings exist in the database.")
            print("   This means the MLS IDX files don't contain sold listings,")
            print("   or the automated downloader isn't processing them.")
        elif remaining_total > 0:
            print("\n⚠️  POTENTIAL ISSUE: Some listings may be excluded by the 25,000 API limit.")
            print("   Consider increasing the limit or implementing pagination.")

if __name__ == "__main__":
    main()