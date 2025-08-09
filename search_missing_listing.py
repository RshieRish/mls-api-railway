#!/usr/bin/env python3

import os
import psycopg2
import json
from urllib.parse import urlparse

def main():
    # Get Railway database URL from environment
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ DATABASE_URL environment variable not found")
        return
    
    try:
        # Connect to Railway database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        print("🔍 Searching for missing listing MLS 73411408...")
        
        # Search for MLS 73411408 specifically
        cursor.execute("""
            SELECT listing_key, listingid, listprice, listingstatus, 
                   data::text LIKE '%73411408%' as contains_mls,
                   data::text LIKE '%313 Humphrey%' as contains_address,
                   data::text LIKE '%Humphrey%' as contains_street
            FROM listings 
            WHERE data::text LIKE '%73411408%' 
               OR data::text LIKE '%313 Humphrey%'
               OR data::text LIKE '%Humphrey Street%'
            LIMIT 10;
        """)
        
        results = cursor.fetchall()
        
        if results:
            print(f"\n✅ Found {len(results)} potential matches:")
            for row in results:
                listing_key, listingid, listprice, status, has_mls, has_address, has_street = row
                print(f"  - Key: {listing_key}, ID: {listingid}, Price: ${listprice}, Status: {status}")
                print(f"    Contains MLS: {has_mls}, Address: {has_address}, Street: {has_street}")
        else:
            print("❌ No matches found for MLS 73411408 or Humphrey Street")
        
        # Also search for Brandon's agent ID in recent listings
        print("\n🔍 Searching for recent Brandon listings...")
        cursor.execute("""
            SELECT listing_key, listingid, listprice, listingstatus, updated_at
            FROM listings 
            WHERE data::text LIKE '%CN222505%'
            ORDER BY updated_at DESC
            LIMIT 5;
        """)
        
        brandon_results = cursor.fetchall()
        print(f"\n📋 Brandon's {len(brandon_results)} most recent listings:")
        for row in brandon_results:
            listing_key, listingid, listprice, status, created_at = row
            print(f"  - Key: {listing_key}, ID: {listingid}, Price: ${listprice}, Status: {status}, Created: {created_at}")
        
        # Check if there are any listings with MLS starting with 734114
        print("\n🔍 Searching for similar MLS numbers (734114xx)...")
        cursor.execute("""
            SELECT listing_key, listingid, listprice, listingstatus
            FROM listings 
            WHERE data::text LIKE '%734114%'
            LIMIT 10;
        """)
        
        similar_results = cursor.fetchall()
        if similar_results:
            print(f"\n📋 Found {len(similar_results)} listings with similar MLS numbers:")
            for row in similar_results:
                listing_key, listingid, listprice, status = row
                print(f"  - Key: {listing_key}, ID: {listingid}, Price: ${listprice}, Status: {status}")
        
        cursor.close()
        conn.close()
        
        print("\n=== CONCLUSION ===")
        if not results:
            print("🚨 MLS 73411408 (313 Humphrey Street) is NOT in the database")
            print("📝 This confirms the automation is missing this listing")
            print("💡 Possible reasons:")
            print("   1. Listing is in a property type not being downloaded")
            print("   2. Listing was added after last automation run")
            print("   3. Automation encountered an error for this specific listing")
        else:
            print("✅ Found the listing - investigating why it's not showing up in Brandon search")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()