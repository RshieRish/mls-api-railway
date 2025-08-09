#!/usr/bin/env python3
"""
Script to add Brandon's sample listings to the database for testing prioritization
"""

import os
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
import uuid
from datetime import datetime

# Load environment variables
load_dotenv('.env')

def get_conn():
    return psycopg2.connect(
        os.getenv('DATABASE_URL'),
        cursor_factory=psycopg2.extras.RealDictCursor
    )

# Brandon's sample listings
BRANDON_LISTINGS = [
    {
        "ListingID": "BRANDON001",
        "ListingKey": "brandon-beacon-st",
        "ListPrice": "7250000",
        "StreetName": "Beacon Street",
        "City": "Boston",
        "StateOrProvince": "MA",
        "PostalCode": "02116",
        "BedroomsTotal": "4",
        "BathroomsTotalInteger": "3",
        "LivingArea": "3200",
        "Latitude": "42.3505",
        "Longitude": "-71.0743",
        "ListingStatus": "ACT",
        "LIST_AGENT": "Cn222505",
        "LIST_NO": "381BEACON",
        "ModificationTimestamp": datetime.now().isoformat()
    },
    {
        "ListingID": "BRANDON002",
        "ListingKey": "brandon-oak-st",
        "ListPrice": "675000",
        "StreetName": "Oak Street",
        "City": "Dracut",
        "StateOrProvince": "MA",
        "PostalCode": "01826",
        "BedroomsTotal": "3",
        "BathroomsTotalInteger": "2",
        "LivingArea": "1800",
        "Latitude": "42.6701",
        "Longitude": "-71.3023",
        "ListingStatus": "ACT",
        "LIST_AGENT": "Cn222505",
        "LIST_NO": "45OAK",
        "ModificationTimestamp": datetime.now().isoformat()
    },
    {
        "ListingID": "BRANDON003",
        "ListingKey": "brandon-harbor-view",
        "ListPrice": "2200000",
        "StreetName": "Harbor View Drive",
        "City": "Marblehead",
        "StateOrProvince": "MA",
        "PostalCode": "01945",
        "BedroomsTotal": "5",
        "BathroomsTotalInteger": "4",
        "LivingArea": "3800",
        "Latitude": "42.5001",
        "Longitude": "-70.8578",
        "ListingStatus": "ACT",
        "LIST_AGENT": "Cn222505",
        "LIST_NO": "88HARBOR",
        "ModificationTimestamp": datetime.now().isoformat()
    }
]

def add_brandon_listings():
    """Add Brandon's listings to the database"""
    with get_conn() as conn, conn.cursor() as cur:
        for listing in BRANDON_LISTINGS:
            # Check if listing already exists
            cur.execute(
                "SELECT listing_key FROM listings WHERE listing_key = %s",
                (listing["ListingKey"],)
            )
            if cur.fetchone():
                print(f"Listing {listing['ListingKey']} already exists, updating...")
                cur.execute(
                    "UPDATE listings SET data = %s, updated_at = NOW() WHERE listing_key = %s",
                    (Json(listing), listing["ListingKey"])
                )
            else:
                print(f"Adding new listing {listing['ListingKey']}...")
                cur.execute(
                    "INSERT INTO listings (listing_key, data, updated_at) VALUES (%s, %s, NOW())",
                    (listing["ListingKey"], Json(listing))
                )
        
        conn.commit()
        print(f"Successfully added/updated {len(BRANDON_LISTINGS)} Brandon listings")

if __name__ == "__main__":
    add_brandon_listings()