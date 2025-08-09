#!/usr/bin/env python3
"""
Railway deployment version of the MLS API
Simplified version that works without database for initial deployment
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mls_api")

# Sample data for testing
SAMPLE_LISTINGS = [
    {
        "LIST_NO": "381BEACON",
        "LIST_PRICE": "7250000",
        "STREET_NO": "381",
        "STREET_NAME": "Beacon Street",
        "City": "Boston",
        "STATE": "MA",
        "ZIP_CODE": "02116",
        "NO_BEDROOMS": "4",
        "NO_FULL_BATHS": "3",
        "SQUARE_FEET": "3200",
        "STATUS": "Active",
        "REMARKS": "Stunning Back Bay Victorian with modern amenities",
        "LATITUDE": "42.3505",
        "LONGITUDE": "-71.0743",
        "LIST_AGENT_NAME": "Brandon Sweeney",
        "LIST_AGENT_ID": "Cn222505",
        "IS_BRANDON_LISTING": True
    },
    {
        "LIST_NO": "125COMM",
        "LIST_PRICE": "1850000",
        "STREET_NO": "125",
        "STREET_NAME": "Commonwealth Avenue",
        "City": "Cambridge",
        "STATE": "MA",
        "ZIP_CODE": "02139",
        "NO_BEDROOMS": "3",
        "NO_FULL_BATHS": "2",
        "SQUARE_FEET": "2100",
        "STATUS": "Active",
        "REMARKS": "Beautiful Cambridge condo with city views",
        "LATITUDE": "42.3736",
        "LONGITUDE": "-71.1097",
        "LIST_AGENT_NAME": "Sarah Johnson",
        "LIST_AGENT_ID": "SARAH002",
        "IS_BRANDON_LISTING": False
    },
    {
        "LIST_NO": "45OAK",
        "LIST_PRICE": "675000",
        "STREET_NO": "45",
        "STREET_NAME": "Oak Street",
        "City": "Dracut",
        "STATE": "MA",
        "ZIP_CODE": "01826",
        "NO_BEDROOMS": "3",
        "NO_FULL_BATHS": "2",
        "SQUARE_FEET": "1800",
        "STATUS": "Active",
        "REMARKS": "Charming family home in quiet neighborhood",
        "LATITUDE": "42.6701",
        "LONGITUDE": "-71.3023",
        "LIST_AGENT_NAME": "Brandon Sweeney",
        "LIST_AGENT_ID": "Cn222505",
        "IS_BRANDON_LISTING": True
    },
    {
        "LIST_NO": "200MAIN",
        "LIST_PRICE": "950000",
        "STREET_NO": "200",
        "STREET_NAME": "Main Street",
        "City": "Lexington",
        "STATE": "MA",
        "ZIP_CODE": "02421",
        "NO_BEDROOMS": "4",
        "NO_FULL_BATHS": "3",
        "SQUARE_FEET": "2500",
        "STATUS": "Active",
        "REMARKS": "Historic colonial with modern updates",
        "LATITUDE": "42.4430",
        "LONGITUDE": "-71.2289",
        "LIST_AGENT_NAME": "Mike Davis",
        "LIST_AGENT_ID": "MIKE003",
        "IS_BRANDON_LISTING": False
    },
    {
        "LIST_NO": "88HARBOR",
        "LIST_PRICE": "2200000",
        "STREET_NO": "88",
        "STREET_NAME": "Harbor View Drive",
        "City": "Marblehead",
        "STATE": "MA",
        "ZIP_CODE": "01945",
        "NO_BEDROOMS": "5",
        "NO_FULL_BATHS": "4",
        "SQUARE_FEET": "3800",
        "STATUS": "Active",
        "REMARKS": "Waterfront luxury home with panoramic ocean views",
        "LATITUDE": "42.5001",
        "LONGITUDE": "-70.8578",
        "LIST_AGENT_NAME": "Jennifer Wilson",
        "LIST_AGENT_ID": "JENNIFER004",
        "IS_BRANDON_LISTING": False
    }
]

# FastAPI app
app = FastAPI(
    title="MLS API for Railway",
    version="1.0.0",
    description="MLS listings API deployed on Railway",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class Listing(BaseModel):
    data: Dict[str, Any]

@app.get("/")
def root():
    return {
        "message": "MLS API is running on Railway",
        "version": "1.0.0",
        "endpoints": [
            "/listings",
            "/listings/{listing_id}",
            "/listings/featured",
            "/health"
        ]
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "OK",
        "message": "MLS API is running on Railway",
        "listings_count": len(SAMPLE_LISTINGS),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/listings")
def get_listings(
    city: Optional[str] = Query(None),
    min_price: Optional[int] = Query(None),
    max_price: Optional[int] = Query(None),
    bedrooms: Optional[int] = Query(None),
    limit: int = Query(50, gt=0, le=100)
):
    """Get listings with optional filters"""
    filtered_listings = SAMPLE_LISTINGS.copy()
    
    # Apply filters
    if city:
        filtered_listings = [
            listing for listing in filtered_listings
            if listing.get("City", "").lower() == city.lower()
        ]
    
    if min_price is not None:
        filtered_listings = [
            listing for listing in filtered_listings
            if int(listing.get("LIST_PRICE", 0)) >= min_price
        ]
    
    if max_price is not None:
        filtered_listings = [
            listing for listing in filtered_listings
            if int(listing.get("LIST_PRICE", 0)) <= max_price
        ]
    
    if bedrooms is not None:
        filtered_listings = [
            listing for listing in filtered_listings
            if int(listing.get("NO_BEDROOMS", 0)) >= bedrooms
        ]
    
    # Apply limit
    filtered_listings = filtered_listings[:limit]
    
    # Format response to match expected structure
    return [{
        "data": listing
    } for listing in filtered_listings]

@app.get("/listings/featured")
def get_featured_listings():
    """Get featured listings with Brandon Sweeney's listings prioritized first"""
    # Separate Brandon's listings (using agent ID Cn222505) and other listings
    brandon_listings = [listing for listing in SAMPLE_LISTINGS if listing.get("LIST_AGENT_ID") == "Cn222505"]
    other_listings = [listing for listing in SAMPLE_LISTINGS if listing.get("LIST_AGENT_ID") != "Cn222505"]
    
    # Combine with Brandon's listings first, then others
    featured = brandon_listings + other_listings
    
    # Return first 3 listings (Brandon's will always be first)
    return [{
        "data": listing
    } for listing in featured[:3]]

@app.get("/listings/{listing_id}")
def get_listing_by_id(listing_id: str):
    """Get a specific listing by ID"""
    for listing in SAMPLE_LISTINGS:
        if listing.get("LIST_NO") == listing_id:
            return {"data": listing}
    
    raise HTTPException(status_code=404, detail="Listing not found")

@app.post("/search")
def advanced_search(criteria: dict):
    """Advanced search endpoint"""
    # For now, return all listings
    # In a real implementation, this would filter based on criteria
    return [{
        "data": listing
    } for listing in SAMPLE_LISTINGS]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)