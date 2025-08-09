#!/usr/bin/env python3
"""
Simplified test version of the IDX server script to test basic functionality
without requiring FTP credentials.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import uvicorn

# ─────────────────────────────── CONFIG ──────────────────────────────── #

ENV_PATH = os.getenv("IDX_ENV_FILE", ".env")
load_dotenv(ENV_PATH)

DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/idx_test")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# List of fields you want to promote to columns for faster search
FIELD_MAP: List[str] = [
    "ListingID",
    "ListingKey", 
    "ListPrice",
    "StreetName",
    "City",
    "StateOrProvince",
    "PostalCode",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "LivingArea",
    "Latitude",
    "Longitude",
    "ListingStatus",
    "ModificationTimestamp",
]

# ─────────────────────────────── LOGGING ─────────────────────────────── #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("idx_server")

# ────────────────────────────── DATABASE ────────────────────────────── #

def get_conn():
    return psycopg2.connect(dsn=DB_DSN, cursor_factory=psycopg2.extras.RealDictCursor)

def init_db():
    cols = ",\n    ".join(f"{f.lower()} TEXT" for f in FIELD_MAP)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS listings (
                id SERIAL PRIMARY KEY,
                listing_key TEXT UNIQUE,
                {cols},
                data JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS listings_gin ON listings USING GIN (data);
            """
        )
    log.info("Database ready.")

def insert_sample_data():
    """Insert some sample data for testing"""
    sample_listings = [
        {
            "ListingKey": "TEST001",
            "ListingID": "1001",
            "ListPrice": "500000",
            "City": "Boston",
            "StateOrProvince": "MA",
            "BedroomsTotal": "3",
            "BathroomsTotalInteger": "2",
            "LivingArea": "1500",
            "ListingStatus": "Active"
        },
        {
            "ListingKey": "TEST002", 
            "ListingID": "1002",
            "ListPrice": "750000",
            "City": "Cambridge",
            "StateOrProvince": "MA",
            "BedroomsTotal": "4",
            "BathroomsTotalInteger": "3",
            "LivingArea": "2000",
            "ListingStatus": "Active"
        }
    ]
    
    cols = ["listing_key"] + [f.lower() for f in FIELD_MAP] + ["data"]
    records = []
    for r in sample_listings:
        record = [r.get("ListingKey")]
        record += [r.get(f, None) for f in FIELD_MAP]
        record.append(Json(r))
        records.append(record)
    
    template = ", ".join(["%s"] * len(cols))
    query = f"""
        INSERT INTO listings ({', '.join(cols)})
        VALUES ({template})
        ON CONFLICT (listing_key)
        DO UPDATE SET
            {', '.join(f"{c}=EXCLUDED.{c}" for c in cols[1:])},
            updated_at = now();
    """
    
    with get_conn() as conn, conn.cursor() as cur:
        for record in records:
            cur.execute(query, record)
    log.info("Sample data inserted.")

# ──────────────────────────────── API ───────────────────────────────── #

app = FastAPI(
    title="Free IDX API (MLS PIN) - Test Version",
    version="0.1.0",
    description="Minimal RESO-like API for testing database and API functionality.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class Listing(BaseModel):
    listing_key: str
    data: Dict[str, Any]
    class Config:
        extra = "allow"

@app.get("/")
def root():
    return {"message": "IDX API Test Server is running!", "status": "ok"}

@app.get("/health")
def health_check():
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/listings", response_model=List[Listing])
def list_listings(
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    min_price: Optional[int] = Query(None, ge=0),
    max_price: Optional[int] = Query(None, ge=0),
    limit: int = Query(50, gt=0, le=500),
    offset: int = Query(0, ge=0),
):
    where, params = [], []
    if city:
        where.append("data->>'City' ILIKE %s")
        params.append(f"%{city}%")
    if state:
        where.append("data->>'StateOrProvince' = %s")
        params.append(state)
    if min_price is not None:
        where.append("(data->>'ListPrice')::int >= %s")
        params.append(min_price)
    if max_price is not None:
        where.append("(data->>'ListPrice')::int <= %s")
        params.append(max_price)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
        SELECT listing_key, data, updated_at
        FROM listings
        {where_sql}
        ORDER BY updated_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return rows

@app.get("/listings/{listing_key}", response_model=Listing)
def get_listing(listing_key: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT listing_key, data, updated_at FROM listings WHERE listing_key = %s",
            (listing_key,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Listing not found")
    return row

# ─────────────────────────────── main ──────────────────────────────── #

def main():
    log.info("Starting IDX Test Server...")
    init_db()
    insert_sample_data()
    log.info(f"Server starting on {API_HOST}:{API_PORT}")
    uvicorn.run(app, host=API_HOST, port=API_PORT)

if __name__ == "__main__":
    main()