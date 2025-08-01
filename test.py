#!/usr/bin/env python3
"""
idx_server.py – FREE IDX ETL + API for MLS PIN "Manual Download"

• Fetches nightly pipe-delimited listing file + photos via FTP (if enabled)
• Loads/merges data into Postgres (JSONB for future-proof schema)
• Launches FastAPI w/ lightweight search endpoints
• Updated: Increased listing limit to 25000 to return all available listings

Author : Your Name Here
License: MIT
"""

import os
import io
import csv
import logging
import json
import asyncio
import ftplib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import psycopg2
from psycopg2.extras import Json, execute_values
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
import uvicorn

# Import our new automation modules
from scheduler import (
    lifespan_scheduler, 
    get_scheduler_status, 
    trigger_manual_run,
    start_scheduler,
    stop_scheduler
)

# ─────────────────────────────── CONFIG ──────────────────────────────── #

ENV_PATH = os.getenv("IDX_ENV_FILE", ".env")
load_dotenv(ENV_PATH)

FTP_HOST        = os.getenv("FTP_HOST")        # e.g. ftp.mlspin.com
FTP_USER        = os.getenv("FTP_USER")        # MLS PIN username
FTP_PASS        = os.getenv("FTP_PASS")        # MLS PIN password
FTP_LISTINGS_DIR= os.getenv("FTP_LISTINGS_DIR", "/")  # usually root
LISTING_FILE    = os.getenv("LISTING_FILE", "LISTINGS.TXT")
PHOTO_DIR       = os.getenv("PHOTO_DIR", "/photos")   # if you need photos
DOWNLOAD_PHOTOS = os.getenv("DOWNLOAD_PHOTOS", "false").lower() == "true"

DB_DSN          = os.getenv("DATABASE_URL")  # postgres://user:pass@host:5432/db
BATCH_SIZE      = int(os.getenv("UPSERT_BATCH_SIZE", "100"))
SCHEDULE_CRON   = os.getenv("CRON_EXPR", "0 2 * * *")  # every day 02:00 local

API_HOST        = os.getenv("API_HOST", "0.0.0.0")
API_PORT        = int(os.getenv("PORT", os.getenv("API_PORT", "8000")))

# List of fields you want to promote to columns for faster search
# Leave empty to keep everything inside a JSONB blob
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

# ────────────────────────────── ETL STEPS ───────────────────────────── #

def fetch_listing_file() -> io.StringIO:
    """
    Reads listing file from local directory (manually downloaded files).
    """
    # List of listing files to process (prioritizing single family homes)
    listing_files = [
        "idx_sf.txt",  # Single Family
        "idx_mf.txt",  # Multi-Family
        "idx_cc.txt",  # Condo/Coop
        "idx_rn.txt",  # Rental
        "idx_bu.txt",  # Business
        "idx_ld.txt",  # Land
        "idx_ci.txt",  # Commercial/Industrial
        "idx_mh.txt",  # Mobile Home
    ]
    
    all_content = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    for filename in listing_files:
        file_path = os.path.join(script_dir, filename)
        if os.path.exists(file_path):
            try:
                log.info("Reading local file: %s", filename)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read().strip()
                    if content:
                        all_content.append(content)
                        log.info("Loaded %s characters from %s", len(content), filename)
            except Exception as e:
                log.warning("Failed to read %s: %s", filename, e)
                continue
        else:
            log.warning("File not found: %s", file_path)
    
    if not all_content:
        raise FileNotFoundError("No listing files found in local directory")
    
    # Combine all files, handling headers properly
    combined_lines = []
    header_added = False
    
    for content in all_content:
        lines = content.split('\n')
        if not lines:
            continue
            
        # Add header only once (from first file)
        if not header_added:
            combined_lines.extend(lines)
            header_added = True
        else:
            # Skip header line for subsequent files
            if len(lines) > 1:
                combined_lines.extend(lines[1:])
    
    combined_content = '\n'.join(combined_lines)
    log.info("Combined %s listing files into %s total characters", len([f for f in listing_files if os.path.exists(os.path.join(script_dir, f))]), len(combined_content))
    
    return io.StringIO(combined_content)

def parse_rows(fh: io.StringIO) -> List[Dict[str, Any]]:
    reader = csv.DictReader(fh, delimiter="|")
    rows = []
    for row in reader:
        # Use LIST_NO as the primary key (MLS PIN format)
        if not row.get("LIST_NO"):
            continue
        # Map MLS PIN fields to our expected format
        row["ListingKey"] = row.get("LIST_NO")
        row["ListingID"] = row.get("LIST_NO")
        row["ListPrice"] = row.get("LIST_PRICE")
        row["StreetName"] = row.get("STREET_NAME")
        row["City"] = row.get("TOWN_NUM")  # Will need town lookup
        row["StateOrProvince"] = "MA"  # MLS PIN is Massachusetts
        row["PostalCode"] = row.get("ZIP_CODE")
        row["BedroomsTotal"] = row.get("NO_BEDROOMS")
        row["BathroomsTotalInteger"] = row.get("NO_FULL_BATHS")
        row["LivingArea"] = row.get("SQUARE_FEET")
        row["ListingStatus"] = row.get("STATUS")
        row["ModificationTimestamp"] = datetime.now().isoformat()
        rows.append(row)
    log.info("Parsed %s rows from file.", len(rows))
    return rows

def upsert_rows(rows: List[Dict[str, Any]]):
    """Insert/update rows into the listings table with duplicate prevention."""
    if not rows:
        log.warning("No rows to upsert.")
        return

    # Filter out rows without ListingKey to prevent invalid data
    valid_rows = [r for r in rows if r.get("ListingKey")]
    if len(valid_rows) != len(rows):
        log.warning("Filtered out %s rows without ListingKey", len(rows) - len(valid_rows))
    
    if not valid_rows:
        log.warning("No valid rows to upsert after filtering.")
        return

    cols = ["listing_key"] + [f.lower() for f in FIELD_MAP] + ["data"]
    records = []
    seen_keys = set()
    
    for r in valid_rows:
        listing_key = r.get("ListingKey")
        # Skip duplicates within the same batch
        if listing_key in seen_keys:
            log.debug("Skipping duplicate listing_key in batch: %s", listing_key)
            continue
        seen_keys.add(listing_key)
        
        record = [listing_key]
        record += [r.get(f, None) for f in FIELD_MAP]
        record.append(Json(r))
        records.append(record)

    if not records:
        log.warning("No unique records to upsert after deduplication.")
        return

    # Use execute_values with proper template
    query = f"""
        INSERT INTO listings ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (listing_key)
        DO UPDATE SET
            {', '.join(f"{c}=EXCLUDED.{c}" for c in cols[1:])},
            updated_at = now()
    """
    
    with get_conn() as conn, conn.cursor() as cur:
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(records), BATCH_SIZE):
            batch_num = (i // BATCH_SIZE) + 1
            batch = records[i : i + BATCH_SIZE]
            log.info("Processing batch %s/%s (%s records)", batch_num, total_batches, len(batch))
            execute_values(
                cur,
                query,
                batch,
                template=None,  # Let execute_values handle the template
                page_size=BATCH_SIZE
            )
            log.info("Completed batch %s/%s", batch_num, total_batches)
    log.info("Upserted %s unique listings (filtered %s duplicates).", len(records), len(valid_rows) - len(records))

def insert_sample_data():
    """Insert some sample data for testing when FTP is not available"""
    sample_listings = [
        {
            "ListingKey": "MA001",
            "ListingID": "2001",
            "ListPrice": "650000",
            "StreetName": "Main Street",
            "City": "Boston",
            "StateOrProvince": "MA",
            "PostalCode": "02101",
            "BedroomsTotal": "3",
            "BathroomsTotalInteger": "2",
            "LivingArea": "1800",
            "Latitude": "42.3601",
            "Longitude": "-71.0589",
            "ListingStatus": "Active",
            "ModificationTimestamp": datetime.now().isoformat()
        },
        {
            "ListingKey": "MA002",
            "ListingID": "2002",
            "ListPrice": "850000",
            "StreetName": "Commonwealth Avenue",
            "City": "Cambridge",
            "StateOrProvince": "MA",
            "PostalCode": "02139",
            "BedroomsTotal": "4",
            "BathroomsTotalInteger": "3",
            "LivingArea": "2200",
            "Latitude": "42.3736",
            "Longitude": "-71.1097",
            "ListingStatus": "Active",
            "ModificationTimestamp": datetime.now().isoformat()
        },
        {
            "ListingKey": "MA003",
            "ListingID": "2003",
            "ListPrice": "425000",
            "StreetName": "Oak Street",
            "City": "Dracut",
            "StateOrProvince": "MA",
            "PostalCode": "01826",
            "BedroomsTotal": "2",
            "BathroomsTotalInteger": "1",
            "LivingArea": "1200",
            "Latitude": "42.6701",
            "Longitude": "-71.3023",
            "ListingStatus": "Active",
            "ModificationTimestamp": datetime.now().isoformat()
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
    log.info("Sample data inserted (%s listings).", len(records))

def check_existing_data():
    """Check if we already have data in the database"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM listings WHERE listing_key NOT LIKE 'MA%'")
            real_listings_count = cur.fetchone()[0]
            return real_listings_count > 0
    except Exception as e:
        log.warning("Error checking existing data: %s", e)
        return False

def etl_job(force_reload=False):
    try:
        # Check if we already have data and this isn't a forced reload
        if not force_reload and check_existing_data():
            log.info("Data already exists in database, skipping ETL job. Use force_reload=True to override.")
            return
        
        # Try to read from local files first (manually downloaded)
        log.info("Starting ETL job with local files")
        fh = fetch_listing_file()
        rows = parse_rows(fh)
        upsert_rows(rows)
        if DOWNLOAD_PHOTOS:
            log.warning("Photo download not yet implemented – set DOWNLOAD_PHOTOS=false or extend here.")
    except Exception as e:
        log.warning("ETL job failed: %s. No sample data will be inserted - using existing database data.", e)

# ──────────────────────────────── API ───────────────────────────────── #

app = FastAPI(
    title="MLS PIN ETL and API", 
    version="2.0.0",
    description="Automated MLS PIN data processing with hourly updates",
    lifespan=lifespan_scheduler  # Add scheduler lifecycle management
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Listing(BaseModel):
    listing_key: str
    data: Dict[str, Any]
    # Dynamically add any typed fields you promoted
    class Config:
        extra = "allow"

@app.get("/listings")
def list_listings(
    city: Optional[str] = Query(None),
    state: Optional[str] = Query(None, alias="state"),
    min_price: Optional[int] = Query(None, ge=0),
    max_price: Optional[int] = Query(None, ge=0),
    bedrooms: Optional[int] = Query(None, ge=0),
    bathrooms: Optional[int] = Query(None, ge=0),
    limit: int = Query(25000, gt=0, le=25000),
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
        where.append("COALESCE((data->>'ListPrice')::numeric, 0) >= %s")
        params.append(min_price)
    if max_price is not None:
        where.append("COALESCE((data->>'ListPrice')::numeric, 0) <= %s")
        params.append(max_price)
    if bedrooms is not None:
        where.append("COALESCE((data->>'BedroomsTotal')::numeric, (data->>'NO_BEDROOMS')::numeric, 0) >= %s")
        params.append(bedrooms)
    if bathrooms is not None:
        where.append("COALESCE((data->>'BathroomsTotalInteger')::numeric, (data->>'NO_FULL_BATHS')::numeric, 0) >= %s")
        params.append(bathrooms)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
        SELECT listing_key, data, updated_at
        FROM listings
        {where_sql}
        ORDER BY 
            CASE WHEN data->>'LIST_AGENT_ID' = 'CN222505' THEN 0 ELSE 1 END,
            updated_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])
    
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    
    # Format response to match frontend expectations
    formatted_listings = []
    for row in rows:
        formatted_listings.append({
            "listing_key": row['listing_key'],
            "data": row['data'],
            "updated_at": row['updated_at'].isoformat() if row['updated_at'] else None
        })
    
    return {
        "success": True,
        "data": formatted_listings,
        "count": len(formatted_listings),
        "filters": {
            "city": city,
            "state": state,
            "min_price": min_price,
            "max_price": max_price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "limit": limit,
            "offset": offset
        }
    }

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM listings")
            result = cur.fetchone()
            count = result['count'] if result else 0
        
        return {
            "status": "OK",
            "message": "Python MLS API is running",
            "listings_count": count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={
            "status": "ERROR",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        })

# ─────────────────────────── AUTOMATION ENDPOINTS ────────────────────────── #

@app.get("/automation/status")
async def get_automation_status():
    """Get the status of the automated download scheduler"""
    try:
        status = await get_scheduler_status()
        return {
            "status": "success",
            "data": status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get scheduler status: {e}")


@app.post("/automation/run")
async def trigger_automation_run():
    """Manually trigger an automated download and processing run"""
    try:
        result = await trigger_manual_run()
        return {
            "status": "success",
            "data": result,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger manual run: {e}")


@app.post("/automation/start")
async def start_automation(interval_hours: int = Query(1, ge=1, le=24)):
    """Start the automated download scheduler"""
    try:
        await start_scheduler(interval_hours)
        return {
            "status": "success",
            "message": f"Scheduler started with {interval_hours} hour interval",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start scheduler: {e}")


@app.post("/automation/stop")
async def stop_automation():
    """Stop the automated download scheduler"""
    try:
        await stop_scheduler()
        return {
            "status": "success",
            "message": "Scheduler stopped",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop scheduler: {e}")


@app.get("/automation/logs")
async def get_automation_logs(limit: int = Query(10, ge=1, le=100)):
    """Get recent automation run logs"""
    try:
        status = await get_scheduler_status()
        run_history = status.get('stats', {}).get('run_history', [])
        
        # Return the most recent runs
        recent_runs = run_history[-limit:] if run_history else []
        
        return {
            "status": "success",
            "data": {
                "recent_runs": recent_runs,
                "total_runs": len(run_history),
                "stats": status.get('stats', {})
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get automation logs: {e}")

@app.get("/listings/{listing_key}")
def get_listing(listing_key: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT listing_key, data, updated_at FROM listings WHERE listing_key = %s",
            (listing_key,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Listing not found")
    
    return {
        "success": True,
        "data": {
            "listing_key": row['listing_key'],
            "data": row['data'],
            "updated_at": row['updated_at'].isoformat() if row['updated_at'] else None
        }
    }

@app.get("/listings/featured/all")
def get_featured_listings():
    """Get featured listings (first 6 listings) with Brandon's listings prioritized"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT listing_key, data, updated_at FROM listings 
               ORDER BY 
                    CASE WHEN data->>'LIST_AGENT_ID' = 'CN222505' THEN 0 ELSE 1 END,
                    updated_at DESC 
               LIMIT 6"""
        )
        rows = cur.fetchall()
    
    formatted_listings = []
    for row in rows:
        formatted_listings.append({
            "listing_key": row['listing_key'],
            "data": row['data'],
            "updated_at": row['updated_at'].isoformat() if row['updated_at'] else None
        })
    
    return {
        "success": True,
        "data": {
            "data": formatted_listings
        },
        "count": len(formatted_listings)
    }

@app.post("/admin/reload-data")
def reload_data(force: bool = True):
    """Manually trigger ETL job with optional force reload"""
    try:
        etl_job(force_reload=force)
        return {
            "success": True,
            "message": "ETL job completed successfully",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={
            "success": False,
            "message": f"ETL job failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

# ───────────────────────────── SCHEDULER ────────────────────────────── #

# Scheduler functionality moved to scheduler.py and managed via lifespan_scheduler

# ─────────────────────────────── main ──────────────────────────────── #

def main():
    init_db()
    # Only run ETL job if no data exists (first time setup)
    if not check_existing_data():
        log.info("No existing data found, running initial ETL job")
        etl_job()
    else:
        log.info("Existing data found, skipping initial ETL job")
    # Scheduler is now managed by FastAPI lifespan_scheduler
    uvicorn.run(app, host=API_HOST, port=API_PORT)

if __name__ == "__main__":
    main()

# ────────────────────────── requirements.txt ────────────────────────── #
"""
fastapi==0.111.0
uvicorn[standard]==0.29.0
psycopg2-binary==2.9.9
python-dotenv==1.0.1
apscheduler==3.10.4
pydantic==2.7.1
"""

# ─────────────────────────── .env TEMPLATE ─────────────────────────── #
"""
# FTP creds from MLS PIN “Manual Download” agreement
FTP_HOST=ftp.mlspin.com
FTP_USER=YOUR_FTP_USERNAME
FTP_PASS=YOUR_FTP_PASSWORD
FTP_LISTINGS_DIR=/  # leave as "/" unless MLS PIN tells you otherwise
LISTING_FILE=LISTINGS.TXT

# Postgres (example)
DATABASE_URL=postgresql://idx_user:idx_pass@localhost:5432/idx

# Set to "true" only after you implement photo ingestion
DOWNLOAD_PHOTOS=false
"""