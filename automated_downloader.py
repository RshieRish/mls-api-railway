#!/usr/bin/env python3
"""
Automated MLS PIN Downloader and Processor

This module handles:
1. Automated downloading of IDX files from MLS PIN website using Playwright
2. Intelligent processing with change detection
3. CRUD operations for listings (Create, Update, Delete/Mark Sold)
4. Hourly scheduling for continuous updates
5. Deduplication and status management

Author: Brandon RE Team
License: MIT
"""

import os
import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Set, Optional
from pathlib import Path
import hashlib
import json

from playwright.async_api import async_playwright, Browser, Page
import psycopg2
from psycopg2.extras import Json, execute_values, RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
MLS_PIN_URL = os.getenv("MLS_PIN_URL", "https://pinergy.mlspin.com")
MLS_PIN_USERNAME = os.getenv("MLS_PIN_USERNAME")
MLS_PIN_PASSWORD = os.getenv("MLS_PIN_PASSWORD")
DB_DSN = os.getenv("DATABASE_URL")
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
BATCH_SIZE = int(os.getenv("UPSERT_BATCH_SIZE", "100"))

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("automated_downloader")

# IDX file types to download - mapped to MLS PIN property types
IDX_FILES = [
    {"name": "idx_sf.txt", "type": "Single Family Residential", "proptype": "SF", "priority": 1},
    {"name": "idx_mf.txt", "type": "Multi Family", "proptype": "MF", "priority": 2},
    {"name": "idx_cc.txt", "type": "Condo/Coop", "proptype": "CC", "priority": 3},
    {"name": "idx_rn.txt", "type": "Rental", "proptype": "RN", "priority": 4},
    {"name": "idx_bu.txt", "type": "Business", "proptype": "BU", "priority": 5},
    {"name": "idx_ld.txt", "type": "Land", "proptype": "LD", "priority": 6},
    {"name": "idx_ci.txt", "type": "Commercial/Industrial", "proptype": "CI", "priority": 7},
    {"name": "idx_mh.txt", "type": "Mobile Home", "proptype": "MH", "priority": 8},
]

class MLSPinDownloader:
    """Handles automated downloading from MLS PIN website"""
    
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.download_dir = Path(DOWNLOAD_DIR)
        self.download_dir.mkdir(exist_ok=True)
        
    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
            
    async def login(self) -> bool:
        """Login to MLS PIN website"""
        try:
            # Check if credentials are available
            if not MLS_PIN_USERNAME or not MLS_PIN_PASSWORD:
                log.error("MLS PIN credentials not configured. Please set MLS_PIN_USERNAME and MLS_PIN_PASSWORD environment variables.")
                return False
            
            log.info("Navigating to MLS PIN login page")
            await self.page.goto("https://pinergy.mlspin.com/")
            
            # Handle cookie consent if present
            try:
                await self.page.wait_for_selector("#cookieConsentBootstrapModal", timeout=3000)
                await self.page.click("button.mls-js-cookie-consent-action")
                log.info("Cookie consent handled")
            except:
                # Cookie consent not present or already handled
                pass
            
            # Wait for login form
            await self.page.wait_for_selector("input[name='user_name']")
            
            # Fill login form
            await self.page.fill("input[name='user_name']", MLS_PIN_USERNAME)
            await self.page.fill("input[name='pass']", MLS_PIN_PASSWORD)
            
            # Submit form
            await self.page.click("button[type='submit']")
            
            # Wait for form submission and check for successful login
            await self.page.wait_for_timeout(3000)  # Wait for form processing
            
            # Check if we're still on the login page (which might indicate login failure)
            current_url = self.page.url
            if "signin.asp" in current_url:
                # Check for error messages
                error_elements = await self.page.query_selector_all(".error, .alert, [class*='error'], [class*='alert']")
                if error_elements:
                    error_text = await error_elements[0].inner_text()
                    raise Exception(f"Login failed with error: {error_text}")
                # If no error messages but still on signin page, assume success
                log.info("Login form submitted successfully")
            
            # Try to navigate to the main MLS area
            try:
                # Look for navigation links or try to access MLS functionality
                await self.page.wait_for_timeout(2000)
                log.info(f"Current URL after login: {current_url}")
            except Exception as nav_error:
                log.warning(f"Navigation after login failed: {nav_error}")
            
            log.info("Successfully logged into MLS PIN")
            return True
            
        except Exception as e:
            log.error(f"Login failed: {e}")
            return False
            
    async def download_idx_files(self) -> List[str]:
        """Download all IDX files from MLS PIN"""
        downloaded_files = []
        
        try:
            # Navigate to Pine interface after login
            await self.page.click("button:has-text('Click Here to Continue to Pine'), .btn:has-text('Click Here to Continue to Pine')")
            await asyncio.sleep(3)
            
            # Navigate to Tools page
            await self.page.click("a[href*='tools'], a:has-text('Tools')")
            await asyncio.sleep(3)
            
            # Navigate to IDX Downloads
            await self.page.click("a[href*='tools/idx'], a:has-text('IDX Downloads')")
            await asyncio.sleep(3)
            
            # Extract the user token from the page
            user_token = None
            try:
                # Look for any link that contains the user token
                links = await self.page.evaluate("""
                    Array.from(document.querySelectorAll('a')).filter(el => 
                        el.href && el.href.includes('idx.mlspin.com') && el.href.includes('user=')
                    ).map(el => el.href)
                """)
                
                if links:
                    # Extract user token from the first link
                    match = re.search(r'user=([^&]+)', links[0])
                    if match:
                        user_token = match.group(1)
                        log.info(f"Extracted user token: {user_token[:20]}...")
                    
            except Exception as e:
                log.warning(f"Could not extract user token: {e}")
            
            if not user_token:
                log.error("Could not find user token for downloads")
                return downloaded_files
            
            # Download each IDX file using direct URLs
            for idx_file in IDX_FILES:
                try:
                    log.info(f"Downloading {idx_file['name']} ({idx_file['type']})")
                    
                    # Construct the direct download URL
                    download_url = f"https://idx.mlspin.com/idx.asp?user={user_token}&proptype={idx_file['proptype']}"
                    
                    # Use HTTP request to download the file
                    response = await self.page.request.get(download_url)
                    
                    if response.status == 200:
                        # Save file to download directory
                        file_path = self.download_dir / idx_file['name']
                        content = await response.body()
                        
                        with open(file_path, 'wb') as f:
                            f.write(content)
                        
                        downloaded_files.append(str(file_path))
                        log.info(f"Successfully downloaded {idx_file['name']} to {file_path}")
                    else:
                        log.warning(f"Failed to download {idx_file['name']}: HTTP {response.status}")
                    
                    # Small delay between downloads
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    log.warning(f"Failed to download {idx_file['name']}: {e}")
                    continue
                    
        except Exception as e:
            log.error(f"Error during file downloads: {e}")
            
        return downloaded_files

class ListingProcessor:
    """Handles intelligent processing of listing data with CRUD operations"""
    
    def __init__(self, download_dir=None):
        self.conn = None
        self.download_dir = Path(download_dir) if download_dir else Path(DOWNLOAD_DIR)
        self.download_dir.mkdir(exist_ok=True)
        self.processed_files_log = self.download_dir / "processed_files.json"
        
    def get_conn(self):
        """Get database connection"""
        return psycopg2.connect(dsn=DB_DSN, cursor_factory=RealDictCursor)
        
    def get_file_hash(self, file_path: str) -> str:
        """Calculate file hash for change detection"""
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
            
    def load_processed_files_log(self) -> Dict[str, Any]:
        """Load log of previously processed files"""
        if self.processed_files_log.exists():
            with open(self.processed_files_log, 'r') as f:
                return json.load(f)
        return {}
        
    def save_processed_files_log(self, log_data: Dict[str, Any]):
        """Save log of processed files"""
        with open(self.processed_files_log, 'w') as f:
            json.dump(log_data, f, indent=2)
            
    def has_file_changed(self, file_path: str) -> bool:
        """Check if file has changed since last processing"""
        current_hash = self.get_file_hash(file_path)
        log_data = self.load_processed_files_log()
        
        file_name = os.path.basename(file_path)
        last_hash = log_data.get(file_name, {}).get('hash')
        
        return current_hash != last_hash
        
    def mark_file_processed(self, file_path: str):
        """Mark file as processed in log"""
        file_hash = self.get_file_hash(file_path)
        log_data = self.load_processed_files_log()
        
        file_name = os.path.basename(file_path)
        log_data[file_name] = {
            'hash': file_hash,
            'processed_at': datetime.now().isoformat(),
            'file_path': file_path
        }
        
        self.save_processed_files_log(log_data)
        
    def parse_idx_file(self, file_path: str) -> List[Dict[str, Any]]:
        """Parse IDX file and return listing data"""
        import csv
        
        listings = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.DictReader(f, delimiter='|')
                
                for row in reader:
                    if not row.get("LIST_NO"):
                        continue
                        
                    # Map MLS PIN fields to our format
                    listing = {
                        "ListingKey": row.get("LIST_NO"),
                        "ListingID": row.get("LIST_NO"),
                        "ListPrice": row.get("LIST_PRICE"),
                        "StreetName": row.get("STREET_NAME"),
                        "City": row.get("TOWN_NUM"),
                        "StateOrProvince": "MA",
                        "PostalCode": row.get("ZIP_CODE"),
                        "BedroomsTotal": row.get("NO_BEDROOMS"),
                        "BathroomsTotalInteger": row.get("NO_FULL_BATHS"),
                        "LivingArea": row.get("SQUARE_FEET"),
                        "ListingStatus": row.get("STATUS"),
                        "ModificationTimestamp": datetime.now().isoformat(),
                        "_raw_data": row,  # Keep original data
                        "_file_source": os.path.basename(file_path)
                    }
                    
                    listings.append(listing)
                    
        except Exception as e:
            log.error(f"Error parsing {file_path}: {e}")
            
        return listings
        
    def get_existing_listings(self) -> Dict[str, Dict[str, Any]]:
        """Get all existing listings from database"""
        existing = {}
        
        with self.get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT listing_key, data, updated_at FROM listings")
            
            for row in cur.fetchall():
                existing[row['listing_key']] = {
                    'data': row['data'],
                    'updated_at': row['updated_at']
                }
                
        return existing
        
    def detect_changes(self, new_listings: List[Dict[str, Any]], existing_listings: Dict[str, Dict[str, Any]]) -> Dict[str, List]:
        """Detect what listings are new, updated, or removed"""
        new_keys = {listing['ListingKey'] for listing in new_listings}
        existing_keys = set(existing_listings.keys())
        
        # Categorize changes
        to_create = []
        to_update = []
        to_mark_sold = list(existing_keys - new_keys)  # Listings no longer in files
        
        for listing in new_listings:
            key = listing['ListingKey']
            
            if key not in existing_listings:
                # New listing
                to_create.append(listing)
            else:
                # Check if listing data has changed
                existing_data = existing_listings[key]['data']
                
                # Compare key fields for changes
                key_fields = ['ListPrice', 'ListingStatus', 'ModificationTimestamp']
                has_changes = any(
                    str(listing.get(field, '')).strip() != str(existing_data.get(field, '')).strip()
                    for field in key_fields
                )
                
                if has_changes:
                    to_update.append(listing)
                    
        return {
            'create': to_create,
            'update': to_update,
            'mark_sold': to_mark_sold
        }
        
    def execute_crud_operations(self, changes: Dict[str, List]):
        """Execute CRUD operations on database"""
        with self.get_conn() as conn, conn.cursor() as cur:
            
            # CREATE: Insert new listings
            if changes['create']:
                log.info(f"Creating {len(changes['create'])} new listings")
                self._insert_listings(cur, changes['create'])
                
            # UPDATE: Update existing listings
            if changes['update']:
                log.info(f"Updating {len(changes['update'])} existing listings")
                self._update_listings(cur, changes['update'])
                
            # MARK SOLD: Update status for removed listings
            if changes['mark_sold']:
                log.info(f"Marking {len(changes['mark_sold'])} listings as sold/inactive")
                self._mark_listings_sold(cur, changes['mark_sold'])
                
    def _insert_listings(self, cursor, listings: List[Dict[str, Any]]):
        """Insert new listings with UPSERT to handle duplicates"""
        for listing in listings:
            cursor.execute(
                """
                INSERT INTO listings (
                    listing_key, listingid, listprice, streetname, city, 
                    stateorprovince, postalcode, bedroomstotal, bathroomstotalinteger,
                    livingarea, latitude, longitude, listingstatus, modificationtimestamp, data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (listing_key) DO UPDATE SET
                    listingid = EXCLUDED.listingid,
                    listprice = EXCLUDED.listprice,
                    streetname = EXCLUDED.streetname,
                    city = EXCLUDED.city,
                    stateorprovince = EXCLUDED.stateorprovince,
                    postalcode = EXCLUDED.postalcode,
                    bedroomstotal = EXCLUDED.bedroomstotal,
                    bathroomstotalinteger = EXCLUDED.bathroomstotalinteger,
                    livingarea = EXCLUDED.livingarea,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    listingstatus = EXCLUDED.listingstatus,
                    modificationtimestamp = EXCLUDED.modificationtimestamp,
                    data = EXCLUDED.data,
                    updated_at = now()
                """,
                (
                    listing['ListingKey'],
                    listing.get('ListingID'),
                    listing.get('ListPrice'),
                    listing.get('StreetName'),
                    listing.get('City'),
                    listing.get('StateOrProvince'),
                    listing.get('PostalCode'),
                    listing.get('BedroomsTotal'),
                    listing.get('BathroomsTotalInteger'),
                    listing.get('LivingArea'),
                    None,  # Latitude
                    None,  # Longitude
                    listing.get('ListingStatus'),
                    listing.get('ModificationTimestamp'),
                    Json(listing)  # Full data as JSONB
                )
            )
        
    def _update_listings(self, cursor, listings: List[Dict[str, Any]]):
        """Update existing listings"""
        for listing in listings:
            cursor.execute(
                """
                UPDATE listings SET
                    listprice = %s,
                    streetname = %s,
                    city = %s,
                    stateorprovince = %s,
                    postalcode = %s,
                    bedroomstotal = %s,
                    bathroomstotalinteger = %s,
                    livingarea = %s,
                    listingstatus = %s,
                    modificationtimestamp = %s,
                    data = %s,
                    updated_at = now()
                WHERE listing_key = %s
                """,
                (
                    listing.get('ListPrice'),
                    listing.get('StreetName'),
                    listing.get('City'),
                    listing.get('StateOrProvince'),
                    listing.get('PostalCode'),
                    listing.get('BedroomsTotal'),
                    listing.get('BathroomsTotalInteger'),
                    listing.get('LivingArea'),
                    listing.get('ListingStatus'),
                    listing.get('ModificationTimestamp'),
                    Json(listing),
                    listing['ListingKey']
                )
            )
            
    def _mark_listings_sold(self, cursor, listing_keys: List[str]):
        """Mark listings as sold/inactive"""
        for key in listing_keys:
            cursor.execute(
                """
                UPDATE listings SET
                    listingstatus = 'SOLD',
                    data = data || %s,
                    updated_at = now()
                WHERE listing_key = %s
                """,
                (Json({'auto_marked_sold': True, 'marked_sold_at': datetime.now().isoformat()}), key)
            )
            
    def process_files(self, file_paths: List[str]) -> Dict[str, Any]:
        """Process downloaded files with intelligent change detection"""
        results = {
            'processed_files': 0,
            'skipped_files': 0,
            'total_changes': {'create': 0, 'update': 0, 'mark_sold': 0},
            'errors': []
        }
        
        try:
            # Get existing listings once
            existing_listings = self.get_existing_listings()
            log.info(f"Found {len(existing_listings)} existing listings in database")
            
            # Combine all new listings from all files
            all_new_listings = []
            
            for file_path in file_paths:
                try:
                    # Check if file has changed
                    if not self.has_file_changed(file_path):
                        log.info(f"Skipping {file_path} - no changes detected")
                        results['skipped_files'] += 1
                        continue
                        
                    # Parse file
                    listings = self.parse_idx_file(file_path)
                    all_new_listings.extend(listings)
                    
                    # Mark file as processed
                    self.mark_file_processed(file_path)
                    results['processed_files'] += 1
                    
                    log.info(f"Processed {file_path}: {len(listings)} listings")
                    
                except Exception as e:
                    error_msg = f"Error processing {file_path}: {e}"
                    log.error(error_msg)
                    results['errors'].append(error_msg)
                    
            if all_new_listings:
                # Detect changes across all listings
                changes = self.detect_changes(all_new_listings, existing_listings)
                
                # Execute CRUD operations
                self.execute_crud_operations(changes)
                
                # Update results
                results['total_changes'] = {
                    'create': len(changes['create']),
                    'update': len(changes['update']),
                    'mark_sold': len(changes['mark_sold'])
                }
                
                log.info(f"Processing complete: {results['total_changes']}")
            else:
                log.info("No new listings to process")
                
        except Exception as e:
            error_msg = f"Error during processing: {e}"
            log.error(error_msg)
            results['errors'].append(error_msg)
            
        return results

async def automated_download_and_process():
    """Main function to download and process listings"""
    log.info("Starting automated download and processing")
    
    try:
        # Download files
        async with MLSPinDownloader() as downloader:
            if not await downloader.login():
                log.error("Failed to login to MLS PIN")
                return
                
            downloaded_files = await downloader.download_idx_files()
            
        if not downloaded_files:
            log.warning("No files were downloaded")
            return
            
        # Process files
        processor = ListingProcessor()
        results = processor.process_files(downloaded_files)
        
        log.info(f"Automation complete: {results}")
        
    except Exception as e:
        log.error(f"Automation failed: {e}")

if __name__ == "__main__":
    # For testing - run once
    asyncio.run(automated_download_and_process())