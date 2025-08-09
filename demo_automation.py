#!/usr/bin/env python3
"""
Demo Script for MLS PIN Automation System

This script demonstrates the automation system capabilities:
1. Shows how to check automation status
2. Demonstrates manual trigger functionality
3. Shows how to monitor the system
4. Tests the API endpoints

Usage:
    python demo_automation.py

Author: Brandon RE Team
License: MIT
"""

import asyncio
import json
import time
from datetime import datetime

# Import our automation modules
from scheduler import MLSScheduler, get_scheduler_status, trigger_manual_run
from automated_downloader import MLSPinDownloader, ListingProcessor

async def demo_scheduler():
    """Demonstrate the scheduler functionality"""
    print("\n" + "="*60)
    print("SCHEDULER DEMO")
    print("="*60)
    
    # Create scheduler instance
    scheduler = MLSScheduler()
    
    print("1. Creating scheduler instance...")
    print(f"   ✓ Scheduler created: {type(scheduler).__name__}")
    
    print("\n2. Getting initial status...")
    status = scheduler.get_status()
    print(f"   ✓ Is running: {status['is_running']}")
    print(f"   ✓ Run count: {status['run_count']}")
    print(f"   ✓ Error count: {status['error_count']}")
    
    print("\n3. Starting scheduler (demo mode - 10 second interval)...")
    try:
        # Start with very short interval for demo
        scheduler.start(interval_hours=1/360)  # Every 10 seconds
        print("   ✓ Scheduler started successfully")
        
        # Wait a bit to see it in action
        print("\n4. Waiting 5 seconds to observe scheduler...")
        await asyncio.sleep(5)
        
        # Check status again
        status = scheduler.get_status()
        print(f"   ✓ Is running: {status['is_running']}")
        print(f"   ✓ Next run: {status['next_run']}")
        
        print("\n5. Stopping scheduler...")
        scheduler.stop()
        print("   ✓ Scheduler stopped successfully")
        
    except Exception as e:
        print(f"   ✗ Scheduler demo failed: {e}")
        if scheduler.is_running:
            scheduler.stop()

async def demo_downloader():
    """Demonstrate the downloader functionality (without actual login)"""
    print("\n" + "="*60)
    print("DOWNLOADER DEMO")
    print("="*60)
    
    print("1. Creating downloader instance...")
    
    # Note: We won't actually run the downloader since we don't have credentials
    # This just shows the structure
    
    try:
        async with MLSPinDownloader() as downloader:
            print(f"   ✓ Downloader created: {type(downloader).__name__}")
            print(f"   ✓ Download directory: {downloader.download_dir}")
            print(f"   ✓ Browser initialized: {downloader.browser is not None}")
            
        print("   ✓ Downloader context manager works correctly")
        
    except Exception as e:
        print(f"   ✗ Downloader demo failed: {e}")
        print("   Note: This is expected without proper MLS PIN credentials")

def demo_processor():
    """Demonstrate the listing processor functionality"""
    print("\n" + "="*60)
    print("PROCESSOR DEMO")
    print("="*60)
    
    print("1. Creating processor instance...")
    processor = ListingProcessor()
    print(f"   ✓ Processor created: {type(processor).__name__}")
    
    print("\n2. Testing file hash calculation...")
    # Create a test file
    test_file = "/tmp/test_listing.txt"
    with open(test_file, 'w') as f:
        f.write("LIST_NO|LIST_PRICE|STREET_NAME\n")
        f.write("12345|500000|123 Main St\n")
    
    hash1 = processor.get_file_hash(test_file)
    print(f"   ✓ File hash calculated: {hash1[:8]}...")
    
    # Test change detection
    changed = processor.has_file_changed(test_file)
    print(f"   ✓ File change detected: {changed} (expected: True for new file)")
    
    # Mark as processed
    processor.mark_file_processed(test_file)
    print("   ✓ File marked as processed")
    
    # Test again
    changed = processor.has_file_changed(test_file)
    print(f"   ✓ File change detected: {changed} (expected: False for unchanged file)")
    
    print("\n3. Testing file parsing...")
    listings = processor.parse_idx_file(test_file)
    print(f"   ✓ Parsed {len(listings)} listings from test file")
    
    if listings:
        listing = listings[0]
        print(f"   ✓ Sample listing: {listing['ListingKey']} - ${listing['ListPrice']}")
    
    # Cleanup
    import os
    os.remove(test_file)
    print("   ✓ Test file cleaned up")

def demo_api_structure():
    """Show the API endpoint structure"""
    print("\n" + "="*60)
    print("API ENDPOINTS DEMO")
    print("="*60)
    
    endpoints = [
        ("GET", "/automation/status", "Get scheduler status and statistics"),
        ("POST", "/automation/run", "Manually trigger a download and processing run"),
        ("POST", "/automation/start", "Start the scheduler with optional interval"),
        ("POST", "/automation/stop", "Stop the scheduler"),
        ("GET", "/automation/logs", "Get recent automation run logs"),
        ("GET", "/listings", "Get listings (existing endpoint with Brandon prioritization)"),
        ("GET", "/listings/featured/all", "Get featured listings (existing endpoint)"),
        ("GET", "/health", "Health check (existing endpoint)"),
    ]
    
    print("Available API endpoints:")
    for method, endpoint, description in endpoints:
        print(f"   {method:4} {endpoint:25} - {description}")
    
    print("\nExample usage:")
    print("   curl http://localhost:8000/automation/status")
    print("   curl -X POST http://localhost:8000/automation/run")
    print("   curl http://localhost:8000/automation/logs?limit=5")

def demo_configuration():
    """Show configuration requirements"""
    print("\n" + "="*60)
    print("CONFIGURATION DEMO")
    print("="*60)
    
    print("Required environment variables:")
    
    import os
    required_vars = [
        ("DATABASE_URL", "PostgreSQL connection string"),
        ("MLS_PIN_USERNAME", "MLS PIN website username"),
        ("MLS_PIN_PASSWORD", "MLS PIN website password"),
        ("MLS_PIN_URL", "MLS PIN website URL (optional)"),
    ]
    
    for var, description in required_vars:
        value = os.getenv(var)
        status = "✓ SET" if value else "✗ MISSING"
        masked_value = "***" if value and "PASSWORD" in var else (value or "Not set")
        print(f"   {status} {var:20} - {description}")
        print(f"       Current value: {masked_value}")
    
    print("\nOptional configuration:")
    optional_vars = [
        ("UPSERT_BATCH_SIZE", "100", "Database batch size"),
        ("DEFAULT_INTERVAL_HOURS", "1", "Scheduler interval"),
        ("BRANDON_AGENT_ID", "Cn222505", "Brandon's agent ID"),
    ]
    
    for var, default, description in optional_vars:
        value = os.getenv(var, default)
        print(f"   ✓ {var:25} - {description} (current: {value})")

async def main():
    """Main demo function"""
    print("MLS PIN AUTOMATION SYSTEM DEMO")
    print("Generated at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        # Run all demos
        demo_configuration()
        demo_api_structure()
        demo_processor()
        await demo_downloader()
        await demo_scheduler()
        
        print("\n" + "="*60)
        print("DEMO COMPLETE")
        print("="*60)
        print("\nNext steps:")
        print("1. Set up your .env file with MLS PIN credentials")
        print("2. Update automated_downloader.py with correct website selectors")
        print("3. Start the application: python test.py")
        print("4. Monitor via API endpoints or logs")
        print("5. The system will automatically download and process listings every hour")
        
        print("\nFor more information, see README_AUTOMATION.md")
        
    except Exception as e:
        print(f"\n✗ Demo failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())