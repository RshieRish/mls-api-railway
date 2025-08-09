#!/usr/bin/env python3

import asyncio
import os
from playwright.async_api import async_playwright
from dotenv import load_dotenv

load_dotenv()

MLS_PIN_URL = os.getenv("MLS_PIN_URL", "https://pinergy.mlspin.com")
MLS_PIN_USERNAME = os.getenv("MLS_PIN_USERNAME")
MLS_PIN_PASSWORD = os.getenv("MLS_PIN_PASSWORD")

async def check_available_property_types():
    """Check what property types are available for download on MLS PIN"""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        try:
            print("🔍 Navigating to MLS PIN...")
            await page.goto(MLS_PIN_URL)
            await page.wait_for_load_state('networkidle')
            
            # Handle cookie consent if present
            try:
                cookie_button = page.locator('button:has-text("Accept")')
                if await cookie_button.is_visible(timeout=3000):
                    await cookie_button.click()
                    print("✅ Accepted cookies")
            except:
                pass
            
            print("🔑 Logging in...")
            await page.fill('input[name="username"]', MLS_PIN_USERNAME)
            await page.fill('input[name="password"]', MLS_PIN_PASSWORD)
            await page.click('button[type="submit"]')
            
            # Wait for login to complete
            await page.wait_for_load_state('networkidle')
            
            print("📁 Navigating to IDX downloads...")
            # Look for IDX or download links
            await page.click('text="IDX"')
            await page.wait_for_load_state('networkidle')
            
            # Look for property type options
            print("🏠 Checking available property types...")
            
            # Get all download links or property type options
            links = await page.locator('a').all()
            property_types = []
            
            for link in links:
                text = await link.inner_text()
                href = await link.get_attribute('href')
                if 'idx_' in str(href) or any(prop in text.lower() for prop in ['residential', 'condo', 'land', 'commercial', 'rental', 'business']):
                    property_types.append(f"Text: '{text}' | Href: {href}")
            
            print("\n📋 Available property types/downloads:")
            for prop_type in property_types:
                print(f"  - {prop_type}")
            
            # Also check for any select dropdowns
            selects = await page.locator('select').all()
            for select in selects:
                options = await select.locator('option').all()
                if options:
                    print(f"\n📋 Select dropdown options:")
                    for option in options:
                        value = await option.get_attribute('value')
                        text = await option.inner_text()
                        print(f"  - Value: {value} | Text: {text}")
            
            # Take a screenshot for manual inspection
            await page.screenshot(path='mls_pin_property_types.png')
            print("\n📸 Screenshot saved as 'mls_pin_property_types.png'")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            await page.screenshot(path='mls_pin_error.png')
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(check_available_property_types())