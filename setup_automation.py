#!/usr/bin/env python3
"""
Setup Script for MLS PIN Automation

This script helps set up the automated MLS PIN download system by:
1. Installing required dependencies
2. Setting up Playwright browsers
3. Creating necessary directories
4. Validating configuration
5. Testing the automation system

Usage:
    python setup_automation.py

Author: Brandon RE Team
License: MIT
"""

import os
import sys
import subprocess
import asyncio
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def run_command(command, description):
    """Run a shell command with error handling"""
    log.info(f"Running: {description}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        log.info(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"✗ {description} failed: {e}")
        log.error(f"Error output: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    log.info("Checking Python version...")
    if sys.version_info < (3, 8):
        log.error("Python 3.8 or higher is required")
        return False
    log.info(f"✓ Python {sys.version} is compatible")
    return True

def install_dependencies():
    """Install required Python packages"""
    log.info("Installing Python dependencies...")
    
    # Check if requirements_automation.txt exists
    req_file = Path(__file__).parent / "requirements_automation.txt"
    if not req_file.exists():
        log.error("requirements_automation.txt not found")
        return False
    
    # Install dependencies
    return run_command(
        f"pip install -r {req_file}",
        "Installing Python packages"
    )

def setup_playwright():
    """Install Playwright browsers"""
    log.info("Setting up Playwright browsers...")
    return run_command(
        "playwright install chromium",
        "Installing Playwright Chromium browser"
    )

def create_directories():
    """Create necessary directories"""
    log.info("Creating necessary directories...")
    
    base_dir = Path(__file__).parent
    directories = [
        base_dir / "downloads",
        base_dir / "logs",
        base_dir / "backups"
    ]
    
    for directory in directories:
        try:
            directory.mkdir(exist_ok=True)
            log.info(f"✓ Created directory: {directory}")
        except Exception as e:
            log.error(f"✗ Failed to create directory {directory}: {e}")
            return False
    
    return True

def setup_environment():
    """Set up environment configuration"""
    log.info("Setting up environment configuration...")
    
    base_dir = Path(__file__).parent
    env_example = base_dir / ".env.example"
    env_file = base_dir / ".env"
    
    if not env_file.exists() and env_example.exists():
        try:
            # Copy .env.example to .env
            with open(env_example, 'r') as src, open(env_file, 'w') as dst:
                dst.write(src.read())
            log.info("✓ Created .env file from .env.example")
            log.warning("⚠️  Please edit .env file with your MLS PIN credentials")
        except Exception as e:
            log.error(f"✗ Failed to create .env file: {e}")
            return False
    elif env_file.exists():
        log.info("✓ .env file already exists")
    else:
        log.warning("⚠️  No .env.example file found")
    
    return True

def validate_configuration():
    """Validate the configuration"""
    log.info("Validating configuration...")
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        required_vars = [
            "DATABASE_URL",
            "MLS_PIN_USERNAME", 
            "MLS_PIN_PASSWORD"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            log.warning(f"⚠️  Missing environment variables: {', '.join(missing_vars)}")
            log.warning("Please update your .env file with the required credentials")
            return False
        
        log.info("✓ Configuration validation passed")
        return True
        
    except ImportError:
        log.error("✗ python-dotenv not installed")
        return False
    except Exception as e:
        log.error(f"✗ Configuration validation failed: {e}")
        return False

async def test_automation():
    """Test the automation system"""
    log.info("Testing automation system...")
    
    try:
        # Test database connection
        log.info("Testing database connection...")
        from test import get_conn
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                if result:
                    log.info("✓ Database connection successful")
                else:
                    log.error("✗ Database connection failed")
                    return False
        
        # Test scheduler import
        log.info("Testing scheduler import...")
        from scheduler import MLSScheduler
        scheduler = MLSScheduler()
        log.info("✓ Scheduler import successful")
        
        # Test downloader import
        log.info("Testing downloader import...")
        from automated_downloader import MLSPinDownloader
        log.info("✓ Downloader import successful")
        
        log.info("✓ All automation tests passed")
        return True
        
    except Exception as e:
        log.error(f"✗ Automation test failed: {e}")
        return False

def print_next_steps():
    """Print next steps for the user"""
    log.info("\n" + "="*60)
    log.info("SETUP COMPLETE - NEXT STEPS:")
    log.info("="*60)
    log.info("1. Edit the .env file with your MLS PIN credentials:")
    log.info("   - MLS_PIN_USERNAME=your_username")
    log.info("   - MLS_PIN_PASSWORD=your_password")
    log.info("   - DATABASE_URL=your_database_url")
    log.info("")
    log.info("2. Test the automation system:")
    log.info("   python -c 'import asyncio; from automated_downloader import automated_download_and_process; asyncio.run(automated_download_and_process())'")
    log.info("")
    log.info("3. Start the application with automation:")
    log.info("   python test.py")
    log.info("")
    log.info("4. Monitor automation via API endpoints:")
    log.info("   - GET /automation/status")
    log.info("   - POST /automation/run")
    log.info("   - GET /automation/logs")
    log.info("")
    log.info("5. The system will automatically download and process")
    log.info("   MLS PIN files every hour once started.")
    log.info("="*60)

def main():
    """Main setup function"""
    log.info("Starting MLS PIN Automation Setup...")
    log.info("="*60)
    
    steps = [
        ("Checking Python version", check_python_version),
        ("Installing dependencies", install_dependencies),
        ("Setting up Playwright", setup_playwright),
        ("Creating directories", create_directories),
        ("Setting up environment", setup_environment),
        ("Validating configuration", validate_configuration),
    ]
    
    failed_steps = []
    
    for step_name, step_func in steps:
        log.info(f"\nStep: {step_name}")
        if not step_func():
            failed_steps.append(step_name)
            log.error(f"Step '{step_name}' failed")
        else:
            log.info(f"Step '{step_name}' completed successfully")
    
    # Test automation (async step)
    log.info("\nStep: Testing automation system")
    try:
        if not asyncio.run(test_automation()):
            failed_steps.append("Testing automation system")
    except Exception as e:
        log.error(f"Testing automation system failed: {e}")
        failed_steps.append("Testing automation system")
    
    # Summary
    log.info("\n" + "="*60)
    if failed_steps:
        log.error(f"Setup completed with {len(failed_steps)} failed steps:")
        for step in failed_steps:
            log.error(f"  ✗ {step}")
        log.error("Please resolve the issues above before proceeding.")
        return False
    else:
        log.info("✓ Setup completed successfully!")
        print_next_steps()
        return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)