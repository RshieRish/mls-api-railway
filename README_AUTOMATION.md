# MLS PIN Automated Download System

This system automates the process of downloading MLS PIN IDX files and updating the database with new, updated, or sold listings every hour.

## 🚀 Features

- **Automated Downloads**: Uses Playwright to automatically download IDX files from MLS PIN website
- **Intelligent Processing**: Detects changes and performs CRUD operations (Create, Update, Delete/Mark Sold)
- **Hourly Scheduling**: Runs automatically every hour with configurable intervals
- **Change Detection**: Only processes files that have changed since last run
- **Deduplication**: Prevents duplicate listings and handles updates correctly
- **Brandon Prioritization**: Maintains Brandon's listing prioritization (Agent ID: Cn222505)
- **API Monitoring**: RESTful endpoints to monitor and control the automation
- **Error Handling**: Robust error handling with retry logic and logging
- **Status Tracking**: Comprehensive logging and status tracking

## 📋 Prerequisites

- Python 3.8 or higher
- PostgreSQL database
- MLS PIN account with download access
- Required Python packages (see requirements_automation.txt)

## 🛠️ Installation

### 1. Quick Setup (Recommended)

Run the automated setup script:

```bash
python setup_automation.py
```

This will:
- Check Python version compatibility
- Install required dependencies
- Set up Playwright browsers
- Create necessary directories
- Set up environment configuration
- Validate the setup

### 2. Manual Setup

If you prefer manual setup:

```bash
# Install dependencies
pip install -r requirements_automation.txt

# Install Playwright browsers
playwright install chromium

# Create directories
mkdir -p downloads logs backups

# Copy environment template
cp .env.example .env
```

## ⚙️ Configuration

### Environment Variables

Edit the `.env` file with your credentials:

```env
# Database Configuration
DATABASE_URL=postgresql://username:password@localhost:5432/database_name

# MLS PIN Credentials
MLS_PIN_URL=https://www.mlspin.com
MLS_PIN_USERNAME=your_username_here
MLS_PIN_PASSWORD=your_password_here

# Automation Settings
UPSERT_BATCH_SIZE=100
DEFAULT_INTERVAL_HOURS=1

# Brandon's Agent ID
BRANDON_AGENT_ID=Cn222505
```

### MLS PIN Website Setup

**Important**: You need to update the selectors in `automated_downloader.py` to match the actual MLS PIN website structure:

1. Login form selectors
2. Download page navigation
3. File download links

The current selectors are placeholders and need to be customized for the actual MLS PIN website.

## 🚀 Usage

### Starting the Application

The automation system is integrated into the main FastAPI application:

```bash
python test.py
```

This will:
- Start the FastAPI server
- Initialize the scheduler
- Begin hourly automated downloads
- Serve the existing API endpoints
- Provide new automation monitoring endpoints

### API Endpoints

#### Automation Control

- **GET `/automation/status`** - Get scheduler status and statistics
- **POST `/automation/run`** - Manually trigger a download and processing run
- **POST `/automation/start?interval_hours=1`** - Start the scheduler
- **POST `/automation/stop`** - Stop the scheduler
- **GET `/automation/logs?limit=10`** - Get recent automation run logs

#### Example API Usage

```bash
# Check automation status
curl http://localhost:8000/automation/status

# Manually trigger a run
curl -X POST http://localhost:8000/automation/run

# Get recent logs
curl http://localhost:8000/automation/logs?limit=20
```

### Manual Testing

Test the automation system independently:

```bash
# Test the downloader
python -c "import asyncio; from automated_downloader import automated_download_and_process; asyncio.run(automated_download_and_process())"

# Test the scheduler
python scheduler.py
```

## 🔄 How It Works

### 1. Automated Download Process

1. **Login**: Uses Playwright to log into MLS PIN website
2. **Navigate**: Goes to the downloads/exports page
3. **Download**: Downloads all IDX files (SF, MF, CC, RN, BU, LD, CI, MH)
4. **Save**: Stores files in the `downloads/` directory

### 2. Intelligent Processing

1. **Change Detection**: Calculates file hashes to detect changes
2. **Parsing**: Parses IDX files and maps fields to database schema
3. **Comparison**: Compares new data with existing database records
4. **CRUD Operations**:
   - **Create**: Add new listings
   - **Update**: Update changed listings
   - **Mark Sold**: Mark listings no longer in files as sold

### 3. Database Operations

- **Batch Processing**: Processes records in configurable batches
- **Deduplication**: Uses `listing_key` as unique identifier
- **Status Tracking**: Maintains listing status and modification timestamps
- **Brandon Prioritization**: Ensures Brandon's listings remain at the top

### 4. Scheduling

- **Hourly Runs**: Configurable interval (default: 1 hour)
- **Overlap Prevention**: Prevents multiple runs from overlapping
- **Error Recovery**: Continues running even if individual runs fail
- **Daily Cleanup**: Removes old files and resets error counters

## 📊 Monitoring

### Status Information

The automation system provides comprehensive status information:

```json
{
  "is_running": true,
  "last_run": "2024-01-15T10:00:00",
  "last_run_status": "success",
  "next_run": "2024-01-15T11:00:00",
  "stats": {
    "total_runs": 24,
    "successful_runs": 23,
    "failed_runs": 1,
    "average_duration": 45.2,
    "last_success": "2024-01-15T10:00:00",
    "last_failure": "2024-01-15T08:00:00"
  }
}
```

### Log Files

- Application logs: Console output and log files
- Run history: Stored in scheduler statistics
- File processing logs: Tracks which files were processed

## 🔧 Customization

### MLS PIN Website Selectors

Update `automated_downloader.py` with the correct selectors for your MLS PIN website:

```python
# Login form selectors
await self.page.fill("input[name='username']", MLS_PIN_USERNAME)
await self.page.fill("input[name='password']", MLS_PIN_PASSWORD)

# Download links
await self.page.click(f"a[href*='{idx_file['name']}']")
```

### Field Mapping

Customize field mapping in the `parse_idx_file` method:

```python
listing = {
    "ListingKey": row.get("LIST_NO"),
    "ListPrice": row.get("LIST_PRICE"),
    # Add or modify field mappings as needed
}
```

### Scheduling

Modify scheduling intervals in `scheduler.py`:

```python
# Change default interval
scheduler.start(interval_hours=2)  # Run every 2 hours

# Add custom schedules
scheduler.add_job(
    custom_job,
    trigger=CronTrigger(hour=6, minute=0),  # Run daily at 6 AM
    id='daily_report'
)
```

## 🚨 Troubleshooting

### Common Issues

1. **Login Failures**
   - Verify MLS PIN credentials in `.env`
   - Check if website selectors need updating
   - Ensure account has download permissions

2. **Download Failures**
   - Update download page selectors
   - Check network connectivity
   - Verify file download permissions

3. **Database Errors**
   - Verify database connection string
   - Check database permissions
   - Ensure tables exist (run `init_db()`)

4. **Scheduler Issues**
   - Check for port conflicts
   - Verify system resources
   - Review error logs

### Debug Mode

Enable debug logging:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

Run with visible browser for debugging:

```python
# In automated_downloader.py
self.browser = await self.playwright.chromium.launch(headless=False)
```

## 📈 Performance

### Optimization Tips

1. **Batch Size**: Adjust `UPSERT_BATCH_SIZE` based on your system
2. **Intervals**: Balance between freshness and system load
3. **File Cleanup**: Regular cleanup prevents disk space issues
4. **Database Indexing**: Ensure proper indexes on `listing_key` and `updated_at`

### Resource Usage

- **Memory**: ~100-200MB during processing
- **CPU**: Moderate during downloads and processing
- **Disk**: Temporary storage for IDX files
- **Network**: Downloads depend on file sizes

## 🔒 Security

### Best Practices

1. **Credentials**: Store in `.env` file, never commit to version control
2. **Database**: Use connection pooling and proper permissions
3. **Browser**: Run in headless mode in production
4. **Logs**: Avoid logging sensitive information
5. **Network**: Use HTTPS and secure connections

### Production Deployment

```env
# Production settings
HEADLESS_BROWSER=true
DISABLE_BROWSER_SANDBOX=false
LOG_LEVEL=INFO
```

## 📝 License

MIT License - See LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For issues and questions:

1. Check the troubleshooting section
2. Review the logs for error details
3. Test individual components
4. Contact the development team

---

**Note**: This automation system requires proper MLS PIN website access and credentials. The selectors and navigation logic may need to be updated based on the actual MLS PIN website structure.