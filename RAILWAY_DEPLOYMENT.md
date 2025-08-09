# Railway Deployment Guide for MLS PIN Automation

This guide explains how to deploy the automated MLS PIN downloader to Railway with proper scheduling.

## Overview

The application is configured to run automatically on Railway with:
- **Hourly automated downloads** from MLS PIN website
- **Automatic scheduling** via FastAPI lifespan management
- **Playwright browser automation** for login and file downloads
- **Database integration** with PostgreSQL
- **Health monitoring** and error handling

## Railway Configuration

### 1. Environment Variables

Set these environment variables in your Railway project:

```bash
# Database (Railway will provide this automatically if you add PostgreSQL)
DATABASE_URL=postgresql://username:password@host:port/database

# MLS PIN Credentials (REQUIRED)
MLS_PIN_URL=https://pinergy.mlspin.com
MLS_PIN_USERNAME=your_mlspin_username
MLS_PIN_PASSWORD=your_mlspin_password

# Optional Configuration
UPSERT_BATCH_SIZE=100
DOWNLOAD_TIMEOUT=300
MAX_RETRIES=3
DEFAULT_INTERVAL_HOURS=1
LOG_LEVEL=INFO
```

### 2. Deployment Files

The following files are configured for Railway:

- **`railway.json`** - Railway deployment configuration with Playwright setup
- **`Procfile`** - Process definition for web server
- **`requirements.txt`** - Python dependencies including Playwright

### 3. Automatic Scheduling

The automation runs automatically when deployed to Railway:

1. **On startup**: Initializes database and loads existing data
2. **Every hour**: Downloads new IDX files from MLS PIN
3. **Continuous**: Processes changes and updates database
4. **Daily cleanup**: Removes old files and logs

## How It Works

### 1. Application Startup
```
1. FastAPI app starts with lifespan_scheduler
2. Database tables are created/verified
3. Scheduler starts with 1-hour interval
4. Web server becomes available on Railway URL
```

### 2. Automated Downloads
```
1. Playwright browser launches (headless)
2. Logs into MLS PIN website
3. Downloads available IDX files
4. Processes files for changes
5. Updates database with new/changed listings
6. Cleans up temporary files
```

### 3. API Endpoints

Once deployed, these endpoints are available:

- **`GET /health`** - Health check for Railway
- **`GET /automation/status`** - Check automation status
- **`POST /automation/run`** - Trigger manual download
- **`GET /listings`** - Search listings
- **`GET /automation/logs`** - View recent logs

## Monitoring

### Check Automation Status
```bash
curl https://your-app.railway.app/automation/status
```

### Trigger Manual Run
```bash
curl -X POST https://your-app.railway.app/automation/run
```

### View Logs
```bash
curl https://your-app.railway.app/automation/logs
```

## Troubleshooting

### Common Issues

1. **Login Failures**
   - Verify MLS_PIN_USERNAME and MLS_PIN_PASSWORD
   - Check if account is already logged in elsewhere
   - Ensure credentials have download permissions

2. **Download Timeouts**
   - Increase DOWNLOAD_TIMEOUT environment variable
   - Check MLS PIN website availability
   - Verify network connectivity

3. **Database Errors**
   - Ensure DATABASE_URL is correctly set
   - Check PostgreSQL service is running
   - Verify database permissions

### Railway Logs

View logs in Railway dashboard or via CLI:
```bash
railway logs
```

## Manual Control

While the automation runs automatically, you can control it via API:

```bash
# Stop automation
curl -X POST https://your-app.railway.app/automation/stop

# Start automation with custom interval
curl -X POST "https://your-app.railway.app/automation/start?interval_hours=2"

# Get current status
curl https://your-app.railway.app/automation/status
```

## Security Notes

- Never commit `.env` files with real credentials
- Use Railway's environment variable management
- MLS PIN credentials are encrypted in transit
- Database connections use SSL by default

## Support

If you encounter issues:
1. Check Railway deployment logs
2. Verify environment variables are set
3. Test MLS PIN credentials manually
4. Check `/automation/status` endpoint
5. Review `/automation/logs` for detailed errors