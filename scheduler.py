#!/usr/bin/env python3
"""
Scheduler for Automated MLS PIN Downloads

This module provides:
1. Hourly scheduling for automated downloads
2. Background task management
3. Integration with FastAPI application
4. Error handling and retry logic
5. Monitoring and logging

Author: Brandon RE Team
License: MIT
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import threading
import time
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from automated_downloader import automated_download_and_process

# Logging setup
log = logging.getLogger("scheduler")

class MLSScheduler:
    """Manages scheduled downloads and processing"""
    
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.is_running = False
        self.last_run: Optional[datetime] = None
        self.last_run_status: Optional[str] = None
        self.run_count = 0
        self.error_count = 0
        self.stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_success': None,
            'last_failure': None,
            'average_duration': 0,
            'run_history': []
        }
        
    def _job_listener(self, event):
        """Listen to job execution events"""
        if event.exception:
            log.error(f"Job failed: {event.exception}")
            self.error_count += 1
            self.last_run_status = "failed"
            self.stats['failed_runs'] += 1
            self.stats['last_failure'] = datetime.now().isoformat()
        else:
            log.info("Job completed successfully")
            self.last_run_status = "success"
            self.stats['successful_runs'] += 1
            self.stats['last_success'] = datetime.now().isoformat()
            
        self.stats['total_runs'] += 1
        self.last_run = datetime.now()
        
    async def _scheduled_job(self):
        """The actual job that gets scheduled"""
        start_time = datetime.now()
        
        try:
            log.info("Starting scheduled MLS PIN download and processing")
            
            # Run the automated download and process
            await automated_download_and_process()
            
            # Calculate duration
            duration = (datetime.now() - start_time).total_seconds()
            
            # Update stats
            self.stats['run_history'].append({
                'timestamp': start_time.isoformat(),
                'duration': duration,
                'status': 'success'
            })
            
            # Keep only last 50 runs in history
            if len(self.stats['run_history']) > 50:
                self.stats['run_history'] = self.stats['run_history'][-50:]
                
            # Update average duration
            successful_runs = [r for r in self.stats['run_history'] if r['status'] == 'success']
            if successful_runs:
                self.stats['average_duration'] = sum(r['duration'] for r in successful_runs) / len(successful_runs)
                
            log.info(f"Scheduled job completed in {duration:.2f} seconds")
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            
            # Update stats for failure
            self.stats['run_history'].append({
                'timestamp': start_time.isoformat(),
                'duration': duration,
                'status': 'failed',
                'error': str(e)
            })
            
            log.error(f"Scheduled job failed after {duration:.2f} seconds: {e}")
            raise
            
    def start(self, interval_hours: int = 1):
        """Start the scheduler"""
        if self.is_running:
            log.warning("Scheduler is already running")
            return
            
        try:
            self.scheduler = AsyncIOScheduler()
            
            # Add job listener
            self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
            
            # Schedule the job to run every hour
            self.scheduler.add_job(
                self._scheduled_job,
                trigger=IntervalTrigger(hours=interval_hours),
                id='mls_download_job',
                name='MLS PIN Download and Process',
                max_instances=1,  # Prevent overlapping runs
                coalesce=True,    # Combine missed runs
                misfire_grace_time=300  # 5 minutes grace period
            )
            
            # Also schedule a daily cleanup job at 2 AM
            self.scheduler.add_job(
                self._cleanup_job,
                trigger=CronTrigger(hour=2, minute=0),
                id='daily_cleanup_job',
                name='Daily Cleanup',
                max_instances=1
            )
            
            self.scheduler.start()
            self.is_running = True
            
            log.info(f"Scheduler started - MLS downloads will run every {interval_hours} hour(s)")
            log.info("Next run scheduled for: " + str(self.get_next_run_time()))
            
        except Exception as e:
            log.error(f"Failed to start scheduler: {e}")
            raise
            
    def stop(self):
        """Stop the scheduler"""
        if not self.is_running:
            log.warning("Scheduler is not running")
            return
            
        try:
            if self.scheduler:
                self.scheduler.shutdown(wait=True)
                self.scheduler = None
                
            self.is_running = False
            log.info("Scheduler stopped")
            
        except Exception as e:
            log.error(f"Error stopping scheduler: {e}")
            
    async def run_now(self) -> Dict[str, Any]:
        """Manually trigger a download and processing run"""
        if not self.is_running:
            return {"error": "Scheduler is not running"}
            
        try:
            log.info("Manual run triggered")
            await self._scheduled_job()
            return {"status": "success", "message": "Manual run completed successfully"}
            
        except Exception as e:
            error_msg = f"Manual run failed: {e}"
            log.error(error_msg)
            return {"status": "error", "message": error_msg}
            
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        return {
            "is_running": self.is_running,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_run_status": self.last_run_status,
            "next_run": self.get_next_run_time().isoformat() if self.get_next_run_time() else None,
            "run_count": self.run_count,
            "error_count": self.error_count,
            "stats": self.stats
        }
        
    def get_next_run_time(self) -> Optional[datetime]:
        """Get the next scheduled run time"""
        if not self.scheduler:
            return None
            
        job = self.scheduler.get_job('mls_download_job')
        if job:
            return job.next_run_time
        return None
        
    async def _cleanup_job(self):
        """Daily cleanup job"""
        try:
            log.info("Running daily cleanup")
            
            # Clean up old download files (keep last 7 days)
            from pathlib import Path
            import os
            
            download_dir = Path(os.path.dirname(__file__)) / "downloads"
            if download_dir.exists():
                cutoff_time = datetime.now() - timedelta(days=7)
                
                for file_path in download_dir.glob("*.txt"):
                    if file_path.stat().st_mtime < cutoff_time.timestamp():
                        file_path.unlink()
                        log.info(f"Cleaned up old file: {file_path}")
                        
            # Reset error count daily
            self.error_count = 0
            
            log.info("Daily cleanup completed")
            
        except Exception as e:
            log.error(f"Daily cleanup failed: {e}")

# Global scheduler instance
_scheduler_instance: Optional[MLSScheduler] = None

def get_scheduler() -> MLSScheduler:
    """Get the global scheduler instance"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = MLSScheduler()
    return _scheduler_instance

@asynccontextmanager
async def lifespan_scheduler(app):
    """FastAPI lifespan context manager for scheduler"""
    scheduler = get_scheduler()
    
    # Start scheduler on startup
    try:
        scheduler.start(interval_hours=1)  # Run every hour
        log.info("Scheduler started with application")
        yield
    finally:
        # Stop scheduler on shutdown
        scheduler.stop()
        log.info("Scheduler stopped with application")

# Convenience functions for FastAPI integration
async def start_scheduler(interval_hours: int = 1):
    """Start the scheduler (for manual control)"""
    scheduler = get_scheduler()
    scheduler.start(interval_hours)
    
async def stop_scheduler():
    """Stop the scheduler (for manual control)"""
    scheduler = get_scheduler()
    scheduler.stop()
    
async def get_scheduler_status() -> Dict[str, Any]:
    """Get scheduler status (for API endpoints)"""
    scheduler = get_scheduler()
    return scheduler.get_status()
    
async def trigger_manual_run() -> Dict[str, Any]:
    """Trigger a manual run (for API endpoints)"""
    scheduler = get_scheduler()
    return await scheduler.run_now()

if __name__ == "__main__":
    # For testing - run scheduler for a few minutes
    async def test_scheduler():
        scheduler = MLSScheduler()
        scheduler.start(interval_hours=1)
        
        # Wait for a few seconds to see if it works
        await asyncio.sleep(10)
        
        # Trigger a manual run
        result = await scheduler.run_now()
        print(f"Manual run result: {result}")
        
        # Check status
        status = scheduler.get_status()
        print(f"Scheduler status: {status}")
        
        scheduler.stop()
        
    asyncio.run(test_scheduler())