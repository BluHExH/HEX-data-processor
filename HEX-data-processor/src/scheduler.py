"""
Scheduler for HEX Data Processor.

Uses APScheduler to manage scheduled data processing jobs.
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

from .logger import get_logger, log_function_call
from .config import SchedulerConfig, JobConfig, IntervalConfig


class JobRunner:
    """Job runner wrapper for async job execution."""
    
    def __init__(self, job_func: Callable, *args, **kwargs):
        """Initialize job runner."""
        self.job_func = job_func
        self.args = args
        self.kwargs = kwargs
        self.logger = get_logger(__name__)
    
    def run(self):
        """Run the job (called by APScheduler)."""
        try:
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Run async job function
                if asyncio.iscoroutinefunction(self.job_func):
                    result = loop.run_until_complete(self.job_func(*self.args, **self.kwargs))
                else:
                    result = self.job_func(*self.args, **self.kwargs)
                
                self.logger.info(f"Job completed successfully: {result}")
                return result
                
            finally:
                loop.close()
                
        except Exception as e:
            self.logger.error(f"Job execution failed: {str(e)}", exc_info=True)
            raise


class SchedulerManager:
    """Manager for scheduled data processing jobs."""
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        """Initialize scheduler manager."""
        self.config = config or SchedulerConfig()
        self.logger = get_logger(__name__)
        
        # Initialize APScheduler
        self.scheduler = BackgroundScheduler(timezone=self.config.timezone)
        
        # Job registry
        self.job_functions: Dict[str, Callable] = {}
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        
        # Statistics
        self.job_stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0
        }
    
    def register_job_function(self, name: str, func: Callable):
        """
        Register a job function that can be scheduled.
        
        Args:
            name: Function name
            func: Function to execute
        """
        self.job_functions[name] = func
        self.logger.info(f"Registered job function: {name}")
    
    @log_function_call()
    def start(self):
        """Start the scheduler."""
        if not self.scheduler.running:
            self.scheduler.start()
            self.logger.info("Scheduler started")
            
            # Add jobs from configuration
            if self.config.jobs:
                for job_config in self.config.jobs:
                    if job_config.enabled:
                        self.add_job_from_config(job_config)
    
    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            self.logger.info("Scheduler stopped")
    
    @log_function_call()
    def add_job_from_config(self, job_config: JobConfig):
        """
        Add job from configuration.
        
        Args:
            job_config: Job configuration
        """
        if job_config.target not in self.job_functions:
            self.logger.error(f"Job function '{job_config.target}' not registered")
            return False
        
        # Create trigger based on type
        trigger = self._create_trigger(job_config)
        if not trigger:
            return False
        
        # Create job runner
        job_func = self.job_functions[job_config.target]
        job_runner = JobRunner(job_func, job_config.target, job_config.id)
        
        try:
            # Add job to scheduler
            job = self.scheduler.add_job(
                func=job_runner.run,
                trigger=trigger,
                id=job_config.id,
                name=job_config.name or job_config.id,
                replace_existing=True
            )
            
            # Track job
            self.active_jobs[job_config.id] = {
                'config': job_config,
                'job': job,
                'created_at': datetime.utcnow(),
                'last_run': None,
                'next_run': job.next_run_time
            }
            
            self.logger.info(f"Added scheduled job: {job_config.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add job '{job_config.id}': {str(e)}")
            return False
    
    def _create_trigger(self, job_config: JobConfig):
        """Create APScheduler trigger from job configuration."""
        try:
            if job_config.trigger == "cron":
                if job_config.cron:
                    return CronTrigger(
                        hour=job_config.cron.hour,
                        minute=job_config.cron.minute,
                        day=job_config.cron.day,
                        month=job_config.cron.month,
                        day_of_week=job_config.cron.day_of_week
                    )
            elif job_config.trigger == "interval":
                if job_config.interval:
                    return IntervalTrigger(
                        weeks=job_config.interval.weeks,
                        days=job_config.interval.days,
                        hours=job_config.interval.hours,
                        minutes=job_config.interval.minutes,
                        seconds=job_config.interval.seconds
                    )
            elif job_config.trigger == "date":
                if job_config.date:
                    run_date = datetime.fromisoformat(job_config.date)
                    return DateTrigger(run_date=run_date)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to create trigger for job '{job_config.id}': {str(e)}")
            return None
    
    def remove_job(self, job_id: str) -> bool:
        """
        Remove a scheduled job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.scheduler.remove_job(job_id)
            
            if job_id in self.active_jobs:
                del self.active_jobs[job_id]
            
            self.logger.info(f"Removed scheduled job: {job_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to remove job '{job_id}': {str(e)}")
            return False
    
    def pause_job(self, job_id: str) -> bool:
        """Pause a scheduled job."""
        try:
            self.scheduler.pause_job(job_id)
            self.logger.info(f"Paused scheduled job: {job_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to pause job '{job_id}': {str(e)}")
            return False
    
    def resume_job(self, job_id: str) -> bool:
        """Resume a paused scheduled job."""
        try:
            self.scheduler.resume_job(job_id)
            self.logger.info(f"Resumed scheduled job: {job_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to resume job '{job_id}': {str(e)}")
            return False
    
    def run_job_now(self, job_id: str) -> bool:
        """Run a scheduled job immediately."""
        if job_id in self.active_jobs:
            job_config = self.active_jobs[job_id]['config']
            if job_config.target in self.job_functions:
                job_func = self.job_functions[job_config.target]
                job_runner = JobRunner(job_func, job_config.target, job_id)
                
                try:
                    job_runner.run()
                    self.logger.info(f"Ran job immediately: {job_id}")
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to run job '{job_id}': {str(e)}")
                    return False
        
        return False
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific job."""
        if job_id in self.active_jobs:
            job_info = self.active_jobs[job_id]
            scheduler_job = job_info['job']
            
            return {
                'id': job_id,
                'name': scheduler_job.name,
                'target': job_info['config'].target,
                'trigger': str(scheduler_job.trigger),
                'next_run_time': scheduler_job.next_run_time.isoformat() if scheduler_job.next_run_time else None,
                'created_at': job_info['created_at'].isoformat(),
                'last_run': job_info.get('last_run'),
                'status': 'active' if scheduler_job else 'removed'
            }
        
        return None
    
    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """Get status of all jobs."""
        jobs = []
        for job_id in self.active_jobs:
            status = self.get_job_status(job_id)
            if status:
                jobs.append(status)
        
        return jobs
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        return {
            'running': self.scheduler.running,
            'timezone': str(self.scheduler.timezone),
            'active_jobs': len(self.active_jobs),
            'total_jobs': len(self.scheduler.get_jobs()),
            'job_stats': self.job_stats.copy()
        }
    
    def update_job_stats(self, success: bool):
        """Update job execution statistics."""
        self.job_stats['total_runs'] += 1
        if success:
            self.job_stats['successful_runs'] += 1
        else:
            self.job_stats['failed_runs'] += 1
    
    def get_upcoming_runs(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get upcoming job runs within specified hours."""
        upcoming = []
        cutoff = datetime.utcnow().timestamp() + (hours * 3600)
        
        for job_id, job_info in self.active_jobs.items():
            scheduler_job = job_info['job']
            if scheduler_job and scheduler_job.next_run_time:
                next_run_ts = scheduler_job.next_run_time.timestamp()
                if next_run_ts <= cutoff:
                    upcoming.append({
                        'job_id': job_id,
                        'name': scheduler_job.name,
                        'next_run': scheduler_job.next_run_time.isoformat(),
                        'hours_until': (next_run_ts - datetime.utcnow().timestamp()) / 3600
                    })
        
        # Sort by next run time
        upcoming.sort(key=lambda x: x['next_run'])
        return upcoming


# Global scheduler instance
_scheduler_manager = None


def get_scheduler_manager() -> SchedulerManager:
    """Get global scheduler manager instance."""
    global _scheduler_manager
    if _scheduler_manager is None:
        _scheduler_manager = SchedulerManager()
    return _scheduler_manager


if __name__ == "__main__":
    # Test scheduler
    async def test_job_function(target: str, job_id: str):
        """Test job function."""
        print(f"Running job: {target} (ID: {job_id})")
        await asyncio.sleep(2)  # Simulate work
        print(f"Job completed: {target}")
        return {"status": "success", "items_processed": 10}
    
    def test_scheduler():
        # Create scheduler
        config = SchedulerConfig(
            timezone="UTC",
            jobs=[
                JobConfig(
                    id="test_job",
                    name="Test Job",
                    target="test_function",
                    trigger="interval",
                    interval=IntervalConfig(seconds=30),
                    enabled=True
                )
            ]
        )
        
        scheduler = SchedulerManager(config)
        
        # Register job function
        scheduler.register_job_function("test_function", test_job_function)
        
        # Start scheduler
        scheduler.start()
        
        try:
            print("Scheduler started. Jobs will run every 30 seconds.")
            print("Press Ctrl+C to stop...")
            
            # Keep running
            import time
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("Stopping scheduler...")
            scheduler.stop()
            print("Scheduler stopped.")
    
    test_scheduler()