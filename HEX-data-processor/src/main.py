"""
Main entry point for HEX Data Processor.

Coordinates all components for data processing operations.
"""

import time
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

from .config import Config, load_config
from .logger import setup_logging, get_logger
from .http_client import HTTPClient
from .scraper import Scraper
from .cleaner import DataCleaner
from .transformer import DataTransformer
from .storage import create_storage_adapter
from .notifier import NotificationManager
from .metrics import get_metrics_collector


class DataProcessor:
    """Main data processor that coordinates all components."""
    
    def __init__(self, config: Config):
        """
        Initialize data processor.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.logger = get_logger(__name__)
        
        # Setup logging
        setup_logging(
            level=self.config.logging.level,
            format_type=self.config.logging.format,
            log_file=self.config.logging.file,
            console=self.config.logging.console
        )
        
        # Initialize components
        self._setup_components()
        
        # Metrics
        self.metrics = get_metrics_collector()
        self.metrics.set_config_info({
            'project': self.config.project.name,
            'version': self.config.project.version,
            'targets': list(self.config.targets.keys()),
            'storage_type': self.config.storage.type
        })
        
        self.logger.info(f"DataProcessor initialized: {self.config.project.name} v{self.config.project.version}")
    
    def _setup_components(self):
        """Setup all processing components."""
        # HTTP client
        self.http_client = HTTPClient(
            timeout=self.config.scraper.timeout,
            max_retries=self.config.scraper.max_retries,
            rate_limit=self.config.scraper.rate_limit,
            user_agent=self.config.scraper.user_agent,
            headers=self.config.scraper.headers,
            max_concurrent=self.config.scraper.max_concurrent
        )
        
        # Scraper
        self.scraper = Scraper(self.http_client)
        
        # Cleaner
        self.cleaner = DataCleaner(self.config.cleaner)
        
        # Transformer
        self.transformer = DataTransformer(self.config.transformer)
        
        # Storage
        self._setup_storage()
        
        # Notifications
        self.notifier = NotificationManager(self.config.notifications) if self.config.notifications else None
    
    def _setup_storage(self):
        """Setup storage adapter based on configuration."""
        storage_config = self.config.storage
        
        if storage_config.type == "csv":
            filename = self._generate_output_filename("csv")
            self.storage = create_storage_adapter(
                "csv",
                filename=filename,
                delimiter=storage_config.csv.delimiter if storage_config.csv else ",",
                quoting=storage_config.csv.quoting if storage_config.csv else 1,
                encoding=storage_config.csv.encoding if storage_config.csv else "utf-8"
            )
        elif storage_config.type == "jsonl":
            filename = self._generate_output_filename("jsonl")
            self.storage = create_storage_adapter(
                "jsonl",
                filename=filename,
                encoding=storage_config.jsonl.encoding if storage_config.jsonl else "utf-8",
                ensure_ascii=storage_config.jsonl.ensure_ascii if storage_config.jsonl else False
            )
        elif storage_config.type == "sqlite":
            database_url = storage_config.sqlite.database_url if storage_config.sqlite else "sqlite:///./data/output/hex_processor.db"
            table_name = storage_config.sqlite.table_name if storage_config.sqlite else "scraped_data"
            self.storage = create_storage_adapter(
                "sqlite",
                database_url=database_url,
                table_name=table_name
            )
        else:
            raise ValueError(f"Unsupported storage type: {storage_config.type}")
    
    def _generate_output_filename(self, extension: str) -> str:
        """Generate output filename based on configuration."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        template = self.config.storage.filename_template
        
        filename = template.format(
            timestamp=timestamp,
            extension=extension,
            target_name="processed"
        )
        
        return str(Path(self.config.storage.path) / filename)
    
    async def run_target_once(self, target_name: str, dry_run: bool = False) -> Dict[str, Any]:
        """
        Run data processing for a single target once.
        
        Args:
            target_name: Name of target to process
            dry_run: If True, don't save data
            
        Returns:
            Dictionary with processing results
        """
        run_id = f"{target_name}_{int(time.time())}"
        start_time = time.time()
        
        self.logger.info(f"Starting processing run {run_id} for target: {target_name}")
        
        # Update metrics
        self.metrics.set_active_runs(1, target_name)
        
        try:
            # Get target configuration
            target_config = self.config.get_target(target_name)
            
            # Step 1: Scrape data
            self.logger.info(f"Scraping data from {target_name}")
            with self.metrics.start_scraping_timer(target_name):
                scraped_items = await self.scraper.scrape_target(target_config, run_id)
            
            self.metrics.record_scraped_items(len(scraped_items), target_name, run_id)
            self.logger.info(f"Scraped {len(scraped_items)} items")
            
            # Step 2: Clean data
            self.logger.info(f"Cleaning {len(scraped_items)} items")
            with self.metrics.start_processing_timer("cleaning"):
                cleaned_items = self.cleaner.clean_data(scraped_items)
            
            self.metrics.record_processed_items(len(cleaned_items), target_name, run_id)
            self.logger.info(f"Cleaned {len(cleaned_items)} items")
            
            # Step 3: Transform data
            self.logger.info(f"Transforming {len(cleaned_items)} items")
            with self.metrics.start_processing_timer("transformation"):
                transformed_items = self.transformer.transform_data(cleaned_items)
            
            self.logger.info(f"Transformed {len(transformed_items)} items")
            
            # Step 4: Save data
            saved_items = []
            output_path = None
            
            if not dry_run and transformed_items:
                self.logger.info(f"Saving {len(transformed_items)} items")
                with self.metrics.start_storage_timer(self.config.storage.type):
                    save_success = await self.storage.save(transformed_items)
                
                if save_success:
                    saved_items = transformed_items
                    self.metrics.record_saved_items(
                        len(saved_items), 
                        self.config.storage.type, 
                        target_name, 
                        run_id
                    )
                    
                    # Get output path
                    storage_info = self.storage.get_storage_info()
                    if 'filename' in storage_info:
                        output_path = storage_info['filename']
                    elif 'database_path' in storage_info:
                        output_path = storage_info['database_path']
                    
                    self.logger.info(f"Saved {len(saved_items)} items to {output_path}")
                else:
                    self.logger.error("Failed to save data")
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Prepare stats
            stats = {
                'run_id': run_id,
                'target': target_name,
                'duration': f"{duration:.2f}s",
                'scraped_count': len(scraped_items),
                'processed_count': len(cleaned_items),
                'saved_count': len(saved_items),
                'output_path': output_path,
                'dry_run': dry_run
            }
            
            # Send success notifications
            if self.notifier:
                notification_results = await self.notifier.send_success_notifications(run_id, stats)
                self.logger.info(f"Notifications sent: {notification_results}")
            
            # Update metrics
            self.metrics.update_last_run(target_name)
            
            # Log completion
            self.logger.info(
                f"Processing run {run_id} completed successfully",
                extra=stats
            )
            
            return {
                'success': True,
                'run_id': run_id,
                'stats': stats,
                'items': saved_items if not dry_run else transformed_items
            }
            
        except Exception as e:
            # Calculate duration
            duration = time.time() - start_time
            
            # Prepare error context
            error_context = {
                'target': target_name,
                'start_time': datetime.utcnow().isoformat() + 'Z',
                'duration': f"{duration:.2f}s"
            }
            
            # Record error metrics
            self.metrics.record_failed_items(1, "processing", target_name, run_id)
            self.metrics.update_last_run(target_name)
            
            # Send error notifications
            if self.notifier:
                notification_results = await self.notifier.send_error_notifications(
                    run_id, str(e), error_context
                )
                self.logger.info(f"Error notifications sent: {notification_results}")
            
            # Log error
            self.logger.error(
                f"Processing run {run_id} failed: {str(e)}",
                extra={
                    'run_id': run_id,
                    'target': target_name,
                    'error': str(e),
                    'duration': f"{duration:.2f}s"
                },
                exc_info=True
            )
            
            return {
                'success': False,
                'run_id': run_id,
                'error': str(e),
                'context': error_context
            }
            
        finally:
            # Update metrics
            self.metrics.set_active_runs(0, target_name)
    
    async def run_all_targets(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Run data processing for all configured targets.
        
        Args:
            dry_run: If True, don't save data
            
        Returns:
            Dictionary with combined results
        """
        results = {}
        total_stats = {
            'runs_completed': 0,
            'runs_failed': 0,
            'total_items_saved': 0,
            'total_duration': 0.0
        }
        
        self.logger.info(f"Starting processing for all targets: {list(self.config.targets.keys())}")
        
        for target_name in self.config.targets.keys():
            try:
                result = await self.run_target_once(target_name, dry_run)
                results[target_name] = result
                
                if result['success']:
                    total_stats['runs_completed'] += 1
                    total_stats['total_items_saved'] += result['stats'].get('saved_count', 0)
                    duration_str = result['stats'].get('duration', '0s').replace('s', '')
                    total_stats['total_duration'] += float(duration_str)
                else:
                    total_stats['runs_failed'] += 1
                    
            except Exception as e:
                self.logger.error(f"Failed to process target {target_name}: {str(e)}")
                results[target_name] = {
                    'success': False,
                    'error': str(e)
                }
                total_stats['runs_failed'] += 1
        
        # Summary
        total_stats['average_duration'] = (
            total_stats['total_duration'] / total_stats['runs_completed'] 
            if total_stats['runs_completed'] > 0 else 0
        )
        
        self.logger.info(
            f"All targets processing completed",
            extra={
                'completed': total_stats['runs_completed'],
                'failed': total_stats['runs_failed'],
                'total_saved': total_stats['total_items_saved']
            }
        )
        
        return {
            'results': results,
            'summary': total_stats
        }
    
    async def cleanup(self):
        """Cleanup resources."""
        self.logger.info("Cleaning up resources...")
        
        if self.http_client:
            await self.http_client.close()
        
        if self.storage:
            await self.storage.close()
        
        self.logger.info("Cleanup completed")
    
    def get_system_info(self) -> Dict[str, Any]:
        """Get system information."""
        return {
            'project': {
                'name': self.config.project.name,
                'version': self.config.project.version,
                'description': self.config.project.description
            },
            'targets': list(self.config.targets.keys()),
            'storage': {
                'type': self.config.storage.type,
                'path': self.config.storage.path
            },
            'metrics_enabled': self.config.metrics.enabled if self.config.metrics else False,
            'notifications_enabled': self.config.notifications.enabled if self.config.notifications else False,
            'scheduler_enabled': self.config.scheduler.enabled if self.config.scheduler else False
        }


async def main():
    """Main function for standalone execution."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.main <config_file> [target_name]")
        sys.exit(1)
    
    config_path = sys.argv[1]
    target_name = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Create processor
        processor = DataProcessor(config)
        
        # Run processing
        if target_name:
            result = await processor.run_target_once(target_name)
        else:
            result = await processor.run_all_targets()
        
        print(f"Processing completed: {result}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    
    finally:
        if 'processor' in locals():
            await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main())