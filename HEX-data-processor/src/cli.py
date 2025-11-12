"""
Command Line Interface for HEX Data Processor.

Provides Click-based CLI for running and managing data processing operations.
"""

import os
import sys
import time
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any
import click

from .config import load_config, Config
from .logger import setup_logging, get_logger
from .main import DataProcessor
from .scheduler import get_scheduler_manager
from .api import DataProcessorAPI


@click.group()
@click.option(
    '--config', '-c',
    default='config.json',
    help='Configuration file path',
    type=click.Path(exists=True)
)
@click.option(
    '--log-level',
    default='INFO',
    help='Logging level',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])
)
@click.option(
    '--log-format',
    default='json',
    help='Log format',
    type=click.Choice(['json', 'text'])
)
@click.pass_context
def cli(ctx, config, log_level, log_format):
    """HEX Data Processor - Production-ready data processing tool."""
    # Ensure context exists
    ctx.ensure_object(dict)
    
    # Store global options in context
    ctx.obj['config_path'] = config
    ctx.obj['log_level'] = log_level
    ctx.obj['log_format'] = log_format
    
    # Setup logging
    setup_logging(
        level=log_level,
        format_type=log_format,
        console=True
    )


@cli.command()
@click.argument('target', required=True)
@click.option(
    '--once',
    is_flag=True,
    help='Run only once (no scheduling)'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Perform a dry run without saving data'
)
@click.option(
    '--output-format',
    default='csv',
    help='Output format',
    type=click.Choice(['csv', 'jsonl', 'sqlite'])
)
@click.option(
    '--output-path',
    help='Custom output path'
)
@click.pass_context
def run(ctx, target, once, dry_run, output_format, output_path):
    """Run data processing for a specific target."""
    config_path = ctx.obj['config_path']
    log_level = ctx.obj['log_level']
    log_format = ctx.obj['log_format']
    
    logger = get_logger(__name__)
    
    try:
        # Load configuration
        config = load_config(config_path)
        
        # Override output settings if specified
        if output_format:
            config.storage.type = output_format
        if output_path:
            config.storage.path = output_path
        
        # Create processor
        processor = DataProcessor(config)
        
        if once:
            # Run once
            logger.info(f"Running single processing job for target: {target}")
            
            # Run in async context
            result = asyncio.run(processor.run_target_once(target, dry_run=dry_run))
            
            if result['success']:
                logger.info(f"Processing completed successfully: {result}")
                click.echo(f"✅ Processing completed successfully")
                click.echo(f"📊 Items processed: {result.get('stats', {}).get('processed_count', 0)}")
                click.echo(f"💾 Items saved: {result.get('stats', {}).get('saved_count', 0)}")
                click.echo(f"📁 Output: {result.get('stats', {}).get('output_path', 'N/A')}")
            else:
                logger.error(f"Processing failed: {result.get('error', 'Unknown error')}")
                click.echo(f"❌ Processing failed: {result.get('error', 'Unknown error')}", err=True)
                sys.exit(1)
        else:
            # Run with scheduler
            logger.info(f"Starting scheduled processing for target: {target}")
            
            # Setup scheduler
            scheduler = get_scheduler_manager()
            
            # Register processing function
            async def process_job(target_name: str, job_id: str):
                return await processor.run_target_once(target_name)
            
            scheduler.register_job_function(target, process_job)
            
            # Create job config
            from .config import JobConfig, IntervalConfig
            job_config = JobConfig(
                id=f"manual_{target}_{int(time.time())}",
                name=f"Manual processing for {target}",
                target=target,
                trigger="interval",
                interval=IntervalConfig(minutes=30),
                enabled=True
            )
            
            # Add and start job
            scheduler.add_job_from_config(job_config)
            scheduler.start()
            
            click.echo(f"🚀 Started scheduled processing for {target}")
            click.echo("Press Ctrl+C to stop...")
            
            try:
                # Keep running
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                click.echo("\n🛑 Stopping scheduler...")
                scheduler.stop()
                click.echo("✅ Scheduler stopped")
                
    except Exception as e:
        logger.error(f"CLI error: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def validate_config(ctx):
    """Validate configuration file."""
    config_path = ctx.obj['config_path']
    logger = get_logger(__name__)
    
    try:
        config = load_config(config_path)
        
        # Basic validation
        is_valid = config.validate()
        
        if is_valid:
            click.echo(f"✅ Configuration is valid")
            click.echo(f"📋 Project: {config.project.name} v{config.project.version}")
            click.echo(f"🎯 Targets: {', '.join(config.targets.keys())}")
            click.echo(f"💾 Storage: {config.storage.type}")
            click.echo(f"📊 Metrics: {'Enabled' if config.metrics and config.metrics.enabled else 'Disabled'}")
            click.echo(f"🔔 Notifications: {'Enabled' if config.notifications and config.notifications.enabled else 'Disabled'}")
        else:
            click.echo(f"❌ Configuration validation failed", err=True)
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Configuration validation error: {str(e)}")
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def run_scheduler(ctx):
    """Run the scheduler with all configured jobs."""
    config_path = ctx.obj['config_path']
    logger = get_logger(__name__)
    
    try:
        config = load_config(config_path)
        processor = DataProcessor(config)
        scheduler = get_scheduler_manager()
        
        # Register processing function for all targets
        async def process_job(target_name: str, job_id: str):
            return await processor.run_target_once(target_name)
        
        for target_name in config.targets.keys():
            scheduler.register_job_function(target_name, process_job)
        
        # Add configured jobs
        if config.scheduler and config.scheduler.jobs:
            for job_config in config.scheduler.jobs:
                if job_config.enabled:
                    scheduler.add_job_from_config(job_config)
        
        # Start scheduler
        scheduler.start()
        
        click.echo("🚀 Scheduler started")
        click.echo(f"📊 Active jobs: {len(scheduler.get_all_jobs())}")
        click.echo("Press Ctrl+C to stop...")
        
        # Show job status
        jobs = scheduler.get_all_jobs()
        for job in jobs:
            click.echo(f"  📋 {job['name']}: {job.get('next_run_time', 'No next run')}")
        
        try:
            # Keep running
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            click.echo("\n🛑 Stopping scheduler...")
            scheduler.stop()
            click.echo("✅ Scheduler stopped")
            
    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    '--format', '-f',
    default='csv',
    help='Export format',
    type=click.Choice(['csv', 'jsonl', 'sqlite'])
)
@click.option(
    '--path', '-p',
    help='Export path'
)
@click.option(
    '--target', '-t',
    help='Target to export (all if not specified)'
)
@click.pass_context
def export(ctx, format, path, target):
    """Export data from storage."""
    config_path = ctx.obj['config_path']
    logger = get_logger(__name__)
    
    try:
        config = load_config(config_path)
        
        # Override storage settings
        if path:
            config.storage.path = path
        config.storage.type = format
        
        # Create storage adapter
        from .storage import create_storage_adapter
        
        storage_kwargs = {}
        if format == 'csv':
            storage_kwargs['filename'] = os.path.join(path or config.storage.path, 'export.csv')
        elif format == 'jsonl':
            storage_kwargs['filename'] = os.path.join(path or config.storage.path, 'export.jsonl')
        elif format == 'sqlite':
            storage_kwargs['database_url'] = f"sqlite:///{os.path.join(path or config.storage.path, 'export.db')}"
        
        storage = create_storage_adapter(format, **storage_kwargs)
        
        # Load data
        items = asyncio.run(storage.load())
        
        if target:
            # Filter by target
            items = [item for item in items if item.get('target_name') == target]
        
        # Save to export location
        result = asyncio.run(storage.save(items))
        
        if result:
            click.echo(f"✅ Exported {len(items)} items to {format.upper()} format")
            if path:
                click.echo(f"📁 Location: {path}")
        else:
            click.echo(f"❌ Export failed", err=True)
            sys.exit(1)
            
        # Close storage
        asyncio.run(storage.close())
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    '--host',
    default='127.0.0.1',
    help='API server host'
)
@click.option(
    '--port',
    default=8000,
    help='API server port'
)
@click.pass_context
def serve(ctx, host, port):
    """Start the API server."""
    config_path = ctx.obj['config_path']
    logger = get_logger(__name__)
    
    try:
        # Create API application
        api = DataProcessorAPI(config_path)
        
        click.echo(f"🚀 Starting API server on http://{host}:{port}")
        click.echo("📊 Metrics available at: /metrics")
        click.echo("💚 Health check at: /health")
        click.echo("📈 Statistics at: /stats")
        click.echo("Press Ctrl+C to stop...")
        
        # Run API server
        api.run(host=host, port=port)
        
    except Exception as e:
        logger.error(f"API server error: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
def version():
    """Show version information."""
    from . import __version__
    click.echo(f"HEX Data Processor v{__version__}")


@cli.command()
def init():
    """Initialize a new configuration file."""
    config_path = 'config.json'
    
    if os.path.exists(config_path):
        if not click.confirm(f"Configuration file {config_path} already exists. Overwrite?"):
            click.echo("Initialization cancelled")
            return
    
    try:
        # Copy example config
        import shutil
        shutil.copy('config_example.json', config_path)
        
        click.echo(f"✅ Configuration initialized: {config_path}")
        click.echo("📝 Edit the file to customize your data processing pipeline")
        
    except Exception as e:
        click.echo(f"❌ Failed to initialize configuration: {str(e)}", err=True)
        sys.exit(1)


def main():
    """Main CLI entry point."""
    cli()


if __name__ == "__main__":
    main()