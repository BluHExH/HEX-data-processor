"""
API module for HEX Data Processor.

Provides FastAPI endpoints for health checks and metrics.
"""

from .app import create_app

__all__ = ['create_app']