"""
FastAPI application for HEX Data Processor.

Provides health check, metrics, and control endpoints.
"""

import asyncio
from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn

from ..metrics import get_metrics_collector, get_health_checker, generate_latest, CONTENT_TYPE_LATEST
from ..logger import get_logger, setup_logging
from ..config import load_config


class DataProcessorAPI:
    """FastAPI application for data processor management."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize API application."""
        self.logger = get_logger(__name__)
        self.config = None
        
        # Load configuration
        if config_path:
            try:
                self.config = load_config(config_path)
            except Exception as e:
                self.logger.error(f"Failed to load config: {str(e)}")
        
        # Initialize components
        self.metrics = get_metrics_collector()
        self.health_checker = get_health_checker()
        
        # Create FastAPI app
        self.app = FastAPI(
            title="HEX Data Processor API",
            description="API for monitoring and managing data processing operations",
            version="1.0.0"
        )
        
        # Setup routes
        self._setup_routes()
        
        # Register health checks
        self._register_health_checks()
        
        self.logger.info("DataProcessorAPI initialized")
    
    def _setup_routes(self):
        """Setup API routes."""
        
        @self.app.get("/health", response_class=JSONResponse)
        async def health_check():
            """Health check endpoint."""
            try:
                health_status = self.health_checker.run_health_checks()
                status_code = 200 if health_status["status"] == "healthy" else 503
                return JSONResponse(content=health_status, status_code=status_code)
            except Exception as e:
                self.logger.error(f"Health check failed: {str(e)}")
                return JSONResponse(
                    content={
                        "status": "error",
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat() + "Z"
                    },
                    status_code=500
                )
        
        @self.app.get("/ready", response_class=JSONResponse)
        async def readiness_check():
            """Readiness check endpoint."""
            return JSONResponse(content={
                "status": "ready",
                "timestamp": datetime.utcnow().isoformat() + "Z"
            })
        
        @self.app.get("/metrics", response_class=PlainTextResponse)
        async def prometheus_metrics():
            """Prometheus metrics endpoint."""
            try:
                metrics_data = generate_latest()
                return PlainTextResponse(
                    content=metrics_data,
                    media_type=CONTENT_TYPE_LATEST
                )
            except Exception as e:
                self.logger.error(f"Failed to generate metrics: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/stats", response_class=JSONResponse)
        async def get_stats():
            """Get processing statistics."""
            try:
                stats = self.metrics.get_metrics_summary()
                return JSONResponse(content=stats)
            except Exception as e:
                self.logger.error(f"Failed to get stats: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/config", response_class=JSONResponse)
        async def get_config():
            """Get current configuration."""
            if not self.config:
                raise HTTPException(status_code=404, detail="Configuration not loaded")
            
            try:
                # Return sanitized config (remove sensitive data)
                config_dict = self.config.dict()
                config_dict = self._sanitize_config(config_dict)
                return JSONResponse(content=config_dict)
            except Exception as e:
                self.logger.error(f"Failed to get config: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/reset-metrics", response_class=JSONResponse)
        async def reset_metrics():
            """Reset all metrics (admin endpoint)."""
            try:
                self.metrics.reset_metrics()
                return JSONResponse(content={
                    "status": "success",
                    "message": "Metrics reset successfully",
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                })
            except Exception as e:
                self.logger.error(f"Failed to reset metrics: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
    
    def _register_health_checks(self):
        """Register health check functions."""
        
        def check_metrics():
            """Check metrics system health."""
            try:
                summary = self.metrics.get_metrics_summary()
                return "error" not in summary
            except:
                return False
        
        def check_config():
            """Check configuration health."""
            return self.config is not None
        
        def check_logging():
            """Check logging system health."""
            try:
                logger = get_logger("health_check")
                logger.info("Health check log test")
                return True
            except:
                return False
        
        self.health_checker.register_check("metrics", check_metrics)
        self.health_checker.register_check("config", check_config)
        self.health_checker.register_check("logging", check_logging)
    
    def _sanitize_config(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive information from config."""
        sensitive_keys = [
            "password", "token", "secret", "key", "auth",
            "bot_token", "smtp_password"
        ]
        
        def sanitize_recursive(obj):
            if isinstance(obj, dict):
                sanitized = {}
                for key, value in obj.items():
                    if any(sensitive in key.lower() for sensitive in sensitive_keys):
                        sanitized[key] = "***REDACTED***"
                    else:
                        sanitized[key] = sanitize_recursive(value)
                return sanitized
            elif isinstance(obj, list):
                return [sanitize_recursive(item) for item in obj]
            else:
                return obj
        
        return sanitize_recursive(config_dict)
    
    def run(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        log_level: str = "info"
    ):
        """Run the FastAPI application."""
        self.logger.info(f"Starting API server on {host}:{port}")
        
        uvicorn.run(
            self.app,
            host=host,
            port=port,
            log_level=log_level.lower()
        )


def create_app(config_path: Optional[str] = None) -> FastAPI:
    """
    Factory function to create FastAPI application.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        FastAPI application instance
    """
    api = DataProcessorAPI(config_path)
    return api.app


if __name__ == "__main__":
    # Test API
    setup_logging(level="INFO", console=True)
    
    api = DataProcessorAPI("config_example.json")
    
    try:
        api.run(host="127.0.0.1", port=8000)
    except KeyboardInterrupt:
        print("\nShutting down API server...")
    except Exception as e:
        print(f"API server error: {str(e)}")