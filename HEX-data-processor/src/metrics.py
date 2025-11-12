"""
Metrics collection for HEX Data Processor.

Provides Prometheus metrics and HTTP endpoints for monitoring.
"""

import time
from typing import Dict, Any, Optional
from datetime import datetime

from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    Info,
    generate_latest,
    CONTENT_TYPE_LATEST
)

from .logger import get_logger


class MetricsCollector:
    """Prometheus metrics collector for data processing operations."""
    
    def __init__(self, namespace: str = "hex_processor"):
        """Initialize metrics collector."""
        self.namespace = namespace
        self.logger = get_logger(__name__)
        
        # Counters
        self.scraped_items_total = Counter(
            f'{namespace}_scraped_items_total',
            'Total number of items scraped',
            ['target', 'run_id']
        )
        
        self.processed_items_total = Counter(
            f'{namespace}_processed_items_total',
            'Total number of items processed',
            ['target', 'run_id']
        )
        
        self.saved_items_total = Counter(
            f'{namespace}_saved_items_total',
            'Total number of items saved',
            ['storage_type', 'target', 'run_id']
        )
        
        self.failed_items_total = Counter(
            f'{namespace}_failed_items_total',
            'Total number of failed items',
            ['operation', 'target', 'run_id']
        )
        
        self.requests_total = Counter(
            f'{namespace}_requests_total',
            'Total HTTP requests made',
            ['target', 'status']
        )
        
        # Histograms
        self.scraping_duration = Histogram(
            f'{namespace}_scraping_duration_seconds',
            'Time spent scraping data',
            ['target'],
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]
        )
        
        self.processing_duration = Histogram(
            f'{namespace}_processing_duration_seconds',
            'Time spent processing data',
            ['operation'],
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]
        )
        
        self.storage_duration = Histogram(
            f'{namespace}_storage_duration_seconds',
            'Time spent storing data',
            ['storage_type'],
            buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]
        )
        
        # Gauges
        self.active_runs = Gauge(
            f'{namespace}_active_runs',
            'Number of active processing runs',
            ['target']
        )
        
        self.queue_size = Gauge(
            f'{namespace}_queue_size',
            'Size of processing queue'
        )
        
        self.last_run_timestamp = Gauge(
            f'{namespace}_last_run_timestamp',
            'Timestamp of last successful run',
            ['target']
        )
        
        # Info
        self.build_info = Info(
            f'{namespace}_build_info',
            'Build information'
        )
        
        self.config_info = Info(
            f'{namespace}_config_info',
            'Configuration information'
        )
        
        # Initialize build info
        self.build_info.info({
            'version': '1.0.0',
            'build_date': datetime.utcnow().isoformat() + 'Z',
            'python_version': '3.10+'
        })
    
    def record_scraped_items(self, count: int, target: str, run_id: str):
        """Record scraped items count."""
        self.scraped_items_total.labels(target=target, run_id=run_id).inc(count)
        self.logger.debug(f"Recorded {count} scraped items for {target}")
    
    def record_processed_items(self, count: int, target: str, run_id: str):
        """Record processed items count."""
        self.processed_items_total.labels(target=target, run_id=run_id).inc(count)
        self.logger.debug(f"Recorded {count} processed items for {target}")
    
    def record_saved_items(self, count: int, storage_type: str, target: str, run_id: str):
        """Record saved items count."""
        self.saved_items_total.labels(
            storage_type=storage_type, 
            target=target, 
            run_id=run_id
        ).inc(count)
        self.logger.debug(f"Recorded {count} saved items for {target} to {storage_type}")
    
    def record_failed_items(self, count: int, operation: str, target: str, run_id: str):
        """Record failed items count."""
        self.failed_items_total.labels(
            operation=operation, 
            target=target, 
            run_id=run_id
        ).inc(count)
        self.logger.debug(f"Recorded {count} failed items for {operation} on {target}")
    
    def record_request(self, target: str, status: str):
        """Record HTTP request."""
        self.requests_total.labels(target=target, status=status).inc()
    
    def start_scraping_timer(self, target: str):
        """Start scraping duration timer."""
        return self.scraping_duration.labels(target=target).time()
    
    def start_processing_timer(self, operation: str):
        """Start processing duration timer."""
        return self.processing_duration.labels(operation=operation).time()
    
    def start_storage_timer(self, storage_type: str):
        """Start storage duration timer."""
        return self.storage_duration.labels(storage_type=storage_type).time()
    
    def set_active_runs(self, count: int, target: str):
        """Set number of active runs."""
        self.active_runs.labels(target=target).set(count)
    
    def set_queue_size(self, size: int):
        """Set processing queue size."""
        self.queue_size.set(size)
    
    def update_last_run(self, target: str):
        """Update last successful run timestamp."""
        timestamp = time.time()
        self.last_run_timestamp.labels(target=target).set(timestamp)
    
    def set_config_info(self, config: Dict[str, Any]):
        """Set configuration information."""
        config_str = {k: str(v) for k, v in config.items()}
        self.config_info.info(config_str)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a summary of current metrics."""
        try:
            # Get metric values (this is a simplified version)
            summary = {
                'namespace': self.namespace,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }
            
            # In a real implementation, you would collect actual values
            # from the metrics registry here
            
            return summary
        except Exception as e:
            self.logger.error(f"Failed to get metrics summary: {str(e)}")
            return {'error': str(e)}
    
    def reset_metrics(self):
        """Reset all metrics (useful for testing)."""
        from prometheus_client import REGISTRY
        
        # Clear all metrics
        collectors = list(REGISTRY._collector_to_names.keys())
        for collector in collectors:
            REGISTRY.unregister(collector)
        
        # Reinitialize metrics
        self.__init__(self.namespace)
        
        self.logger.info("Metrics reset")


class MetricsMiddleware:
    """Middleware for collecting HTTP request metrics."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        """Initialize middleware."""
        self.metrics = metrics_collector
        self.logger = get_logger(__name__)
    
    def __call__(self, request_handler):
        """Wrap request handler with metrics collection."""
        def wrapped_handler(*args, **kwargs):
            start_time = time.time()
            
            try:
                # Call original handler
                response = request_handler(*args, **kwargs)
                
                # Record successful request
                duration = time.time() - start_time
                self.metrics.record_request("api", "success")
                
                return response
                
            except Exception as e:
                # Record failed request
                duration = time.time() - start_time
                self.metrics.record_request("api", "error")
                raise
        
        return wrapped_handler


class HealthChecker:
    """Health check functionality."""
    
    def __init__(self):
        """Initialize health checker."""
        self.logger = get_logger(__name__)
        self._health_status = "healthy"
        self._last_check = datetime.utcnow()
        self._checks = {}
    
    def register_check(self, name: str, check_func: callable):
        """Register a health check function."""
        self._checks[name] = check_func
        self.logger.info(f"Registered health check: {name}")
    
    def run_health_checks(self) -> Dict[str, Any]:
        """Run all registered health checks."""
        results = {}
        overall_healthy = True
        
        for name, check_func in self._checks.items():
            try:
                result = check_func()
                results[name] = {
                    "status": "healthy" if result else "unhealthy",
                    "details": result if isinstance(result, dict) else {},
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
                if not result:
                    overall_healthy = False
            except Exception as e:
                results[name] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
                overall_healthy = False
        
        self._health_status = "healthy" if overall_healthy else "unhealthy"
        self._last_check = datetime.utcnow()
        
        return {
            "status": self._health_status,
            "timestamp": self._last_check.isoformat() + "Z",
            "checks": results
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return {
            "status": self._health_status,
            "last_check": self._last_check.isoformat() + "Z",
            "uptime": str(datetime.utcnow() - self._last_check)
        }


# Global metrics instance
_metrics_collector = None
_health_checker = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def get_health_checker() -> HealthChecker:
    """Get global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


if __name__ == "__main__":
    # Test metrics
    metrics = MetricsCollector()
    
    # Test recording metrics
    metrics.record_scraped_items(10, "test_target", "test_run")
    metrics.record_processed_items(8, "test_target", "test_run")
    metrics.record_saved_items(8, "csv", "test_target", "test_run")
    metrics.record_failed_items(2, "scraping", "test_target", "test_run")
    
    # Test timers
    with metrics.start_scraping_timer("test_target"):
        time.sleep(0.1)  # Simulate work
    
    # Get metrics summary
    summary = metrics.get_metrics_summary()
    print("Metrics summary:", summary)
    
    # Test health checker
    health = HealthChecker()
    
    def test_check():
        return True
    
    health.register_check("database", test_check)
    health_status = health.run_health_checks()
    print("Health status:", health_status)
    
    # Generate Prometheus metrics
    prometheus_data = generate_latest()
    print(f"Prometheus metrics generated ({len(prometheus_data)} bytes)")