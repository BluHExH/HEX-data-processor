"""
Structured logging configuration for HEX Data Processor.

Provides JSON and text logging with rotation and multiple output targets.
"""

import os
import sys
import json
import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

from pythonjsonlogger import jsonlogger


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON or plain text."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {
                'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                'filename', 'module', 'lineno', 'funcName', 'created',
                'msecs', 'relativeCreated', 'thread', 'threadName',
                'processName', 'process', 'getMessage', 'exc_info',
                'exc_text', 'stack_info'
            }:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """Custom formatter for text logging."""
    
    def __init__(self):
        super().__init__(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


def setup_logging(
    level: str = "INFO",
    format_type: str = "json",
    log_file: Optional[str] = None,
    max_size: str = "10MB",
    backup_count: int = 5,
    console: bool = True
) -> None:
    """Setup logging configuration."""
    
    # Create logs directory if it doesn't exist
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Set root logger level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger().setLevel(log_level)
    
    # Clear existing handlers
    logging.getLogger().handlers.clear()
    
    # Choose formatter
    if format_type.lower() == "json":
        formatter = StructuredFormatter()
    else:
        formatter = TextFormatter()
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logging.getLogger().addHandler(console_handler)
    
    # File handler with rotation
    if log_file:
        # Parse max_size
        if max_size.endswith('MB'):
            max_bytes = int(max_size[:-2]) * 1024 * 1024
        elif max_size.endswith('KB'):
            max_bytes = int(max_size[:-2]) * 1024
        else:
            max_bytes = int(max_size)
        
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name."""
    return logging.getLogger(name)


class LoggerMixin:
    """Mixin class to add logging capabilities to other classes."""
    
    @property
    def logger(self) -> logging.Logger:
        """Get logger for this class."""
        return get_logger(self.__class__.__name__)


def log_function_call(logger: Optional[logging.Logger] = None):
    """Decorator to log function calls."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            func_logger = logger or get_logger(func.__module__)
            func_logger.debug(
                f"Calling {func.__name__}",
                extra={
                    "function": func.__name__,
                    "args_count": len(args),
                    "kwargs_keys": list(kwargs.keys())
                }
            )
            try:
                result = func(*args, **kwargs)
                func_logger.debug(
                    f"Completed {func.__name__}",
                    extra={"function": func.__name__}
                )
                return result
            except Exception as e:
                func_logger.error(
                    f"Error in {func.__name__}: {str(e)}",
                    extra={"function": func.__name__, "error": str(e)},
                    exc_info=True
                )
                raise
        return wrapper
    return decorator


def log_async_function_call(logger: Optional[logging.Logger] = None):
    """Decorator to log async function calls."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            func_logger = logger or get_logger(func.__module__)
            func_logger.debug(
                f"Calling async {func.__name__}",
                extra={
                    "function": func.__name__,
                    "args_count": len(args),
                    "kwargs_keys": list(kwargs.keys())
                }
            )
            try:
                result = await func(*args, **kwargs)
                func_logger.debug(
                    f"Completed async {func.__name__}",
                    extra={"function": func.__name__}
                )
                return result
            except Exception as e:
                func_logger.error(
                    f"Error in async {func.__name__}: {str(e)}",
                    extra={"function": func.__name__, "error": str(e)},
                    exc_info=True
                )
                raise
        return wrapper
    return decorator


if __name__ == "__main__":
    # Test logging setup
    setup_logging(
        level="INFO",
        format_type="json",
        log_file="logs/test.log",
        console=True
    )
    
    logger = get_logger(__name__)
    logger.info("Test message", extra={"test_key": "test_value"})
    logger.error("Test error message")
    
    # Test decorator
    @log_function_call()
    def test_function(x, y):
        return x + y
    
    result = test_function(1, 2)
    logger.info(f"Result: {result}")