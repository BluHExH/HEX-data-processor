"""
Configuration management for HEX Data Processor.

Uses Pydantic for validation and environment variable substitution.
"""

import os
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from pydantic import BaseModel, Field, validator, HttpUrl
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class ProjectConfig(BaseModel):
    """Project metadata configuration."""
    name: str
    version: str
    description: Optional[str] = None


class HeadersConfig(BaseModel):
    """HTTP headers configuration."""
    Accept: str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    Accept_Language: str = "en-US,en;q=0.5"
    Accept_Encoding: str = "gzip, deflate"


class ScraperConfig(BaseModel):
    """Scraper configuration."""
    user_agent: str = "HEX-Data-Processor/1.0"
    timeout: int = Field(default=30, ge=1, le=300)
    max_retries: int = Field(default=3, ge=0, le=10)
    rate_limit: float = Field(default=1.0, ge=0.1)
    max_concurrent: int = Field(default=10, ge=1, le=50)
    headers: Optional[Dict[str, str]] = None


class PaginationConfig(BaseModel):
    """Pagination configuration."""
    enabled: bool = False
    next_selector: Optional[str] = None
    max_pages: int = Field(default=10, ge=1)


class TargetConfig(BaseModel):
    """Target website configuration."""
    name: str
    base_url: HttpUrl
    start_urls: List[HttpUrl]
    selectors: Dict[str, str]
    pagination: Optional[PaginationConfig] = None
    js_render: bool = False
    rate_limit: Optional[float] = None

    @validator('pagination')
    def validate_pagination(cls, v, values):
        if v and v.enabled and not v.next_selector:
            raise ValueError("next_selector is required when pagination is enabled")
        return v


class FieldValidation(BaseModel):
    """Field validation rules."""
    required: bool = False
    type: str = "string"


class HandleMissingConfig(BaseModel):
    """Missing value handling configuration."""
    strategy: str = Field(default="default", pattern="^(default|drop|interpolate)$")
    default_values: Dict[str, Any] = {}


class CleanerConfig(BaseModel):
    """Data cleaner configuration."""
    remove_duplicates: bool = False
    duplicate_keys: List[str] = []
    handle_missing: Optional[HandleMissingConfig] = None
    field_validation: Dict[str, FieldValidation] = {}


class TransformerConfig(BaseModel):
    """Data transformer configuration."""
    field_mapping: Dict[str, str] = {}
    type_conversions: Dict[str, str] = {}
    custom_functions: Dict[str, str] = {}


class CSVConfig(BaseModel):
    """CSV storage configuration."""
    delimiter: str = ","
    quoting: int = 1
    encoding: str = "utf-8"


class JSONLConfig(BaseModel):
    """JSONL storage configuration."""
    encoding: str = "utf-8"
    ensure_ascii: bool = False


class SQLiteConfig(BaseModel):
    """SQLite storage configuration."""
    database_url: str = "sqlite:///./data/output/hex_processor.db"
    table_name: str = "scraped_data"


class StorageConfig(BaseModel):
    """Storage configuration."""
    type: str = Field(pattern="^(csv|jsonl|sqlite)$")
    path: str
    filename_template: str = "{target_name}_{timestamp}.{extension}"
    csv: Optional[CSVConfig] = None
    jsonl: Optional[JSONLConfig] = None
    sqlite: Optional[SQLiteConfig] = None


class TelegramConfig(BaseModel):
    """Telegram notification configuration."""
    enabled: bool = False
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    on_success: bool = True
    on_error: bool = True


class EmailConfig(BaseModel):
    """Email notification configuration."""
    enabled: bool = False
    smtp_host: str
    smtp_port: int = 587
    username: str
    password: str
    use_tls: bool = True
    from_address: str
    to_addresses: List[str]
    on_success: bool = True
    on_error: bool = True


class WebhookConfig(BaseModel):
    """Webhook notification configuration."""
    enabled: bool = False
    url: str
    timeout: int = 10
    on_success: bool = True
    on_error: bool = True


class NotificationsConfig(BaseModel):
    """Notifications configuration."""
    enabled: bool = False
    telegram: Optional[TelegramConfig] = None
    email: Optional[EmailConfig] = None
    webhook: Optional[WebhookConfig] = None


class CronConfig(BaseModel):
    """Cron trigger configuration."""
    hour: Optional[str] = None
    minute: Optional[str] = None
    day: Optional[str] = None
    month: Optional[str] = None
    day_of_week: Optional[str] = None


class IntervalConfig(BaseModel):
    """Interval trigger configuration."""
    weeks: int = 0
    days: int = 0
    hours: int = 0
    minutes: int = 0
    seconds: int = 0


class JobConfig(BaseModel):
    """Scheduler job configuration."""
    id: str
    name: Optional[str] = None
    target: str
    trigger: str = Field(pattern="^(cron|interval|date)$")
    cron: Optional[CronConfig] = None
    interval: Optional[IntervalConfig] = None
    date: Optional[str] = None
    enabled: bool = True


class SchedulerConfig(BaseModel):
    """Scheduler configuration."""
    enabled: bool = False
    timezone: str = "UTC"
    jobs: List[JobConfig] = []


class MetricsConfig(BaseModel):
    """Metrics configuration."""
    enabled: bool = False
    port: int = Field(default=8000, ge=1024, le=65535)
    endpoint: str = "/metrics"
    health_endpoint: str = "/health"


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = Field(default="INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    format: str = Field(default="json", pattern="^(json|text)$")
    file: str = "logs/app.log"
    max_size: str = "10MB"
    backup_count: int = Field(default=5, ge=1)
    console: bool = True


class Config(BaseModel):
    """Main configuration class."""
    project: ProjectConfig
    scraper: ScraperConfig
    targets: Dict[str, TargetConfig]
    cleaner: Optional[CleanerConfig] = None
    transformer: Optional[TransformerConfig] = None
    storage: StorageConfig
    notifications: Optional[NotificationsConfig] = None
    scheduler: Optional[SchedulerConfig] = None
    metrics: Optional[MetricsConfig] = None
    logging: LoggingConfig = LoggingConfig()

    class Config:
        """Pydantic configuration."""
        arbitrary_types_allowed = True

    @classmethod
    def load_from_file(cls, config_path: Union[str, Path]) -> "Config":
        """Load configuration from JSON file with environment variable substitution."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)

        # Substitute environment variables
        config_data = cls._substitute_env_vars(config_data)

        return cls(**config_data)

    @classmethod
    def _substitute_env_vars(cls, data: Any) -> Any:
        """Recursively substitute environment variables in configuration data."""
        if isinstance(data, dict):
            return {k: cls._substitute_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [cls._substitute_env_vars(item) for item in data]
        elif isinstance(data, str) and data.startswith('${') and data.endswith('}'):
            env_var = data[2:-1]
            default_value = None
            if ':' in env_var:
                env_var, default_value = env_var.split(':', 1)
            return os.getenv(env_var, default_value)
        else:
            return data

    def validate(self) -> bool:
        """Validate configuration and return True if valid."""
        try:
            # Pydantic validation is already done during loading
            # Additional custom validation can be added here
            return True
        except Exception as e:
            raise ValueError(f"Configuration validation failed: {e}")

    def get_target(self, target_name: str) -> TargetConfig:
        """Get target configuration by name."""
        if target_name not in self.targets:
            raise ValueError(f"Target '{target_name}' not found in configuration")
        return self.targets[target_name]

    def get_timestamp(self) -> str:
        """Get current timestamp for filename generation."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")


def load_config(config_path: Union[str, Path] = "config.json") -> Config:
    """Load and validate configuration."""
    if not os.path.exists(config_path):
        config_path = "config_example.json"
    
    return Config.load_from_file(config_path)


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = load_config("config_example.json")
        print("Configuration loaded successfully!")
        print(f"Project: {config.project.name} v{config.project.version}")
        print(f"Targets: {list(config.targets.keys())}")
    except Exception as e:
        print(f"Error loading configuration: {e}")