# HEX Data Processor

[![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](Dockerfile)

A production-ready, async Python-based data processing tool with web scraping, cleaning, transformation, storage, notifications, scheduling, and monitoring capabilities.

## ✨ Features

- **🕷️ Async Web Scraping**: HTTP client with retry logic, rate limiting, and optional JS rendering
- **🧹 Data Cleaning**: Missing value handling, deduplication, and field validation
- **🔄 Data Transformation**: Field mapping, type conversions, and custom Python functions
- **💾 Multiple Storage**: CSV, JSONL, and SQLite storage adapters
- **🔔 Notifications**: Telegram, Email SMTP, and Webhook support
- **⏰ Scheduling**: APScheduler-based cron and interval scheduling
- **📊 Metrics**: Prometheus metrics and health endpoints
- **🐳 Docker Ready**: Containerized deployment with docker-compose
- **🖥️ CLI Interface**: Comprehensive Click-based command-line interface

## 🚀 Quick Start

### Option 1: Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/BluHExH/HEX-data-processor
cd HEX-data-processor

# Run with Docker Compose
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f hex-processor
```

### Option 2: Local Development

```bash
# Clone the repository
git clone https://github.com/BluHExH/HEX-data-processor
cd HEX-data-processor

# Setup environment
bash scripts/setup.sh

# Interactive run menu
bash scripts/run.sh

# Or run directly
source venv/bin/activate
python -m src.cli run quotes_toscrape --once
```

### Option 3: One-liner Test

```bash
# Quick test run (requires setup)
python -m src.cli --config config_example.json run quotes_toscrape --once --dry-run
```

## 📋 Configuration

The processor is configured via JSON files with Pydantic validation:

```json
{
  "project": {
    "name": "My Data Processor",
    "version": "1.0.0"
  },
  "scraper": {
    "user_agent": "MyBot/1.0",
    "timeout": 30,
    "rate_limit": 1.0
  },
  "targets": {
    "example_site": {
      "name": "Example Site",
      "base_url": "https://example.com",
      "start_urls": ["https://example.com/data"],
      "selectors": {
        "item": "div.item",
        "title": "h2.title::text",
        "content": "div.content::text"
      }
    }
  },
  "storage": {
    "type": "csv",
    "path": "data/output"
  }
}
```

### Environment Variables

Create `.env` file from `.env.example`:

```bash
cp .env.example .env
# Edit .env with your settings
```

## 🖥️ CLI Usage

### Basic Commands

```bash
# Validate configuration
python -m src.cli validate-config

# Run target once
python -m src.cli run quotes_toscrape --once

# Run with dry-run (no data saved)
python -m src.cli run quotes_toscrape --once --dry-run

# Start scheduler
python -m src.cli run-scheduler

# Start API server
python -m src.cli serve --host 0.0.0.0 --port 8000

# Export data
python -m src.cli export --format csv --path ./exports

# Initialize new config
python -m src.cli init
```

### Advanced Options

```bash
# Custom output format
python -m src.cli run target --once --output-format jsonl

# Custom output path
python -m src.cli run target --once --output-path ./custom_data

# Override log level
python -m src.cli --log-level DEBUG run target --once
```

## 📊 Monitoring & Metrics

### API Endpoints

When the API server is running (`python -m src.cli serve`):

- **Health Check**: `GET /health`
- **Readiness Check**: `GET /ready`
- **Prometheus Metrics**: `GET /metrics`
- **Statistics**: `GET /stats`
- **Configuration**: `GET /config`

### Prometheus Metrics

Available metrics include:

- `hex_processor_scraped_items_total` - Total items scraped
- `hex_processor_processed_items_total` - Total items processed
- `hex_processor_saved_items_total` - Total items saved
- `hex_processor_failed_items_total` - Total failed items
- `hex_processor_scraping_duration_seconds` - Scraping duration
- `hex_processor_active_runs` - Number of active runs

## 🔧 Configuration Reference

### Scraper Configuration

```json
{
  "scraper": {
    "user_agent": "Bot/1.0",
    "timeout": 30,
    "max_retries": 3,
    "rate_limit": 1.0,
    "max_concurrent": 10,
    "headers": {
      "Accept": "text/html"
    }
  }
}
```

### Target Configuration

```json
{
  "targets": {
    "my_target": {
      "name": "My Target",
      "base_url": "https://example.com",
      "start_urls": ["https://example.com/page1"],
      "selectors": {
        "item": "div.item",
        "title": "h2::text",
        "link": "a::attr(href)",
        "date": "span.date::text"
      },
      "pagination": {
        "enabled": true,
        "next_selector": "li.next a::attr(href)",
        "max_pages": 10
      },
      "js_render": false,
      "rate_limit": 1.0
    }
  }
}
```

### Storage Configuration

```json
{
  "storage": {
    "type": "csv",
    "path": "data/output",
    "filename_template": "{target_name}_{timestamp}.csv",
    "csv": {
      "delimiter": ",",
      "encoding": "utf-8"
    }
  }
}
```

### Notification Configuration

```json
{
  "notifications": {
    "enabled": true,
    "telegram": {
      "enabled": true,
      "bot_token": "${TELEGRAM_BOT_TOKEN}",
      "chat_id": "${TELEGRAM_CHAT_ID}",
      "on_success": true,
      "on_error": true
    },
    "email": {
      "enabled": false,
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "username": "${SMTP_USERNAME}",
      "password": "${SMTP_PASSWORD}",
      "use_tls": true,
      "to_addresses": ["admin@example.com"]
    }
  }
}
```

### Scheduler Configuration

```json
{
  "scheduler": {
    "enabled": true,
    "timezone": "UTC",
    "jobs": [
      {
        "id": "daily_job",
        "name": "Daily Data Collection",
        "target": "my_target",
        "trigger": "cron",
        "cron": {
          "hour": "9",
          "minute": "0"
        },
        "enabled": true
      }
    ]
  }
}
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_scraper.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run performance tests
pytest tests/ -m performance
```

## 🐳 Docker Deployment

### Basic Deployment

```bash
# Build and run
docker-compose up -d

# Scale services
docker-compose up -d --scale hex-processor=3

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Production with Monitoring

```bash
# Deploy with monitoring stack
docker-compose --profile monitoring up -d

# Access Grafana (admin/admin)
open http://localhost:3000

# Access Prometheus
open http://localhost:9090
```

### Environment-specific Configuration

```bash
# Development
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## 📁 Project Structure

```
HEX-data-processor/
├── src/                    # Source code
│   ├── config.py          # Configuration management
│   ├── logger.py          # Logging setup
│   ├── http_client.py     # Async HTTP client
│   ├── scraper.py         # Web scraper
│   ├── cleaner.py         # Data cleaner
│   ├── transformer.py     # Data transformer
│   ├── storage/           # Storage adapters
│   ├── notifier.py        # Notifications
│   ├── scheduler.py       # Job scheduler
│   ├── metrics.py         # Metrics collection
│   ├── api/               # FastAPI endpoints
│   └── cli.py             # Command line interface
├── tests/                 # Test suite
├── scripts/               # Utility scripts
├── docs/                  # Documentation
├── data/                  # Data directory
├── logs/                  # Log files
├── docker-compose.yml     # Docker configuration
├── Dockerfile            # Docker image
├── requirements.txt      # Python dependencies
└── config_example.json   # Example configuration
```

## 🔄 Data Processing Pipeline

1. **Scraping**: Fetch data from websites using async HTTP requests
2. **Cleaning**: Remove duplicates, handle missing values, validate fields
3. **Transformation**: Map fields, convert types, apply custom functions
4. **Storage**: Save to CSV, JSONL, or SQLite database
5. **Notification**: Send success/error notifications via configured channels
6. **Metrics**: Record processing metrics for monitoring

## ⚙️ Advanced Usage

### Custom Storage Adapters

```python
from src.storage.base import StorageAdapter

class CustomStorageAdapter(StorageAdapter):
    async def save(self, items):
        # Custom implementation
        pass
    
    async def load(self, limit=None):
        # Custom implementation
        pass
```

### Custom Transformation Functions

```json
{
  "transformer": {
    "custom_functions": {
      "processed_at": "lambda item: datetime.utcnow().isoformat()",
      "text_hash": "lambda item: hashlib.md5(item.get('text', '').encode()).hexdigest()",
      "category": "lambda item: 'important' if 'urgent' in item.get('text', '').lower() else 'normal'"
    }
  }
}
```

### JavaScript Rendering (Optional)

Install Playwright for JS rendering:

```bash
# Install Playwright
pip install playwright
playwright install

# Enable in config
{
  "targets": {
    "my_target": {
      "js_render": true
    }
  }
}
```

## 🛠️ Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/BluHExH/HEX-data-processor
cd HEX-data-processor

# Setup development environment
bash scripts/setup.sh

# Install development dependencies
pip install -r requirements.txt
pip install black isort flake8 mypy pytest

# Setup pre-commit hooks
pre-commit install
```

### Code Quality

```bash
# Format code
black src/ tests/
isort src/ tests/

# Lint code
flake8 src/ tests/

# Type checking
mypy src/

# Run tests
pytest tests/ -v --cov=src
```

### Adding New Features

1. Create feature branch: `git checkout -b feature/new-feature`
2. Write code with tests
3. Update documentation
4. Run quality checks: `pre-commit run --all-files`
5. Submit pull request

## 🔒 Security & Legal

### robots.txt Compliance

The processor includes automatic robots.txt checking:

```python
from src.scraper import Scraper

scraper = Scraper(http_client)
robots_info = await scraper.check_robots_txt("https://example.com")
```

### Rate Limiting

Built-in rate limiting prevents overwhelming target servers:

```json
{
  "scraper": {
    "rate_limit": 1.0,
    "max_concurrent": 10
  }
}
```

### Data Privacy

- Never collect personal data without explicit consent
- Follow GDPR and other privacy regulations
- Implement data retention policies
- Use encryption for sensitive data storage

## 🐛 Troubleshooting

### Common Issues

1. **Virtual environment not activated**
   ```bash
   source venv/bin/activate
   ```

2. **Configuration validation failed**
   ```bash
   python -m src.cli validate-config
   ```

3. **Permission denied errors**
   ```bash
   chmod +x scripts/*.sh
   ```

4. **Docker build fails**
   ```bash
   docker system prune -f
   docker-compose build --no-cache
   ```

### Debug Mode

```bash
# Enable debug logging
python -m src.cli --log-level DEBUG run target --once

# View detailed logs
tail -f logs/app.log
```

### Performance Tuning

```json
{
  "scraper": {
    "max_concurrent": 20,
    "rate_limit": 0.1
  },
  "storage": {
    "type": "sqlite"
  }
}
```

## 📚 API Reference

### CLI Commands

- `run` - Execute data processing
- `validate-config` - Validate configuration
- `run-scheduler` - Start job scheduler
- `serve` - Start API server
- `export` - Export stored data
- `init` - Initialize configuration

### HTTP API

- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics
- `GET /stats` - Processing statistics
- `GET /config` - Current configuration
- `POST /reset-metrics` - Reset metrics

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Make changes with tests
4. Ensure code quality
5. Submit pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) for HTML parsing
- [httpx](https://www.python-httpx.org/) for async HTTP requests
- [Pydantic](https://pydantic-docs.helpmanual.io/) for data validation
- [APScheduler](https://apscheduler.readthedocs.io/) for task scheduling
- [FastAPI](https://fastapi.tiangolo.com/) for API framework
- [Prometheus](https://prometheus.io/) for metrics collection

## 📞 Support

- 📖 [Documentation](docs/)
- 🐛 [Issue Tracker](https://github.com/hex-data-processor/issues)
- 💬 [Discussions](https://github.com/hex-data-processor/discussions)

---

**⚠️ IMPORTANT**: Always respect robots.txt and website terms of service. Do not collect personal or sensitive data without permission.
