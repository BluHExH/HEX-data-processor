# HEX Data Processor Usage Guide

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [CLI Reference](#cli-reference)
4. [API Reference](#api-reference)
5. [Advanced Usage](#advanced-usage)
6. [Troubleshooting](#troubleshooting)
7. [Best Practices](#best-practices)

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd HEX-data-processor

# Run setup script
bash scripts/setup.sh

# Or manual setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### First Run

```bash
# Show interactive menu
bash scripts/run.sh

# Or run directly
python -m src.cli --config config_example.json run quotes_toscrape --once

# Check results
ls data/output/
```

### Docker Quick Start

```bash
# Run with Docker
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f
```

## Configuration

### Basic Configuration

Create or edit `config.json`:

```json
{
  "project": {
    "name": "My Data Processor",
    "version": "1.0.0",
    "description": "Data processing for my project"
  },
  "scraper": {
    "user_agent": "MyBot/1.0",
    "timeout": 30,
    "max_retries": 3,
    "rate_limit": 1.0,
    "max_concurrent": 10
  },
  "targets": {
    "example": {
      "name": "Example Website",
      "base_url": "https://example.com",
      "start_urls": ["https://example.com/data"],
      "selectors": {
        "item": "div.item",
        "title": "h2.title::text",
        "content": "div.content::text",
        "link": "a.link::attr(href)"
      },
      "pagination": {
        "enabled": true,
        "next_selector": "li.next a::attr(href)",
        "max_pages": 5
      }
    }
  },
  "storage": {
    "type": "csv",
    "path": "data/output",
    "filename_template": "{target_name}_{timestamp}.csv"
  }
}
```

### Environment Variables

Create `.env` file:

```bash
# Database
DATABASE_URL=sqlite:///./data/output/processor.db

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/app.log

# HTTP Client
HTTP_TIMEOUT=30
HTTP_RATE_LIMIT=1.0

# Telegram Notifications
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# Email Notifications
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your_email@gmail.com
SMTP_PASSWORD=your_app_password
```

### Target Configuration

#### Static Website

```json
{
  "targets": {
    "blog_posts": {
      "name": "Blog Posts",
      "base_url": "https://blog.example.com",
      "start_urls": ["https://blog.example.com/posts"],
      "selectors": {
        "post": "article.post",
        "title": "h1.post-title::text",
        "content": "div.post-content::text",
        "author": "span.author::text",
        "date": "time.post-date::text",
        "tags": "div.tags a.tag::text",
        "url": "a.read-more::attr(href)"
      },
      "pagination": {
        "enabled": true,
        "next_selector": "nav.pagination a.next::attr(href)",
        "max_pages": 10
      },
      "rate_limit": 2.0
    }
  }
}
```

#### E-commerce Site

```json
{
  "targets": {
    "products": {
      "name": "Products",
      "base_url": "https://shop.example.com",
      "start_urls": ["https://shop.example.com/category/electronics"],
      "selectors": {
        "product": "div.product-item",
        "name": "h3.product-name::text",
        "price": "span.price::text",
        "description": "p.description::text",
        "rating": "div.rating::attr(data-rating)",
        "availability": "span.stock::text",
        "image": "img.product-image::attr(src)",
        "product_url": "a.product-link::attr(href)"
      },
      "pagination": {
        "enabled": true,
        "next_selector": "a.pagination-next::attr(href)",
        "max_pages": 20
      }
    }
  }
}
```

#### JavaScript-Rendered Site

```json
{
  "targets": {
    "spa_site": {
      "name": "SPA Site",
      "base_url": "https://spa.example.com",
      "start_urls": ["https://spa.example.com/data"],
      "selectors": {
        "item": "div.data-item",
        "title": "h2::text",
        "value": "span.value::text"
      },
      "js_render": true,
      "rate_limit": 3.0
    }
  }
}
```

### Data Cleaning Configuration

```json
{
  "cleaner": {
    "remove_duplicates": true,
    "duplicate_keys": ["title", "url"],
    "handle_missing": {
      "strategy": "default",
      "default_values": {
        "title": "Untitled",
        "author": "Unknown",
        "tags": []
      }
    },
    "field_validation": {
      "title": {"required": true, "type": "string"},
      "url": {"required": true, "type": "string"},
      "price": {"required": false, "type": "float"}
    }
  }
}
```

### Data Transformation Configuration

```json
{
  "transformer": {
    "field_mapping": {
      "post_title": "title",
      "post_content": "content",
      "post_author": "author"
    },
    "type_conversions": {
      "title": "string",
      "price": "float",
      "rating": "int",
      "in_stock": "bool",
      "tags": "array"
    },
    "custom_functions": {
      "word_count": "lambda item: len(item.get('content', '').split())",
      "price_clean": "lambda item: float(item.get('price', '0').replace('$', '').replace(',', ''))",
      "category": "lambda item: item.get('url', '').split('/')[2] if '/' in item.get('url', '') else 'unknown'",
      "processed_at": "lambda item: datetime.utcnow().isoformat() + 'Z'"
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
      "from_address": "${SMTP_USERNAME}",
      "to_addresses": ["admin@example.com", "team@example.com"],
      "on_success": true,
      "on_error": true
    },
    "webhook": {
      "enabled": false,
      "url": "${WEBHOOK_URL}",
      "timeout": 10,
      "on_success": true,
      "on_error": true
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
        "id": "daily_blog_scrape",
        "name": "Daily Blog Scraping",
        "target": "blog_posts",
        "trigger": "cron",
        "cron": {
          "hour": "9",
          "minute": "0"
        },
        "enabled": true
      },
      {
        "id": "hourly_products",
        "name": "Hourly Product Updates",
        "target": "products",
        "trigger": "interval",
        "interval": {
          "hours": 1
        },
        "enabled": false
      }
    ]
  }
}
```

## CLI Reference

### Global Options

```bash
# Configuration file
--config, -c    Path to configuration file (default: config.json)

# Logging options
--log-level     DEBUG, INFO, WARNING, ERROR, CRITICAL
--log-format    json, text (default: json)
```

### Commands

#### validate-config

Validate configuration file:

```bash
python -m src.cli validate-config

# With custom config
python -m src.cli --config custom.json validate-config
```

#### run

Execute data processing:

```bash
# Run single target once
python -m src.cli run quotes_toscrape --once

# Run with dry-run (no data saved)
python -m src.cli run quotes_toscrape --once --dry-run

# Run all targets once
python -m src.cli run all --once

# Custom output format
python -m src.cli run quotes_toscrape --once --output-format jsonl

# Custom output path
python -m src.cli run quotes_toscrape --once --output-path ./custom_data

# Override log level
python -m src.cli --log-level DEBUG run quotes_toscrape --once
```

#### run-scheduler

Start job scheduler:

```bash
python -m src.cli run-scheduler

# Runs all configured jobs continuously
```

#### serve

Start API server:

```bash
# Default settings
python -m src.cli serve

# Custom host and port
python -m src.cli serve --host 0.0.0.0 --port 8080

# Development mode
python -m src.cli --log-level DEBUG serve
```

#### export

Export stored data:

```bash
# Export to CSV
python -m src.cli export --format csv

# Export to JSONL
python -m src.cli export --format jsonl --path ./exports

# Export specific target
python -m src.cli export --target blog_posts --format sqlite
```

#### init

Initialize new configuration:

```bash
python -m src.cli init

# Creates config.json from example
```

#### version

Show version information:

```bash
python -m src.cli version
```

## API Reference

### Authentication

Currently no authentication (add as needed):

```bash
# Health check
curl http://localhost:8000/health

# Metrics
curl http://localhost:8000/metrics

# Statistics
curl http://localhost:8000/stats
```

### Endpoints

#### GET /health

Health check endpoint:

```bash
curl http://localhost:8000/health
```

Response:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "checks": {
    "metrics": {"status": "healthy"},
    "config": {"status": "healthy"},
    "logging": {"status": "healthy"}
  }
}
```

#### GET /ready

Readiness check:

```bash
curl http://localhost:8000/ready
```

Response:
```json
{
  "status": "ready",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

#### GET /metrics

Prometheus metrics:

```bash
curl http://localhost:8000/metrics
```

Response (Prometheus format):
```
# HELP hex_processor_scraped_items_total Total number of items scraped
# TYPE hex_processor_scraped_items_total counter
hex_processor_scraped_items_total{target="quotes_toscrape",run_id="run_123"} 10
```

#### GET /stats

Processing statistics:

```bash
curl http://localhost:8000/stats
```

Response:
```json
{
  "namespace": "hex_processor",
  "timestamp": "2024-01-15T10:30:00Z",
  "active_runs": 0,
  "total_processed": 150,
  "success_rate": 0.95
}
```

#### GET /config

Current configuration (sanitized):

```bash
curl http://localhost:8000/config
```

#### POST /reset-metrics

Reset all metrics (admin):

```bash
curl -X POST http://localhost:8000/reset-metrics
```

## Advanced Usage

### Custom Storage Adapter

```python
from src.storage.base import StorageAdapter

class S3StorageAdapter(StorageAdapter):
    def __init__(self, bucket, access_key, secret_key):
        self.bucket = bucket
        self.client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
    
    async def save(self, items):
        # Convert to CSV
        csv_data = self._items_to_csv(items)
        
        # Upload to S3
        filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.client.put_object(
            Bucket=self.bucket,
            Key=filename,
            Body=csv_data
        )
        return True
```

### Custom Transformation Function

```python
def extract_domain(item):
    """Extract domain from URL."""
    from urllib.parse import urlparse
    url = item.get('url', '')
    if url:
        return urlparse(url).netloc
    return ''

# Add to config
{
  "transformer": {
    "custom_functions": {
      "domain": "lambda item: extract_domain(item)"
    }
  }
}
```

### JavaScript Rendering Setup

Install Playwright:

```bash
pip install playwright
playwright install chromium

# Enable in config
{
  "targets": {
    "spa_site": {
      "js_render": true
    }
  }
}
```

### Custom Notifier

```python
from src.notifier import NotificationManager

class SlackNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
    
    async def send_message(self, message):
        async with httpx.AsyncClient() as client:
            await client.post(self.webhook_url, json={"text": message})
```

### Batch Processing

```python
# Process multiple targets in parallel
async def process_batch(targets):
    tasks = []
    for target in targets:
        task = processor.run_target_once(target)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results
```

### Rate Limiting Strategies

```json
{
  "scraper": {
    "rate_limit": 1.0,  // 1 second between requests
    "max_concurrent": 5,  // Max 5 concurrent requests
    "jitter": 0.1  // Add 0-100ms random delay
  }
}
```

### Error Handling

```python
# Custom error handling
try:
    result = await processor.run_target_once("target")
except Exception as e:
    logger.error(f"Processing failed: {e}")
    
    # Send notification
    if notifier:
        await notifier.send_error_notification("target", str(e), {})
```

## Troubleshooting

### Common Issues

#### 1. Configuration Validation Failed

```bash
# Check configuration
python -m src.cli validate-config

# Common errors:
# - Missing required fields
# - Invalid URL formats
# - Type mismatches
```

#### 2. HTTP Connection Errors

```bash
# Check network connectivity
curl -I https://target-website.com

# Verify user agent
python -c "
import httpx
with httpx.Client() as client:
    response = client.get('https://httpbin.org/user-agent')
    print(response.json())
"
```

#### 3. Storage Permission Errors

```bash
# Check directory permissions
ls -la data/output/

# Create directories
mkdir -p data/input data/output logs
chmod 755 data/output
```

#### 4. Docker Build Issues

```bash
# Clean Docker cache
docker system prune -f

# Rebuild without cache
docker-compose build --no-cache

# Check logs
docker-compose logs hex-processor
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python -m src.cli --log-level DEBUG run target --once

# View detailed logs
tail -f logs/app.log | jq '.'
```

### Performance Issues

#### Monitor Resource Usage

```bash
# Memory usage
python -c "
import psutil
import os
process = psutil.Process(os.getpid())
print(f'Memory: {process.memory_info().rss / 1024 / 1024:.1f} MB')
"

# Check for memory leaks
python -m src.cli run target --once
# Monitor with: watch -n 1 'ps aux | grep python'
```

#### Optimize Configuration

```json
{
  "scraper": {
    "max_concurrent": 5,  // Reduce if hitting rate limits
    "timeout": 60,  // Increase for slow sites
    "rate_limit": 2.0  // Increase delay
  }
}
```

### Data Quality Issues

#### Validate Output

```python
import pandas as pd

# Load CSV and check quality
df = pd.read_csv('data/output/target_20240115_103000.csv')
print(f"Total records: {len(df)}")
print(f"Missing values: {df.isnull().sum().sum()}")
print(f"Duplicates: {df.duplicated().sum()}")
```

#### Check Selectors

```python
from bs4 import BeautifulSoup
import requests

# Test selectors
response = requests.get('https://example.com')
soup = BeautifulSoup(response.content, 'html.parser')

items = soup.select('div.item')
print(f"Found {len(items)} items")

if items:
    first = items[0]
    title = first.select_one('h2.title')
    print(f"Title: {title.text if title else 'Not found'}")
```

## Best Practices

### 1. Configuration Management

- Use environment variables for secrets
- Version control configuration templates
- Validate configurations before deployment
- Use separate configs for different environments

### 2. Web Scraping Ethics

- Always check robots.txt
- Respect rate limits
- Identify your bot with appropriate user agent
- Don't scrape personal or sensitive data

### 3. Error Handling

- Implement retry logic for transient failures
- Log detailed error information
- Use graceful degradation
- Monitor error rates

### 4. Performance Optimization

- Use appropriate concurrency levels
- Implement caching where possible
- Monitor resource usage
- Optimize selectors for speed

### 5. Data Quality

- Validate input data
- Clean and normalize text
- Handle missing values appropriately
- Remove duplicates

### 6. Monitoring

- Track key metrics
- Set up health checks
- Monitor resource usage
- Set up alerting

### 7. Security

- Never commit secrets to version control
- Use HTTPS for external requests
- Validate all input data
- Implement access controls for APIs

### 8. Deployment

- Use containerization
- Implement health checks
- Use environment-specific configurations
- Automate deployments

### 9. Testing

- Write unit tests for components
- Test with realistic data
- Mock external dependencies
- Test error scenarios

### 10. Documentation

- Document configuration options
- Provide usage examples
- Keep API documentation current
- Document deployment procedures