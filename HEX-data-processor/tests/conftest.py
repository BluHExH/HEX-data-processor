"""
Pytest configuration and fixtures.
"""

import pytest
import asyncio
import tempfile
import json
from pathlib import Path

from src.config import Config, TargetConfig, CleanerConfig, TransformerConfig, StorageConfig


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def temp_config_file():
    """Create a temporary configuration file."""
    config_data = {
        "project": {
            "name": "Test Project",
            "version": "1.0.0",
            "description": "Test configuration"
        },
        "scraper": {
            "user_agent": "TestBot/1.0",
            "timeout": 30,
            "max_retries": 3,
            "rate_limit": 1.0,
            "max_concurrent": 5
        },
        "targets": {
            "test_target": {
                "name": "Test Target",
                "base_url": "https://example.com",
                "start_urls": ["https://example.com"],
                "selectors": {
                    "item": "div.item",
                    "title": "h2.title::text",
                    "content": "div.content::text"
                },
                "pagination": {
                    "enabled": False,
                    "next_selector": None,
                    "max_pages": 10
                },
                "js_render": False,
                "rate_limit": 1.0
            }
        },
        "cleaner": {
            "remove_duplicates": True,
            "duplicate_keys": ["title"],
            "handle_missing": {
                "strategy": "default",
                "default_values": {
                    "title": "Untitled",
                    "content": "No content"
                }
            }
        },
        "transformer": {
            "field_mapping": {
                "title": "item_title",
                "content": "item_content"
            },
            "type_conversions": {
                "item_title": "string",
                "item_content": "string"
            },
            "custom_functions": {
                "title_length": "lambda item: len(item.get('item_title', ''))"
            }
        },
        "storage": {
            "type": "csv",
            "path": "./test_data",
            "filename_template": "test_{timestamp}.csv"
        },
        "notifications": {
            "enabled": False
        },
        "scheduler": {
            "enabled": False
        },
        "metrics": {
            "enabled": True,
            "port": 8001
        },
        "logging": {
            "level": "INFO",
            "format": "text",
            "console": True
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(config_data, f)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    import os
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def sample_config():
    """Create a sample configuration object."""
    return Config(
        project={
            "name": "Test Project",
            "version": "1.0.0"
        },
        scraper={
            "user_agent": "TestBot/1.0",
            "timeout": 30,
            "max_retries": 3,
            "rate_limit": 1.0
        },
        targets={
            "test": TargetConfig(
                name="Test Target",
                base_url="https://example.com",
                start_urls=["https://example.com"],
                selectors={"title": "h1::text"}
            )
        },
        storage=StorageConfig(type="csv", path="./test_data")
    )


@pytest.fixture
def sample_items():
    """Create sample data items for testing."""
    return [
        {
            "title": "First Item",
            "content": "This is the first item content.",
            "author": "John Doe",
            "tags": ["test", "sample"],
            "url": "https://example.com/1",
            "date": "2024-01-15"
        },
        {
            "title": "Second Item",
            "content": "This is the second item content.",
            "author": "Jane Smith",
            "tags": ["example", "demo"],
            "url": "https://example.com/2",
            "date": "2024-01-16"
        },
        {
            "title": "First Item",  # Duplicate
            "content": "This is the first item content.",
            "author": "John Doe",
            "tags": ["test", "sample"],
            "url": "https://example.com/1",
            "date": "2024-01-15"
        }
    ]


@pytest.fixture
def temp_dir():
    """Create a temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def mock_html_content():
    """Sample HTML content for scraping tests."""
    return """
    <html>
        <head><title>Test Page</title></head>
        <body>
            <div class="container">
                <div class="item">
                    <h2 class="title">First Title</h2>
                    <div class="content">First content</div>
                    <span class="author">Author 1</span>
                </div>
                <div class="item">
                    <h2 class="title">Second Title</h2>
                    <div class="content">Second content</div>
                    <span class="author">Author 2</span>
                </div>
                <nav class="pagination">
                    <a class="next" href="/page/2">Next</a>
                </nav>
            </div>
        </body>
    </html>
    """


# Test markers
pytest_plugins = []

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m &quot;not slow&quot;')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers."""
    for item in items:
        # Add unit marker to tests in test_*.py files that aren't integration/performance
        if "integration" not in item.nodeid and "performance" not in item.nodeid:
            item.add_marker(pytest.mark.unit)