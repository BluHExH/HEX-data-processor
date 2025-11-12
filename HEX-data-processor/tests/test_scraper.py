"""
Tests for scraper module.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.scraper import Scraper, JSError
from src.config import TargetConfig, PaginationConfig
from src.http_client import HTTPClient


class TestScraper:
    """Test scraper functionality."""
    
    @pytest.fixture
    def mock_http_client(self):
        """Create mock HTTP client."""
        client = MagicMock(spec=HTTPClient)
        client.fetch = AsyncMock()
        return client
    
    @pytest.fixture
    def target_config(self):
        """Create test target configuration."""
        return TargetConfig(
            name="Test Target",
            base_url="https://example.com",
            start_urls=["https://example.com"],
            selectors={
                "quote": "div.quote",
                "text": "span.text::text",
                "author": "small.author::text"
            },
            pagination=PaginationConfig(
                enabled=False,
                next_selector=None,
                max_pages=10
            ),
            js_render=False,
            rate_limit=1.0
        )
    
    @pytest.fixture
    def scraper(self, mock_http_client):
        """Create scraper instance."""
        return Scraper(mock_http_client)
    
    @pytest.mark.asyncio
    async def test_scrape_static_success(self, scraper, target_config, mock_http_client):
        """Test successful static scraping."""
        # Mock HTML response
        mock_response = {
            "url": "https://example.com",
            "status_code": 200,
            "text": """
            <html>
                <body>
                    <div class="quote">
                        <span class="text">Test quote 1</span>
                        <small class="author">Author 1</small>
                    </div>
                    <div class="quote">
                        <span class="text">Test quote 2</span>
                        <small class="author">Author 2</small>
                    </div>
                </body>
            </html>
            """
        }
        
        mock_http_client.fetch.return_value = mock_response
        
        # Run scraper
        items = await scraper.scrape_target(target_config, "test_run")
        
        # Assertions
        assert len(items) == 2
        assert items[0]["text"] == "Test quote 1"
        assert items[0]["author"] == "Author 1"
        assert items[0]["source_url"] == "https://example.com"
        assert items[0]["run_id"] == "test_run"
        assert items[0]["target_name"] == "Test Target"
        
        assert items[1]["text"] == "Test quote 2"
        assert items[1]["author"] == "Author 2"
    
    @pytest.mark.asyncio
    async def test_scrape_with_pagination(self, scraper, mock_http_client):
        """Test scraping with pagination."""
        # Configure target with pagination
        target_config = TargetConfig(
            name="Test Target",
            base_url="https://example.com",
            start_urls=["https://example.com"],
            selectors={
                "quote": "div.quote",
                "text": "span.text::text",
                "author": "small.author::text"
            },
            pagination=PaginationConfig(
                enabled=True,
                next_selector="li.next a::attr(href)",
                max_pages=2
            ),
            js_render=False,
            rate_limit=1.0
        )
        
        # Mock first page
        first_page_response = {
            "url": "https://example.com",
            "status_code": 200,
            "text": """
            <html>
                <body>
                    <div class="quote">
                        <span class="text">Quote 1</span>
                        <small class="author">Author 1</small>
                    </div>
                    <li class="next">
                        <a href="/page/2">Next</a>
                    </li>
                </body>
            </html>
            """
        }
        
        # Mock second page
        second_page_response = {
            "url": "https://example.com/page/2",
            "status_code": 200,
            "text": """
            <html>
                <body>
                    <div class="quote">
                        <span class="text">Quote 2</span>
                        <small class="author">Author 2</small>
                    </div>
                </body>
            </html>
            """
        }
        
        mock_http_client.fetch.side_effect = [first_page_response, second_page_response]
        
        # Run scraper
        items = await scraper.scrape_target(target_config, "test_run")
        
        # Assertions
        assert len(items) == 2
        assert items[0]["text"] == "Quote 1"
        assert items[1]["text"] == "Quote 2"
        assert mock_http_client.fetch.call_count == 2
    
    @pytest.mark.asyncio
    async def test_scrape_with_js_render_unavailable(self, scraper, target_config):
        """Test JS rendering when not available."""
        # Enable JS rendering
        target_config.js_render = True
        
        # Should raise JSError or fall back to static
        items = await scraper.scrape_target(target_config, "test_run")
        
        # Should still work by falling back to static scraping
        assert isinstance(items, list)
    
    @pytest.mark.asyncio
    async def test_extract_items_no_selector(self, scraper, target_config, mock_http_client):
        """Test extraction when no item selector is provided."""
        # Remove item selector
        target_config.selectors = {"text": "span.text::text"}
        
        mock_response = {
            "url": "https://example.com",
            "status_code": 200,
            "text": "<html><body><span class='text'>Test</span></body></html>"
        }
        
        mock_http_client.fetch.return_value = mock_response
        
        # Run scraper
        items = await scraper.scrape_target(target_config, "test_run")
        
        # Should return empty list
        assert len(items) == 0
    
    @pytest.mark.asyncio
    async def test_get_next_url(self, scraper):
        """Test getting next URL from pagination."""
        from bs4 import BeautifulSoup
        
        html = """
        <html>
            <li class="next">
                <a href="/page/2">Next Page</a>
            </li>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        target_config = TargetConfig(
            name="Test",
            base_url="https://example.com",
            start_urls=["https://example.com"],
            selectors={"quote": "div.quote"},
            pagination=PaginationConfig(enabled=True, next_selector="li.next a")
        )
        
        next_url = scraper._get_next_url(soup, target_config, "https://example.com")
        assert next_url == "https://example.com/page/2"
    
    @pytest.mark.asyncio
    async def test_check_robots_txt(self, scraper, mock_http_client):
        """Test robots.txt checking."""
        mock_response = {
            "url": "https://example.com/robots.txt",
            "status_code": 200,
            "text": "User-agent: *\nAllow: /"
        }
        
        mock_http_client.fetch.return_value = mock_response
        
        result = await scraper.check_robots_txt("https://example.com")
        
        assert result["accessible"] is True
        assert "User-agent: *" in result["content"]
    
    @pytest.mark.asyncio
    async def test_extract_item_data_error_handling(self, scraper, target_config):
        """Test error handling in item extraction."""
        from bs4 import BeautifulSoup
        
        # Create malformed element
        html = '<div class="quote"><span class="text">Valid</span></div>'
        soup = BeautifulSoup(html, 'html.parser')
        element = soup.find('div')
        
        # This should work
        item = await scraper._extract_item_data(element, target_config, "test_url", "test_run")
        assert item is not None
    
    def test_js_renderer_check(self):
        """Test JS renderer availability check."""
        scraper = Scraper(MagicMock())
        
        # Should return boolean
        assert isinstance(scraper._js_renderer_available, bool)


if __name__ == "__main__":
    pytest.main([__file__])