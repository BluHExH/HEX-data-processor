"""
Web scraper for HEX Data Processor.

Supports static scraping with BeautifulSoup and optional JS rendering.
"""

import asyncio
import time
from typing import Dict, List, Any, Optional, Set
from urllib.parse import urljoin, urlparse
from datetime import datetime

from bs4 import BeautifulSoup
import httpx

from .http_client import HTTPClient
from .logger import get_logger, log_async_function_call
from .config import TargetConfig


class JSError(Exception):
    """Raised when JavaScript rendering is required but not available."""
    pass


class Scraper:
    """Async web scraper with static and JS rendering support."""
    
    def __init__(self, http_client: HTTPClient):
        """Initialize scraper with HTTP client."""
        self.http_client = http_client
        self.logger = get_logger(__name__)
        self._js_renderer_available = self._check_js_renderer()
    
    def _check_js_renderer(self) -> bool:
        """Check if JavaScript renderer is available."""
        try:
            import playwright  # noqa: F401
            return True
        except ImportError:
            try:
                import selenium  # noqa: F401
                return True
            except ImportError:
                return False
    
    @log_async_function_call()
    async def scrape_target(
        self,
        target_config: TargetConfig,
        run_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape data from a target website.
        
        Args:
            target_config: Target configuration
            run_id: Unique identifier for this scraping run
            
        Returns:
            List of scraped items
        """
        run_id = run_id or f"run_{int(time.time())}"
        
        self.logger.info(
            f"Starting scraping run {run_id} for target {target_config.name}",
            extra={"run_id": run_id, "target": target_config.name}
        )
        
        try:
            if target_config.js_render:
                return await self._scrape_with_js(target_config, run_id)
            else:
                return await self._scrape_static(target_config, run_id)
        except Exception as e:
            self.logger.error(
                f"Scraping failed for target {target_config.name}: {str(e)}",
                extra={"run_id": run_id, "target": target_config.name, "error": str(e)},
                exc_info=True
            )
            raise
    
    @log_async_function_call()
    async def _scrape_static(
        self,
        target_config: TargetConfig,
        run_id: str
    ) -> List[Dict[str, Any]]:
        """Scrape using static HTML parsing."""
        base_url = str(target_config.base_url)
        items = []
        processed_urls: Set[str] = set()
        
        # Get initial URLs
        urls_to_process = list(target_config.start_urls)
        
        for url in urls_to_process:
            if url in processed_urls:
                continue
                
            processed_urls.add(str(url))
            
            try:
                response = await self.http_client.fetch(str(url))
                soup = BeautifulSoup(response['text'], 'html.parser')
                
                # Extract items from current page
                page_items = await self._extract_items(soup, target_config, str(url), run_id)
                items.extend(page_items)
                
                self.logger.info(
                    f"Extracted {len(page_items)} items from {url}",
                    extra={"run_id": run_id, "url": str(url), "items_count": len(page_items)}
                )
                
                # Handle pagination if enabled
                if target_config.pagination and target_config.pagination.enabled:
                    next_url = self._get_next_url(soup, target_config, base_url)
                    if next_url and next_url not in processed_urls:
                        urls_to_process.append(next_url)
                        # Limit pagination to avoid infinite loops
                        if len(urls_to_process) > target_config.pagination.max_pages:
                            break
                
            except Exception as e:
                self.logger.error(
                    f"Failed to scrape {url}: {str(e)}",
                    extra={"run_id": run_id, "url": str(url), "error": str(e)}
                )
                continue
        
        self.logger.info(
            f"Static scraping completed. Total items: {len(items)}",
            extra={"run_id": run_id, "total_items": len(items)}
        )
        
        return items
    
    @log_async_function_call()
    async def _scrape_with_js(
        self,
        target_config: TargetConfig,
        run_id: str
    ) -> List[Dict[str, Any]]:
        """Scrape using JavaScript rendering."""
        if not self._js_renderer_available:
            raise JSError(
                "JavaScript rendering requested but no JS renderer available. "
                "Install playwright or selenium to enable JS rendering."
            )
        
        self.logger.warning(
            "JS rendering not implemented in this starter version. "
            "Install playwright and implement JS rendering logic.",
            extra={"run_id": run_id, "target": target_config.name}
        )
        
        # Fallback to static scraping for demo
        return await self._scrape_static(target_config, run_id)
    
    @log_async_function_call()
    async def _extract_items(
        self,
        soup: BeautifulSoup,
        target_config: TargetConfig,
        source_url: str,
        run_id: str
    ) -> List[Dict[str, Any]]:
        """Extract items from BeautifulSoup object."""
        items = []
        
        # Find all item containers
        item_selector = target_config.selectors.get('quote')  # Default to 'quote'
        if not item_selector:
            self.logger.warning(
                "No item selector found in configuration",
                extra={"run_id": run_id, "url": source_url}
            )
            return items
        
        item_elements = soup.select(item_selector)
        
        for element in item_elements:
            item = await self._extract_item_data(element, target_config, source_url, run_id)
            if item:
                items.append(item)
        
        return items
    
    @log_async_function_call()
    async def _extract_item_data(
        self,
        element,
        target_config: TargetConfig,
        source_url: str,
        run_id: str
    ) -> Optional[Dict[str, Any]]:
        """Extract data from a single item element."""
        try:
            item_data = {}
            
            # Extract each field based on selectors
            for field, selector in target_config.selectors.items():
                if field == 'quote':  # Skip the container selector
                    continue
                
                if selector.endswith('::text'):
                    # Extract text content
                    css_selector = selector.replace('::text', '')
                    elements = element.select(css_selector)
                    if elements:
                        text_content = elements[0].get_text(strip=True)
                        item_data[field] = text_content
                    else:
                        item_data[field] = None
                elif selector.endswith('::attr(href)'):
                    # Extract attribute value
                    css_selector = selector.replace('::attr(href)', '')
                    elements = element.select(css_selector)
                    if elements:
                        attr_value = elements[0].get('href', '')
                        # Convert relative URLs to absolute
                        if attr_value:
                            item_data[field] = urljoin(source_url, attr_value)
                        else:
                            item_data[field] = None
                    else:
                        item_data[field] = None
                else:
                    # Extract full HTML content
                    elements = element.select(selector)
                    if elements:
                        item_data[field] = elements[0].get_text(strip=True)
                    else:
                        item_data[field] = None
            
            # Add metadata
            item_data.update({
                'source_url': source_url,
                'fetch_time': datetime.utcnow().isoformat() + 'Z',
                'run_id': run_id,
                'target_name': target_config.name
            })
            
            return item_data
            
        except Exception as e:
            self.logger.error(
                f"Failed to extract item data: {str(e)}",
                extra={"run_id": run_id, "url": source_url, "error": str(e)}
            )
            return None
    
    def _get_next_url(
        self,
        soup: BeautifulSoup,
        target_config: TargetConfig,
        base_url: str
    ) -> Optional[str]:
        """Get next page URL for pagination."""
        if not target_config.pagination or not target_config.pagination.next_selector:
            return None
        
        try:
            next_element = soup.select_one(target_config.pagination.next_selector)
            if next_element:
                next_url = next_element.get('href') or next_element.get_text(strip=True)
                if next_url:
                    return urljoin(base_url, next_url)
        except Exception as e:
            self.logger.error(f"Error getting next URL: {str(e)}")
        
        return None
    
    async def check_robots_txt(self, url: str) -> Dict[str, Any]:
        """Check robots.txt for scraping permissions."""
        try:
            parsed_url = urlparse(url)
            robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
            
            response = await self.http_client.fetch(robots_url)
            content = response['text']
            
            return {
                "url": robots_url,
                "content": content,
                "accessible": True
            }
        except Exception as e:
            self.logger.warning(f"Could not fetch robots.txt: {str(e)}")
            return {
                "url": robots_url if 'robots_url' in locals() else url + "/robots.txt",
                "content": None,
                "accessible": False,
                "error": str(e)
            }


if __name__ == "__main__":
    # Test scraper
    async def test_scraper():
        from .config import load_config
        
        config = load_config("config_example.json")
        target_config = config.get_target("quotes_toscrape")
        
        http_client = HTTPClient(
            timeout=30,
            max_retries=3,
            rate_limit=1.0
        )
        
        scraper = Scraper(http_client)
        
        async with http_client:
            items = await scraper.scrape_target(target_config, "test_run")
            print(f"Scraped {len(items)} items")
            
            if items:
                print("First item:", items[0])
    
    asyncio.run(test_scraper())