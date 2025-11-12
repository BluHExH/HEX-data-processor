"""
Async HTTP client wrapper for HEX Data Processor.

Provides retry logic, rate limiting, and error handling for web requests.
"""

import asyncio
import random
import time
import logging
from typing import Dict, Any, Optional, Union, List
from urllib.parse import urljoin, urlparse

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

from .logger import get_logger, log_async_function_call


class HTTPClient:
    """Async HTTP client with retry logic and rate limiting."""
    
    def __init__(
        self,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit: float = 1.0,
        user_agent: str = "HEX-Data-Processor/1.0",
        headers: Optional[Dict[str, str]] = None,
        proxies: Optional[Dict[str, str]] = None,
        follow_redirects: bool = True
    ):
        """
        Initialize HTTP client.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            rate_limit: Minimum time between requests in seconds
            user_agent: User agent string
            headers: Additional headers
            proxies: Proxy configuration
            follow_redirects: Whether to follow redirects
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit = rate_limit
        self.user_agent = user_agent
        self.headers = headers or {}
        self.proxies = proxies
        self.follow_redirects = follow_redirects
        
        self.logger = get_logger(__name__)
        
        # Rate limiting
        self._last_request_time = 0.0
        self._request_lock = asyncio.Lock()
        
        # Default headers
        self.default_headers = {
            "User-Agent": user_agent,
            **self.headers
        }
        
        # Initialize HTTP client
        self._client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_client()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def _ensure_client(self):
        """Ensure HTTP client is initialized."""
        if self._client is None or self._client.is_closed:
            timeout_config = httpx.Timeout(self.timeout)
            self._client = httpx.AsyncClient(
                timeout=timeout_config,
                headers=self.default_headers,
                proxies=self.proxies,
                follow_redirects=self.follow_redirects
            )
    
    async def close(self):
        """Close HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
    
    async def _apply_rate_limit(self):
        """Apply rate limiting between requests."""
        async with self._request_lock:
            current_time = time.time()
            time_since_last = current_time - self._last_request_time
            
            if time_since_last < self.rate_limit:
                sleep_time = self.rate_limit - time_since_last + random.uniform(0, 0.1)
                self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
            
            self._last_request_time = time.time()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((
            httpx.RequestError,
            httpx.TimeoutException,
            httpx.NetworkError
        )),
        before_sleep=before_sleep_log(get_logger(__name__), logging.WARNING)
    )
    @log_async_function_call()
    async def fetch(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Union[str, Dict[str, Any], bytes]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch URL with retry logic and rate limiting.
        
        Args:
            url: URL to fetch
            method: HTTP method
            headers: Additional headers
            params: Query parameters
            data: Request body data
            json_data: JSON request body
            **kwargs: Additional httpx arguments
            
        Returns:
            Dictionary with response data
        """
        await self._ensure_client()
        await self._apply_rate_limit()
        
        # Merge headers
        request_headers = {**self.default_headers}
        if headers:
            request_headers.update(headers)
        
        self.logger.info(f"Making {method} request to {url}")
        
        try:
            response = await self._client.request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                data=data,
                json=json_data,
                **kwargs
            )
            
            response.raise_for_status()
            
            result = {
                "url": str(response.url),
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "content": response.content,
                "text": response.text,
                "encoding": response.encoding,
                "request_time": response.elapsed.total_seconds() if response.elapsed else None
            }
            
            # Try to parse JSON if possible
            try:
                result["json"] = response.json()
            except:
                result["json"] = None
            
            self.logger.info(
                f"Request successful: {response.status_code} from {url}",
                extra={
                    "status_code": response.status_code,
                    "url": url,
                    "request_time": result["request_time"]
                }
            )
            
            return result
            
        except httpx.HTTPStatusError as e:
            self.logger.error(
                f"HTTP error {e.response.status_code} for {url}: {e.response.text}",
                extra={
                    "status_code": e.response.status_code,
                    "url": url,
                    "error_text": e.response.text
                }
            )
            raise
        except httpx.RequestError as e:
            self.logger.error(f"Request error for {url}: {str(e)}", extra={"url": url})
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error for {url}: {str(e)}", extra={"url": url})
            raise
    
    async def fetch_multiple(
        self,
        urls: List[str],
        method: str = "GET",
        max_concurrent: int = 10,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple URLs concurrently with rate limiting.
        
        Args:
            urls: List of URLs to fetch
            method: HTTP method
            max_concurrent: Maximum concurrent requests
            **kwargs: Additional fetch arguments
            
        Returns:
            List of response dictionaries
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(url: str) -> Dict[str, Any]:
            async with semaphore:
                try:
                    return await self.fetch(url, method=method, **kwargs)
                except Exception as e:
                    self.logger.error(f"Failed to fetch {url}: {str(e)}")
                    return {
                        "url": url,
                        "error": str(e),
                        "status_code": None,
                        "content": None,
                        "text": None,
                        "json": None
                    }
        
        tasks = [fetch_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and return successful results
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Request failed with exception: {str(result)}")
            else:
                valid_results.append(result)
        
        return valid_results
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get session information."""
        return {
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "rate_limit": self.rate_limit,
            "user_agent": self.user_agent,
            "headers": self.headers,
            "proxies": self.proxies,
            "client_closed": self._client.is_closed if self._client else True
        }


if __name__ == "__main__":
    # Test HTTP client
    async def test_client():
        client = HTTPClient(rate_limit=0.5)
        
        async with client:
            # Test single request
            response = await client.fetch("https://httpbin.org/get")
            print(f"Status: {response['status_code']}")
            print(f"URL: {response['url']}")
            
            # Test multiple requests
            urls = [
                "https://httpbin.org/delay/1",
                "https://httpbin.org/delay/2",
                "https://httpbin.org/status/200"
            ]
            responses = await client.fetch_multiple(urls, max_concurrent=2)
            print(f"Received {len(responses)} responses")
            
            for resp in responses:
                print(f"  {resp['url']}: {resp['status_code']}")
    
    asyncio.run(test_client())