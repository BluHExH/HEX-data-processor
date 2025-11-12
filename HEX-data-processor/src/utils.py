"""
Utility functions for HEX Data Processor.

Provides helper functions for parsing, date handling, and common operations.
"""

import re
import json
import hashlib
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, date
from urllib.parse import urljoin, urlparse
from pathlib import Path


def clean_text(text: str) -> str:
    """
    Clean and normalize text content.
    
    Args:
        text: Input text to clean
        
    Returns:
        Cleaned text
    """
    if not isinstance(text, str):
        text = str(text) if text is not None else ""
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove control characters except newlines and tabs
    text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
    
    # Normalize quotes
    text = text.replace('"', '"').replace('"', '"')
    text = text.replace(''', "'").replace(''', "'")
    
    return text


def extract_domain(url: str) -> Optional[str]:
    """
    Extract domain name from URL.
    
    Args:
        url: URL to extract domain from
        
    Returns:
        Domain name or None if invalid URL
    """
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except Exception:
        return None


def normalize_url(url: str, base_url: Optional[str] = None) -> str:
    """
    Normalize and convert relative URLs to absolute.
    
    Args:
        url: URL to normalize
        base_url: Base URL for relative URLs
        
    Returns:
        Normalized absolute URL
    """
    if not url:
        return ""
    
    # Remove fragment
    if "#" in url:
        url = url.split("#")[0]
    
    # Convert to absolute if base URL provided
    if base_url and not url.startswith(('http://', 'https://')):
        url = urljoin(base_url, url)
    
    return url


def generate_hash(content: Union[str, Dict[str, Any]], algorithm: str = "md5") -> str:
    """
    Generate hash for content.
    
    Args:
        content: Content to hash
        algorithm: Hash algorithm ('md5', 'sha1', 'sha256')
        
    Returns:
        Hexadecimal hash string
    """
    if isinstance(content, dict):
        content = json.dumps(content, sort_keys=True, ensure_ascii=False)
    elif not isinstance(content, str):
        content = str(content)
    
    if algorithm == "md5":
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    elif algorithm == "sha1":
        return hashlib.sha1(content.encode('utf-8')).hexdigest()
    elif algorithm == "sha256":
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")


def parse_date(date_str: str, formats: Optional[List[str]] = None) -> Optional[datetime]:
    """
    Parse date string with multiple format attempts.
    
    Args:
        date_str: Date string to parse
        formats: List of date formats to try
        
    Returns:
        Parsed datetime or None if parsing fails
    """
    if not date_str or not isinstance(date_str, str):
        return None
    
    # Default formats to try
    if formats is None:
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%m-%d-%Y',
            '%d-%m-%Y',
            '%B %d, %Y',
            '%b %d, %Y',
            '%d %B %Y',
            '%d %b %Y'
        ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    return None


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    
    while size_bytes >= 1024.0 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f}{size_names[i]}"


def safe_filename(filename: str) -> str:
    """
    Convert string to safe filename.
    
    Args:
        filename: Input filename
        
    Returns:
        Safe filename string
    """
    # Remove or replace unsafe characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Remove control characters
    filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)
    
    # Limit length
    if len(filename) > 255:
        name, ext = Path(filename).stem, Path(filename).suffix
        max_name_len = 255 - len(ext)
        filename = name[:max_name_len] + ext
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    
    return filename or "unnamed"


def extract_emails(text: str) -> List[str]:
    """
    Extract email addresses from text.
    
    Args:
        text: Text to extract emails from
        
    Returns:
        List of email addresses found
    """
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return re.findall(email_pattern, text, re.IGNORECASE)


def extract_phone_numbers(text: str) -> List[str]:
    """
    Extract phone numbers from text.
    
    Args:
        text: Text to extract phone numbers from
        
    Returns:
        List of phone numbers found
    """
    # Simple phone number patterns
    patterns = [
        r'\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}',  # US format
        r'\+?[0-9]{1,3}[-.\s]?[0-9]{3,4}[-.\s]?[0-9]{3,4}[-.\s]?[0-9]{4}',  # International
    ]
    
    phones = []
    for pattern in patterns:
        phones.extend(re.findall(pattern, text))
    
    return phones


def slugify(text: str) -> str:
    """
    Convert text to URL-friendly slug.
    
    Args:
        text: Text to convert
        
    Returns:
        URL-friendly slug
    """
    # Convert to lowercase and replace spaces with hyphens
    slug = text.lower().strip()
    
    # Replace spaces and special characters with hyphens
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'[-\s]+', '-', slug)
    
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    
    return slug or "untitled"


def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple dictionaries recursively.
    
    Args:
        *dicts: Dictionaries to merge
        
    Returns:
        Merged dictionary
    """
    result = {}
    
    for d in dicts:
        if not isinstance(d, dict):
            continue
        
        for key, value in d.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = merge_dicts(result[key], value)
            else:
                result[key] = value
    
    return result


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")
    
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def flatten_dict(d: Dict[str, Any], separator: str = '.') -> Dict[str, Any]:
    """
    Flatten nested dictionary.
    
    Args:
        d: Dictionary to flatten
        separator: Separator for nested keys
        
    Returns:
        Flattened dictionary
    """
    def _flatten(obj, parent_key=''):
        items = []
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{parent_key}{separator}{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    items.extend(_flatten(value, new_key).items())
                else:
                    items.append((new_key, value))
        elif isinstance(obj, list):
            for i, value in enumerate(obj):
                new_key = f"{parent_key}{separator}{i}" if parent_key else str(i)
                if isinstance(value, (dict, list)):
                    items.extend(_flatten(value, new_key).items())
                else:
                    items.append((new_key, value))
        else:
            items.append((parent_key, obj))
        
        return dict(items)
    
    return _flatten(d)


if __name__ == "__main__":
    # Test utility functions
    test_text = "  Hello   World!  \n\tThis is a test.\n"
    print(f"Cleaned text: '{clean_text(test_text)}'")
    
    test_url = "https://example.com/path/to/page"
    print(f"Domain: {extract_domain(test_url)}")
    
    test_date = "2024-01-15 10:30:00"
    parsed_date = parse_date(test_date)
    print(f"Parsed date: {parsed_date}")
    
    print(f"File size: {format_file_size(1024 * 1024 * 5.5)}")
    
    test_filename = "file<>name|with*invalid?chars.txt"
    print(f"Safe filename: {safe_filename(test_filename)}")
    
    test_slug = "Hello World! This is a Test"
    print(f"Slug: {slugify(test_slug)}")
    
    test_dict = {"a": {"b": {"c": 1}}, "x": [1, 2, {"y": 3}]}
    print(f"Flattened dict: {flatten_dict(test_dict)}")