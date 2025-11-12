"""
JSONL storage adapter for HEX Data Processor.

Stores data in JSON Lines format (one JSON object per line).
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

import aiofiles

from .base import StorageAdapter
from ..logger import get_logger


class JSONLStorageAdapter(StorageAdapter):
    """Storage adapter for JSONL files."""
    
    def __init__(
        self,
        filename: str,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
        mode: str = "a"  # 'a' for append, 'w' for write
    ):
        """
        Initialize JSONL storage adapter.
        
        Args:
            filename: Path to JSONL file
            encoding: File encoding
            ensure_ascii: Whether to ensure ASCII encoding
            mode: File write mode
        """
        super().__init__(
            filename=filename,
            encoding=encoding,
            ensure_ascii=ensure_ascii,
            mode=mode
        )
        
        self.filename = filename
        self.encoding = encoding
        self.ensure_ascii = ensure_ascii
        self.mode = mode
        
        self.logger = get_logger(__name__)
        
        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        self._file_lock = asyncio.Lock()
    
    async def save(self, items: List[Dict[str, Any]]) -> bool:
        """Save multiple items to JSONL file."""
        if not items:
            return True
        
        try:
            async with self._file_lock:
                mode = 'w' if self.mode == 'w' else 'a'
                async with aiofiles.open(self.filename, mode=mode, encoding=self.encoding) as f:
                    for item in items:
                        # Convert item to JSON string
                        json_line = json.dumps(
                            item,
                            ensure_ascii=self.ensure_ascii,
                            default=self._json_serializer,
                            separators=(',', ':')  # Compact JSON
                        )
                        await f.write(json_line + '\n')
                
                self.logger.info(f"Saved {len(items)} items to {self.filename}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to save items to JSONL: {str(e)}", exc_info=True)
            return False
    
    def _json_serializer(self, obj):
        """Custom JSON serializer for non-serializable objects."""
        if hasattr(obj, 'isoformat'):  # datetime objects
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):  # Custom objects
            return obj.__dict__
        else:
            return str(obj)
    
    async def save_one(self, item: Dict[str, Any]) -> bool:
        """Save a single item to JSONL file."""
        return await self.save([item])
    
    async def load(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Load items from JSONL file."""
        if not os.path.exists(self.filename):
            return []
        
        try:
            items = []
            
            async with self._file_lock:
                async with aiofiles.open(self.filename, 'r', encoding=self.encoding) as f:
                    async for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            item = json.loads(line)
                            items.append(item)
                        except json.JSONDecodeError as e:
                            self.logger.warning(f"Invalid JSON line: {str(e)}")
                            continue
                        
                        if limit and len(items) >= limit:
                            break
            
            self.logger.info(f"Loaded {len(items)} items from {self.filename}")
            return items
            
        except Exception as e:
            self.logger.error(f"Failed to load items from JSONL: {str(e)}", exc_info=True)
            return []
    
    async def count(self) -> int:
        """Count items in JSONL file."""
        if not os.path.exists(self.filename):
            return 0
        
        try:
            count = 0
            async with self._file_lock:
                async with aiofiles.open(self.filename, 'r', encoding=self.encoding) as f:
                    async for line in f:
                        if line.strip():  # Skip empty lines
                            count += 1
            
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to count JSONL items: {str(e)}")
            return 0
    
    async def clear(self) -> bool:
        """Clear all items from JSONL file."""
        try:
            async with self._file_lock:
                async with aiofiles.open(self.filename, 'w', encoding=self.encoding) as f:
                    # Create empty file
                    pass
            
            self.logger.info(f"Cleared JSONL file: {self.filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clear JSONL file: {str(e)}")
            return False
    
    async def search(
        self, 
        query: Dict[str, Any], 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search items in JSONL file by field values.
        
        Args:
            query: Dictionary of field-value pairs to match
            limit: Maximum number of results
            
        Returns:
            List of matching items
        """
        if not os.path.exists(self.filename):
            return []
        
        try:
            matches = []
            
            async with self._file_lock:
                async with aiofiles.open(self.filename, 'r', encoding=self.encoding) as f:
                    async for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            item = json.loads(line)
                            
                            # Check if item matches all query conditions
                            is_match = True
                            for field, value in query.items():
                                if field not in item or item[field] != value:
                                    is_match = False
                                    break
                            
                            if is_match:
                                matches.append(item)
                                if limit and len(matches) >= limit:
                                    break
                                    
                        except json.JSONDecodeError:
                            continue
            
            self.logger.info(f"Found {len(matches)} matching items in {self.filename}")
            return matches
            
        except Exception as e:
            self.logger.error(f"Failed to search JSONL file: {str(e)}")
            return []
    
    async def filter_by_field(
        self, 
        field: str, 
        value: Any, 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Filter items by a specific field value.
        
        Args:
            field: Field name to filter by
            value: Value to match
            limit: Maximum number of results
            
        Returns:
            List of matching items
        """
        return await self.search({field: value}, limit)
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about the JSONL storage."""
        info = super().get_storage_info()
        
        # Add file-specific information
        if os.path.exists(self.filename):
            stat = os.stat(self.filename)
            info.update({
                "file_size": stat.st_size,
                "file_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "file_exists": True
            })
        else:
            info.update({
                "file_size": 0,
                "file_exists": False
            })
        
        return info


if __name__ == "__main__":
    # Test JSONL storage
    async def test_jsonl_storage():
        storage = JSONLStorageAdapter("test_output.jsonl", mode="w")
        
        # Test save
        test_items = [
            {"name": "John", "age": 30, "tags": ["dev", "python"], "active": True},
            {"name": "Jane", "age": 25, "tags": ["design", "ui"], "active": False},
            {"name": "Bob", "age": 35, "tags": ["dev", "java"], "active": True},
        ]
        
        result = await storage.save(test_items)
        print(f"Save result: {result}")
        
        # Test load
        loaded = await storage.load()
        print(f"Loaded {len(loaded)} items:")
        for item in loaded:
            print(item)
        
        # Test search
        active_users = await storage.search({"active": True})
        print(f"Active users: {len(active_users)}")
        
        # Test count
        count = await storage.count()
        print(f"Item count: {count}")
        
        await storage.close()
    
    asyncio.run(test_jsonl_storage())