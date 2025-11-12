"""
CSV storage adapter for HEX Data Processor.

Stores data in CSV format with configurable delimiters and encoding.
"""

import csv
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

import aiofiles

import json
from .base import StorageAdapter
from ..logger import get_logger


class CSVStorageAdapter(StorageAdapter):
    """Storage adapter for CSV files."""
    
    def __init__(
        self,
        filename: str,
        delimiter: str = ",",
        quoting: int = csv.QUOTE_MINIMAL,
        encoding: str = "utf-8",
        write_header: bool = True,
        mode: str = "a"  # 'a' for append, 'w' for write
    ):
        """
        Initialize CSV storage adapter.
        
        Args:
            filename: Path to CSV file
            delimiter: CSV delimiter character
            quoting: CSV quoting mode
            encoding: File encoding
            write_header: Whether to write header row
            mode: File write mode
        """
        super().__init__(
            filename=filename,
            delimiter=delimiter,
            quoting=quoting,
            encoding=encoding,
            write_header=write_header,
            mode=mode
        )
        
        self.filename = filename
        self.delimiter = delimiter
        self.quoting = quoting
        self.encoding = encoding
        self.write_header = write_header
        self.mode = mode
        
        self.logger = get_logger(__name__)
        
        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        self._file_lock = asyncio.Lock()
        self._header_written = False
    
    async def save(self, items: List[Dict[str, Any]]) -> bool:
        """Save multiple items to CSV file."""
        if not items:
            return True
        
        try:
            async with self._file_lock:
                # Determine if we need to write header
                file_exists = os.path.exists(self.filename)
                should_write_header = self.write_header and (not file_exists or self.mode == 'w')
                
                # Open file for writing
                mode = 'w' if self.mode == 'w' else 'a'
                async with aiofiles.open(self.filename, mode=mode, newline='', encoding=self.encoding) as f:
                    # Get all possible fields from all items
                    all_fields = set()
                    for item in items:
                        all_fields.update(item.keys())
                    
                    fieldnames = sorted(list(all_fields))
                    
                    # Write CSV content synchronously (csv module doesn't support async)
                    content = await self._prepare_csv_content(items, fieldnames, should_write_header)
                    await f.write(content)
                
                self.logger.info(f"Saved {len(items)} items to {self.filename}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to save items to CSV: {str(e)}", exc_info=True)
            return False
    
    async def _prepare_csv_content(
        self, 
        items: List[Dict[str, Any]], 
        fieldnames: List[str], 
        write_header: bool
    ) -> str:
        """Prepare CSV content as string."""
        import io
        
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            delimiter=self.delimiter,
            quoting=self.quoting
        )
        
        if write_header:
            writer.writeheader()
        
        for item in items:
            # Convert all values to strings for CSV
            row = {}
            for field in fieldnames:
                value = item.get(field, "")
                if value is None:
                    row[field] = ""
                elif isinstance(value, (list, dict)):
                    # Convert complex types to JSON strings
                    import json
                    row[field] = json.dumps(value, ensure_ascii=False)
                else:
                    row[field] = str(value)
            
            writer.writerow(row)
        
        return output.getvalue()
    
    async def save_one(self, item: Dict[str, Any]) -> bool:
        """Save a single item to CSV file."""
        return await self.save([item])
    
    async def load(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Load items from CSV file."""
        if not os.path.exists(self.filename):
            return []
        
        try:
            items = []
            
            async with self._file_lock:
                async with aiofiles.open(self.filename, 'r', encoding=self.encoding) as f:
                    content = await f.read()
                    
                    # Parse CSV content synchronously
                    import io
                    input_io = io.StringIO(content)
                    reader = csv.DictReader(input_io, delimiter=self.delimiter)
                    
                    for row in reader:
                        # Try to parse JSON fields
                        for key, value in row.items():
                            if value and value.startswith(('{', '[')):
                                try:
                                    row[key] = json.loads(value)
                                except:
                                    pass  # Keep as string if not valid JSON
                        
                        items.append(row)
                        
                        if limit and len(items) >= limit:
                            break
            
            self.logger.info(f"Loaded {len(items)} items from {self.filename}")
            return items
            
        except Exception as e:
            self.logger.error(f"Failed to load items from CSV: {str(e)}", exc_info=True)
            return []
    
    async def count(self) -> int:
        """Count items in CSV file."""
        if not os.path.exists(self.filename):
            return 0
        
        try:
            count = 0
            async with self._file_lock:
                async with aiofiles.open(self.filename, 'r', encoding=self.encoding) as f:
                    # Skip header row
                    first_line = await f.readline()
                    if not first_line:
                        return 0
                    
                    # Count remaining lines
                    async for line in f:
                        if line.strip():  # Skip empty lines
                            count += 1
            
            return count
            
        except Exception as e:
            self.logger.error(f"Failed to count CSV items: {str(e)}")
            return 0
    
    async def clear(self) -> bool:
        """Clear all items from CSV file."""
        try:
            async with self._file_lock:
                if os.path.exists(self.filename):
                    async with aiofiles.open(self.filename, 'w', encoding=self.encoding) as f:
                        # Write empty file or just header
                        if self.write_header:
                            await f.write("")  # Will be handled by next save operation
                
            self.logger.info(f"Cleared CSV file: {self.filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clear CSV file: {str(e)}")
            return False
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about the CSV storage."""
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
    # Test CSV storage
    async def test_csv_storage():
        storage = CSVStorageAdapter("test_output.csv", mode="w")
        
        # Test save
        test_items = [
            {"name": "John", "age": 30, "tags": ["dev", "python"]},
            {"name": "Jane", "age": 25, "tags": ["design", "ui"]},
        ]
        
        result = await storage.save(test_items)
        print(f"Save result: {result}")
        
        # Test load
        loaded = await storage.load()
        print(f"Loaded {len(loaded)} items:")
        for item in loaded:
            print(item)
        
        # Test count
        count = await storage.count()
        print(f"Item count: {count}")
        
        await storage.close()
    
    asyncio.run(test_csv_storage())