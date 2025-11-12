"""
SQLite storage adapter for HEX Data Processor.

Stores data in SQLite database with dynamic table creation.
"""

import json
import os
import sqlite3
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import threading

from .base import StorageAdapter
from ..logger import get_logger


class SQLiteStorageAdapter(StorageAdapter):
    """Storage adapter for SQLite database."""
    
    def __init__(
        self,
        database_url: str = "sqlite:///./data/output/hex_processor.db",
        table_name: str = "scraped_data",
        auto_create_table: bool = True,
        primary_key: str = "id"
    ):
        """
        Initialize SQLite storage adapter.
        
        Args:
            database_url: SQLite database URL
            table_name: Name of the table to store data
            auto_create_table: Whether to automatically create table
            primary_key: Primary key column name
        """
        super().__init__(
            database_url=database_url,
            table_name=table_name,
            auto_create_table=auto_create_table,
            primary_key=primary_key
        )
        
        self.database_url = database_url
        self.table_name = table_name
        self.auto_create_table = auto_create_table
        self.primary_key = primary_key
        
        self.logger = get_logger(__name__)
        
        # Parse database URL to get file path
        if database_url.startswith("sqlite:///"):
            self.db_path = database_url[10:]  # Remove "sqlite:///" prefix
        else:
            raise ValueError("Invalid SQLite database URL format")
        
        # Ensure database directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Thread lock for database operations
        self._db_lock = threading.Lock()
        
        # Initialize database
        if self.auto_create_table:
            self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database and create table if needed."""
        try:
            with self._get_connection() as conn:
                # Create table with basic structure
                conn.execute(f'''
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        {self.primary_key} INTEGER PRIMARY KEY AUTOINCREMENT,
                        data TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        run_id TEXT,
                        target_name TEXT
                    )
                ''')
                conn.commit()
                
                self.logger.info(f"SQLite database initialized: {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {str(e)}")
            raise
    
    def _get_connection(self):
        """Get database connection."""
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    async def save(self, items: List[Dict[str, Any]]) -> bool:
        """Save multiple items to SQLite database."""
        if not items:
            return True
        
        try:
            # Run database operations in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._save_sync, items)
            return result
        except Exception as e:
            self.logger.error(f"Failed to save items to SQLite: {str(e)}", exc_info=True)
            return False
    
    def _save_sync(self, items: List[Dict[str, Any]]) -> bool:
        """Synchronous save operation."""
        try:
            with self._db_lock:
                with self._get_connection() as conn:
                    for item in items:
                        # Convert item to JSON string
                        data_json = json.dumps(item, default=self._json_serializer)
                        
                        # Extract metadata
                        run_id = item.get('run_id')
                        target_name = item.get('target_name')
                        
                        # Insert item
                        conn.execute(f'''
                            INSERT INTO {self.table_name} (data, run_id, target_name)
                            VALUES (?, ?, ?)
                        ''', (data_json, run_id, target_name))
                    
                    conn.commit()
            
            self.logger.info(f"Saved {len(items)} items to SQLite table {self.table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save items to SQLite: {str(e)}")
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
        """Save a single item to SQLite database."""
        return await self.save([item])
    
    async def load(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Load items from SQLite database."""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._load_sync, limit)
            return result
        except Exception as e:
            self.logger.error(f"Failed to load items from SQLite: {str(e)}", exc_info=True)
            return []
    
    def _load_sync(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Synchronous load operation."""
        try:
            items = []
            
            with self._db_lock:
                with self._get_connection() as conn:
                    query = f"SELECT data FROM {self.table_name} ORDER BY created_at DESC"
                    
                    if limit:
                        query += f" LIMIT {limit}"
                    
                    cursor = conn.execute(query)
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        try:
                            item = json.loads(row[0])
                            items.append(item)
                        except json.JSONDecodeError as e:
                            self.logger.warning(f"Invalid JSON in database: {str(e)}")
                            continue
            
            self.logger.info(f"Loaded {len(items)} items from SQLite table {self.table_name}")
            return items
            
        except Exception as e:
            self.logger.error(f"Failed to load items from SQLite: {str(e)}")
            return []
    
    async def count(self) -> int:
        """Count items in SQLite table."""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._count_sync)
            return result
        except Exception as e:
            self.logger.error(f"Failed to count SQLite items: {str(e)}")
            return 0
    
    def _count_sync(self) -> int:
        """Synchronous count operation."""
        try:
            with self._db_lock:
                with self._get_connection() as conn:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                    result = cursor.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            self.logger.error(f"Failed to count SQLite items: {str(e)}")
            return 0
    
    async def clear(self) -> bool:
        """Clear all items from SQLite table."""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._clear_sync)
            return result
        except Exception as e:
            self.logger.error(f"Failed to clear SQLite table: {str(e)}")
            return False
    
    def _clear_sync(self) -> bool:
        """Synchronous clear operation."""
        try:
            with self._db_lock:
                with self._get_connection() as conn:
                    conn.execute(f"DELETE FROM {self.table_name}")
                    conn.commit()
            
            self.logger.info(f"Cleared SQLite table {self.table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clear SQLite table: {str(e)}")
            return False
    
    async def search(
        self, 
        query: Dict[str, Any], 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search items in SQLite table by field values.
        
        Args:
            query: Dictionary of field-value pairs to match
            limit: Maximum number of results
            
        Returns:
            List of matching items
        """
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._search_sync, query, limit)
            return result
        except Exception as e:
            self.logger.error(f"Failed to search SQLite table: {str(e)}")
            return []
    
    def _search_sync(self, query: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Synchronous search operation."""
        try:
            matches = []
            
            with self._db_lock:
                with self._get_connection() as conn:
                    # Get all data and filter in Python (SQLite doesn't have JSON query support by default)
                    sql_query = f"SELECT data FROM {self.table_name} ORDER BY created_at DESC"
                    
                    if limit:
                        sql_query += f" LIMIT {limit * 2}"  # Get more items to filter
                    
                    cursor = conn.execute(sql_query)
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        try:
                            item = json.loads(row[0])
                            
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
            
            self.logger.info(f"Found {len(matches)} matching items in SQLite table {self.table_name}")
            return matches
            
        except Exception as e:
            self.logger.error(f"Failed to search SQLite table: {str(e)}")
            return []
    
    async def get_table_info(self) -> Dict[str, Any]:
        """Get information about the SQLite table."""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._get_table_info_sync)
            return result
        except Exception as e:
            self.logger.error(f"Failed to get table info: {str(e)}")
            return {}
    
    def _get_table_info_sync(self) -> Dict[str, Any]:
        """Synchronous table info operation."""
        try:
            with self._db_lock:
                with self._get_connection() as conn:
                    cursor = conn.execute(f"PRAGMA table_info({self.table_name})")
                    columns = cursor.fetchall()
                    
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                    count = cursor.fetchone()[0]
                    
                    return {
                        "table_name": self.table_name,
                        "columns": [col[1] for col in columns],
                        "column_info": columns,
                        "row_count": count
                    }
        except Exception as e:
            self.logger.error(f"Failed to get table info: {str(e)}")
            return {}
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about the SQLite storage."""
        info = super().get_storage_info()
        
        # Add database-specific information
        if os.path.exists(self.db_path):
            stat = os.stat(self.db_path)
            info.update({
                "database_path": self.db_path,
                "table_name": self.table_name,
                "file_size": stat.st_size,
                "file_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "database_exists": True
            })
        else:
            info.update({
                "database_path": self.db_path,
                "table_name": self.table_name,
                "file_size": 0,
                "database_exists": False
            })
        
        return info


if __name__ == "__main__":
    # Test SQLite storage
    async def test_sqlite_storage():
        storage = SQLiteStorageAdapter(
            database_url="sqlite:///test.db",
            table_name="test_data"
        )
        
        # Test save
        test_items = [
            {"name": "John", "age": 30, "tags": ["dev", "python"], "active": True, "run_id": "test1"},
            {"name": "Jane", "age": 25, "tags": ["design", "ui"], "active": False, "run_id": "test1"},
            {"name": "Bob", "age": 35, "tags": ["dev", "java"], "active": True, "run_id": "test2"},
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
        
        # Test table info
        table_info = await storage.get_table_info()
        print(f"Table info: {table_info}")
        
        # Test count
        count = await storage.count()
        print(f"Item count: {count}")
        
        await storage.close()
    
    asyncio.run(test_sqlite_storage())