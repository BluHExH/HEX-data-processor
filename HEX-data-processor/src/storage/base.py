"""
Base storage adapter for HEX Data Processor.

Defines the interface that all storage adapters must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime


class StorageAdapter(ABC):
    """Abstract base class for storage adapters."""
    
    def __init__(self, **kwargs):
        """Initialize storage adapter with configuration."""
        self.config = kwargs
    
    @abstractmethod
    async def save(self, items: List[Dict[str, Any]]) -> bool:
        """
        Save multiple items to storage.
        
        Args:
            items: List of data items to save
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def save_one(self, item: Dict[str, Any]) -> bool:
        """
        Save a single item to storage.
        
        Args:
            item: Data item to save
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    async def load(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Load items from storage.
        
        Args:
            limit: Maximum number of items to load
            
        Returns:
            List of loaded items
        """
        pass
    
    @abstractmethod
    async def count(self) -> int:
        """
        Count items in storage.
        
        Returns:
            Number of items stored
        """
        pass
    
    @abstractmethod
    async def clear(self) -> bool:
        """
        Clear all items from storage.
        
        Returns:
            True if successful, False otherwise
        """
        pass
    
    async def backup(self, backup_path: str) -> bool:
        """
        Create a backup of the storage.
        
        Args:
            backup_path: Path for backup file
            
        Returns:
            True if successful, False otherwise
        """
        # Default implementation - can be overridden by subclasses
        items = await self.load()
        try:
            if backup_path.endswith('.csv'):
                from .csv_storage import CSVStorageAdapter
                backup_adapter = CSVStorageAdapter(filename=backup_path)
            elif backup_path.endswith('.jsonl'):
                from .jsonl_storage import JSONLStorageAdapter
                backup_adapter = JSONLStorageAdapter(filename=backup_path)
            else:
                raise ValueError(f"Unsupported backup format: {backup_path}")
            
            return await backup_adapter.save(items)
        except Exception:
            return False
    
    async def close(self):
        """Close storage adapter and clean up resources."""
        pass
    
    def get_storage_info(self) -> Dict[str, Any]:
        """
        Get information about the storage.
        
        Returns:
            Dictionary with storage information
        """
        return {
            "type": self.__class__.__name__,
            "config": self.config
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on storage.
        
        Returns:
            Dictionary with health status
        """
        try:
            # Basic health check - try to count items
            count = await self.count()
            return {
                "status": "healthy",
                "item_count": count,
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }