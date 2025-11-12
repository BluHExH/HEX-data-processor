"""
Storage adapters for HEX Data Processor.

Provides multiple storage backends for processed data.
"""

from .base import StorageAdapter
from .csv_storage import CSVStorageAdapter
from .jsonl_storage import JSONLStorageAdapter
from .sqlite_storage import SQLiteStorageAdapter

__all__ = [
    'StorageAdapter',
    'CSVStorageAdapter', 
    'JSONLStorageAdapter',
    'SQLiteStorageAdapter'
]


def create_storage_adapter(storage_type: str, **kwargs) -> StorageAdapter:
    """
    Factory function to create storage adapter by type.
    
    Args:
        storage_type: Type of storage ('csv', 'jsonl', 'sqlite')
        **kwargs: Storage-specific configuration
        
    Returns:
        StorageAdapter instance
    """
    adapters = {
        'csv': CSVStorageAdapter,
        'jsonl': JSONLStorageAdapter,
        'sqlite': SQLiteStorageAdapter
    }
    
    if storage_type not in adapters:
        raise ValueError(f"Unknown storage type: {storage_type}")
    
    return adapters[storage_type](**kwargs)