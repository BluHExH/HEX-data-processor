"""
Data cleaner for HEX Data Processor.

Handles missing values, deduplication, and field validation.
"""

import hashlib
from typing import Dict, List, Any, Optional, Set, Union
from datetime import datetime

import pandas as pd
from pydantic import BaseModel, validator

from .logger import get_logger, log_function_call
from .config import CleanerConfig, FieldValidation


class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


class ItemValidator(BaseModel):
    """Pydantic model for item validation."""
    
    @classmethod
    def create_from_config(cls, field_validation: Dict[str, FieldValidation]) -> type:
        """Create a dynamic Pydantic model from validation config."""
        fields = {}
        
        for field_name, validation in field_validation.items():
            field_type = cls._get_python_type(validation.type)
            
            if validation.required:
                fields[field_name] = (field_type, ...)
            else:
                fields[field_name] = (field_type, None)
        
        return cls
    
    @staticmethod
    def _get_python_type(type_str: str) -> type:
        """Convert string type to Python type."""
        type_map = {
            "string": str,
            "int": int,
            "float": float,
            "bool": bool,
            "array": list,
            "dict": dict,
            "datetime": datetime
        }
        return type_map.get(type_str.lower(), str)


class DataCleaner:
    """Data cleaner with deduplication, missing value handling, and validation."""
    
    def __init__(self, config: Optional[CleanerConfig] = None):
        """Initialize data cleaner with configuration."""
        self.config = config or CleanerConfig()
        self.logger = get_logger(__name__)
        
        # Create validator if field validation is configured
        self.item_validator = None
        if self.config.field_validation:
            self.item_validator = ItemValidator.create_from_config(self.config.field_validation)
    
    @log_function_call()
    def clean_data(
        self,
        items: List[Dict[str, Any]],
        cleaning_config: Optional[CleanerConfig] = None
    ) -> List[Dict[str, Any]]:
        """
        Clean a list of data items.
        
        Args:
            items: List of data items to clean
            cleaning_config: Optional override config
            
        Returns:
            List of cleaned items
        """
        config = cleaning_config or self.config
        cleaned_items = items.copy()
        
        self.logger.info(f"Starting data cleaning for {len(items)} items")
        
        # Handle missing values
        if config.handle_missing:
            cleaned_items = self._handle_missing_values(cleaned_items, config.handle_missing)
            self.logger.info(f"After missing value handling: {len(cleaned_items)} items")
        
        # Remove duplicates
        if config.remove_duplicates and config.duplicate_keys:
            cleaned_items = self._remove_duplicates(cleaned_items, config.duplicate_keys)
            self.logger.info(f"After deduplication: {len(cleaned_items)} items")
        
        # Validate fields
        if config.field_validation:
            cleaned_items = self._validate_fields(cleaned_items, config.field_validation)
            self.logger.info(f"After validation: {len(cleaned_items)} items")
        
        # Clean individual items
        cleaned_items = [self._clean_item(item) for item in cleaned_items if item]
        
        self.logger.info(f"Data cleaning completed. Final count: {len(cleaned_items)} items")
        
        return cleaned_items
    
    def _clean_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean individual item."""
        if not item:
            return {}
        
        cleaned = {}
        
        for key, value in item.items():
            if value is None:
                cleaned[key] = None
            elif isinstance(value, str):
                # Clean string values
                cleaned[key] = self._clean_string(value)
            elif isinstance(value, list):
                # Clean list values
                cleaned[key] = [self._clean_string(v) if isinstance(v, str) else v for v in value]
            else:
                cleaned[key] = value
        
        return cleaned
    
    def _clean_string(self, value: str) -> str:
        """Clean string value."""
        if not isinstance(value, str):
            return str(value) if value is not None else ""
        
        # Strip whitespace and normalize
        cleaned = value.strip()
        
        # Replace multiple spaces with single space
        while '  ' in cleaned:
            cleaned = cleaned.replace('  ', ' ')
        
        # Remove control characters except newlines and tabs
        cleaned = ''.join(char for char in cleaned if ord(char) >= 32 or char in '\n\t')
        
        return cleaned
    
    def _handle_missing_values(
        self,
        items: List[Dict[str, Any]],
        missing_config
    ) -> List[Dict[str, Any]]:
        """Handle missing values based on configuration."""
        if missing_config.strategy == "drop":
            return self._drop_missing_values(items, missing_config.default_values)
        elif missing_config.strategy == "default":
            return self._fill_missing_values(items, missing_config.default_values)
        elif missing_config.strategy == "interpolate":
            return self._interpolate_missing_values(items, missing_config.default_values)
        else:
            return items
    
    def _drop_missing_values(
        self,
        items: List[Dict[str, Any]],
        default_values: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Drop items with missing required values."""
        required_fields = [field for field, config in self.config.field_validation.items() 
                          if config.required]
        
        if not required_fields:
            return items
        
        filtered_items = []
        for item in items:
            missing_required = any(
                item.get(field) is None or item.get(field) == "" 
                for field in required_fields
            )
            
            if not missing_required:
                filtered_items.append(item)
        
        return filtered_items
    
    def _fill_missing_values(
        self,
        items: List[Dict[str, Any]],
        default_values: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Fill missing values with defaults."""
        filled_items = []
        
        for item in items:
            filled_item = item.copy()
            
            for field, default_value in default_values.items():
                if field in filled_item and (filled_item[field] is None or filled_item[field] == ""):
                    filled_item[field] = default_value
                elif field not in filled_item:
                    filled_item[field] = default_value
            
            filled_items.append(filled_item)
        
        return filled_items
    
    def _interpolate_missing_values(
        self,
        items: List[Dict[str, Any]],
        default_values: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Interpolate missing values (simple implementation)."""
        # For now, just use default value filling
        return self._fill_missing_values(items, default_values)
    
    def _remove_duplicates(
        self,
        items: List[Dict[str, Any]],
        duplicate_keys: List[str]
    ) -> List[Dict[str, Any]]:
        """Remove duplicate items based on specified keys."""
        seen_hashes: Set[str] = set()
        unique_items = []
        
        for item in items:
            # Create hash based on duplicate keys
            hash_input = {}
            for key in duplicate_keys:
                hash_input[key] = item.get(key)
            
            # Generate hash
            hash_str = str(sorted(hash_input.items()))
            item_hash = hashlib.md5(hash_str.encode()).hexdigest()
            
            if item_hash not in seen_hashes:
                seen_hashes.add(item_hash)
                unique_items.append(item)
            else:
                self.logger.debug(f"Duplicate item removed: {hash_input}")
        
        return unique_items
    
    def _validate_fields(
        self,
        items: List[Dict[str, Any]],
        field_validation: Dict[str, FieldValidation]
    ) -> List[Dict[str, Any]]:
        """Validate fields using Pydantic models."""
        if not self.item_validator:
            return items
        
        validated_items = []
        validation_errors = 0
        
        for item in items:
            try:
                # Validate item using Pydantic
                validated_item = self.item_validator(**item)
                validated_items.append(validated_item.dict())
            except Exception as e:
                validation_errors += 1
                self.logger.debug(f"Item validation failed: {str(e)}")
        
        if validation_errors > 0:
            self.logger.warning(f"{validation_errors} items failed validation")
        
        return validated_items
    
    def get_cleaning_stats(self, original_items: List[Dict[str, Any]], 
                          cleaned_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get cleaning statistics."""
        return {
            "original_count": len(original_items),
            "cleaned_count": len(cleaned_items),
            "removed_count": len(original_items) - len(cleaned_items),
            "removal_rate": (len(original_items) - len(cleaned_items)) / len(original_items) if original_items else 0
        }


if __name__ == "__main__":
    # Test data cleaner
    test_items = [
        {"text": "Hello World", "author": "John Doe", "tags": ["test"]},
        {"text": "  Hello   World  ", "author": None, "tags": []},
        {"text": "Hello World", "author": "John Doe", "tags": ["test"]},  # Duplicate
        {"text": "", "author": "Jane Smith", "tags": None},
    ]
    
    config = CleanerConfig(
        remove_duplicates=True,
        duplicate_keys=["text", "author"],
        handle_missing={
            "strategy": "default",
            "default_values": {
                "text": "N/A",
                "author": "Unknown",
                "tags": []
            }
        }
    )
    
    cleaner = DataCleaner(config)
    cleaned = cleaner.clean_data(test_items)
    
    print(f"Original: {len(test_items)} items")
    print(f"Cleaned: {len(cleaned)} items")
    
    for item in cleaned:
        print(item)