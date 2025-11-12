"""
Data transformer for HEX Data Processor.

Handles field mapping, type conversions, and custom transformations.
"""

import ast
import re
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, date
import json

from .logger import get_logger, log_function_call
from .config import TransformerConfig


class TransformationError(Exception):
    """Raised when data transformation fails."""
    pass


class SafeEvaluator:
    """Safe Python expression evaluator for custom functions."""
    
    # Allowed built-in functions and modules
    ALLOWED_BUILTINS = {
        'len': len,
        'str': str,
        'int': int,
        'float': float,
        'bool': bool,
        'abs': abs,
        'min': min,
        'max': max,
        'sum': sum,
        'round': round,
        'sorted': sorted,
        'list': list,
        'dict': dict,
        'set': set,
        'tuple': tuple,
        'range': range,
        'enumerate': enumerate,
        'zip': zip,
        'any': any,
        'all': all,
        'isinstance': isinstance,
        'type': type,
    }
    
    # Date/time utilities
    ALLOWED_DATETIME = {
        'datetime': datetime,
        'date': date,
    }
    
    # String utilities
    ALLOWED_STRING = {
        're': re,
    }
    
    @classmethod
    def evaluate_lambda(cls, lambda_str: str, item: Dict[str, Any]) -> Any:
        """
        Safely evaluate a lambda expression.
        
        Args:
            lambda_str: Lambda expression as string
            item: Data item to transform
            
        Returns:
            Result of lambda evaluation
        """
        try:
            # Parse the lambda expression
            if not lambda_str.strip().startswith('lambda'):
                raise ValueError("Expression must start with 'lambda'")
            
            # Create a safe evaluation environment
            safe_globals = {
                '__builtins__': cls.ALLOWED_BUILTINS,
                **cls.ALLOWED_DATETIME,
                **cls.ALLOWED_STRING,
                'item': item
            }
            
            # Evaluate the lambda
            func = eval(lambda_str, safe_globals)
            
            # Execute the lambda
            return func()
            
        except Exception as e:
            raise TransformationError(f"Failed to evaluate lambda '{lambda_str}': {str(e)}")
    
    @classmethod
    def evaluate_expression(cls, expr_str: str, item: Dict[str, Any]) -> Any:
        """
        Safely evaluate a simple expression.
        
        Args:
            expr_str: Expression as string
            item: Data item to use in evaluation
            
        Returns:
            Result of expression evaluation
        """
        try:
            # Create a safe evaluation environment
            safe_globals = {
                '__builtins__': cls.ALLOWED_BUILTINS,
                **cls.ALLOWED_DATETIME,
                **cls.ALLOWED_STRING,
                'item': item
            }
            
            # Add item fields as variables
            safe_locals = {k: v for k, v in item.items() if isinstance(k, str)}
            
            # Evaluate the expression
            return eval(expr_str, safe_globals, safe_locals)
            
        except Exception as e:
            raise TransformationError(f"Failed to evaluate expression '{expr_str}': {str(e)}")


class DataTransformer:
    """Data transformer with field mapping, type conversion, and custom functions."""
    
    def __init__(self, config: Optional[TransformerConfig] = None):
        """Initialize data transformer with configuration."""
        self.config = config or TransformerConfig()
        self.logger = get_logger(__name__)
        
        # Compile custom functions
        self.custom_functions = {}
        if self.config.custom_functions:
            self._compile_custom_functions()
    
    def _compile_custom_functions(self):
        """Compile custom functions from configuration."""
        for name, func_str in self.config.custom_functions.items():
            try:
                # Store the lambda string for later evaluation
                self.custom_functions[name] = func_str
                self.logger.debug(f"Compiled custom function: {name}")
            except Exception as e:
                self.logger.error(f"Failed to compile custom function '{name}': {str(e)}")
    
    @log_function_call()
    def transform_data(
        self,
        items: List[Dict[str, Any]],
        transform_config: Optional[TransformerConfig] = None
    ) -> List[Dict[str, Any]]:
        """
        Transform a list of data items.
        
        Args:
            items: List of data items to transform
            transform_config: Optional override config
            
        Returns:
            List of transformed items
        """
        config = transform_config or self.config
        transformed_items = []
        
        self.logger.info(f"Starting data transformation for {len(items)} items")
        
        for item in items:
            try:
                transformed_item = self._transform_item(item, config)
                transformed_items.append(transformed_item)
            except Exception as e:
                self.logger.error(f"Failed to transform item: {str(e)}", exc_info=True)
                # Include original item if transformation fails
                transformed_items.append(item)
        
        self.logger.info(f"Data transformation completed for {len(transformed_items)} items")
        
        return transformed_items
    
    def _transform_item(self, item: Dict[str, Any], config: TransformerConfig) -> Dict[str, Any]:
        """Transform a single data item."""
        # Apply field mapping
        mapped_item = self._apply_field_mapping(item, config.field_mapping)
        
        # Apply type conversions
        typed_item = self._apply_type_conversions(mapped_item, config.type_conversions)
        
        # Apply custom functions
        final_item = self._apply_custom_functions(typed_item, config.custom_functions)
        
        return final_item
    
    def _apply_field_mapping(self, item: Dict[str, Any], field_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Apply field mapping to rename fields."""
        if not field_mapping:
            return item.copy()
        
        mapped_item = {}
        
        # Map existing fields to new names
        for old_field, new_field in field_mapping.items():
            if old_field in item:
                mapped_item[new_field] = item[old_field]
        
        # Keep fields that weren't mapped
        for field, value in item.items():
            if field not in field_mapping:
                mapped_item[field] = value
        
        return mapped_item
    
    def _apply_type_conversions(self, item: Dict[str, Any], type_conversions: Dict[str, str]) -> Dict[str, Any]:
        """Apply type conversions to fields."""
        if not type_conversions:
            return item.copy()
        
        converted_item = item.copy()
        
        for field, target_type in type_conversions.items():
            if field in converted_item and converted_item[field] is not None:
                try:
                    converted_item[field] = self._convert_type(
                        converted_item[field], target_type
                    )
                except Exception as e:
                    self.logger.warning(f"Type conversion failed for field '{field}': {str(e)}")
        
        return converted_item
    
    def _convert_type(self, value: Any, target_type: str) -> Any:
        """Convert value to target type."""
        if value is None:
            return None
        
        try:
            if target_type.lower() == "string":
                return str(value)
            elif target_type.lower() == "int":
                return int(float(value)) if isinstance(value, str) else int(value)
            elif target_type.lower() == "float":
                return float(value)
            elif target_type.lower() == "bool":
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                return bool(value)
            elif target_type.lower() == "array":
                if isinstance(value, str):
                    # Try to parse as JSON array
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, list):
                            return parsed
                    except:
                        pass
                    # Split by comma if not JSON
                    return [item.strip() for item in value.split(',')]
                elif isinstance(value, (list, tuple)):
                    return list(value)
                else:
                    return [value]
            elif target_type.lower() == "dict":
                if isinstance(value, str):
                    return json.loads(value)
                elif isinstance(value, dict):
                    return value
                else:
                    return {"value": value}
            elif target_type.lower() == "datetime":
                if isinstance(value, str):
                    # Try common datetime formats
                    formats = [
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d',
                        '%m/%d/%Y',
                        '%d/%m/%Y'
                    ]
                    for fmt in formats:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    # If no format matches, return original string
                    return value
                return value
            else:
                return value
        except Exception as e:
            raise TransformationError(f"Failed to convert {value} to {target_type}: {str(e)}")
    
    def _apply_custom_functions(self, item: Dict[str, Any], custom_functions: Dict[str, str]) -> Dict[str, Any]:
        """Apply custom functions to generate new fields."""
        if not custom_functions:
            return item.copy()
        
        transformed_item = item.copy()
        
        for field_name, func_str in custom_functions.items():
            try:
                result = SafeEvaluator.evaluate_lambda(func_str, item)
                transformed_item[field_name] = result
            except Exception as e:
                self.logger.warning(f"Custom function '{field_name}' failed: {str(e)}")
                transformed_item[field_name] = None
        
        return transformed_item
    
    def add_custom_function(self, name: str, func_str: str):
        """Add a custom function."""
        self.custom_functions[name] = func_str
    
    def remove_custom_function(self, name: str):
        """Remove a custom function."""
        if name in self.custom_functions:
            del self.custom_functions[name]
    
    def get_transformation_stats(self, original_items: List[Dict[str, Any]], 
                                transformed_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get transformation statistics."""
        original_fields = set()
        transformed_fields = set()
        
        for item in original_items:
            original_fields.update(item.keys())
        
        for item in transformed_items:
            transformed_fields.update(item.keys())
        
        return {
            "original_count": len(original_items),
            "transformed_count": len(transformed_items),
            "original_field_count": len(original_fields),
            "transformed_field_count": len(transformed_fields),
            "added_fields": list(transformed_fields - original_fields),
            "removed_fields": list(original_fields - transformed_fields),
            "common_fields": list(original_fields & transformed_fields)
        }


if __name__ == "__main__":
    # Test data transformer
    test_items = [
        {"quote_text": "Hello World", "quote_author": "John Doe", "quote_tags": ["test"]},
        {"quote_text": "Be yourself", "quote_author": "Jane Smith", "quote_tags": ["inspiration", "life"]},
    ]
    
    config = TransformerConfig(
        field_mapping={
            "quote_text": "text",
            "quote_author": "author",
            "quote_tags": "tags"
        },
        type_conversions={
            "text": "string",
            "author": "string",
            "tags": "array"
        },
        custom_functions={
            "text_length": "lambda item: len(item.get('text', ''))",
            "author_initials": "lambda item: ''.join([name[0].upper() for name in item.get('author', '').split()])",
            "tag_count": "lambda item: len(item.get('tags', []))"
        }
    )
    
    transformer = DataTransformer(config)
    transformed = transformer.transform_data(test_items)
    
    print("Transformed items:")
    for item in transformed:
        print(item)
    
    stats = transformer.get_transformation_stats(test_items, transformed)
    print("\nTransformation stats:", stats)