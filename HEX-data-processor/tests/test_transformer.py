"""
Tests for data transformer module.
"""

import pytest
from src.transformer import DataTransformer, TransformerConfig, SafeEvaluator, TransformationError


class TestDataTransformer:
    """Test data transformer functionality."""
    
    @pytest.fixture
    def sample_items(self):
        """Create sample data items for testing."""
        return [
            {"quote_text": "Hello World", "quote_author": "John Doe", "quote_tags": ["test"]},
            {"quote_text": "Be yourself", "quote_author": "Jane Smith", "quote_tags": ["inspiration", "life"]},
            {"quote_text": "Never give up", "quote_author": "Bob", "quote_tags": []},
        ]
    
    @pytest.fixture
    def transformer_config(self):
        """Create transformer configuration."""
        return TransformerConfig(
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
    
    def test_transform_data_basic(self, sample_items, transformer_config):
        """Test basic data transformation."""
        transformer = DataTransformer(transformer_config)
        transformed = transformer.transform_data(sample_items)
        
        assert len(transformed) == len(sample_items)
        
        # Check field mapping
        for item in transformed:
            assert "text" in item
            assert "author" in item
            assert "tags" in item
            assert "quote_text" not in item
            assert "quote_author" not in item
            assert "quote_tags" not in item
        
        # Check custom functions
        first_item = transformed[0]
        assert first_item["text_length"] == len("Hello World")
        assert first_item["author_initials"] == "JD"
        assert first_item["tag_count"] == 1
    
    def test_field_mapping(self):
        """Test field mapping functionality."""
        transformer = DataTransformer()
        
        original = {
            "old_field1": "value1",
            "old_field2": "value2",
            "unchanged": "value3"
        }
        
        mapping = {
            "old_field1": "new_field1",
            "old_field2": "new_field2"
        }
        
        mapped = transformer._apply_field_mapping(original, mapping)
        
        assert mapped["new_field1"] == "value1"
        assert mapped["new_field2"] == "value2"
        assert mapped["unchanged"] == "value3"
        assert "old_field1" not in mapped
        assert "old_field2" not in mapped
    
    def test_type_conversions(self):
        """Test type conversion functionality."""
        transformer = DataTransformer()
        
        test_item = {
            "text": "Hello",
            "number": "42",
            "boolean": "true",
            "array": "[1, 2, 3]",
            "tags": "tag1,tag2,tag3"
        }
        
        conversions = {
            "text": "string",
            "number": "int",
            "boolean": "bool",
            "array": "array",
            "tags": "array"
        }
        
        converted = transformer._apply_type_conversions(test_item, conversions)
        
        assert converted["text"] == "Hello"
        assert converted["number"] == 42
        assert converted["boolean"] is True
        assert isinstance(converted["array"], list)
        assert isinstance(converted["tags"], list)
    
    def test_convert_type_edge_cases(self):
        """Test type conversion edge cases."""
        transformer = DataTransformer()
        
        # Test None values
        assert transformer._convert_type(None, "string") is None
        assert transformer._convert_type(None, "int") is None
        
        # Test invalid int conversion
        assert transformer._convert_type("not_a_number", "int") == "not_a_number"
        
        # Test boolean conversion
        assert transformer._convert_type("1", "bool") is True
        assert transformer._convert_type("0", "bool") is False
        assert transformer._convert_type("yes", "bool") is True
        assert transformer._convert_type("no", "bool") is False
        assert transformer._convert_type(1, "bool") is True
        assert transformer._convert_type(0, "bool") is False
        
        # Test array conversion
        result = transformer._convert_type("item1,item2,item3", "array")
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0] == "item1"
    
    def test_custom_functions(self):
        """Test custom function execution."""
        transformer = DataTransformer()
        
        item = {"text": "Hello World", "author": "John Doe"}
        
        functions = {
            "upper_text": "lambda item: item.get('text', '').upper()",
            "word_count": "lambda item: len(item.get('text', '').split())"
        }
        
        result = transformer._apply_custom_functions(item, functions)
        
        assert result["upper_text"] == "HELLO WORLD"
        assert result["word_count"] == 2
        assert "text" not in result
        assert "author" not in result
    
    def test_safe_evaluator(self):
        """Test safe lambda evaluation."""
        item = {"name": "John", "age": 30}
        
        # Valid lambda
        result = SafeEvaluator.evaluate_lambda("lambda item: item.get('name', '')", item)
        assert result == "John"
        
        # Lambda with calculation
        result = SafeEvaluator.evaluate_lambda("lambda item: item.get('age', 0) * 2", item)
        assert result == 60
        
        # Invalid lambda should raise error
        with pytest.raises(TransformationError):
            SafeEvaluator.evaluate_lambda("invalid lambda", item)
        
        # Lambda with dangerous code should be blocked
        with pytest.raises(TransformationError):
            SafeEvaluator.evaluate_lambda("lambda item: __import__('os').system('ls')", item)
    
    def test_add_remove_custom_function(self):
        """Test adding and removing custom functions."""
        transformer = DataTransformer()
        
        # Add custom function
        transformer.add_custom_function("test_func", "lambda item: len(item.get('text', ''))")
        assert "test_func" in transformer.custom_functions
        
        # Remove custom function
        transformer.remove_custom_function("test_func")
        assert "test_func" not in transformer.custom_functions
    
    def test_get_transformation_stats(self, sample_items):
        """Test transformation statistics."""
        transformer = DataTransformer()
        transformed = transformer.transform_data(sample_items)
        
        stats = transformer.get_transformation_stats(sample_items, transformed)
        
        assert "original_count" in stats
        assert "transformed_count" in stats
        assert "original_field_count" in stats
        assert "transformed_field_count" in stats
        assert "added_fields" in stats
        assert "removed_fields" in stats
        assert "common_fields" in stats
        
        assert stats["original_count"] == len(sample_items)
        assert stats["transformed_count"] == len(transformed)
    
    def test_transform_empty_list(self):
        """Test transforming empty list."""
        transformer = DataTransformer()
        transformed = transformer.transform_data([])
        assert transformed == []
    
    def test_transform_with_config_override(self, sample_items):
        """Test transformation with config override."""
        transformer = DataTransformer()
        
        override_config = TransformerConfig(
            field_mapping={"quote_text": "new_text"}
        )
        
        transformed = transformer.transform_data(sample_items, override_config)
        
        for item in transformed:
            assert "new_text" in item
            assert "quote_text" not in item
    
    def test_custom_function_error_handling(self):
        """Test custom function error handling."""
        transformer = DataTransformer()
        
        item = {"text": "test"}
        
        functions = {
            "broken_func": "lambda item: item.get('nonexistent', {}).get('key')"  # Will cause error
        }
        
        result = transformer._apply_custom_functions(item, functions)
        
        # Should handle error gracefully and return None for failed function
        assert result["broken_func"] is None
    
    def test_datetime_conversion(self):
        """Test datetime type conversion."""
        transformer = DataTransformer()
        
        # Test various date formats
        dates = [
            "2024-01-15 10:30:00",
            "2024-01-15",
            "01/15/2024",
            "15/01/2024"
        ]
        
        for date_str in dates:
            result = transformer._convert_type(date_str, "datetime")
            # Should return datetime object or original string if parsing fails
            assert hasattr(result, 'year') or isinstance(result, str)
    
    def test_dict_conversion(self):
        """Test dictionary type conversion."""
        transformer = DataTransformer()
        
        # Test JSON string to dict
        json_str = '{"key": "value", "number": 42}'
        result = transformer._convert_type(json_str, "dict")
        assert isinstance(result, dict)
        assert result["key"] == "value"
        
        # Test non-dict to dict
        result = transformer._convert_type("simple_string", "dict")
        assert isinstance(result, dict)
        assert result["value"] == "simple_string"


if __name__ == "__main__":
    pytest.main([__file__])