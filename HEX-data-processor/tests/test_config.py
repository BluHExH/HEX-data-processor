"""
Tests for configuration module.
"""

import pytest
import json
import tempfile
import os
from pathlib import Path

from src.config import Config, load_config, ProjectConfig, ScraperConfig, StorageConfig


class TestConfig:
    """Test configuration loading and validation."""
    
    def test_minimal_config(self):
        """Test loading minimal valid configuration."""
        minimal_config = {
            "project": {
                "name": "Test Project",
                "version": "1.0.0"
            },
            "scraper": {
                "user_agent": "Test Agent",
                "timeout": 30,
                "max_retries": 3,
                "rate_limit": 1.0
            },
            "targets": {
                "test": {
                    "name": "Test Target",
                    "base_url": "https://example.com",
                    "start_urls": ["https://example.com"],
                    "selectors": {"title": "h1"}
                }
            },
            "storage": {
                "type": "csv",
                "path": "./data"
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(minimal_config, f)
            temp_path = f.name
        
        try:
            config = Config.load_from_file(temp_path)
            assert config.project.name == "Test Project"
            assert config.project.version == "1.0.0"
            assert config.scraper.user_agent == "Test Agent"
            assert config.storage.type == "csv"
            assert "test" in config.targets
        finally:
            os.unlink(temp_path)
    
    def test_env_var_substitution(self):
        """Test environment variable substitution."""
        os.environ['TEST_TOKEN'] = 'secret123'
        
        config_with_env = {
            "project": {
                "name": "Test",
                "version": "1.0.0"
            },
            "scraper": {
                "user_agent": "Test Agent",
                "timeout": 30,
                "max_retries": 3,
                "rate_limit": 1.0
            },
            "targets": {
                "test": {
                    "name": "Test Target",
                    "base_url": "https://example.com",
                    "start_urls": ["https://example.com"],
                    "selectors": {"title": "h1"}
                }
            },
            "storage": {
                "type": "csv",
                "path": "./data"
            },
            "notifications": {
                "telegram": {
                    "bot_token": "${TEST_TOKEN}",
                    "chat_id": "123456"
                }
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_with_env, f)
            temp_path = f.name
        
        try:
            config = Config.load_from_file(temp_path)
            assert config.notifications.telegram.bot_token == "secret123"
        finally:
            os.unlink(temp_path)
            del os.environ['TEST_TOKEN']
    
    def test_invalid_config(self):
        """Test loading invalid configuration."""
        invalid_config = {
            "project": {
                "name": "Test",
                "version": "1.0.0"
            },
            # Missing required sections
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(invalid_config, f)
            temp_path = f.name
        
        try:
            with pytest.raises(Exception):  # Should raise validation error
                Config.load_from_file(temp_path)
        finally:
            os.unlink(temp_path)
    
    def test_get_target(self):
        """Test getting target configuration."""
        config = {
            "project": {"name": "Test", "version": "1.0.0"},
            "scraper": {"user_agent": "Test", "timeout": 30, "max_retries": 3, "rate_limit": 1.0},
            "targets": {
                "target1": {
                    "name": "Target 1",
                    "base_url": "https://example1.com",
                    "start_urls": ["https://example1.com"],
                    "selectors": {"title": "h1"}
                }
            },
            "storage": {"type": "csv", "path": "./data"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            temp_path = f.name
        
        try:
            config_obj = Config.load_from_file(temp_path)
            target = config_obj.get_target("target1")
            assert target.name == "Target 1"
            
            with pytest.raises(ValueError):
                config_obj.get_target("nonexistent")
        finally:
            os.unlink(temp_path)
    
    def test_config_validation(self):
        """Test configuration validation."""
        config = {
            "project": {"name": "Test", "version": "1.0.0"},
            "scraper": {"user_agent": "Test", "timeout": 30, "max_retries": 3, "rate_limit": 1.0},
            "targets": {
                "test": {
                    "name": "Test",
                    "base_url": "https://example.com",
                    "start_urls": ["https://example.com"],
                    "selectors": {"title": "h1"}
                }
            },
            "storage": {"type": "csv", "path": "./data"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config, f)
            temp_path = f.name
        
        try:
            config_obj = Config.load_from_file(temp_path)
            assert config_obj.validate() is True
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    pytest.main([__file__])