"""
Integration test to verify the complete HEX Data Processor system works.
"""

import asyncio
import sys
import os
import tempfile
import json
from pathlib import Path

# Add src to path for testing
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import load_config
from src.main import DataProcessor


async def test_complete_workflow():
    """Test complete data processing workflow."""
    print("🧪 Testing HEX Data Processor Integration...")
    
    # Create test configuration
    test_config = {
        "project": {
            "name": "Integration Test",
            "version": "1.0.0"
        },
        "scraper": {
            "user_agent": "TestBot/1.0",
            "timeout": 30,
            "max_retries": 3,
            "rate_limit": 1.0,
            "max_concurrent": 5
        },
        "targets": {
            "quotes_toscrape": {
                "name": "Quotes to Scrape",
                "base_url": "http://quotes.toscrape.com",
                "start_urls": ["http://quotes.toscrape.com"],
                "selectors": {
                    "quote": "div.quote",
                    "text": "span.text::text",
                    "author": "small.author::text",
                    "tags": "div.tags a.tag::text"
                },
                "pagination": {
                    "enabled": True,
                    "next_selector": "li.next a::attr(href)",
                    "max_pages": 2
                },
                "js_render": False,
                "rate_limit": 1.0
            }
        },
        "cleaner": {
            "remove_duplicates": True,
            "duplicate_keys": ["text", "author"],
            "handle_missing": {
                "strategy": "default",
                "default_values": {
                    "text": "N/A",
                    "author": "Unknown",
                    "tags": []
                }
            }
        },
        "transformer": {
            "field_mapping": {
                "text": "quote_text",
                "author": "quote_author"
            },
            "type_conversions": {
                "quote_text": "string",
                "quote_author": "string"
            },
            "custom_functions": {
                "quote_length": "lambda item: len(item.get('quote_text', ''))",
                "author_initials": "lambda item: ''.join([name[0].upper() for name in item.get('quote_author', '').split()])"
            }
        },
        "storage": {
            "type": "jsonl",
            "path": "./test_output",
            "filename_template": "quotes_{timestamp}.jsonl"
        },
        "notifications": {
            "enabled": False
        },
        "metrics": {
            "enabled": True,
            "port": 8001
        },
        "logging": {
            "level": "INFO",
            "format": "text",
            "console": True
        }
    }
    
    # Save test config
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(test_config, f)
        config_path = f.name
    
    try:
        # Load configuration
        print("📋 Loading configuration...")
        config = load_config(config_path)
        assert config.project.name == "Integration Test"
        
        # Create processor
        print("🔧 Initializing data processor...")
        processor = DataProcessor(config)
        
        # Run processing
        print("🚀 Running data processing...")
        result = await processor.run_target_once("quotes_toscrape", dry_run=True)
        
        # Verify results
        assert result['success'] is True
        assert 'stats' in result
        assert result['stats']['scraped_count'] > 0
        assert result['stats']['processed_count'] > 0
        
        print(f"✅ Successfully scraped {result['stats']['scraped_count']} quotes")
        print(f"✅ Successfully processed {result['stats']['processed_count']} quotes")
        
        # Cleanup
        await processor.cleanup()
        
        print("🎉 Integration test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup temp file
        if os.path.exists(config_path):
            os.unlink(config_path)


if __name__ == "__main__":
    # Run integration test
    success = asyncio.run(test_complete_workflow())
    
    if success:
        print("\n✨ HEX Data Processor is ready for production!")
        sys.exit(0)
    else:
        print("\n💥 Integration test failed. Check the logs above.")
        sys.exit(1)