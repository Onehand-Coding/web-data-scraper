# File: web-data-scraper/scraper/utils/config_loader.py

"""
Configuration loading and validation utilities.
"""

import yaml
from typing import Dict, Any
from pathlib import Path
import logging
from jsonschema import validate, ValidationError
# Removed json import as it's not used directly here

class ConfigLoader:
    """Handles loading and validation of scraping configurations."""

    # --- Updated CONFIG_SCHEMA ---
    CONFIG_SCHEMA = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
            "urls": {
                "type": "array",
                "items": {"type": "string", "format": "uri"},
                "minItems": 1 # Ensure at least one URL
            },
            "dynamic": {"type": "boolean", "default": False},
            "selectors": {
                "type": "object",
                "properties": {
                    "type": {"type": "string", "enum": ["css", "xpath"], "default": "css"},
                    "container": {"type": "string"},
                    "item": {"type": "string"},
                    "fields": {
                        "type": "object",
                        "minProperties": 1, # Ensure at least one field
                        "additionalProperties": {
                            "anyOf": [
                                {"type": "string"},
                                {
                                    "type": "object",
                                    "properties": {
                                        "selector": {"type": "string"},
                                        "attr": {"type": "string"}
                                    },
                                    "required": ["selector"],
                                    "additionalProperties": False
                                }
                            ]
                        }
                    }
                },
                "required": ["item", "fields"],
                "additionalProperties": False
            },
            # --- Added processing_rules definition ---
            "processing_rules": {
                "type": "object",
                "properties": {
                    "field_types": { # Type conversion rules
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string", "enum": ["int", "float", "string", "boolean", "datetime", "date"]},
                                "format": {"type": "string"} # e.g., '%Y-%m-%d' for dates
                            },
                            "required": ["type"],
                            "additionalProperties": False
                        }
                    },
                    "text_cleaning": { # Text cleaning rules per field
                         "type": "object",
                         "additionalProperties": {
                            "type": "object",
                            "properties": {
                                "trim": {"type": "boolean", "default": True},
                                "lowercase": {"type": "boolean", "default": False},
                                "uppercase": {"type": "boolean", "default": False},
                                "remove_newlines": {"type": "boolean", "default": True},
                                "remove_extra_spaces": {"type": "boolean", "default": True},
                                "remove_special_chars": {"type": "boolean", "default": False}, # Use with caution
                                "regex_replace": {
                                    "type": "object",
                                    "additionalProperties": {"type": "string"} # pattern: replacement
                                }
                            },
                            "additionalProperties": False # Only allow defined cleaning rules
                         }
                    },
                    "transformations": { # Custom transformations (use eval carefully)
                        "type": "object",
                        "additionalProperties": {"type": "string"} # field_name: 'expression involving value or item'
                    },
                    "validations": { # Validation rules
                        "type": "object",
                        "additionalProperties": {
                             "type": "object",
                             "properties": {
                                 "required": {"type": "boolean", "default": False},
                                 "min_length": {"type": "integer", "minimum": 0},
                                 "max_length": {"type": "integer", "minimum": 0},
                                 "pattern": {"type": "string", "format": "regex"} # Regex pattern string
                             },
                             "additionalProperties": False
                        }
                    },
                    "drop_fields": { # Fields to remove after processing
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "additionalProperties": False # Only allow defined rule categories
            },
            # --- End of added processing_rules ---

            # --- Added pagination definition ---
            "pagination": {
                "type": "object",
                "properties": {
                    "next_page_selector": {
                        "type": "string",
                        "description": "CSS selector for the 'Next' page link/button."
                    },
                    "max_pages": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Maximum number of pages to scrape (including initial URLs)."
                    }
                    # Could add 'type': 'xpath' later if needed
                },
                "required": ["next_page_selector"], # Require the selector if pagination is used
                "additionalProperties": False
            },
            # --- End of added pagination ---

            "output_dir": {"type": "string", "default": "outputs"},
            "request_delay": {"type": "number", "minimum": 0, "default": 1},
            "max_retries": {"type": "integer", "minimum": 0, "default": 3}, # Allow 0 retries
            "user_agent": {"type": "string"},
            "respect_robots": {"type": "boolean", "default": True},
            # Dynamic specific settings
            "wait_for_selector": {"type": "string"},
            "headless": {"type": "boolean", "default": True},
            "disable_images": {"type": "boolean", "default": True},
            "page_load_timeout": {"type": "integer", "minimum": 5, "default": 30},
            "wait_time": {"type": "number", "minimum": 0, "default": 5}, # General wait after load

        },
        "required": ["urls", "selectors"] # Core requirements
    }

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration from YAML file."""
        config = {}
        try:
            config = self._load_yaml(config_path)
            # --- Defaults can be applied here before validation if needed ---
            # Or handle defaults more robustly using a library if schema gets complex
            self.validate_config(config)
            self.logger.info(f"Configuration loaded and validated: {config_path}")
            return config
        except FileNotFoundError:
             self.logger.error(f"Configuration file not found: {config_path}")
             raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML configuration file {config_path}: {e}")
            raise
        except ValidationError as e:
            # Provide more context for validation errors
            error_path = " -> ".join(map(str, e.path)) or "root"
            self.logger.error(f"Configuration validation error in {config_path} at '{error_path}': {e.message}")
            # Log the schema snippet causing the error if possible
            # self.logger.debug(f"Schema context: {e.schema}")
            raise

    def _load_yaml(self, file_path: str) -> Dict[str, Any]:
        """Load YAML file and return as dictionary."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {} # Return empty dict if file is empty

    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration against schema."""
        validate(instance=config, schema=self.CONFIG_SCHEMA)
        return True # Return true or raise ValidationError

    def generate_sample_config(self, output_path: str) -> None:
        """Generate a sample configuration file including pagination rules."""
        sample_config = {
            "name": "Example Paged Scraper",
            "description": "Sample config scraping multiple pages (e.g., quotes.toscrape.com)",
            "urls": ["https://quotes.toscrape.com/"], # Start with the first page
            "dynamic": False,
            "selectors": {
                "type": "css",
                # "container": "div.row div.col-md-8", # Container ignored in current HTMLScraper
                "item": "div.quote",
                "fields": {
                    "quote_text": "span.text",
                    "author": "small.author",
                    "tags_raw": "div.tags"
                }
            },
            # --- Example Pagination Rules ---
            "pagination": {
                 "next_page_selector": "li.next a", # Selector for the 'Next →' link's <a> tag
                 "max_pages": 5 # Limit to scraping 5 pages total
            },
            # --- End Example Pagination Rules ---
            "processing_rules": {
                 "text_cleaning": {
                     "quote_text": {"trim": True, "regex_replace": {'^[“”"]': '', '[“”"]$': ''}},
                     "author": {"trim": True}
                 },
                 "validations": {
                     "quote_text": {"required": True, "min_length": 5},
                     "author": {"required": True}
                 },
                 "transformations": {
                     "tags": "| ', '.join(tag.strip() for tag in value.replace('Tags:', '').split('\\n') if tag.strip()) if isinstance(value, str) else ''"
                 },
                 "drop_fields": ["tags_raw"]
            },
            "output_dir": "outputs/example_paged",
            "request_delay": 1,
            "max_retries": 2,
            "user_agent": "Mozilla/5.0 (compatible; MyPagedScraperBot/1.0)",
            "respect_robots": True
        }

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                 yaml.dump(sample_config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            self.logger.info(f"Sample configuration generated at: {output_path}")
        except Exception as e:
             self.logger.error(f"Failed to generate sample config: {e}")
             raise
