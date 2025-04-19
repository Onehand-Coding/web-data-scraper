# File: web-data-scraper/scraper/utils/config_loader.py

import yaml
from typing import Dict, Any
from pathlib import Path
import logging
from jsonschema import validate, ValidationError
# import json # Not needed directly

# --- Added definition for api_config schema ---
API_CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "base_url": {"type": "string", "format": "uri", "description": "Base URL for the API endpoints"},
        "endpoints": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "description": "List of endpoint paths to query (relative to base_url)"
        },
        "method": {
            "type": "string",
            "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"], # Add other methods if needed
            "default": "GET",
            "description": "HTTP method for the request"
        },
        "params": {"type": "object", "description": "URL parameters (for GET requests), key-value pairs"},
        "headers": {"type": "object", "description": "HTTP headers, key-value pairs"},
        "data": {"type": ["object", "string"], "description": "Request body (for POST/PUT/PATCH), usually JSON object or string"},
        "data_path": {
            "type": "string",
            "description": "Dot-notation path to the list of items within the JSON response (e.g., 'results.items')"
        },
        "field_mappings": {
            "type": "object",
            "additionalProperties": {"type": "string"},
            "description": "Mapping from desired output field name to source field name in API response item"
        },
        "pagination": { # API Pagination (can be different from web) - Placeholder for now
             "type": "object",
             "properties": {
                 "type": {"type": "string", "enum": ["page_param", "next_url", "offset_limit"]},
                 # Define specific pagination strategy params later
             },
             "description": "(Future) Define API pagination strategy"
        }
    },
    "required": ["base_url", "endpoints"], # Base requirements for API config
    "additionalProperties": False
}

# --- Definition for web selectors config ---
WEB_SELECTORS_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "enum": ["css", "xpath"], "default": "css"},
        "container": {"type": "string"}, # Keep optional
        "item": {"type": "string"},
        "fields": {
            "type": "object",
            "minProperties": 1,
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
    "required": ["item", "fields"], # Item and fields are required for web scraping
    "additionalProperties": False
}

# --- Definition for web pagination config ---
WEB_PAGINATION_SCHEMA = {
    "type": "object",
    "properties": {
        "next_page_selector": {"type": "string"},
        "max_pages": {"type": "integer", "minimum": 1}
    },
    "required": ["next_page_selector"],
    "additionalProperties": False
}

class ConfigLoader:
    """Handles loading and validation of scraping configurations."""

    # --- Updated CONFIG_SCHEMA ---
    CONFIG_SCHEMA = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Name of the scraping job"},
            "description": {"type": "string", "description": "Optional description"},
            "job_type": {
                "type": "string",
                "enum": ["web", "api"],
                "default": "web",
                "description": "Type of job: 'web' (HTML/Dynamic) or 'api'"
            },

            # --- Web Scraping Specific ---
            "urls": { # Required for web scraping
                "type": "array",
                "items": {"type": "string", "format": "uri"},
                "description": "List of starting URLs (for job_type: web)"
            },
            "dynamic": {"type": "boolean", "default": False, "description": "Use Selenium for JS rendering (for job_type: web)"},
            "selectors": WEB_SELECTORS_SCHEMA, # Use defined schema (for job_type: web)
            "pagination": WEB_PAGINATION_SCHEMA, # Use defined schema (for job_type: web)
             "wait_for_selector": {"type": "string", "description": "Wait for this CSS selector (for dynamic web)"},
             "headless": {"type": "boolean", "default": True, "description": "Run headless browser (for dynamic web)"},
             "disable_images": {"type": "boolean", "default": True, "description": "Disable images (for dynamic web)"},
             "page_load_timeout": {"type": "integer", "minimum": 5, "default": 30, "description": "Page load timeout in seconds (for dynamic web)"},
             "wait_time": {"type": "number", "minimum": 0, "default": 5, "description": "General wait time after load (for dynamic web)"},


            # --- API Specific ---
            "api_config": API_CONFIG_SCHEMA, # Use defined schema (for job_type: api)

            # --- Common / General ---
            "processing_rules": { # Shared processing rules
                "type": "object",
                # (Keep existing detailed definition from previous steps)
                 "properties": {
                     "field_types": {"type": "object", "additionalProperties": {"type": "object", "properties": {"type": {"type": "string", "enum": ["int", "float", "string", "boolean", "datetime", "date"]}, "format": {"type": "string"}}, "required": ["type"], "additionalProperties": False}},
                     "text_cleaning": {"type": "object", "additionalProperties": {"type": "object", "properties": {"trim": {"type": "boolean", "default": True}, "lowercase": {"type": "boolean", "default": False}, "uppercase": {"type": "boolean", "default": False}, "remove_newlines": {"type": "boolean", "default": True}, "remove_extra_spaces": {"type": "boolean", "default": True}, "remove_special_chars": {"type": "boolean", "default": False}, "regex_replace": {"type": "object", "additionalProperties": {"type": "string"}}}, "additionalProperties": False}},
                     "transformations": {"type": "object", "additionalProperties": {"type": "string"}},
                     "validations": {"type": "object", "additionalProperties": {"type": "object", "properties": {"required": {"type": "boolean", "default": False}, "min_length": {"type": "integer", "minimum": 0}, "max_length": {"type": "integer", "minimum": 0}, "pattern": {"type": "string", "format": "regex"}}, "additionalProperties": False}},
                     "drop_fields": {"type": "array", "items": {"type": "string"}}
                 },
                 "additionalProperties": False
            },
            "output_dir": {"type": "string", "default": "outputs"},
            "request_delay": {"type": "number", "minimum": 0, "default": 1},
            "max_retries": {"type": "integer", "minimum": 0, "default": 3},
            "user_agent": {"type": "string"}, # Can be used for API requests too
            "respect_robots": {"type": "boolean", "default": True} # Less relevant for APIs usually
        },
        # --- Conditional Requirements using 'if/then' ---
        # Note: 'if/then' requires jsonschema draft 7 or later.
        # Ensure your jsonschema library version supports this.
        "allOf": [
            {
                "if": {
                    "properties": {"job_type": {"const": "web"}}
                },
                "then": {
                    "required": ["urls", "selectors"]
                }
            },
            {
                 "if": {
                     "properties": {"job_type": {"const": "api"}}
                 },
                 "then": {
                     "required": ["api_config"]
                 }
             }
        ],
        # Fallback required if no job_type specified (defaults to web)
        "required": ["name"] # Only name is universally required
    }
    # --- End of CONFIG_SCHEMA ---

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load_config(self, config_path: str) -> Dict[str, Any]:
        # ... (Keep existing load_config method, validation will now use the updated schema) ...
        config = {}
        try:
            config = self._load_yaml(config_path)
            # Apply default job_type if missing before validation
            config.setdefault('job_type', 'web')
            self.validate_config(config)
            self.logger.info(f"Configuration loaded and validated: {config_path}")
            return config
        except FileNotFoundError: self.logger.error(f"Config file not found: {config_path}"); raise
        except yaml.YAMLError as e: self.logger.error(f"Error parsing YAML {config_path}: {e}"); raise
        except ValidationError as e:
            error_path = " -> ".join(map(str, e.path)) or "root"; msg = f"Config validation error in {config_path} at '{error_path}': {e.message}"
            self.logger.error(msg); self.logger.debug(f"Schema context: {e.schema}"); raise


    def _load_yaml(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    def validate_config(self, config: Dict[str, Any]) -> bool:
        validate(instance=config, schema=self.CONFIG_SCHEMA)
        return True

    def generate_sample_config(self, output_path: str) -> None:
        # Now generates either web or api sample based on filename? Or just web?
        # Let's generate a web sample by default. User can adapt it for API.
        # (Or create two generation commands later)
        # Sample config below is for WEB scraping (includes pagination)
        sample_config = {
            "name": "Sample Web Scraper Job",
            "description": "Example config for scraping a multi-page website",
            "job_type": "web", # Specify job type
            "urls": ["https://quotes.toscrape.com/"],
            "dynamic": False,
            "selectors": {
                "type": "css",
                "item": "div.quote",
                "fields": {
                    "quote_text": "span.text", "author": "small.author", "tags_raw": "div.tags"
                }
            },
            "pagination": {
                "next_page_selector": "li.next a", "max_pages": 3
            },
            "processing_rules": {
                 "text_cleaning": {"quote_text": {"trim": True}}, "transformations": {"tags": "| ', '.join(tag.strip() for tag in value.replace('Tags:', '').split('\\n') if tag.strip()) if isinstance(value, str) else ''"}, "drop_fields": ["tags_raw"]
            },
            "output_dir": "outputs/sample_web",
            "request_delay": 1, "max_retries": 3, "user_agent": "SampleScraper/1.0", "respect_robots": True
        }
        # --- API Sample (for reference or separate generation command) ---
        api_sample = {
             "name": "Sample API Job - JSONPlaceholder Users",
             "description": "Fetch user data from JSONPlaceholder API",
             "job_type": "api", # Specify job type
             "api_config": {
                 "base_url": "https://jsonplaceholder.typicode.com",
                 "endpoints": ["/users"], # Single endpoint in this case
                 "method": "GET",
                 # "params": {"_limit": 5}, # Example API parameter
                 # "headers": {"Accept": "application/json"}, # Example header
                 "data_path": "", # Data is the root list in this API
                 "field_mappings": { # Rename fields for output
                      "user_id": "id",
                      "full_name": "name",
                      "user_name": "username",
                      "email_address": "email",
                      "city": "address.city" # Example of accessing nested field
                 }
             },
             "processing_rules": { # Can still process API results
                 "text_cleaning": {"full_name": {"trim": True}}
             },
             "output_dir": "outputs/sample_api",
             "request_delay": 0.5 # Often less delay needed for APIs
        }
        # --- End API Sample ---

        # Save the WEB sample config by default
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                 yaml.dump(sample_config, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            self.logger.info(f"Sample WEB configuration generated at: {output_path}")
            # Optionally generate API sample too?
            api_output_path = Path(output_path).parent / f"{Path(output_path).stem}_api_example.yaml"
            with open(api_output_path, 'w', encoding='utf-8') as f:
                  yaml.dump(api_sample, f, sort_keys=False, default_flow_style=False, allow_unicode=True)
            self.logger.info(f"Sample API configuration generated at: {api_output_path}")

        except Exception as e:
             self.logger.error(f"Failed to generate sample config: {e}")
             raise
