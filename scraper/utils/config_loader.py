import yaml
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from jsonschema import validate, ValidationError

# --- API Config Schema ---
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
            "enum": ["GET", "POST", "PUT", "DELETE", "PATCH"],
            "default": "GET",
            "description": "HTTP method for the request"
        },
        "params": {"type": "object", "description": "URL parameters (for GET requests), key-value pairs", "additionalProperties": True}, # Allow any params
        "headers": {"type": "object", "description": "HTTP headers, key-value pairs", "additionalProperties": True}, # Allow any headers
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
        "pagination": {
             "type": "object",
             "properties": {
                 "type": {"type": "string", "enum": ["page_param", "next_url", "offset_limit"]},
                 # Add specific pagination params here if needed later
             },
             "description": "(Future) Define API pagination strategy"
        }
    },
    "required": ["base_url", "endpoints"],
    "additionalProperties": False # Disallow unknown keys within api_config
}

# --- Web Selectors Schema ---
WEB_SELECTORS_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {
            "type": "string",
            "enum": ["css", "xpath"],
            "default": "css",
            "description": "The type of selectors to use (css or xpath)."
        },
        "container": {"type": "string", "description": "(Optional) Base selector for the container holding items."},
        "item": {"type": "string", "description": "Selector for each individual item element within the container or page."},
        "fields": {
            "type": "object",
            "minProperties": 1,
            "additionalProperties": {
                "anyOf": [
                    {"type": "string", "description": "Selector for the field's text content."},
                    {
                        "type": "object",
                        "properties": {
                            "selector": {"type": "string", "description": "Selector for the field's element."},
                            "attr": {"type": "string", "description": "(Optional) Attribute to extract (e.g., 'href', 'src'). Extracts text if omitted."}
                        },
                        "required": ["selector"],
                        "additionalProperties": False
                    }
                ]
            },
            "description": "Dictionary mapping output field names to their selectors."
        }
    },
    "required": ["item", "fields"],
    "additionalProperties": False
}

# --- Web Pagination Schema ---
WEB_PAGINATION_SCHEMA = {
    "type": "object",
    "properties": {
        "next_page_selector": {"type": "string"},
        "max_pages": {"type": "integer", "minimum": 1}
    },
    # "required": ["next_page_selector"], # Making it optional
    "additionalProperties": False
}

# --- Proxy Item Schema ---
PROXY_ITEM_SCHEMA = {
    "type": "object",
    "properties": {
        "http": {"type": "string", "format": "uri", "pattern": r"^http(s)?://"},
        "https": {"type": "string", "format": "uri", "pattern": r"^http(s)?://"}
    },
    "anyOf": [
        {"required": ["http"]},
        {"required": ["https"]}
    ],
    "additionalProperties": False,
    "description": "Proxy server details for HTTP and/or HTTPS protocols."
}

# --- Login Config Schema ---
LOGIN_CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "login_url": {"type": "string", "format": "uri", "description": "URL of the login page."},
        "username_selector": {"type": "string", "description": "CSS selector for the username input field."},
        "password_selector": {"type": "string", "description": "CSS selector for the password input field."},
        "submit_selector": {"type": "string", "description": "CSS selector for the login submit button."},
        "username": {"type": "string", "description": "The username credential. WARNING: Storing credentials in config is insecure."},
        "password": {"type": "string", "description": "The password credential. WARNING: Storing credentials in config is insecure."},
        "success_selector": {"type": "string", "description": "(Optional) CSS selector for an element that appears only after successful login."},
        "success_url_contains": {"type": "string", "description": "(Optional) A substring that the URL must contain after successful login."},
        "wait_after_login": {"type": "number", "minimum": 0, "default": 3, "description": "Seconds to wait after submitting login before checking success."}
    },
    "required": [
        "login_url",
        "username_selector",
        "password_selector",
        "submit_selector",
        "username",
        "password"
    ],
    # Login success requires at least one check method
    "anyOf": [
        {"required": ["success_selector"]},
        {"required": ["success_url_contains"]}
    ],
    "additionalProperties": False,
    "description": "Configuration for handling website logins (primarily for DynamicScraper)."
}

# --- Processing Rules Sub-Schemas ---
FIELD_TYPES_SCHEMA = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["int", "float", "string", "boolean", "datetime", "date"]},
            "format": {"type": "string", "description": "Format string (e.g., %Y-%m-%d) for date/datetime conversion."}
        },
        "required": ["type"],
        "additionalProperties": False
    }
}

TEXT_CLEANING_SCHEMA = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "properties": {
            "trim": {"type": "boolean", "default": True},
            "lowercase": {"type": "boolean", "default": False},
            "uppercase": {"type": "boolean", "default": False},
            "remove_newlines": {"type": "boolean", "default": True},
            "remove_extra_spaces": {"type": "boolean", "default": True},
            "remove_special_chars": {"type": "boolean", "default": False},
            "regex_replace": {"type": "object", "additionalProperties": {"type": "string"}, "description": "Dictionary of regex patterns to replace."}
        },
        "additionalProperties": False
    }
}

TRANSFORMATIONS_SCHEMA = {
    "type": "object",
    "additionalProperties": {"type": "string", "description": "Python expression to generate/transform the field value."}
}

VALIDATIONS_SCHEMA = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "properties": {
            "required": {"type": "boolean", "default": False},
            "min_length": {"type": "integer", "minimum": 0},
            "max_length": {"type": "integer", "minimum": 0},
            "pattern": {"type": "string", "format": "regex", "description": "Python regex pattern."}
        },
        "additionalProperties": False
    }
}

DROP_FIELDS_SCHEMA = {
    "type": "array",
    "items": {"type": "string"}
}

PROCESSING_RULES_SCHEMA = {
    "type": "object",
    "properties": {
        "field_types": FIELD_TYPES_SCHEMA,
        "text_cleaning": TEXT_CLEANING_SCHEMA,
        "transformations": TRANSFORMATIONS_SCHEMA,
        "validations": VALIDATIONS_SCHEMA,
        "drop_fields": DROP_FIELDS_SCHEMA
    },
    "additionalProperties": False # No other keys allowed directly under processing_rules
}

# --- ConfigLoader Class ---

class ConfigLoader:
    """Handles loading and validation of scraping configurations."""

    # --- Main Config Schema ---
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

            # --- Web Specific ---
            "urls": { "type": "array", "items": {"type": "string", "format": "uri"}, "description": "List of starting URLs (for web jobs)"},
            "dynamic": {"type": "boolean", "default": False, "description": "Use Selenium if true (for web jobs)"},
            "selectors": WEB_SELECTORS_SCHEMA, # Schema defined above
            "pagination": WEB_PAGINATION_SCHEMA, # Schema defined above
            "wait_for_selector": {"type": "string", "description": "CSS/XPath selector to wait for before extracting (dynamic only)"},
            "headless": {"type": "boolean", "default": True, "description": "Run browser headless (dynamic only)"},
            "disable_images": {"type": "boolean", "default": True, "description": "Disable image loading (dynamic only)"},
            "page_load_timeout": {"type": "integer", "minimum": 5, "default": 30, "description": "Timeout for page loads (dynamic only)"},
            "wait_time": {"type": "number", "minimum": 0, "default": 5, "description": "General wait time after page load/action (dynamic only)"},
            "login_config": LOGIN_CONFIG_SCHEMA, # Schema defined above

            # --- API Specific ---
            "api_config": API_CONFIG_SCHEMA, # Schema defined above

            # --- Common / General ---
            "processing_rules": PROCESSING_RULES_SCHEMA, # Schema defined above
            "output_dir": {"type": "string", "default": "outputs", "description": "Base directory for output files"},
            "output_format": {
                "type": "string",
                "enum": ["csv", "json", "sqlite"],
                "default": "csv",
                "description": "Default format for saving the primary output file."
            },
            "request_delay": {"type": "number", "minimum": 0, "default": 1, "description": "Delay in seconds between requests"},
            "max_retries": {"type": "integer", "minimum": 0, "default": 3, "description": "Max retries on failed requests"},
            "user_agent": {"type": "string", "description": "Custom User-Agent string"},
            "respect_robots": {"type": "boolean", "default": True, "description": "Whether to obey robots.txt rules (web only)"},
            "proxies": { "type": "array", "items": PROXY_ITEM_SCHEMA, "default": [], "description": "List of proxies to use"}
        },
        # --- Conditional requirements ---
        "allOf": [
            {
                "if": {"properties": {"job_type": {"const": "web"}}},
                "then": {"required": ["urls", "selectors"]}
            },
            {
                "if": {"properties": {"job_type": {"const": "api"}}},
                "then": {"required": ["api_config"]}
            },
        ],
        "required": ["name"] # Only 'name' is universally required
    }
    # --- End Main Config Schema ---


    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load_config(self, config_path: str) -> Dict[str, Any]:
        config = {}
        try:
            config = self._load_yaml(config_path)
            config.setdefault('job_type', 'web'); config.setdefault('proxies', [])
            self.validate_config(config)
            self.logger.info(f"Configuration loaded and validated: {config_path}")
            return config
        except FileNotFoundError: self.logger.error(f"Config file not found: {config_path}"); raise
        except yaml.YAMLError as e: self.logger.error(f"Error parsing YAML {config_path}: {e}"); raise
        except ValidationError as e: error_path = " -> ".join(map(str, e.path)) or "root"; msg = f"Config validation error in {config_path} at '{error_path}': {e.message}"; self.logger.error(msg); self.logger.debug(f"Schema context: {e.schema}"); raise
        except Exception as e: self.logger.error(f"Unexpected error loading config {config_path}: {e}", exc_info=True); raise

    def _load_yaml(self, file_path: str) -> Dict[str, Any]:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}

    def validate_config(self, config: Dict[str, Any]) -> bool:
        validate(instance=config, schema=self.CONFIG_SCHEMA)
        if 'login_config' in config and not config.get('dynamic', False):
             self.logger.warning("login_config is present but 'dynamic' is not true. Login will be ignored.")
        # Add specific checks for selector types if needed (e.g., XPath vs CSS)
        return True

    def generate_sample_config(self, output_filename_base: Optional[str] = None) -> List[str]:
        """
        Generates sample configuration files (one for web, one for API)
        and saves them to a 'configs/generated_samples/' subdirectory.
        Creates the directory if it doesn't exist.

        Args:
            output_filename_base: Optional base name for the web config file.
                                  API config will have '_api_example' appended.
                                  If None, defaults are 'sample_web_config.yaml'
                                  and 'sample_api_config.yaml'.

        Returns:
            A list of paths to the generated sample config files.
        """
        # Define the target directory
        sample_config_dir = Path("configs") / "generated_samples"
        try:
            sample_config_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Ensured sample config directory exists: {sample_config_dir.resolve()}")
        except OSError as e:
            self.logger.error(f"Error creating sample config directory {sample_config_dir}: {e}")
            # Fallback to current directory if creation fails
            sample_config_dir = Path(".")

        # Define default filenames if base is not provided
        default_web_filename = "sample_web_config.yaml"
        default_api_filename = "sample_api_config.yaml"

        if output_filename_base:
            # Ensure it ends with .yaml or .yml
            if not (output_filename_base.endswith(".yaml") or output_filename_base.endswith(".yml")):
                web_config_filename = f"{output_filename_base}_web.yaml"
                api_config_filename = f"{output_filename_base}_api.yaml"
            else: # User provided full filename with extension
                web_config_filename = output_filename_base
                base_stem = Path(output_filename_base).stem
                api_config_filename = f"{base_stem}_api_example.yaml"

        else:
            web_config_filename = default_web_filename
            api_config_filename = default_api_filename

        web_config_path = sample_config_dir / web_config_filename
        api_config_path = sample_config_dir / api_config_filename

        # --- Sample Web Config ---
        sample_config_web = {
            "name": "Sample Dynamic Web Job",
            # ... (rest of your sample_config_web definition, ensure it's complete and up-to-date) ...
             "description": "Example config for scraping a dynamic website, possibly requiring login.",
            "job_type": "web",
            "urls": [
                "http://quotes.toscrape.com/js/"
            ],
            "dynamic": True,
            "headless": True,
            "disable_images": True,
            "page_load_timeout": 30,
            # "webdriver_path": "/path/to/your/chromedriver", # Optional
            "wait_for_selector": "div.quote",
            "wait_time": 3,
            # "login_config": { ... }, # Keep commented out or provide a very simple dummy
            "selectors": {
                "type": "css", # Defaulting to CSS for simplicity in sample
                "item": "div.quote",
                "fields": {
                    "quote_text": "span.text",
                    "author_name": "small.author",
                    "tags": "div.tags a.tag"
                }
            },
            "pagination": {
                "next_page_selector": "li.next > a",
                "max_pages": 2
            },
            "output_format": "csv",
            "processing_rules": {
                "text_cleaning": {
                    "author_name": {"trim": True, "uppercase": True}
                }
            },
            "request_delay": 1,
            "max_retries": 3,
            "user_agent": "MySampleScraper/1.0"
        }

        # --- Sample API Config ---
        api_sample = {
            "name": "Sample API Job - JSONPlaceholder",
            # ... (rest of your api_sample definition, ensure it's complete and up-to-date) ...
            "job_type": "api",
            "api_config": {
                "base_url": "https://jsonplaceholder.typicode.com",
                "endpoints": ["/users/1"],
                "method": "GET",
                "field_mappings": {
                    "id_user": "id",
                    "name_user": "name",
                    "email_user": "email"
                }
            },
            "output_format": "json",
            "processing_rules": {
                "field_types": {"id_user": {"type": "int"}}
            },
            "request_delay": 0.5
        }

        generated_files = []
        try:
            with open(web_config_path, 'w', encoding='utf-8') as f_web:
                yaml.dump(sample_config_web, f_web, sort_keys=False, default_flow_style=False, allow_unicode=True)
            self.logger.info(f"Generated sample web config: {web_config_path.resolve()}")
            generated_files.append(str(web_config_path.resolve()))
        except Exception as e:
            self.logger.error(f"Error writing sample web config to {web_config_path}: {e}")

        try:
            with open(api_config_path, 'w', encoding='utf-8') as f_api:
                yaml.dump(api_sample, f_api, sort_keys=False, default_flow_style=False, allow_unicode=True)
            self.logger.info(f"Generated sample API config: {api_config_path.resolve()}")
            generated_files.append(str(api_config_path.resolve()))
        except Exception as e:
            self.logger.error(f"Error writing sample API config to {api_config_path}: {e}")

        return generated_files
