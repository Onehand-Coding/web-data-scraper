# File: web-data-scraper/scraper/api_scraper.py (Corrected)

import time # <-- Added import
import requests
from typing import Dict, List, Optional, Any
from .base_scraper import BaseScraper
import json # <-- Added import
import logging

# --- Helper function for nested access ---
def get_nested_value(data_dict: Dict, key_path: str, default: Any = None) -> Any:
    """Access nested dictionary value using dot notation."""
    keys = key_path.split('.')
    current_value = data_dict
    try:
        for key in keys:
            if isinstance(current_value, dict):
                current_value = current_value.get(key)
            elif isinstance(current_value, list):
                 try:
                     key_index = int(key)
                     if 0 <= key_index < len(current_value):
                         current_value = current_value[key_index]
                     else: return default # Index out of bounds
                 except (ValueError, IndexError):
                     return default # Key is not a valid index or index out of bounds
            else:
                return default # Cannot traverse further
            if current_value is None:
                 return default # Path doesn't exist fully
        return current_value
    except Exception:
        return default
# --- End Helper ---


class APIScraper(BaseScraper):
    """Scraper implementation for API endpoints."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.api_config = config.get('api_config', {})
        if not self.api_config:
            self.logger.error("API Scraper initialized without 'api_config' in configuration!")

    def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Any]:
        """Fetch data from API endpoint."""
        if not self.api_config.get('base_url'):
             self.logger.error("API base_url is missing in configuration.")
             return None

        # --- Use BaseScraper's fetch_page which handles proxies/retries ---
        # Construct full URL
        base_url = self.api_config['base_url'].rstrip('/')
        full_url = f"{base_url}/{endpoint.lstrip('/')}"

        # Prepare parameters (requests handles adding these to the URL for GET)
        request_params = params or self.api_config.get('params')

        # Prepare headers
        headers = self.api_config.get('headers', {})
        headers.setdefault('User-Agent', self.session.headers['User-Agent']) # Use session UA if not specified

        # Prepare data (for POST/PUT etc.) - fetch_page uses session.get,
        # so this part needs adjustment if POST/PUT is required.
        # For now, assuming GET which is handled by fetch_page.
        request_data = self.api_config.get('data')
        if request_data and self.api_config.get('method', 'GET').upper() != 'GET':
             self.logger.warning("API scraper currently uses BaseScraper.fetch_page (GET). 'data' field ignored.")
             # TODO: Implement POST/PUT etc. directly if needed, bypassing fetch_page or enhancing it.

        # Use fetch_page from BaseScraper - it handles retries and proxies via the session
        # Note: fetch_page returns the *text* content. We need to parse JSON.
        response_text = super().fetch_page(full_url) # Inherits proxy logic

        if response_text:
            try:
                # Attempt to parse JSON
                return json.loads(response_text) # <-- Use imported json
            except json.JSONDecodeError as e: # <-- Use imported json
                self.logger.error(f"Failed to decode JSON response from {full_url}: {e}")
                self.logger.debug(f"Response text was: {response_text[:500]}") # Log response start
                # Consider if we should count this as a page failure in stats?
                # BaseScraper already counted the successful fetch.
                return None # Return None if JSON parsing fails
        else:
            # Fetch failed (logged in base_scraper)
            return None


    def extract_data(self, response_data: Any, url: str = "N/A") -> List[Dict]: # Added url param for consistency
        """Extract and transform API response data."""
        items = []
        processed_items = []

        if not response_data:
             self.logger.warning("Received empty response data for extraction.")
             return []

        try:
            data_to_process = response_data
            data_path = self.api_config.get('data_path')

            if data_path:
                 self.logger.debug(f"Accessing data path: {data_path}")
                 data_to_process = get_nested_value(response_data, data_path)
                 if data_to_process is None:
                      self.logger.warning(f"Data path '{data_path}' resulted in None.")
                      data_to_process = []

            if isinstance(data_to_process, list):
                items = data_to_process
                self.logger.debug(f"Found {len(items)} items at data path '{data_path or 'root'}'.")
            elif isinstance(data_to_process, dict):
                 items = [data_to_process]
                 self.logger.debug(f"Data at path '{data_path}' is a single object, processing as one item.")
            else:
                self.logger.warning(f"Data found at path '{data_path or 'root'}' is not a list or dictionary (Type: {type(data_to_process)}).")
                return []

            field_mappings = self.api_config.get('field_mappings')
            if not field_mappings:
                 # If no mappings, assume the items are already dictionaries in the desired format
                 processed_items = [item for item in items if isinstance(item, dict)]
                 if len(processed_items) != len(items):
                      self.logger.warning("Some items in the API response were not dictionaries and were skipped (no field mappings defined).")
            else:
                 self.logger.debug(f"Applying field mappings: {field_mappings}")
                 for i, item in enumerate(items):
                    if not isinstance(item, dict):
                         self.logger.warning(f"Skipping item {i+1} as it is not a dictionary (type: {type(item)}).")
                         continue
                    mapped_item = {}
                    for target_field, source_path in field_mappings.items():
                        mapped_item[target_field] = get_nested_value(item, source_path)
                    processed_items.append(mapped_item)

            return processed_items

        except Exception as e:
            self.logger.error(f"API data extraction failed: {e}", exc_info=True)
            return []


    def run(self) -> Dict:
        """Execute API scraping job."""
        self.stats['start_time'] = time.time() # <-- Use imported time
        all_extracted_data = []
        endpoints = self.api_config.get('endpoints', [])

        if not endpoints:
            self.logger.warning("No API endpoints defined in 'api_config'.")
            self.stats['end_time'] = time.time() # <-- Use imported time
            return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        for endpoint in endpoints:
             # Construct full URL for context in extract_data (though not strictly needed by it now)
             base_url = self.api_config['base_url'].rstrip('/')
             full_url = f"{base_url}/{endpoint.lstrip('/')}"

             # Fetch data using the modified fetch_data method which calls base_scraper.fetch_page
             response_data = self.fetch_data(endpoint)

             if response_data:
                 # Pass the full URL for context if extract_data needs it
                 page_data = self.extract_data(response_data, full_url)
                 all_extracted_data.extend(page_data)

        processed_data = self._process_extracted_data(all_extracted_data)

        self.stats['end_time'] = time.time() # <-- Use imported time
        return {
            'data': processed_data,
            'stats': self.get_stats(),
            'config': self.config
        }
