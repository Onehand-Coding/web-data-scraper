# File: web-data-scraper/scraper/api_scraper.py
import time
import requests
from typing import Dict, List, Optional, Any # Added Any
from .base_scraper import BaseScraper
# import json # Not needed here

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
                 # Handle cases like 'list.0.key' - requires parsing index
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
    except Exception: # Catch any unexpected errors during traversal
        return default
# --- End Helper ---


class APIScraper(BaseScraper):
    """Scraper implementation for API endpoints."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.api_config = config.get('api_config', {})
        if not self.api_config:
            self.logger.error("API Scraper initialized without 'api_config' in configuration!")
            # Consider raising an error here?

    def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Any]: # Return Any, could be list or dict
        """Fetch data from API endpoint."""
        # Ensure api_config is present
        if not self.api_config.get('base_url'):
             self.logger.error("API base_url is missing in configuration.")
             return None

        self.throttle_requests() # Use delay from base config
        url = self.api_config['base_url'].rstrip('/') + '/' + endpoint.lstrip('/')

        headers = self.api_config.get('headers', {})
        # Ensure User-Agent from base config is used if no specific one in API headers
        headers.setdefault('User-Agent', self.session.headers['User-Agent'])

        try:
            self.logger.info(f"Making API request: {self.api_config.get('method', 'GET')} {url}")
            response = self.session.request(
                method=self.api_config.get('method', 'GET'),
                url=url,
                params=params or self.api_config.get('params'), # Combine/prioritize params if needed
                headers=headers,
                json=self.api_config.get('data', None) # Assumes JSON body if 'data' provided
            )
            self.logger.debug(f"API Response Status: {response.status_code}")
            response.raise_for_status() # Check for HTTP errors
            self.stats['pages_scraped'] += 1 # Count API calls as 'pages'
            # Check content type before trying to parse JSON
            content_type = response.headers.get('content-type', '').lower()
            if 'application/json' in content_type:
                 return response.json()
            else:
                 self.logger.warning(f"API response Content-Type is not JSON ({content_type}). Returning raw text.")
                 return response.text # Return text for non-JSON responses

        except requests.exceptions.HTTPError as e:
             self.logger.error(f"API request failed for {url}: HTTP {e.response.status_code} - {e.response.text[:200]}") # Log beginning of error response
             self.stats['pages_failed'] += 1
             return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {url}: {e}")
            self.stats['pages_failed'] += 1
            return None
        except ValueError as e: # Catches JSONDecodeError
             self.logger.error(f"Failed to decode JSON response from {url}: {e}")
             self.logger.debug(f"Response text: {response.text[:500]}") # Log response start
             self.stats['pages_failed'] += 1
             return None


    def extract_data(self, response_data: Any) -> List[Dict]:
        """Extract and transform API response data."""
        items = []
        processed_items = []

        if not response_data:
             self.logger.warning("Received empty response data for extraction.")
             return []

        try:
            data_to_process = response_data
            data_path = self.api_config.get('data_path')

            # --- Navigate to the list/object containing the items ---
            if data_path:
                 self.logger.debug(f"Accessing data path: {data_path}")
                 data_to_process = get_nested_value(response_data, data_path)
                 if data_to_process is None:
                      self.logger.warning(f"Data path '{data_path}' resulted in None.")
                      data_to_process = [] # Process empty list if path invalid

            # --- Ensure we have a list to iterate over ---
            if isinstance(data_to_process, list):
                items = data_to_process
                self.logger.debug(f"Found {len(items)} items at data path '{data_path or 'root'}'.")
            elif isinstance(data_to_process, dict):
                 # If data_path pointed to a single object, wrap it in a list
                 items = [data_to_process]
                 self.logger.debug(f"Data at path '{data_path}' is a single object, processing as one item.")
            else:
                self.logger.warning(f"Data found at path '{data_path or 'root'}' is not a list or dictionary, cannot extract items (Type: {type(data_to_process)}).")
                return [] # Cannot process non-list/dict data

            # --- Apply field mappings ---
            field_mappings = self.api_config.get('field_mappings')
            if not field_mappings:
                 # If no mappings, assume items are already dicts with desired structure
                 # Filter out non-dict items just in case
                 processed_items = [item for item in items if isinstance(item, dict)]
                 if len(processed_items) != len(items):
                      self.logger.warning("Some items in the API response were not dictionaries and were skipped.")
            else:
                 self.logger.debug(f"Applying field mappings: {field_mappings}")
                 for i, item in enumerate(items):
                    if not isinstance(item, dict):
                         self.logger.warning(f"Skipping item {i+1} as it is not a dictionary (type: {type(item)}).")
                         continue # Skip non-dict items
                    mapped_item = {}
                    for target_field, source_path in field_mappings.items():
                        # Use helper function for potentially nested source paths
                        mapped_item[target_field] = get_nested_value(item, source_path)
                    processed_items.append(mapped_item)

            # Note: self.stats is updated in _process_extracted_data in BaseScraper now
            # self.stats['items_collected'] += len(processed_items) # Update count based on successfully processed items
            return processed_items

        except Exception as e:
            # Log the actual error encountered
            self.logger.error(f"API data extraction failed: {e}", exc_info=True) # Log traceback
            return [] # Return empty list on failure


    def run(self) -> Dict:
        """Execute API scraping job."""
        super().run() # Sets start time
        all_extracted_data = []
        endpoints = self.api_config.get('endpoints', [])

        if not endpoints:
            self.logger.warning("No API endpoints defined in 'api_config'.")
            self.stats['end_time'] = time.time()
            return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        for endpoint in endpoints:
            # Params can be defined globally or per-endpoint if needed later
            # For now, using global params for all endpoints
            response_data = self.fetch_data(endpoint) # Fetches data

            if response_data:
                page_data = self.extract_data(response_data) # Extracts data
                all_extracted_data.extend(page_data)
                # Log inside extract_data now
                # self.logger.info(f"Collected {len(page_data)} items from {endpoint}")

        # --- Process all collected data at the end ---
        processed_data = self._process_extracted_data(all_extracted_data)

        self.stats['end_time'] = time.time()
        return {
            'data': processed_data, # Return processed data
            'stats': self.get_stats(),
            'config': self.config
        }
