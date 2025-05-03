import time
import requests # Used indirectly via BaseScraper's session
from typing import Dict, List, Optional, Any
from .base_scraper import BaseScraper
import json # For parsing JSON responses
import logging

# --- Helper function for nested access ---
# Consider moving to a shared utils module if used elsewhere
def get_nested_value(data_dict: Optional[Dict | List], key_path: str, default: Any = None) -> Any:
    """
    Accesses a value in a nested structure (dict or list) using a dot-separated key path.

    Handles dictionary keys and integer list indices.

    Args:
        data_dict: The dictionary or list to access.
        key_path: A string representing the path (e.g., 'user.address.city', 'results.0.name').
                  If empty, returns the original data_dict.
        default: The value to return if the path is invalid or not found.

    Returns:
        The value found at the specified path, or the default value.
    """
    if not key_path: # If path is empty, return the whole structure
        return data_dict

    keys = key_path.split('.')
    current_value = data_dict

    try:
        for key in keys:
            if isinstance(current_value, dict):
                current_value = current_value.get(key)
            elif isinstance(current_value, list):
                 # Allow accessing list elements by index
                 try:
                     key_index = int(key)
                     # Check bounds to prevent IndexError
                     if 0 <= key_index < len(current_value):
                         current_value = current_value[key_index]
                     else:
                         # Index out of bounds
                         logging.getLogger(__name__).debug(f"Index '{key}' out of bounds for list in path '{key_path}'.")
                         return default
                 except (ValueError, IndexError):
                     # Key is not a valid integer index or other list access error
                     logging.getLogger(__name__).debug(f"Invalid index '{key}' for list in path '{key_path}'.")
                     return default
            else:
                # Cannot traverse further if not a dict or list
                logging.getLogger(__name__).debug(f"Cannot traverse non-dict/list element with key '{key}' in path '{key_path}'.")
                return default

            if current_value is None:
                 # Path segment doesn't exist
                 logging.getLogger(__name__).debug(f"Path segment '{key}' resulted in None for path '{key_path}'.")
                 return default
        # Successfully navigated the full path
        return current_value
    except Exception as e:
        # Catch any unexpected errors during traversal
        logging.getLogger(__name__).error(f"Error accessing nested value for key path '{key_path}': {e}")
        return default
# --- End Helper ---


class APIScraper(BaseScraper):
    """
    Scraper implementation specifically for fetching and processing data from JSON APIs.

    Leverages the BaseScraper for common functionalities like session management,
    throttling, retries (via fetch_page), and proxy handling (via requests session).
    Adds logic specific to API interaction like handling base URLs, endpoints,
    JSON parsing, data path navigation, and field mapping.
    """

    def __init__(self, config: Dict):
        """
        Initializes the APIScraper.

        Args:
            config: The validated scraper configuration dictionary. Requires an
                    'api_config' sub-dictionary containing API specifics like
                    'base_url', 'endpoints', and optionally 'method', 'params',
                    'headers', 'data', 'data_path', 'field_mappings'.
        """
        super().__init__(config)
        self.api_config = config.get('api_config', {})
        if not self.api_config or not self.api_config.get('base_url'):
            # Log error if essential API config is missing
            self.logger.error("APIScraper initialized without a valid 'api_config' including 'base_url'!")
            # Consider raising an error here if api_config is absolutely mandatory
            # raise ValueError("Missing required 'api_config' section for APIScraper")

    def fetch_data(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Any]:
        """
        Fetches data from a specific API endpoint relative to the base URL.

        Constructs the full URL, prepares parameters and headers, and uses the
        BaseScraper's fetch_page method (which handles GET requests, retries,
        throttling, proxies). Parses the response text as JSON.

        Args:
            endpoint: The API endpoint path (e.g., "/users", "posts/1").
            params: Optional dictionary of URL parameters to override/add to config.

        Returns:
            The parsed JSON response (can be a dict, list, etc.) or None if
            fetching or JSON parsing fails.
        """
        base_url = self.api_config.get('base_url')
        if not base_url:
             self.logger.error("API base_url is missing in configuration. Cannot fetch data.")
             return None

        # Construct the full URL, ensuring no double slashes
        full_url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        # --- Note: Current implementation relies on BaseScraper.fetch_page ---
        # This means it primarily supports GET requests.
        # For POST/PUT/PATCH/DELETE, the session method in fetch_page would need
        # modification or direct session usage here (e.g., self.session.post(...)).
        http_method = self.api_config.get('method', 'GET').upper()
        if http_method != 'GET':
             self.logger.warning(f"APIScraper currently uses BaseScraper.fetch_page which performs GET requests. Configured method '{http_method}' will be ignored for fetching.")
             # TODO: Enhance BaseScraper.fetch_page or implement specific POST/PUT etc. logic here if needed.
             # request_data = self.api_config.get('data') # Prepare data for non-GET
             # response = self.session.request(method=http_method, url=full_url, ...)

        # --- Use BaseScraper's fetch_page for GET request handling ---
        # It incorporates session (headers, proxies), throttling, retries.
        # It fetches the raw text content of the response.
        response_text = super().fetch_page(full_url) # BaseScraper handles logging fetch attempt

        if response_text:
            # If text content was received, attempt to parse it as JSON
            try:
                return json.loads(response_text)
            except json.JSONDecodeError as e:
                # Log failure to parse JSON
                self.logger.error(f"Failed to decode JSON response from {full_url}: {e}")
                self.logger.debug(f"Response text started with: {response_text[:500]}...") # Log beginning of text
                return None # Indicate JSON parsing failure
        else:
            # fetch_page returned None, meaning fetching failed after retries
            # Error is already logged by fetch_page
            return None


    def extract_data(self, response_data: Any, url: str = "N/A") -> List[Dict]:
        """
        Extracts a list of item dictionaries from the parsed API response data.

        Applies `data_path` to navigate nested structures and `field_mappings`
        to rename and select specific fields from the API items.

        Args:
            response_data: The parsed JSON data returned by `Workspace_data`.
            url: The URL the data came from (unused here, but kept for signature consistency).

        Returns:
            A list of dictionaries, where each dictionary represents a processed item.
        """
        items_list: List = [] # The list of raw items found at data_path
        processed_items: List[Dict] = [] # The final list with mappings applied

        if response_data is None: # Handle case where fetch_data returned None
             self.logger.warning(f"Received None response_data for extraction (URL: {url}).")
             return []

        try:
            # --- Navigate to the list of items using data_path ---
            data_to_process = response_data
            data_path = self.api_config.get('data_path')

            if data_path:
                 # Use helper to navigate potentially nested structure
                 self.logger.debug(f"Accessing items using data path: '{data_path}'")
                 data_to_process = get_nested_value(response_data, data_path)
                 if data_to_process is None:
                      self.logger.warning(f"Data path '{data_path}' resulted in None or was not found in the response from {url}.")
                      data_to_process = [] # Process as empty list if path invalid

            # --- Ensure we have a list (or treat single dict as list) ---
            if isinstance(data_to_process, list):
                items_list = data_to_process
                self.logger.debug(f"Found {len(items_list)} potential items at data path '{data_path or 'root'}'.")
            elif isinstance(data_to_process, dict):
                 # If data_path points to a single object, treat it as a list of one
                 items_list = [data_to_process]
                 self.logger.debug(f"Data at path '{data_path}' is a single object, processing as one item.")
            else:
                # Log warning if the data at the path is not processable as items
                self.logger.warning(f"Data found at path '{data_path or 'root'}' is not a list or dictionary (Type: {type(data_to_process)}). Cannot extract items.")
                return [] # Return empty if data is not list/dict

            # --- Apply field mappings (if configured) ---
            field_mappings = self.api_config.get('field_mappings')
            if not field_mappings:
                 # If no mappings, assume items are already in desired dict format
                 self.logger.debug("No field mappings defined. Assuming items are already dictionaries.")
                 processed_items = [item for item in items_list if isinstance(item, dict)]
                 # Log if some items were not dictionaries
                 if len(processed_items) != len(items_list):
                      self.logger.warning("Some items in the API response were skipped because they were not dictionaries and no field_mappings were provided.")
            else:
                 # Apply mappings: create new dicts with desired keys
                 self.logger.debug(f"Applying field mappings: {field_mappings}")
                 for i, item in enumerate(items_list):
                    # Ensure the source item is a dictionary before mapping
                    if not isinstance(item, dict):
                         self.logger.warning(f"Skipping API item {i+1} because it is not a dictionary (type: {type(item)}). Cannot apply mappings.")
                         continue
                    # Create the new item with mapped fields
                    mapped_item = {}
                    for target_field, source_path in field_mappings.items():
                        # Use helper to get potentially nested source value
                        mapped_item[target_field] = get_nested_value(item, source_path)
                    processed_items.append(mapped_item)

            return processed_items

        except Exception as e:
            # Catch unexpected errors during mapping/processing
            self.logger.error(f"Error during API data extraction/mapping phase: {e}", exc_info=True)
            return []


    def run(self) -> Dict:
        """
        Executes the API scraping job for all configured endpoints.

        Iterates through endpoints, fetches JSON data for each, extracts items
        using data_path and field_mappings, aggregates the results, and applies
        final processing rules.

        Returns:
            A dictionary containing the processed data ('data'), run statistics ('stats'),
            and the original configuration ('config').
        """
        self.stats['start_time'] = time.time() # Record job start
        all_extracted_data: List[Dict] = []
        endpoints = self.api_config.get('endpoints', [])

        if not endpoints:
            self.logger.warning("No API endpoints defined in 'api_config'. Cannot run job.")
            self.stats['end_time'] = time.time()
            return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        # --- Iterate through each configured endpoint ---
        for endpoint in endpoints:
             # Construct full URL primarily for logging/context
             base_url = self.api_config.get('base_url','').rstrip('/')
             full_url = f"{base_url}/{endpoint.lstrip('/')}" if base_url else endpoint

             # Fetch data for the current endpoint
             response_data = self.fetch_data(endpoint) # Handles retries, proxies etc.

             if response_data:
                 # If fetch and JSON parse succeeded, extract items
                 page_data = self.extract_data(response_data, full_url)
                 all_extracted_data.extend(page_data)
             # else: fetch_data already logged the failure

        # --- Process all aggregated data ---
        processed_data = self._process_extracted_data(all_extracted_data)

        # --- Finalize and return ---
        self.stats['end_time'] = time.time()
        return {
            'data': processed_data,
            'stats': self.get_stats(),
            'config': self.config
        }
