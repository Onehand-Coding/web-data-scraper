from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Optional, Any, Set
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
import time
import requests
from requests.exceptions import ProxyError, Timeout, HTTPError, RequestException
from bs4 import BeautifulSoup # Keep for potential future use in base class, though not used directly now
from .data_processor import DataProcessor
from .utils.proxy_rotator import ProxyRotator

# Helper function moved inside or kept separate as preferred
def _proxy_to_str(proxy_dict: Optional[Dict]) -> str:
    """Helper to get a display string for a proxy dict."""
    if not proxy_dict: return "None"
    # Prioritize https URL for display if available
    url = proxy_dict.get('https', proxy_dict.get('http', 'N/A'))
    return url

class BaseScraper(ABC):
    """
    Abstract base class for scrapers.

    Provides common functionalities like:
    - Session management with User-Agent handling.
    - robots.txt checking.
    - Request throttling.
    - Proxy rotation via ProxyRotator.
    - Basic statistics tracking.
    - Data processing delegation to DataProcessor.
    - Retry logic for fetching pages.
    """

    def __init__(self, config: Dict):
        """
        Initializes the BaseScraper.

        Args:
            config: A dictionary containing the validated scraping configuration.
                    Expected keys include 'user_agent', 'respect_robots',
                    'request_delay', 'max_retries', 'proxies',
                    'processing_rules', 'output_dir'.
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__) # Logger named after the subclass
        self.session = requests.Session()

        # Set User-Agent (defaulting to a common bot if not provided)
        default_ua = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
        self.session.headers.update({'User-Agent': config.get('user_agent', default_ua)})
        self.logger.debug(f"Using User-Agent: {self.session.headers['User-Agent']}")

        # Initialize robot parser conditionally
        self.robot_parser: Optional[RobotFileParser] = None
        self._robots_txt_domain: Optional[str] = None # Track current domain for robots.txt
        if config.get('respect_robots', True):
             self.robot_parser = RobotFileParser()
             self.logger.info("robots.txt checking enabled.")
        else:
             self.logger.info("robots.txt checking disabled.")

        # Request throttling state
        self.last_request_time: float = 0.0
        self.request_delay: float = float(config.get('request_delay', 1.0)) # Ensure float

        # Statistics tracking dictionary
        self.stats: Dict[str, Any] = {
            'start_time': time.time(),
            'end_time': None,
            'total_duration': 0,
            'pages_scraped': 0,
            'pages_failed': 0,
            'items_extracted': 0,
            'items_processed': 0,
            'robots_skipped': 0,
            'proxy_failures': 0
        }

        # Data processing setup
        self.data_processor = DataProcessor(config)
        self.processing_rules = config.get('processing_rules', {})

        # Proxy rotation setup
        self.proxy_rotator: Optional[ProxyRotator] = None
        proxies_list = config.get('proxies', [])
        if proxies_list:
            self.logger.info(f"Initializing ProxyRotator with {len(proxies_list)} proxies.")
            self.proxy_rotator = ProxyRotator(proxies_list)
        else:
            self.logger.info("No proxies configured.")


    def _setup_robot_parser_for_domain(self, base_url: str):
        """
        Fetches and parses the robots.txt file for a given base URL.

        Args:
            base_url: The base URL of the domain (e.g., "https://example.com").
        """
        if not self.robot_parser:
            return # Do nothing if robots checking is disabled

        robots_url = urljoin(base_url, '/robots.txt')
        try:
            self.logger.debug(f"Fetching robots.txt from: {robots_url}")
            self.robot_parser.set_url(robots_url)
            self.robot_parser.read()
            self._robots_txt_domain = base_url # Store the domain we fetched for
            self.logger.info(f"Successfully read robots.txt for {base_url}")
        except Exception as e:
            # Log warning but don't necessarily stop scraping; maybe allow if fetch fails
            self.logger.warning(f"Failed to fetch or parse robots.txt from {robots_url}: {e}")
            self._robots_txt_domain = base_url # Still mark as attempted for this domain
            # Optionally, treat as disallowed if robots.txt is unreadable: return False here


    def check_robots_permission(self, url: str) -> bool:
        """
        Checks if the configured User-Agent is allowed to fetch the given URL
        according to the fetched robots.txt for the URL's domain.

        Args:
            url: The full URL to check.

        Returns:
            True if allowed or if robots checking is disabled/failed, False if disallowed.
        """
        if not self.robot_parser:
            # self.logger.debug("Robots.txt checking is disabled.")
            return True # Allowed if disabled

        try:
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

            # Fetch robots.txt if it's for a new domain or hasn't been fetched yet
            if self._robots_txt_domain != base_url:
                 self._setup_robot_parser_for_domain(base_url)
                 # Handle case where _setup failed but we proceed anyway
                 if not self.robot_parser.url or urlparse(self.robot_parser.url).netloc != parsed_url.netloc:
                      self.logger.warning(f"Could not use robots.txt for {base_url}, proceeding with caution.")
                      return True # Allow if robots.txt failed to load

            user_agent = self.session.headers.get('User-Agent', '*')
            allowed = self.robot_parser.can_fetch(user_agent, url)

            if not allowed:
                 self.logger.warning(f"Scraping disallowed by robots.txt for URL: {url} (User-Agent: {user_agent})")
                 self.stats['robots_skipped'] += 1
            else:
                 self.logger.debug(f"Scraping allowed by robots.txt for URL: {url}")
            return allowed
        except Exception as e:
            self.logger.error(f"Error checking robots permission for {url}: {e}", exc_info=True)
            return True # Allow by default if there's an unexpected error during check


    def throttle_requests(self):
        """Ensures a delay between consecutive requests based on `request_delay`."""
        if self.request_delay <= 0:
            return # No delay needed

        elapsed = time.time() - self.last_request_time
        wait_time = self.request_delay - elapsed
        if wait_time > 0:
            self.logger.debug(f"Throttling request: sleeping for {wait_time:.2f} seconds.")
            time.sleep(wait_time)
        # Update last request time *after* potential sleep
        self.last_request_time = time.time()


    def fetch_page(self, url: str, max_retries: Optional[int] = None) -> Optional[str]:
        """
        Fetches the content of a URL using the configured session,
        handling robots.txt, throttling, proxy rotation, and retries.

        Args:
            url: The URL to fetch.
            max_retries: Override the default max retries from config.

        Returns:
            The text content of the page if successful, otherwise None.
        """
        # 1. Check robots.txt permission first
        if not self.check_robots_permission(url):
            return None

        retries = max_retries if max_retries is not None else self.config.get('max_retries', 3)
        self.logger.info(f"Fetching URL: {url}")

        for attempt in range(retries + 1):
            # 2. Throttle request before making it
            self.throttle_requests()

            current_proxy: Optional[Dict] = None
            proxies_to_use: Optional[Dict] = None

            # 3. Get proxy if rotator is enabled
            if self.proxy_rotator:
                current_proxy = self.proxy_rotator.rotate()
                if current_proxy:
                     self.logger.debug(f"Attempt {attempt + 1}: Using proxy {_proxy_to_str(current_proxy)}")
                     proxies_to_use = current_proxy
                else:
                     # If rotator returns None, means no working proxies left
                     self.logger.warning(f"Attempt {attempt + 1}: No working proxy available from rotator. Making direct request.")
                     proxies_to_use = {} # Explicitly use no proxy
            else:
                 proxies_to_use = {} # No rotator configured

            # 4. Make the request
            try:
                response = self.session.get(
                    url,
                    timeout=self.config.get('page_load_timeout', 30),
                    proxies=proxies_to_use # Use the selected proxy (or empty dict for direct)
                )
                response.raise_for_status() # Raise HTTPError for bad status codes (4xx or 5xx)

                # Success
                self.stats['pages_scraped'] += 1
                self.logger.debug(f"Successfully fetched {url} (Status: {response.status_code})")
                # Try to decode content correctly
                response.encoding = response.apparent_encoding or 'utf-8'
                return response.text

            # 5. Handle Exceptions (Order Matters: More specific first)
            except ProxyError as e:
                 self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} failed for {url} with ProxyError: {e}")
                 self.stats['proxy_failures'] += 1
                 if self.proxy_rotator and current_proxy:
                      self.proxy_rotator.mark_bad(current_proxy)
                      self.logger.info(f"Marked proxy {_proxy_to_str(current_proxy)} as bad.")
                 # No need to break, retry might use a different proxy or direct connection

            except Timeout as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} timed out for {url}: {e}")
                # Could potentially mark proxy bad on timeout too, depending on strategy

            except HTTPError as e:
                 # Log specific HTTP errors
                 status_code = e.response.status_code
                 self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} failed for {url} with HTTP Status {status_code}: {e}")
                 # Stop retrying on client errors (4xx) except for 429 (Too Many Requests)
                 if 400 <= status_code < 500 and status_code != 429:
                      self.logger.error(f"Client error {status_code} received. Aborting retries for {url}.")
                      break # Don't retry client errors like 404 Not Found or 403 Forbidden

            except RequestException as e:
                # Catch other general request errors (DNS, Connection, etc.)
                self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} failed for {url} with RequestException: {e}")
                # Potentially mark proxy as bad for certain connection errors

            # 6. Retry Logic
            if attempt < retries:
                # Implement exponential backoff
                backoff_time = 2 ** attempt
                self.logger.info(f"Retrying {url} in {backoff_time} seconds...")
                time.sleep(backoff_time)
            else:
                # Log final failure after all retries
                self.logger.error(f"All {retries + 1} attempts failed for {url}.")
                self.stats['pages_failed'] += 1
                # Ensure session proxies are cleared if last attempt used one
                self.session.proxies = {}
                return None # Return None after all retries fail

        # Fallback return if loop finishes unexpectedly (shouldn't happen with break/return)
        self.session.proxies = {}
        return None


    def _process_extracted_data(self, data: List[Dict]) -> List[Dict]:
        """
        Applies processing rules to the extracted data using the DataProcessor.

        Args:
            data: A list of dictionaries representing the raw extracted items.

        Returns:
            A list of dictionaries representing the processed items.
        """
        self.stats['items_extracted'] += len(data)
        if not self.processing_rules or not data:
            self.logger.debug("No processing rules defined or no data to process.")
            # If no rules, processed count is same as extracted
            self.stats['items_processed'] = self.stats['items_extracted']
            return data

        self.logger.info(f"Applying {len(self.processing_rules)} processing rule categories to {len(data)} items...")
        processed_data = self.data_processor.process(data, self.processing_rules)
        # Update processed count based on the result from the processor
        self.stats['items_processed'] = len(processed_data)
        self.logger.info(f"Finished processing. {len(processed_data)} items remain.")
        return processed_data


    @abstractmethod
    def extract_data(self, content: Any, url: str) -> List[Dict]:
        """
        Abstract method to extract data from fetched content.

        Must be implemented by subclasses (HTMLScraper, DynamicScraper, APIScraper).

        Args:
            content: The fetched content (e.g., HTML string, API response dict).
            url: The URL from which the content was fetched (for context, e.g., resolving relative URLs).

        Returns:
            A list of dictionaries, where each dict represents a scraped item.
            Return an empty list if no items are found or extraction fails.
        """
        pass

    @abstractmethod
    def run(self) -> Dict:
        """
        Abstract method to execute the entire scraping job.

        Must be implemented by subclasses. The implementation should handle:
        - Iterating through target URLs/endpoints.
        - Calling `Workspace_page` (or equivalent for API/dynamic).
        - Calling `extract_data`.
        - Handling pagination logic.
        - Calling `_process_extracted_data` at the end.
        - Updating `self.stats` appropriately.
        - Returning the final data and stats.
        """
        # Example structure reminder for subclasses:
        # self.stats['start_time'] = time.time()
        # all_data = []
        # # ... loop through URLs/endpoints ...
        #     content = self.fetch_page(url) # or specific fetch logic
        #     if content:
        #         extracted = self.extract_data(content, url)
        #         all_data.extend(extracted)
        #     # ... handle pagination ...
        # processed_data = self._process_extracted_data(all_data)
        # self.stats['end_time'] = time.time()
        # return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}
        pass


    def get_stats(self) -> Dict:
        """Calculates final duration and returns the statistics dictionary."""
        if self.stats.get('start_time') and not self.stats.get('end_time'):
             # Ensure end_time is set if run finished but wasn't explicitly set
             self.stats['end_time'] = time.time()
        if self.stats.get('start_time') and self.stats.get('end_time'):
             # Calculate total duration
             self.stats['total_duration'] = round(self.stats['end_time'] - self.stats['start_time'], 2)
        return self.stats
