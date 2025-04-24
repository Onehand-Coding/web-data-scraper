# File: web-data-scraper/scraper/base_scraper.py (Updated)

from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Optional, Any, Set # Added Set
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urljoin
import time
import requests
from requests.exceptions import ProxyError # <-- Import ProxyError
from bs4 import BeautifulSoup
from .data_processor import DataProcessor
from .utils.proxy_rotator import ProxyRotator # <-- Import ProxyRotator

class BaseScraper(ABC):
    """Base scraper class providing common scraping functionality."""

    def __init__(self, config: Dict):
        """
        Initialize the scraper with configuration.
        Args:
            config: Dictionary containing validated scraping configuration.
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__) # Use class name for logger
        self.session = requests.Session()

        # Set User-Agent
        self.session.headers.update({'User-Agent': config.get('user_agent', 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)')}) # Default to Googlebot

        # Initialize robot parser if respecting robots.txt
        self.robot_parser = None
        if config.get('respect_robots', True):
             self.robot_parser = RobotFileParser()

        # Request throttling
        self.last_request_time = 0
        self.request_delay = config.get('request_delay', 1) # Default delay

        # Stats tracking
        self.stats = {
            'start_time': time.time(),
            'end_time': None,
            'total_duration': 0,
            'pages_scraped': 0,
            'pages_failed': 0,
            'items_extracted': 0,
            'items_processed': 0,
            'robots_skipped': 0,
            'proxy_failures': 0 # <-- Add new stat
        }

        # Initialize DataProcessor
        self.data_processor = DataProcessor(config)
        self.processing_rules = config.get('processing_rules', {})

        # --- Initialize Proxy Rotator ---
        self.proxy_rotator = None
        proxies_list = config.get('proxies', []) # Get proxy list safely
        if proxies_list:
            self.logger.info(f"Initializing ProxyRotator with {len(proxies_list)} proxies.")
            self.proxy_rotator = ProxyRotator(proxies_list)
        else:
            self.logger.info("No proxies configured.")
        # --- End Proxy Rotator Init ---


    def _setup_robot_parser_for_domain(self, base_url: str):
        """Sets up the robot parser for a specific domain."""
        if not self.robot_parser:
            return

        robots_url = urljoin(base_url, '/robots.txt')
        try:
            self.logger.debug(f"Fetching robots.txt from: {robots_url}")
            self.robot_parser.set_url(robots_url)
            self.robot_parser.read()
            self.logger.info(f"Successfully read robots.txt for {base_url}")
        except Exception as e:
            self.logger.warning(f"Failed to fetch or parse robots.txt from {robots_url}: {e}")


    def check_robots_permission(self, url: str) -> bool:
        """Check if scraping is allowed by robots.txt for the current domain."""
        if not self.robot_parser:
            self.logger.debug("Robots.txt checking is disabled.")
            return True

        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        if not self.robot_parser.url or urlparse(self.robot_parser.url).netloc != parsed_url.netloc:
             self._setup_robot_parser_for_domain(base_url)

        user_agent = self.session.headers.get('User-Agent', '*')
        allowed = self.robot_parser.can_fetch(user_agent, url)
        if not allowed:
             self.logger.warning(f"Scraping disallowed by robots.txt for URL: {url} (User-Agent: {user_agent})")
             self.stats['robots_skipped'] += 1
        else:
             self.logger.debug(f"Scraping allowed by robots.txt for URL: {url}")
        return allowed

    def throttle_requests(self):
        """Ensure proper delay between requests."""
        elapsed = time.time() - self.last_request_time
        wait_time = self.request_delay - elapsed
        if wait_time > 0:
            self.logger.debug(f"Throttling request: sleeping for {wait_time:.2f} seconds.")
            time.sleep(wait_time)
        self.last_request_time = time.time()

    def fetch_page(self, url: str, max_retries: Optional[int] = None) -> Optional[str]:
        """Fetch page content with retry logic and proxy rotation."""
        if not self.check_robots_permission(url):
            return None

        retries = max_retries if max_retries is not None else self.config.get('max_retries', 3)
        self.logger.info(f"Fetching URL: {url}")

        for attempt in range(retries + 1):
            self.throttle_requests()

            current_proxy: Optional[Dict] = None
            # --- Get Proxy if Rotator is Enabled ---
            if self.proxy_rotator:
                current_proxy = self.proxy_rotator.rotate()
                if current_proxy:
                     self.logger.debug(f"Attempt {attempt + 1}: Using proxy {_proxy_to_str(current_proxy)}")
                     self.session.proxies = current_proxy
                else:
                     self.logger.warning(f"Attempt {attempt + 1}: No working proxy available. Making direct request.")
                     self.session.proxies = {}
            else:
                 self.session.proxies = {}
            # --- End Get Proxy ---

            try:
                response = self.session.get(url, timeout=self.config.get('page_load_timeout', 30))
                response.raise_for_status()
                self.stats['pages_scraped'] += 1
                self.logger.debug(f"Successfully fetched {url} (Status: {response.status_code})")
                response.encoding = response.apparent_encoding
                return response.text
            # --- Catch ProxyError specifically ---
            except ProxyError as e:
                 self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} failed for {url} with ProxyError: {e}")
                 self.stats['proxy_failures'] += 1
                 if self.proxy_rotator and current_proxy:
                      self.proxy_rotator.mark_bad(current_proxy)
                      self.logger.info(f"Marked proxy {_proxy_to_str(current_proxy)} as bad.")
            # --- End Catch ProxyError ---
            except requests.exceptions.Timeout as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} timed out for {url}: {e}")
            except requests.exceptions.HTTPError as e:
                 self.logger.error(f"Attempt {attempt + 1}/{retries + 1} failed for {url} with HTTP Status {e.response.status_code}: {e}")
                 if 400 <= e.response.status_code < 500 and e.response.status_code != 429: break
            except requests.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} failed for {url}: {e}")

            if attempt < retries:
                backoff_time = 2 ** attempt
                self.logger.info(f"Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
            else:
                self.logger.error(f"All {retries + 1} attempts failed for {url}.")
                self.stats['pages_failed'] += 1
                self.session.proxies = {}
                return None

        self.session.proxies = {}
        return None


    def _process_extracted_data(self, data: List[Dict]) -> List[Dict]:
        """Apply processing rules using DataProcessor."""
        self.stats['items_extracted'] += len(data)
        if not self.processing_rules or not data:
            self.logger.debug("No processing rules defined or no data to process.")
            self.stats['items_processed'] = self.stats['items_extracted']
            return data

        self.logger.info(f"Applying {len(self.processing_rules)} processing rule categories to {len(data)} items...")
        processed_data = self.data_processor.process(data, self.processing_rules)
        self.stats['items_processed'] = len(processed_data)
        self.logger.info(f"Finished processing. {len(processed_data)} items remain.")
        return processed_data


    @abstractmethod
    def extract_data(self, content: Any, url: str) -> List[Dict]:
        """
        Extract data from fetched content (HTML string, API response dict, etc.).
        Args:
            content: The fetched content (e.g., HTML string).
            url: The URL from which the content was fetched (for context).
        Returns:
            List of dictionaries, where each dict represents a scraped item.
        """
        pass

    @abstractmethod
    def run(self) -> Dict:
        """
        Execute the scraping job for all configured URLs and return collected data and stats.
        This method should handle fetching, extracting, and processing data.
        """
        # No base implementation, must be overridden by subclasses.
        # Subclasses should call self.stats['start_time'] = time.time() at the beginning.
        pass


    def get_stats(self) -> Dict:
        """Calculate final duration and return scraping statistics."""
        if self.stats.get('start_time') and not self.stats.get('end_time'):
             self.stats['end_time'] = time.time()
        if self.stats.get('start_time') and self.stats.get('end_time'):
             self.stats['total_duration'] = round(self.stats['end_time'] - self.stats['start_time'], 2)
        return self.stats

# --- Helper function ---
def _proxy_to_str(proxy_dict: Optional[Dict]) -> str:
    """Helper to get a display string for a proxy dict."""
    if not proxy_dict: return "None"
    url = proxy_dict.get('https', proxy_dict.get('http', 'N/A'))
    return url
