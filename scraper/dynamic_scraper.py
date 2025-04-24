# File: web-data-scraper/scraper/dynamic_scraper.py (Updated with Proxy Logic)

from typing import Dict, List, Optional, Any, Set
from urllib.parse import urljoin, urlparse
import time
import logging # Import logging

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException

from .base_scraper import BaseScraper
# ProxyRotator is accessed via self.proxy_rotator inherited from BaseScraper

# Helper function for proxy string representation (can be defined here or imported if moved to utils)
def _proxy_to_str(proxy_dict: Optional[Dict]) -> str:
    """Helper to get a display string for a proxy dict."""
    if not proxy_dict: return "None"
    url = proxy_dict.get('https', proxy_dict.get('http', 'N/A'))
    return url

class DynamicScraper(BaseScraper):
    """Scraper implementation using Selenium for dynamic content."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.driver: Optional[WebDriver] = None
        self.selectors = config.get('selectors', {})
        self.pagination_config = config.get('pagination')
        # self.proxy_rotator is initialized in BaseScraper's __init__

    def _init_driver(self) -> Optional[WebDriver]:
        """Initialize Selenium WebDriver with options, including proxy."""
        if self.driver:
             self.logger.warning("WebDriver already initialized.")
             return self.driver

        # --- Get Proxy for Driver ---
        current_proxy_config: Optional[Dict] = None
        proxy_arg: Optional[str] = None
        proxy_display_str: str = "None" # For logging

        if self.proxy_rotator:
            current_proxy_config = self.proxy_rotator.rotate() # Get a proxy
            if current_proxy_config:
                proxy_display_str = _proxy_to_str(current_proxy_config)
                # Selenium's --proxy-server argument typically uses the http proxy for all protocols
                proxy_url = current_proxy_config.get('http', current_proxy_config.get('https'))
                if proxy_url:
                    # Basic host:port is generally safer for --proxy-server.
                    parsed_proxy = urlparse(proxy_url)
                    proxy_host_port = parsed_proxy.netloc.split('@')[-1] # Get host:port part
                    proxy_arg = f"--proxy-server={proxy_host_port}"
                    self.logger.info(f"Attempting to initialize WebDriver with proxy: {proxy_host_port}")
                else:
                    self.logger.warning("Proxy configuration found but no valid http/https URL string.")
                    current_proxy_config = None # Treat as no proxy if URL is invalid
            else:
                self.logger.warning("Proxy rotator enabled, but no working proxies available for driver setup. Trying direct connection.")
        # --- End Get Proxy ---

        options = webdriver.ChromeOptions()
        driver_path = self.config.get('webdriver_path')

        if self.config.get('headless', True):
            options.add_argument('--headless=new')
        if self.config.get('disable_images', True):
            options.add_argument('--blink-settings=imagesEnabled=false')

        # --- Add proxy argument to options if obtained ---
        if proxy_arg:
            options.add_argument(proxy_arg)
        # --- End Add proxy ---

        options.add_argument(f'user-agent={self.session.headers["User-Agent"]}')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        try:
             self.logger.info("Initializing WebDriver...")
             driver: Optional[WebDriver] = None
             if driver_path:
                  from selenium.webdriver.chrome.service import Service
                  service = Service(executable_path=driver_path)
                  driver = webdriver.Chrome(service=service, options=options)
             else:
                  # Assumes chromedriver is in PATH if driver_path is not set
                  driver = webdriver.Chrome(options=options)

             page_load_timeout = self.config.get('page_load_timeout', 30)
             driver.set_page_load_timeout(page_load_timeout)

             self.logger.info(f"WebDriver initialized successfully (Proxy: {proxy_display_str}).")
             return driver
        except WebDriverException as e:
             err_msg = str(e).lower()
             is_proxy_error = "proxy" in err_msg or "connection refused" in err_msg or "net::err" in err_msg
             log_level = logging.WARNING if is_proxy_error else logging.ERROR
             self.logger.log(log_level, f"Failed to initialize WebDriver: {e}", exc_info=False)

             if self.proxy_rotator and current_proxy_config and is_proxy_error:
                  self.logger.warning(f"WebDriver init failed, likely due to proxy. Marking proxy {proxy_display_str} as bad.")
                  self.proxy_rotator.mark_bad(current_proxy_config)
                  self.stats['proxy_failures'] += 1
             elif not is_proxy_error:
                  self.logger.error("Ensure WebDriver (e.g., chromedriver) is installed and accessible.")

             return None
        except Exception as e:
             self.logger.error(f"An unexpected error occurred during WebDriver initialization: {e}", exc_info=True)
             return None

    def _wait_for_page_load(self, current_url: Optional[str] = None):
        """Applies configured waits after a page navigation."""
        if not self.driver: return

        wait_time = self.config.get('wait_time', 5)
        wait_selector = self.config.get('wait_for_selector')

        try:
            if wait_selector:
                self.logger.debug(f"Waiting up to {wait_time}s for selector '{wait_selector}'...")
                condition = EC.visibility_of_element_located((By.CSS_SELECTOR, wait_selector))
                WebDriverWait(self.driver, wait_time).until(condition)
                self.logger.debug(f"Element '{wait_selector}' is visible.")
            elif wait_time > 0:
                 self.logger.debug(f"Waiting {wait_time}s for general page load/scripts...")
                 time.sleep(wait_time)
        except TimeoutException:
             self.logger.warning(f"Timed out after {wait_time}s waiting for condition/selector '{wait_selector or 'general load'}' on {current_url or self.driver.current_url}.")
        except Exception as e:
             self.logger.error(f"Error during explicit wait on {current_url or self.driver.current_url}: {e}")


    def extract_data(self, url: str) -> List[Dict]:
        """Extract data using Selenium's find elements from the current driver state."""
        items = []
        if not self.driver:
            self.logger.error("WebDriver not available for extraction.")
            return []

        container_selector = self.selectors.get('container')
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})

        if not item_selector:
             self.logger.error("Missing 'item' selector in configuration.")
             return []

        try:
            search_context: WebDriver | WebElement = self.driver
            if container_selector:
                try:
                    container_element = WebDriverWait(self.driver, 3).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, container_selector))
                    )
                    search_context = container_element
                    self.logger.debug(f"Using container '{container_selector}' as search context.")
                except (NoSuchElementException, TimeoutException):
                     self.logger.warning(f"Container selector '{container_selector}' not found or visible on page {url}. Searching from page root.")

            self.logger.debug(f"Looking for items using selector: '{item_selector}'")
            elements = search_context.find_elements(By.CSS_SELECTOR, item_selector)
            self.logger.debug(f"Found {len(elements)} potential items using selector '{item_selector}'.")

            if not elements:
                 self.logger.warning(f"No items found using item selector '{item_selector}' on page {url}.")
                 return items

            for i, element in enumerate(elements):
                item_data = {}
                for field, selector_config in field_selectors.items():
                     value = None
                     try:
                        target_element: Optional[WebElement] = None
                        if isinstance(selector_config, str):
                             target_element = element.find_element(By.CSS_SELECTOR, selector_config)
                             value = target_element.text.strip() if target_element else None
                        elif isinstance(selector_config, dict):
                             sel = selector_config.get('selector')
                             attr = selector_config.get('attr')
                             if sel:
                                 target_element = element.find_element(By.CSS_SELECTOR, sel)
                                 if target_element:
                                      if attr:
                                          value = target_element.get_attribute(attr)
                                          if attr in ['href', 'src'] and value and not value.startswith(('http://', 'https://', '//', 'data:')):
                                              try:
                                                   value = urljoin(url, value)
                                              except ValueError:
                                                   self.logger.warning(f"Could not resolve relative URL '{value}' against base '{url}'")
                                      else:
                                          value = target_element.text.strip()
                             else: self.logger.warning(f"Selector dict for field '{field}' missing 'selector'.")
                     except NoSuchElementException:
                          self.logger.debug(f"Field '{field}' selector '{selector_config}' not found within item {i+1} on {url}.")
                          value = None
                     except Exception as e:
                          self.logger.error(f"Error extracting field '{field}' in item {i+1}: {e}")
                          value = None
                     item_data[field] = value

                if any(v is not None for v in item_data.values()): items.append(item_data)
                else: self.logger.debug(f"Skipping item {i+1} as all fields were None.")

            self.logger.info(f"Extracted {len(items)} non-empty items from {url}.")

        except NoSuchElementException as e:
             self.logger.error(f"A required element was not found during extraction on {url}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during Selenium extraction on {url}: {e}", exc_info=True)

        return items


    def _find_and_click_next_page(self, current_url: str) -> bool:
        """Finds the next page element using Selenium, clicks it, and waits."""
        if not self.pagination_config or not self.driver:
            return False

        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector:
            self.logger.debug("No 'next_page_selector' defined.")
            return False

        try:
            wait_time = 5
            next_button = WebDriverWait(self.driver, wait_time).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, next_page_selector))
            )

            if next_button and next_button.is_displayed() and next_button.is_enabled():
                 self.logger.info(f"Found 'Next' page element using '{next_page_selector}'. Clicking...")
                 next_button.click()

                 try:
                      WebDriverWait(self.driver, 10).until(EC.url_changes(current_url))
                      new_url = self.driver.current_url
                      self.logger.info(f"Successfully navigated to next page: {new_url}")
                      self._wait_for_page_load(new_url)
                      return True
                 except TimeoutException:
                      self.logger.warning(f"Clicked 'Next' but URL did not change from {current_url} within timeout.")
                      return False
            else:
                self.logger.info(f"Next page element '{next_page_selector}' found but not clickable/visible/enabled.")
                return False

        except TimeoutException:
            self.logger.info(f"Next page element '{next_page_selector}' not found or clickable within {wait_time}s (Likely last page).")
            return False
        except NoSuchElementException:
            self.logger.info(f"Next page element '{next_page_selector}' not found (Likely last page).")
            return False
        except WebDriverException as e:
             self.logger.error(f"Error clicking next page element '{next_page_selector}': {e}")
             return False
        except Exception as e:
             self.logger.error(f"Unexpected error finding/clicking next page '{next_page_selector}': {e}", exc_info=True)
             return False


    def run(self) -> Dict:
        """Execute dynamic scraping job, handling pagination by clicking."""
        self.stats['start_time'] = time.time() # Reset start time directly
        all_extracted_data = []
        initial_urls = self.config.get('urls', [])
        start_url = initial_urls[0] if initial_urls else None

        scraped_urls: Set[str] = set()

        max_pages = float('inf')
        if self.pagination_config:
            max_pages = self.pagination_config.get('max_pages', float('inf'))
        pages_scraped_count = 0

        if not start_url:
             self.logger.warning("No initial URL provided in configuration.")
             self._close_driver()
             self.stats['end_time'] = time.time()
             return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        current_url: Optional[str] = start_url
        try:
            self.driver = self._init_driver()
            if not self.driver:
                  # Error logged in _init_driver if it failed
                  raise WebDriverException("WebDriver initialization failed, cannot proceed.")

            while current_url and pages_scraped_count < max_pages:
                 if current_url in scraped_urls:
                     self.logger.warning(f"URL cycle detected or already scraped: {current_url}. Stopping pagination.")
                     break

                 self.logger.info(f"Processing URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")

                 if self.driver.current_url != current_url:
                      self.throttle_requests()
                      # Simplified robots check - see note above
                      try:
                           self.driver.get(current_url)
                           # Basic check after get for proxy errors shown in page source
                           page_source_lower = self.driver.page_source.lower()
                           if "err_proxy_connection_failed" in page_source_lower or \
                              "err_connection_refused" in page_source_lower:
                               raise WebDriverException(f"Proxy error indicated in page source for {current_url}")
                      except WebDriverException as e:
                           self.logger.error(f"WebDriverException during driver.get({current_url}): {e}")
                           self.stats['pages_failed'] += 1
                           # Fail the job if page load fails (potentially due to proxy)
                           raise

                 self._wait_for_page_load(current_url)

                 scraped_urls.add(current_url)
                 self.stats['pages_scraped'] += 1
                 pages_scraped_count += 1

                 page_data = self.extract_data(current_url)
                 all_extracted_data.extend(page_data)

                 if pages_scraped_count < max_pages:
                      navigated = self._find_and_click_next_page(current_url)
                      if navigated:
                          current_url = self.driver.current_url
                      else:
                           self.logger.info("No further pages found or pagination stopped.")
                           break
                 else:
                      self.logger.info(f"Reached maximum page limit ({max_pages}).")
                      break

        except WebDriverException as e:
             # Catch errors from driver.get or other WebDriver interactions
             self.logger.error(f"WebDriver error during scraping run: {e}", exc_info=True)
             self.stats['pages_failed'] += 1 # Increment failed pages
        except Exception as e:
             # Catch any other unexpected errors
             self.logger.error(f"Unexpected error during dynamic scraping run: {e}", exc_info=True)
             self.stats['pages_failed'] += 1 # Consider this a page failure too
        finally:
             self._close_driver() # Ensure driver is always closed

        processed_data = self._process_extracted_data(all_extracted_data)

        self.stats['end_time'] = time.time()
        return {
            'data': processed_data,
            'stats': self.get_stats(),
            'config': self.config
        }

    def _close_driver(self):
         """Safely close the Selenium WebDriver if it's open."""
         if self.driver:
             driver_instance = self.driver
             self.driver = None
             try:
                 self.logger.info("Closing WebDriver...")
                 driver_instance.quit()
                 self.logger.info("WebDriver closed successfully.")
             except WebDriverException as e:
                 self.logger.error(f"Error closing WebDriver: {e}")
             except Exception as e:
                  self.logger.error(f"Unexpected error quitting WebDriver: {e}")
