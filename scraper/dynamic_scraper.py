# File: web-data-scraper/scraper/dynamic_scraper.py (Ensuring extract_data is present)

from typing import Dict, List, Optional, Any, Set
from urllib.parse import urljoin, urlparse
import time
import logging

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException, TimeoutException, NoSuchElementException,
    ElementNotInteractableException
)

from .base_scraper import BaseScraper

# Helper function
def _proxy_to_str(proxy_dict: Optional[Dict]) -> str:
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
        self.login_config = config.get('login_config')

    def _init_driver(self) -> Optional[WebDriver]:
        """Initialize Selenium WebDriver with options, including proxy."""
        if self.driver:
             self.logger.warning("WebDriver already initialized.")
             return self.driver

        current_proxy_config: Optional[Dict] = None
        proxy_arg: Optional[str] = None
        proxy_display_str: str = "None"

        if self.proxy_rotator:
            current_proxy_config = self.proxy_rotator.rotate()
            if current_proxy_config:
                proxy_display_str = _proxy_to_str(current_proxy_config)
                proxy_url = current_proxy_config.get('http', current_proxy_config.get('https'))
                if proxy_url:
                    parsed_proxy = urlparse(proxy_url)
                    proxy_host_port = parsed_proxy.netloc.split('@')[-1]
                    proxy_arg = f"--proxy-server={proxy_host_port}"
                    self.logger.info(f"Attempting to initialize WebDriver with proxy: {proxy_host_port}")
                else:
                    self.logger.warning("Proxy configuration found but no valid http/https URL string.")
                    current_proxy_config = None
            else:
                self.logger.warning("Proxy rotator enabled, but no working proxies available for driver setup. Trying direct connection.")

        options = webdriver.ChromeOptions()
        driver_path = self.config.get('webdriver_path')

        if self.config.get('headless', True): options.add_argument('--headless=new')
        if self.config.get('disable_images', True): options.add_argument('--blink-settings=imagesEnabled=false')
        if proxy_arg: options.add_argument(proxy_arg)

        options.add_argument(f'user-agent={self.session.headers["User-Agent"]}')
        options.add_argument('--disable-gpu'); options.add_argument('--no-sandbox'); options.add_argument('--disable-dev-shm-usage')
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
                  driver = webdriver.Chrome(options=options)
             page_load_timeout = self.config.get('page_load_timeout', 30)
             driver.set_page_load_timeout(page_load_timeout)
             self.logger.info(f"WebDriver initialized successfully (Proxy: {proxy_display_str}).")
             return driver
        except WebDriverException as e:
             err_msg = str(e).lower(); is_proxy_error = "proxy" in err_msg or "connection refused" in err_msg or "net::err" in err_msg
             log_level = logging.WARNING if is_proxy_error else logging.ERROR
             self.logger.log(log_level, f"Failed to initialize WebDriver: {e}", exc_info=False)
             if self.proxy_rotator and current_proxy_config and is_proxy_error:
                  self.logger.warning(f"WebDriver init failed, likely due to proxy. Marking proxy {proxy_display_str} as bad.")
                  self.proxy_rotator.mark_bad(current_proxy_config)
                  self.stats['proxy_failures'] += 1
             elif not is_proxy_error: self.logger.error("Ensure WebDriver (e.g., chromedriver) is installed/accessible.")
             return None
        except Exception as e:
             self.logger.error(f"Unexpected error during WebDriver initialization: {e}", exc_info=True)
             return None

    # --- THIS METHOD MUST BE PRESENT ---
    def extract_data(self, url: str) -> List[Dict]:
        """
        Extract data using Selenium's find elements from the current driver state.
        Args:
            url: The current URL being processed (for context).
        """
        items = []
        if not self.driver:
            self.logger.error("WebDriver not available for extraction.")
            return []

        # Ensure selectors config exists before accessing keys
        if not self.selectors:
             self.logger.error("Selectors configuration is missing.")
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
                    # Use presence_of_element_located for container check? Visibility might be too strict sometimes.
                    container_element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, container_selector))
                    )
                    search_context = container_element
                    self.logger.debug(f"Using container '{container_selector}' as search context.")
                except (NoSuchElementException, TimeoutException):
                     self.logger.warning(f"Container selector '{container_selector}' not found or timed out on page {url}. Searching from page root.")

            self.logger.debug(f"Looking for items using selector: '{item_selector}'")
            # Wait briefly for at least one item to be present? Optional but can help.
            try:
                WebDriverWait(search_context if isinstance(search_context, WebDriver) else self.driver, 5).until(
                   EC.presence_of_all_elements_located((By.CSS_SELECTOR, item_selector))
                )
                elements = search_context.find_elements(By.CSS_SELECTOR, item_selector)
            except TimeoutException:
                self.logger.warning(f"Timed out waiting for items with selector '{item_selector}' on page {url}.")
                elements = [] # Proceed with empty list if items don't appear

            self.logger.debug(f"Found {len(elements)} potential items using selector '{item_selector}'.")

            if not elements:
                 # Log already happened above if timeout occurred
                 # self.logger.warning(f"No items found using item selector '{item_selector}' on page {url}.")
                 return items

            # Extract fields for each item
            for i, element in enumerate(elements):
                item_data = {}
                for field, selector_config in field_selectors.items():
                     value = None
                     try:
                        target_element: Optional[WebElement] = None
                        # Use element.find_element to search within the item context
                        if isinstance(selector_config, str):
                             # Wait briefly for the sub-element? Can slow things down significantly.
                             # Usually better to rely on the initial item wait.
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
                                          # Resolve relative URLs
                                          if attr in ['href', 'src'] and value and not value.startswith(('http://', 'https://', '//', 'data:')):
                                              try: value = urljoin(url, value)
                                              except ValueError: self.logger.warning(f"Could not resolve relative URL '{value}' against base '{url}'")
                                      else: value = target_element.text.strip() # Default to text if no attr
                             else: self.logger.warning(f"Selector dict for field '{field}' missing 'selector'.")
                     except NoSuchElementException:
                          # This is common, just log debug level
                          self.logger.debug(f"Field '{field}' selector '{selector_config}' not found within item {i+1} on {url}.")
                          value = None # Ensure value is None if not found
                     except Exception as e:
                          # Log other errors during field extraction
                          self.logger.error(f"Error extracting field '{field}' in item {i+1}: {e}")
                          value = None
                     item_data[field] = value # Assign even if None

                # Keep item only if at least one field was successfully extracted (is not None)
                if any(v is not None for v in item_data.values()):
                    items.append(item_data)
                else:
                    self.logger.debug(f"Skipping item {i+1} as all fields were None.")

            self.logger.info(f"Extracted {len(items)} non-empty items from {url}.")

        except NoSuchElementException as e:
             # Catch cases where maybe the container was found but items weren't, etc.
             self.logger.error(f"A required element was not found during extraction on {url}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during Selenium extraction on {url}: {e}", exc_info=True)

        return items
    # --- END extract_data ---


    def _perform_login(self) -> bool:
        """Attempts to log in to the website using configured credentials and selectors."""
        if not self.login_config or not self.driver:
            return True # Successful if not configured

        self.logger.info(f"Attempting login via: {self.login_config['login_url']}")
        try:
            self.driver.get(self.login_config['login_url'])
            self._wait_for_page_load(self.login_config['login_url'])
            wait = WebDriverWait(self.driver, 10)
            user_field = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, self.login_config['username_selector'])))
            pass_field = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, self.login_config['password_selector'])))
            submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, self.login_config['submit_selector'])))
            self.logger.debug("Entering login credentials.")
            username = self.login_config.get('username', ''); password = self.login_config.get('password', '')
            if not username or not password: self.logger.error("Username/password missing."); return False
            user_field.clear(); user_field.send_keys(username)
            pass_field.clear(); pass_field.send_keys(password)
            self.logger.debug("Clicking submit button."); submit_button.click()
            wait_after = self.login_config.get('wait_after_login', 3)
            self.logger.debug(f"Waiting {wait_after}s after login submission...")
            time.sleep(wait_after)
            success_selector = self.login_config.get('success_selector')
            success_url_contains = self.login_config.get('success_url_contains')
            login_successful = False
            if success_selector:
                self.logger.debug(f"Checking for success selector: '{success_selector}'")
                try:
                    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, success_selector)))
                    self.logger.info("Login successful (success selector found)."); login_successful = True
                except TimeoutException: self.logger.warning(f"Login check failed: Success selector '{success_selector}' not found.")
            elif success_url_contains:
                 self.logger.debug(f"Checking if current URL contains: '{success_url_contains}'")
                 current_url = self.driver.current_url
                 if success_url_contains in current_url: self.logger.info(f"Login successful (URL contains '{success_url_contains}')."); login_successful = True
                 else: self.logger.warning(f"Login check failed: Current URL '{current_url}' does not contain '{success_url_contains}'.")
            else: self.logger.error("Login check failed: No success method defined."); return False
            return login_successful
        except TimeoutException as e: self.logger.error(f"Login failed: Timed out waiting for elements ({e})"); return False
        except (NoSuchElementException, ElementNotInteractableException) as e: self.logger.error(f"Login failed: Cannot find/interact with elements ({e})"); return False
        except WebDriverException as e: self.logger.error(f"Login failed: WebDriverException ({e})", exc_info=True); return False
        except Exception as e: self.logger.error(f"Login failed: Unexpected error ({e})", exc_info=True); return False

    def run(self) -> Dict:
        """Execute dynamic scraping job, handling login and pagination."""
        self.stats['start_time'] = time.time()
        all_extracted_data = []
        initial_urls = self.config.get('urls', [])
        start_url = initial_urls[0] if initial_urls else None
        driver_initialized = False
        login_required = bool(self.login_config)
        login_successful = not login_required

        try:
            self.driver = self._init_driver()
            if not self.driver: raise WebDriverException("WebDriver initialization failed.")
            driver_initialized = True

            if login_required:
                login_successful = self._perform_login()
                if not login_successful: raise Exception("Login failed, aborting.")

            if login_successful:
                scraped_urls: Set[str] = set()
                max_pages = float('inf')
                if self.pagination_config: max_pages = self.pagination_config.get('max_pages', float('inf'))
                pages_scraped_count = 0
                current_url: Optional[str] = start_url
                if not current_url and login_required: current_url = self.driver.current_url # Use current page if no target URL after login
                elif not current_url: self.logger.warning("No target URLs provided. Stopping.")

                while current_url and pages_scraped_count < max_pages:
                    if current_url in scraped_urls: self.logger.warning(f"URL cycle detected: {current_url}. Stopping."); break
                    self.logger.info(f"Processing target URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")
                    if self.driver.current_url != current_url:
                        self.throttle_requests()
                        try:
                             self.driver.get(current_url)
                             page_source_lower = self.driver.page_source.lower()
                             if "err_proxy_connection_failed" in page_source_lower or \
                                "err_connection_refused" in page_source_lower:
                                 raise WebDriverException(f"Proxy error indicated in page source for {current_url}")
                        except WebDriverException as e: self.logger.error(f"WebDriverException during driver.get({current_url}): {e}"); self.stats['pages_failed'] += 1; raise
                    self._wait_for_page_load(current_url)
                    scraped_urls.add(current_url)
                    self.stats['pages_scraped'] += 1
                    pages_scraped_count += 1
                    page_data = self.extract_data(current_url) # Call the implemented method
                    all_extracted_data.extend(page_data)
                    if pages_scraped_count < max_pages:
                         navigated = self._find_and_click_next_page(current_url)
                         if navigated: current_url = self.driver.current_url
                         else: self.logger.info("No further pages found or pagination stopped."); break
                    else: self.logger.info(f"Reached maximum page limit ({max_pages})."); break
        except WebDriverException as e: self.logger.error(f"WebDriver error during run: {e}", exc_info=True); self.stats['pages_failed'] += 1
        except Exception as e: self.logger.error(f"Unexpected error during run: {e}", exc_info=True); # self.stats['pages_failed'] += 1 # Avoid double counting if WebDriverException was caught first
        finally:
             if driver_initialized: self._close_driver()

        processed_data = self._process_extracted_data(all_extracted_data)
        self.stats['end_time'] = time.time()
        return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}

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
        except TimeoutException: self.logger.warning(f"Timed out waiting for condition/selector '{wait_selector or 'general load'}' on {current_url or self.driver.current_url}.")
        except Exception as e: self.logger.error(f"Error during explicit wait on {current_url or self.driver.current_url}: {e}")

    def _find_and_click_next_page(self, current_url: str) -> bool:
        """Finds the next page element using Selenium, clicks it, and waits."""
        if not self.pagination_config or not self.driver: return False
        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector: self.logger.debug("No 'next_page_selector' defined."); return False
        try:
            wait_time = 5
            next_button = WebDriverWait(self.driver, wait_time).until(EC.element_to_be_clickable((By.CSS_SELECTOR, next_page_selector)))
            if next_button and next_button.is_displayed() and next_button.is_enabled():
                 self.logger.info(f"Found 'Next' page element using '{next_page_selector}'. Clicking..."); next_button.click()
                 try:
                      WebDriverWait(self.driver, 10).until(EC.url_changes(current_url))
                      new_url = self.driver.current_url; self.logger.info(f"Navigated to next page: {new_url}"); self._wait_for_page_load(new_url); return True
                 except TimeoutException: self.logger.warning(f"Clicked 'Next' but URL did not change from {current_url}."); return False
            else: self.logger.info(f"Next page element found but not interactive."); return False
        except TimeoutException: self.logger.info(f"Next page element '{next_page_selector}' not found/clickable (Likely last page)."); return False
        except NoSuchElementException: self.logger.info(f"Next page element '{next_page_selector}' not found (Likely last page)."); return False
        except WebDriverException as e: self.logger.error(f"Error clicking next page '{next_page_selector}': {e}"); return False
        except Exception as e: self.logger.error(f"Unexpected error finding/clicking next page: {e}", exc_info=True); return False

    def _close_driver(self):
         """Safely close the Selenium WebDriver if it's open."""
         if self.driver:
             driver_instance = self.driver; self.driver = None
             try: self.logger.info("Closing WebDriver..."); driver_instance.quit(); self.logger.info("WebDriver closed successfully.")
             except WebDriverException as e: self.logger.error(f"Error closing WebDriver: {e}")
             except Exception as e: self.logger.error(f"Unexpected error quitting WebDriver: {e}")
