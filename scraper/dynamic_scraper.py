# File: web-data-scraper/scraper/dynamic_scraper.py

"""
Selenium-based scraper for JavaScript-rendered content.
"""

from typing import Dict, List, Optional, Any
from urllib.parse import urljoin
import time # Keep time for sleeps

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException

from .base_scraper import BaseScraper

class DynamicScraper(BaseScraper):
    """Scraper implementation using Selenium for dynamic content."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.driver = None # Initialize driver to None
        self.selectors = config.get('selectors', {}) # Ensure selectors exist

    def _init_driver(self):
        """Initialize Selenium WebDriver with options."""
        if self.driver: # Prevent re-initialization if already running
             return self.driver

        options = webdriver.ChromeOptions()
        driver_path = self.config.get('webdriver_path') # Optional path to chromedriver

        # Add configurable options
        if self.config.get('headless', True):
            options.add_argument('--headless=new') # Updated headless argument
        if self.config.get('disable_images', True):
            options.add_argument('--blink-settings=imagesEnabled=false')

        # Use User Agent from config/session
        options.add_argument(f'user-agent={self.session.headers["User-Agent"]}')
        options.add_argument('--disable-gpu') # Often needed for headless
        options.add_argument('--no-sandbox') # Often needed in containerized environments
        options.add_argument('--disable-dev-shm-usage') # Overcome limited resource problems

        # Add proxy support later if needed

        try:
             self.logger.info("Initializing WebDriver...")
             # Consider adding support for other drivers (Firefox) based on config
             if driver_path:
                  from selenium.webdriver.chrome.service import Service
                  service = Service(executable_path=driver_path)
                  driver = webdriver.Chrome(service=service, options=options)
             else:
                  # Assumes chromedriver is in PATH or managed by webdriver-manager
                  driver = webdriver.Chrome(options=options)

             driver.set_page_load_timeout(self.config.get('page_load_timeout', 30))
             self.logger.info("WebDriver initialized successfully.")
             return driver
        except WebDriverException as e:
             self.logger.error(f"Failed to initialize WebDriver: {e}")
             self.logger.error("Ensure WebDriver (e.g., chromedriver) is installed and accessible in your PATH or specify 'webdriver_path' in config.")
             raise # Re-raise the exception to stop execution

    def fetch_page(self, url: str, max_retries: Optional[int] = None) -> Optional[str]:
        """Fetch page content using Selenium, return page source."""
        if not self.check_robots_permission(url):
            return None

        # Throttle *before* potentially lengthy browser operations
        self.throttle_requests()

        if not self.driver:
             self.logger.error("WebDriver not initialized. Cannot fetch page.")
             return None

        retries = max_retries if max_retries is not None else self.config.get('max_retries', 3)
        self.logger.info(f"Fetching dynamic URL: {url} (Retries left: {retries})")

        for attempt in range(retries + 1):
            try:
                self.driver.get(url)

                # Configurable wait condition
                wait_time = self.config.get('wait_time', 5) # General wait
                wait_selector = self.config.get('wait_for_selector') # Specific element wait

                if wait_selector:
                    self.logger.debug(f"Waiting up to {wait_time}s for selector '{wait_selector}'...")
                    WebDriverWait(self.driver, wait_time).until(
                        # Use CSS selector as default, consider adding XPath support here too
                        EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
                    )
                    self.logger.debug(f"Element '{wait_selector}' found.")
                elif wait_time > 0:
                     self.logger.debug(f"Waiting {wait_time}s for dynamic content...")
                     time.sleep(wait_time) # General wait if no specific selector

                self.stats['pages_scraped'] += 1
                page_source = self.driver.page_source
                self.logger.debug(f"Successfully fetched dynamic content from {url} (Source length: {len(page_source)})")
                return page_source # Return the HTML source for extraction
            except TimeoutException:
                 self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} timed out waiting for element/page load at {url}.")
                 # No automatic page source return on timeout, retry might help
            except WebDriverException as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retries + 1} failed for {url} with WebDriverException: {e}")
                # Decide if retry is useful based on exception type
                # If driver crashes, maybe stop?

            if attempt < retries:
                backoff_time = 2 ** attempt
                self.logger.info(f"Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
                # Optional: Refresh driver or clear cookies before retry?
                # self.driver.refresh()
            else:
                 self.logger.error(f"All {retries + 1} attempts failed for {url}.")
                 self.stats['pages_failed'] += 1
                 return None
        return None


    def extract_data(self, html: str, url: str) -> List[Dict]:
        """
        Extract data using Selenium's find elements (after page load).
        The 'html' arg is ignored here as we use the live driver state.
        """
        items = []
        if not self.driver:
            self.logger.error("WebDriver not available for extraction.")
            return []

        container_selector = self.selectors.get('container')
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})

        if not container_selector or not item_selector:
             self.logger.error("Missing 'container' or 'item' selector in configuration.")
             return []

        try:
            self.logger.debug(f"Looking for container: '{container_selector}'")
            # Use WebDriverWait for container? Could be useful if container itself loads late.
            container = self.driver.find_element(By.CSS_SELECTOR, container_selector)
            self.logger.debug(f"Found container. Looking for items: '{item_selector}'")
            elements = container.find_elements(By.CSS_SELECTOR, item_selector) # Find items within container
            self.logger.debug(f"Found {len(elements)} potential items using selector '{item_selector}'.")


            if not elements:
                 self.logger.warning(f"No items found using item selector '{item_selector}' within container '{container_selector}' on page {url}.")
                 return items


            for element in elements:
                item_data = {}
                for field, selector_config in field_selectors.items():
                     value = None
                     try:
                        target_element = None
                        if isinstance(selector_config, str): # Simple CSS selector for text
                             target_element = element.find_element(By.CSS_SELECTOR, selector_config)
                             value = target_element.text.strip() if target_element else None
                        elif isinstance(selector_config, dict): # CSS selector with attribute
                             sel = selector_config.get('selector')
                             attr = selector_config.get('attr')
                             if sel:
                                 target_element = element.find_element(By.CSS_SELECTOR, sel)
                                 if target_element:
                                      if attr:
                                          value = target_element.get_attribute(attr)
                                          # Resolve relative URLs
                                          if attr in ['href', 'src'] and value and not value.startswith(('http://', 'https://', '//')):
                                              value = urljoin(url, value)
                                      else:
                                          value = target_element.text.strip() # Default to text if no attr
                             else:
                                  self.logger.warning(f"Selector dictionary for field '{field}' is missing 'selector' key.")

                        # Add XPath support if needed using By.XPATH

                     except NoSuchElementException:
                          self.logger.debug(f"Field '{field}' selector '{selector_config}' not found within item on {url}.")
                          value = None # Explicitly set to None if not found
                     except Exception as e:
                          self.logger.error(f"Error extracting field '{field}' with selector '{selector_config}': {e}")
                          value = None

                     item_data[field] = value

                if any(item_data.values()):
                     items.append(item_data)
                else:
                     self.logger.debug("Skipping empty item.")


            self.logger.info(f"Extracted {len(items)} items from {url}.")

        except NoSuchElementException:
             self.logger.error(f"Container selector '{container_selector}' not found on page {url} using Selenium.")
        except Exception as e:
            self.logger.error(f"Extraction failed on {url} using Selenium: {e}")

        return items

    def run(self) -> Dict:
        """Execute scraping job across all URLs using Selenium."""
        super().run() # Sets start time
        all_extracted_data = []
        urls = self.config.get('urls', [])

        if not urls:
             self.logger.warning("No URLs provided in configuration.")
             self._close_driver() # Close driver if opened
             return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        try:
             # Initialize driver once before processing URLs
             self.driver = self._init_driver()
             if not self.driver:
                  raise WebDriverException("Failed to initialize WebDriver.")

             for url in urls:
                 self.logger.info(f"Processing dynamic URL: {url}")
                 # Fetch page source (optional, could extract directly)
                 # page_source = self.fetch_page(url) # Fetches content
                 # if page_source:
                 #     page_data = self.extract_data(page_source, url) # Extracts data using driver state
                 #     all_extracted_data.extend(page_data)

                 # Alternative: Fetch and extract in one go to use live driver state
                 if self.check_robots_permission(url): # Check robots before potentially long load
                      self.throttle_requests() # Throttle before get
                      if self.driver.current_url != url: # Avoid re-getting if already on page
                          self.driver.get(url)
                      # Add wait conditions similar to fetch_page
                      wait_time = self.config.get('wait_time', 5)
                      wait_selector = self.config.get('wait_for_selector')
                      if wait_selector:
                           WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector)))
                      elif wait_time > 0:
                           time.sleep(wait_time)

                      # Now extract directly from the live page state
                      page_data = self.extract_data("", url) # Pass empty string, url for context
                      all_extracted_data.extend(page_data)
                      self.stats['pages_scraped'] += 1 # Increment here after successful load/extract attempt
                 else:
                      # If robots disallowed, we already logged and updated stats['robots_skipped']
                      pass


        except WebDriverException as e:
             self.logger.error(f"WebDriver error during run: {e}")
             # Stats might be incomplete
        except Exception as e:
             self.logger.error(f"Unexpected error during dynamic scraping run: {e}")
        finally:
             self._close_driver() # Ensure driver is closed

        # --- Process all collected data at the end ---
        processed_data = self._process_extracted_data(all_extracted_data)

        self.stats['end_time'] = time.time() # Set end time
        return {
            'data': processed_data, # Return processed data
            'stats': self.get_stats(),
            'config': self.config
        }

    def _close_driver(self):
         """Close the Selenium WebDriver if it's open."""
         if self.driver:
             try:
                 self.logger.info("Closing WebDriver...")
                 self.driver.quit()
                 self.driver = None
                 self.logger.info("WebDriver closed.")
             except WebDriverException as e:
                 self.logger.error(f"Error closing WebDriver: {e}")
