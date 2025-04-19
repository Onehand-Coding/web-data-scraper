# File: web-data-scraper/scraper/dynamic_scraper.py

"""
Selenium-based scraper for JavaScript-rendered content.
"""

from typing import Dict, List, Optional, Any, Set # Added Set
from urllib.parse import urljoin, urlparse # Added urlparse, urljoin
import time

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver # For type hinting
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException

from .base_scraper import BaseScraper

class DynamicScraper(BaseScraper):
    """Scraper implementation using Selenium for dynamic content."""

    def __init__(self, config: Dict):
        super().__init__(config)
        # Driver is initialized in run() now
        self.driver: Optional[WebDriver] = None
        self.selectors = config.get('selectors', {})
        self.pagination_config = config.get('pagination') # Store pagination config

    def _init_driver(self) -> Optional[WebDriver]:
        """Initialize Selenium WebDriver with options."""
        # Prevent re-initialization if called again while driver exists
        if self.driver:
             self.logger.warning("WebDriver already initialized.")
             return self.driver

        options = webdriver.ChromeOptions()
        driver_path = self.config.get('webdriver_path')

        if self.config.get('headless', True):
            # Use the recommended argument for modern Chrome
            options.add_argument('--headless=new')
        else:
             # Optional: Add arguments for non-headless if needed, e.g., start maximized
             # options.add_argument("--start-maximized")
             pass

        if self.config.get('disable_images', True):
            options.add_argument('--blink-settings=imagesEnabled=false')

        # Use User Agent from config/session
        options.add_argument(f'user-agent={self.session.headers["User-Agent"]}')
        options.add_argument('--disable-gpu') # Necessary for headless in many environments
        options.add_argument('--no-sandbox') # Bypass OS security model, REQUIRED for Docker/root
        options.add_argument('--disable-dev-shm-usage') # Overcome limited resource problems

        # Add experimental options to potentially reduce detection
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        # options.add_argument('--disable-blink-features=AutomationControlled') # May cause issues

        try:
             self.logger.info("Initializing WebDriver...")
             # Consider adding support for other drivers (Firefox) based on config
             if driver_path:
                  from selenium.webdriver.chrome.service import Service
                  service = Service(executable_path=driver_path)
                  driver = webdriver.Chrome(service=service, options=options)
             else:
                  driver = webdriver.Chrome(options=options)

             # Set timeouts
             page_load_timeout = self.config.get('page_load_timeout', 30)
             driver.set_page_load_timeout(page_load_timeout)
             # Implicit wait (generally discouraged, but can be simpler) - Use explicit waits instead
             # driver.implicitly_wait(self.config.get('implicit_wait', 0))

             self.logger.info("WebDriver initialized successfully.")
             return driver
        except WebDriverException as e:
             self.logger.error(f"Failed to initialize WebDriver: {e}", exc_info=True)
             self.logger.error("Ensure WebDriver (e.g., chromedriver) is installed and accessible in your PATH or specify 'webdriver_path' in config.")
             # Don't raise here, let run() handle the None driver
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
                # Use presence or visibility? Visibility is stricter.
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


    # --- Modified extract_data to accept url ---
    def extract_data(self, url: str) -> List[Dict]:
        """
        Extract data using Selenium's find elements from the current driver state.
        Args:
            url: The current URL being processed (for context, like resolving relative URLs).
        """
        items = []
        if not self.driver:
            self.logger.error("WebDriver not available for extraction.")
            return []

        # Selectors from config
        container_selector = self.selectors.get('container') # Optional container
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})

        if not item_selector:
             self.logger.error("Missing 'item' selector in configuration.")
             return []

        try:
            # Find elements - relative to container if specified, otherwise from driver root
            search_context: WebDriver | WebElement = self.driver
            if container_selector:
                try:
                    # Wait briefly for container?
                    container_element = WebDriverWait(self.driver, 3).until(
                        EC.visibility_of_element_located((By.CSS_SELECTOR, container_selector))
                    )
                    # container_element = self.driver.find_element(By.CSS_SELECTOR, container_selector)
                    search_context = container_element # Search within container
                    self.logger.debug(f"Using container '{container_selector}' as search context.")
                except (NoSuchElementException, TimeoutException):
                     self.logger.warning(f"Container selector '{container_selector}' not found or visible on page {url}. Searching from page root.")
                     # Fallback to searching from root if container not found

            self.logger.debug(f"Looking for items using selector: '{item_selector}'")
            elements = search_context.find_elements(By.CSS_SELECTOR, item_selector)
            self.logger.debug(f"Found {len(elements)} potential items using selector '{item_selector}'.")

            if not elements:
                 self.logger.warning(f"No items found using item selector '{item_selector}' on page {url}.")
                 return items

            # --- Extract fields for each item ---
            for i, element in enumerate(elements):
                item_data = {}
                for field, selector_config in field_selectors.items():
                     value = None
                     try:
                        target_element: Optional[WebElement] = None
                        # Find within the current item 'element' context
                        if isinstance(selector_config, str): # Simple CSS selector for text
                             target_element = element.find_element(By.CSS_SELECTOR, selector_config)
                             # Use .text for visible text, get_attribute('textContent') for full text
                             value = target_element.text.strip() if target_element else None
                        elif isinstance(selector_config, dict): # CSS selector with attribute
                             sel = selector_config.get('selector')
                             attr = selector_config.get('attr')
                             if sel:
                                 target_element = element.find_element(By.CSS_SELECTOR, sel)
                                 if target_element:
                                      if attr:
                                          value = target_element.get_attribute(attr)
                                          # Resolve relative URLs for common attributes
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
             # This might catch the container search context failure if it wasn't handled above
             self.logger.error(f"A required element was not found during extraction on {url}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during Selenium extraction on {url}: {e}", exc_info=True)

        return items


    def _find_and_click_next_page(self, current_url: str) -> bool:
        """
        Finds the next page element using Selenium, clicks it, and waits.
        Returns True if navigation to a new page was successful, False otherwise.
        """
        if not self.pagination_config or not self.driver:
            return False

        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector:
            self.logger.debug("No 'next_page_selector' defined.")
            return False

        try:
            # Wait briefly for the next element to be present and clickable
            wait_time = 5 # Short wait specifically for the next button
            next_button = WebDriverWait(self.driver, wait_time).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, next_page_selector))
            )
            # next_button = self.driver.find_element(By.CSS_SELECTOR, next_page_selector)

            # Check if it's visible and enabled (clickable might be enough)
            if next_button and next_button.is_displayed() and next_button.is_enabled():
                 self.logger.info(f"Found 'Next' page element using '{next_page_selector}'. Clicking...")
                 # Optional: Scroll element into view before clicking
                 # self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                 # time.sleep(0.5) # Brief pause after scroll

                 next_button.click()

                 # Wait for URL to change or for a specific element on the new page?
                 # Waiting for URL change is generally safer than just sleeping
                 try:
                      WebDriverWait(self.driver, 10).until(EC.url_changes(current_url))
                      new_url = self.driver.current_url
                      self.logger.info(f"Successfully navigated to next page: {new_url}")
                      # Apply standard waits for the new page content
                      self._wait_for_page_load(new_url)
                      return True
                 except TimeoutException:
                      self.logger.warning(f"Clicked 'Next' but URL did not change from {current_url} within timeout.")
                      # Check if clicking triggered JS but didn't change URL (less common)
                      # Could check for staleness of old elements or presence of new ones
                      return False

            else:
                self.logger.info(f"Next page element '{next_page_selector}' found but not clickable/visible/enabled.")
                return False

        except TimeoutException:
            self.logger.info(f"Next page element '{next_page_selector}' not found or clickable within {wait_time}s (Likely last page).")
            return False
        except NoSuchElementException:
            # Should be caught by TimeoutException with explicit wait, but good failsafe
            self.logger.info(f"Next page element '{next_page_selector}' not found (Likely last page).")
            return False
        except WebDriverException as e:
             # Handle exceptions like element click intercepted, etc.
             self.logger.error(f"Error clicking next page element '{next_page_selector}': {e}")
             return False
        except Exception as e:
             self.logger.error(f"Unexpected error finding/clicking next page '{next_page_selector}': {e}", exc_info=True)
             return False


    def run(self) -> Dict:
        """Execute dynamic scraping job, handling pagination by clicking."""
        super().run() # Sets start time
        all_extracted_data = []
        # Start only with the first URL for pagination, others are ignored if pagination enabled
        initial_urls = self.config.get('urls', [])
        start_url = initial_urls[0] if initial_urls else None

        scraped_urls: Set[str] = set() # Keep track of visited URLs

        max_pages = self.pagination_config.get('max_pages', float('inf')) if self.pagination_config else 1
        pages_scraped_count = 0

        if not start_url:
             self.logger.warning("No initial URL provided in configuration.")
             self._close_driver() # Ensure driver is closed if opened
             self.stats['end_time'] = time.time()
             return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        # --- Main Loop ---
        current_url: Optional[str] = start_url
        try:
            # Initialize driver once before starting
            self.driver = self._init_driver()
            if not self.driver:
                  # Error already logged in _init_driver
                  raise WebDriverException("WebDriver initialization failed.")

            while current_url and pages_scraped_count < max_pages:
                 if current_url in scraped_urls:
                     self.logger.warning(f"URL cycle detected or already scraped: {current_url}. Stopping pagination.")
                     break # Avoid infinite loops

                 self.logger.info(f"Processing URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")

                 # Navigate only if not already on the page (first iteration)
                 if self.driver.current_url != current_url:
                      self.throttle_requests() # Throttle before GET
                      if not self.check_robots_permission(current_url):
                          break # Stop if robots disallowed for this domain
                      self.driver.get(current_url)

                 # Wait for page elements based on config
                 self._wait_for_page_load(current_url)

                 scraped_urls.add(current_url) # Mark as visited after load
                 self.stats['pages_scraped'] += 1 # Count successful loads
                 pages_scraped_count += 1

                 # Extract data from the current page state
                 page_data = self.extract_data(current_url) # Pass URL for context
                 all_extracted_data.extend(page_data)

                 # --- Handle Pagination ---
                 if pages_scraped_count < max_pages:
                      navigated = self._find_and_click_next_page(current_url)
                      if navigated:
                          current_url = self.driver.current_url # Update current URL for next loop/tracking
                      else:
                           self.logger.info("No further pages found or pagination stopped.")
                           break # Exit loop if navigation failed or button not found
                 else:
                      self.logger.info(f"Reached maximum page limit ({max_pages}).")
                      break # Exit loop if max pages reached

        except WebDriverException as e:
             # Log WebDriver specific errors during the run
             self.logger.error(f"WebDriver error during scraping run: {e}", exc_info=True)
             self.stats['pages_failed'] += 1 # Count failure
        except Exception as e:
             # Log other unexpected errors
             self.logger.error(f"Unexpected error during dynamic scraping run: {e}", exc_info=True)
             self.stats['pages_failed'] += 1
        finally:
             self._close_driver() # IMPORTANT: Ensure driver is closed

        # --- Process all collected data at the end ---
        processed_data = self._process_extracted_data(all_extracted_data)

        self.stats['end_time'] = time.time() # Set end time
        return {
            'data': processed_data,
            'stats': self.get_stats(),
            'config': self.config
        }

    def _close_driver(self):
         """Safely close the Selenium WebDriver if it's open."""
         if self.driver:
             driver_instance = self.driver
             self.driver = None # Set to None early to prevent race conditions?
             try:
                 self.logger.info("Closing WebDriver...")
                 driver_instance.quit()
                 self.logger.info("WebDriver closed successfully.")
             except WebDriverException as e:
                 self.logger.error(f"Error closing WebDriver: {e}")
             except Exception as e:
                  self.logger.error(f"Unexpected error quitting WebDriver: {e}")
