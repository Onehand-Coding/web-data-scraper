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
    ElementNotInteractableException, InvalidSelectorException,
    ElementClickInterceptedException # Keep for potential logging
)

from .base_scraper import BaseScraper

# Helper function (Consider moving to utils if used elsewhere)
def _proxy_to_str(proxy_dict: Optional[Dict]) -> str:
    """Helper to get a display string for a proxy dict."""
    if not proxy_dict: return "None"
    url = proxy_dict.get('https', proxy_dict.get('http', 'N/A'))
    return url

class DynamicScraper(BaseScraper):
    """
    Scraper implementation using Selenium WebDriver for dynamic websites.

    Handles websites that require JavaScript execution to render content.
    Supports CSS or XPath selectors, login automation, proxy rotation (via WebDriver options),
    explicit waits for elements, and clicking through pagination links.
    """

    def __init__(self, config: Dict):
        """
        Initializes the DynamicScraper.

        Args:
            config: The validated scraper configuration dictionary. Expects keys like
                    'dynamic' (must be true), 'selectors', optionally 'login_config',
                    'pagination', 'headless', 'wait_for_selector', 'wait_time', etc.
        """
        super().__init__(config)
        self.driver: Optional[WebDriver] = None # Holds the Selenium WebDriver instance
        self.selectors: Dict = config.get('selectors', {})
        self.selector_type: str = self.selectors.get('type', 'css').lower()
        self.pagination_config: Optional[Dict] = config.get('pagination')
        self.login_config: Optional[Dict] = config.get('login_config')
        self.logger.info(f"DynamicScraper initialized (Selector Type: {self.selector_type.upper()})")

    def _init_driver(self) -> Optional[WebDriver]:
        """
        Initializes and returns a Selenium WebDriver instance (Chrome by default).

        Configures the driver with options like headless mode, user agent,
        disabled images, and proxy settings if specified in the config and
        provided by the ProxyRotator.

        Returns:
            A configured WebDriver instance or None if initialization fails.
        """
        if self.driver:
            self.logger.warning("WebDriver already initialized. Returning existing instance.")
            return self.driver

        current_proxy_config: Optional[Dict] = None
        proxy_arg: Optional[str] = None
        proxy_display_str: str = "None"

        # --- Proxy Setup ---
        if self.proxy_rotator:
            current_proxy_config = self.proxy_rotator.rotate() # Get next available proxy
            if current_proxy_config:
                proxy_display_str = _proxy_to_str(current_proxy_config)
                # Selenium uses --proxy-server=host:port format
                # Prefer http proxy URL format if available for webdriver argument
                proxy_url = current_proxy_config.get('http', current_proxy_config.get('https'))
                if proxy_url:
                    try:
                        parsed_proxy = urlparse(proxy_url)
                        # Extract host:port, removing potential user:pass
                        proxy_host_port = parsed_proxy.netloc.split('@')[-1]
                        proxy_arg = f"--proxy-server={proxy_host_port}"
                        self.logger.info(f"Attempting WebDriver init with proxy: {proxy_host_port}")
                    except Exception as parse_err:
                         self.logger.error(f"Failed to parse proxy URL '{proxy_url}': {parse_err}")
                         current_proxy_config = None # Treat as no valid proxy
                else:
                     self.logger.warning("Selected proxy config has no valid http/https URL.")
                     current_proxy_config = None
            else:
                 self.logger.warning("Proxy rotator enabled, but no working proxies are available. Will attempt direct connection.")
        # --- End Proxy Setup ---

        # --- Configure Chrome Options ---
        options = webdriver.ChromeOptions()
        driver_path = self.config.get('webdriver_path') # Optional path to chromedriver executable

        # Headless mode (run browser without GUI)
        if self.config.get('headless', True):
            options.add_argument('--headless=new') # Use the new headless mode

        # Optionally disable images for faster loading
        if self.config.get('disable_images', True):
            options.add_argument('--blink-settings=imagesEnabled=false')

        # Add proxy argument if a valid one was configured
        if proxy_arg:
            options.add_argument(proxy_arg)

        # Common options
        options.add_argument(f'user-agent={self.session.headers["User-Agent"]}') # Use UA from BaseScraper's session
        options.add_argument('--disable-gpu') # Often needed in headless environments
        options.add_argument('--no-sandbox') # Often needed in containerized environments
        options.add_argument('--disable-dev-shm-usage') # Overcomes resource limits in containers
        # Attempt to make detection harder
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        # --- End Configure Chrome Options ---

        # --- Initialize WebDriver ---
        try:
             self.logger.info("Initializing WebDriver...");
             driver: Optional[WebDriver] = None
             if driver_path:
                 # Use specific driver path if provided
                 from selenium.webdriver.chrome.service import Service
                 service = Service(executable_path=driver_path)
                 driver = webdriver.Chrome(service=service, options=options)
             else:
                 # Rely on chromedriver being in PATH or managed by selenium-manager
                 driver = webdriver.Chrome(options=options)

             # Set page load timeout from config
             page_load_timeout = self.config.get('page_load_timeout', 30)
             driver.set_page_load_timeout(page_load_timeout)

             self.logger.info(f"WebDriver initialized (Proxy: {proxy_display_str}).")
             return driver

        except WebDriverException as e:
             # Handle specific WebDriver errors, especially proxy issues
             err_msg = str(e).lower()
             is_proxy_error = "proxy" in err_msg or "connection refused" in err_msg or "net::err" in err_msg
             log_level = logging.WARNING if is_proxy_error else logging.ERROR
             self.logger.log(log_level, f"WebDriver init failed: {e}", exc_info=False) # Don't need full trace for common init errors

             # If it was a proxy error and we used a proxy, mark it as bad
             if self.proxy_rotator and current_proxy_config and is_proxy_error:
                 self.logger.warning(f"Marking proxy {proxy_display_str} as bad due to WebDriver init failure.")
                 self.proxy_rotator.mark_bad(current_proxy_config)
                 self.stats['proxy_failures'] += 1
             elif not is_proxy_error:
                 # If not a proxy error, suggest checking WebDriver installation
                 self.logger.error("Ensure WebDriver (e.g., chromedriver) is installed correctly and accessible in your system PATH or specified in config.")
             return None # Indicate failure
        except Exception as e:
             # Catch any other unexpected errors during initialization
             self.logger.error(f"Unexpected error during WebDriver initialization: {e}", exc_info=True)
             return None
        # --- End Initialize WebDriver ---


    def extract_data(self, url: str) -> List[Dict]:
        """
        Extracts data from the currently loaded page in the WebDriver.

        Uses Selenium's find_elements and the configured selectors (CSS or XPath)
        relative to the item elements found.

        Args:
            url: The URL of the currently loaded page (used for context and logging).

        Returns:
            A list of dictionaries representing the extracted items for the current page.
        """
        items = []
        if not self.driver: self.logger.error("WebDriver not available for extraction."); return []
        if not self.selectors: self.logger.error("Selectors config missing for extraction."); return []

        # Get configured selectors
        container_selector = self.selectors.get('container')
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})
        selector_method = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR

        if not item_selector: self.logger.error("Missing 'item' selector."); return []

        try:
            # Determine the context for searching item elements (whole page or container)
            search_context: WebDriver | WebElement = self.driver
            if container_selector:
                try:
                    # Wait briefly for container presence
                    container_element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((selector_method, container_selector))
                    )
                    search_context = container_element # Search within the container
                    self.logger.debug(f"Using container '{container_selector}' for item search.")
                except Exception as e:
                     # If container not found, log warning and search the whole page
                    self.logger.warning(f"Container selector '{container_selector}' ({self.selector_type}) not found/invalid: {e}. Searching page root instead.")
                    # search_context remains self.driver

            self.logger.debug(f"Looking for item elements using {self.selector_type.upper()}: '{item_selector}'")
            elements: List[WebElement] = []
            try:
                # Wait for at least one item element to be present before finding all
                WebDriverWait(
                    # Use driver as context if search_context is WebDriver, otherwise use search_context (WebElement)
                    self.driver if isinstance(search_context, WebDriver) else search_context,
                    5 # Short wait for items
                ).until(EC.presence_of_all_elements_located((selector_method, item_selector)))
                # Find all item elements within the determined search context
                elements = search_context.find_elements(selector_method, item_selector)
            except TimeoutException:
                 self.logger.warning(f"Timed out waiting for item elements matching '{item_selector}' ({self.selector_type}).")
            except InvalidSelectorException as e:
                 self.logger.error(f"Invalid item selector '{item_selector}' ({self.selector_type}): {e}")
                 return [] # Cannot proceed with invalid selector

            self.logger.debug(f"Found {len(elements)} potential item elements.")
            if not elements:
                return items # Return empty list if no elements found

            # Iterate through found item elements
            for i, element in enumerate(elements):
                item_data = {}
                # Extract each field based on its selector config
                for field, selector_config in field_selectors.items():
                     value: Optional[str] = None
                     current_selector: Optional[str] = None
                     attr: Optional[str] = None # Attribute to extract (e.g., 'href', 'src')

                     # Parse selector config (str or dict)
                     if isinstance(selector_config, str):
                         current_selector = selector_config
                     elif isinstance(selector_config, dict):
                         current_selector = selector_config.get('selector')
                         attr = selector_config.get('attr')

                     if not current_selector:
                         self.logger.warning(f"Selector configuration missing for field '{field}' in item {i+1}. Skipping.")
                         continue

                     # Find the target element *within the current item element*
                     try:
                        # Use find_element (singular) relative to the item 'element' context
                        target_element: Optional[WebElement] = element.find_element(selector_method, current_selector)
                        if target_element:
                             # Get attribute if specified, otherwise get text
                             value = target_element.get_attribute(attr) if attr else target_element.text
                        # else: value remains None if element not found
                     except NoSuchElementException:
                          # Log if specific field selector not found within item
                         self.logger.debug(f"Field '{field}' selector '{current_selector}' ({self.selector_type}) not found in item {i+1}.")
                         value = None
                     except InvalidSelectorException as e:
                          self.logger.error(f"Invalid field selector '{current_selector}' ({self.selector_type}) for field '{field}': {e}")
                          value = None
                     except Exception as e:
                          # Catch other unexpected errors during element finding/text/attribute access
                          self.logger.error(f"Unexpected error extracting field '{field}' ({self.selector_type}) in item {i+1}: {e}")
                          value = None

                     # Post-process value: resolve relative URLs, strip text
                     if attr in ['href', 'src'] and isinstance(value, str) and value:
                        # Check if it looks like a relative URL (doesn't start with common schemes or //)
                        if not value.startswith(('http://','https://','//','data:')):
                             try: value = urljoin(url, value) # Resolve relative URL against the page URL
                             except ValueError: self.logger.warning(f"Could not resolve relative URL '{value}' vs base '{url}'. Keeping original.")
                     elif isinstance(value, str):
                         value = value.strip() # Strip whitespace from text

                     item_data[field] = value

                # Add item only if some data was extracted
                if any(v is not None for v in item_data.values()):
                    items.append(item_data)
                else:
                     # Log if an item element was found but yielded no data
                     self.logger.debug(f"Skipping item {i+1} as all fields evaluated to None.")

            self.logger.info(f"Extracted {len(items)} non-empty items from {url} using {self.selector_type.upper()}.")

        except InvalidSelectorException as e:
             # Error with container or item selector itself
             self.logger.error(f"Invalid container or item selector provided for {url}: {e}")
        except NoSuchElementException as e:
             # Catch errors if a required container/element wasn't found early on
             self.logger.error(f"A required element (container or initial item) was not found on {url}: {e}")
        except Exception as e:
             # Catch broader errors during extraction process
             self.logger.error(f"Unexpected error during dynamic data extraction on {url}: {e}", exc_info=True)

        return items


    def _perform_login(self) -> bool:
        """
        Automates the login process using credentials and selectors from `login_config`.

        Navigates to the login URL, fills username/password, clicks submit (using JS),
        and verifies success based on `success_selector` or `success_url_contains`.

        Returns:
            True if login is successful or not required, False otherwise.
        """
        # Skip if no login configured or driver not available
        if not self.login_config or not self.driver:
            self.logger.debug("Login not required or driver not available.")
            return True # Consider login successful if not configured

        # Ensure required login keys are present (should be caught by schema validation, but good practice)
        required_keys = ['login_url', 'username_selector', 'password_selector', 'submit_selector', 'username', 'password']
        if not all(k in self.login_config for k in required_keys):
             self.logger.error("Login configuration is missing required keys (e.g., selectors, credentials).")
             return False
        if not (self.login_config.get('success_selector') or self.login_config.get('success_url_contains')):
            self.logger.error("Login configuration requires either 'success_selector' or 'success_url_contains' for verification.")
            return False

        self.logger.info(f"Attempting login via: {self.login_config['login_url']}")
        try:
            # 1. Navigate to login page
            self.driver.get(self.login_config['login_url'])
            self._wait_for_page_load(self.login_config['login_url']) # Apply general wait after page load

            # 2. Wait for form elements to be ready (visible and/or clickable)
            wait = WebDriverWait(self.driver, 10) # Explicit wait for elements
            user_selector = self.login_config['username_selector']
            pass_selector = self.login_config['password_selector']
            submit_selector = self.login_config['submit_selector']

            # Wait for visibility for input fields, clickability for button
            user_field = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, user_selector)))
            pass_field = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, pass_selector)))
            submit_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, submit_selector)))

            # 3. Enter Credentials
            self.logger.debug("Entering login credentials.")
            username = self.login_config.get('username', '') # Get credentials from config
            password = self.login_config.get('password', '')
            # Check again if somehow empty, though schema should prevent
            if not username or not password: self.logger.error("Username/password empty in login_config."); return False

            user_field.clear(); user_field.send_keys(username)
            pass_field.clear(); pass_field.send_keys(password)

            # 4. Click Submit using JavaScript (more robust against interception)
            self.logger.debug(f"Clicking submit button ('{submit_selector}') using JavaScript.")
            try:
                 # Scroll into view first to potentially avoid interception issues
                 self.driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
                 time.sleep(0.5) # Brief pause might help before click
                 self.driver.execute_script("arguments[0].click();", submit_button)
            except Exception as js_click_error:
                 self.logger.error(f"JavaScript click failed for submit button ({submit_selector}): {js_click_error}")
                 # Optionally try standard click as fallback or just fail. Raising is safer.
                 # submit_button.click() # Fallback attempt (might still be intercepted)
                 raise # Re-raise the error to be caught by the outer try-except

            # 5. Wait after submission for potential redirects or JS actions
            wait_after = self.login_config.get('wait_after_login', 3)
            self.logger.debug(f"Waiting {wait_after}s after submitting login form...")
            time.sleep(wait_after)

            # 6. Verify Login Success based on configured method
            success_selector = self.login_config.get('success_selector')
            success_url_contains = self.login_config.get('success_url_contains')
            login_successful = False

            if success_selector:
                # Check if an element indicating success is now visible
                self.logger.debug(f"Verifying login success using visibility of selector: '{success_selector}'")
                try:
                    # Use a reasonable wait time for the success element to appear
                    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, success_selector)))
                    self.logger.info("Login successful (success selector found).")
                    login_successful = True
                except TimeoutException:
                    self.logger.warning(f"Login verification failed: Success selector '{success_selector}' not found after timeout.")
            # Check URL only if selector check wasn't definitive or wasn't primary method
            # Note: This logic assumes selector is primary if both are present. Adjust if needed.
            elif success_url_contains:
                 # Check if the current URL contains the expected substring
                 self.logger.debug(f"Verifying login success using URL containing: '{success_url_contains}'")
                 current_url = self.driver.current_url
                 if success_url_contains in current_url:
                     self.logger.info(f"Login successful (URL '{current_url}' contains '{success_url_contains}').")
                     login_successful = True
                 else:
                     self.logger.warning(f"Login verification failed: Current URL '{current_url}' doesn't contain '{success_url_contains}'.")
            # else: This case should be prevented by config validation (anyOf success_selector/url)

            return login_successful

        # --- Exception Handling for Login ---
        except TimeoutException as e:
            self.logger.error(f"Login failed: Timed out waiting for login page elements ({e})")
            return False
        except (NoSuchElementException, ElementNotInteractableException, InvalidSelectorException) as e:
            # Errors finding or interacting with login form elements
            self.logger.error(f"Login failed: Could not find or interact with login elements ({e})")
            return False
        except WebDriverException as e:
            # Catch broader WebDriver errors during login attempt
            self.logger.error(f"Login failed due to WebDriverException: {e}", exc_info=True)
            return False
        except Exception as e:
            # Catch any other unexpected errors during login
            self.logger.error(f"Login failed due to unexpected error: {e}", exc_info=True)
            return False


    def _wait_for_page_load(self, current_url: Optional[str] = None):
        """
        Applies configured waits after a page navigation or action.

        Waits for a specific selector visibility OR a fixed time delay, based on config.

        Args:
            current_url: The URL being waited on (for logging purposes).
        """
        if not self.driver: return # Should not happen if called correctly

        wait_time = float(self.config.get('wait_time', 5.0)) # Ensure float, default 5s
        wait_selector = self.config.get('wait_for_selector')
        selector_method = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR

        try:
            if wait_selector:
                # Wait for a specific element to be visible
                self.logger.debug(f"Waiting up to {wait_time}s for element '{wait_selector}' ({self.selector_type}) to be visible.")
                condition = EC.visibility_of_element_located((selector_method, wait_selector))
                WebDriverWait(self.driver, wait_time).until(condition)
                self.logger.debug(f"Element '{wait_selector}' is visible.")
            elif wait_time > 0:
                # Apply a general wait if no specific selector is given
                self.logger.debug(f"Applying general wait of {wait_time}s...")
                time.sleep(wait_time)
            # else: No wait needed if wait_time is 0 and no selector specified

        except TimeoutException:
             # Log clearly if the wait condition wasn't met
             context_url = current_url or getattr(self.driver, 'current_url', 'unknown URL')
             self.logger.warning(f"Timed out after {wait_time}s waiting for condition '{wait_selector or 'general load'}' on {context_url}.")
        except InvalidSelectorException as e:
             self.logger.error(f"Invalid wait_for_selector '{wait_selector}' ({self.selector_type}): {e}")
        except Exception as e:
             # Catch other potential errors during the wait
             context_url = current_url or getattr(self.driver, 'current_url', 'unknown URL')
             self.logger.error(f"Error during explicit wait on {context_url}: {e}")


    def _find_and_click_next_page(self, current_url: str) -> bool:
        """
        Finds the 'next page' element based on pagination config, checks if it's
        enabled/clickable, and clicks it using JavaScript. Waits for URL change.

        Args:
            current_url: The URL of the page *before* clicking next. Used to detect navigation.

        Returns:
            True if the next page was successfully clicked and navigation occurred (URL changed),
            False otherwise (e.g., button not found, disabled, click failed, URL didn't change).
        """
        if not self.pagination_config or not self.driver:
            self.logger.debug("Pagination not configured or driver unavailable.")
            return False

        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector:
            self.logger.debug("No 'next_page_selector' defined in pagination config.")
            return False

        selector_method = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR
        self.logger.debug(f"Looking for next page element using {self.selector_type.upper()}: '{next_page_selector}'")

        try:
            # 1. Wait briefly for the element just to be present in the DOM
            wait_time_presence = 5 # seconds
            next_button = WebDriverWait(self.driver, wait_time_presence).until(
                EC.presence_of_element_located((selector_method, next_page_selector))
            )

            # 2. Check if the button is disabled (common patterns)
            # This check might need site-specific adjustments
            is_disabled = False
            try:
                if next_button.get_attribute('disabled') or \
                   'disabled' in next_button.get_attribute('class').lower() or \
                   'inactive' in next_button.get_attribute('class').lower(): # Add other common disabled class names
                    is_disabled = True
            except Exception:
                # Ignore errors just checking disabled state, assume it might be clickable
                self.logger.debug("Could not reliably determine if next button is disabled, proceeding with click attempt.")
                pass

            if is_disabled:
                self.logger.info(f"Next page element '{next_page_selector}' found but appears disabled. Assuming last page.")
                return False # Treat disabled as the end of pagination

            # 3. If not disabled, wait for it to be clickable
            wait_time_clickable = 5 # seconds
            clickable_button = WebDriverWait(self.driver, wait_time_clickable).until(
                EC.element_to_be_clickable((selector_method, next_page_selector))
            )

            # 4. Scroll into view and Click using JavaScript
            self.logger.info(f"Found clickable 'Next' page element ({self.selector_type}). Clicking...")
            try:
                 # Scroll element into view just before clicking
                 self.driver.execute_script("arguments[0].scrollIntoView(true);", clickable_button)
                 time.sleep(0.5) # Small pause sometimes helps ensure element is ready after scroll
                 # Use JS click which is often more robust
                 self.driver.execute_script("arguments[0].click();", clickable_button)
            except Exception as click_error:
                 # Catch errors during the JS click itself
                 self.logger.error(f"Failed to click next page button '{next_page_selector}' using JS: {click_error}")
                 return False # Fail if click action errors out

            # 5. Verify navigation by checking for URL change
            try:
                 # Wait up to 10 seconds for the URL to be different from the original
                 WebDriverWait(self.driver, 10).until(EC.url_changes(current_url))
                 new_url = self.driver.current_url
                 self.logger.info(f"Navigated to next page: {new_url}")
                 # Apply standard page load waits for the new page
                 self._wait_for_page_load(new_url)
                 return True # Navigation confirmed
            except TimeoutException:
                 # URL didn't change after click + wait
                 self.logger.warning(f"URL did not change from {current_url} after clicking next page selector '{next_page_selector}'. Stopping pagination (might be end, or JS load without URL change).")
                 # This might indicate the end, or it might be a site that loads content
                 # without changing URL (infinite scroll, etc.), which needs different handling.
                 return False

        except TimeoutException:
            # Element wasn't found present or clickable within the initial waits
            self.logger.info(f"Next page element '{next_page_selector}' ({self.selector_type}) not found or not clickable within timeout.")
            return False
        except (NoSuchElementException, InvalidSelectorException) as e:
            # Selector was invalid or element disappeared after initial presence check
            self.logger.error(f"Next page selector '{next_page_selector}' ({self.selector_type}) is invalid or element not found during click attempt: {e}")
            return False
        except WebDriverException as e:
             # Broader Selenium errors during the process
            self.logger.error(f"WebDriver error attempting to find/click next page '{next_page_selector}': {e}")
            return False
        except Exception as e:
             # Catch-all for other unexpected errors
            self.logger.error(f"Unexpected error finding/clicking next page: {e}", exc_info=True)
            return False


    def run(self) -> Dict:
        """
        Executes the dynamic scraping job.

        Initializes WebDriver, handles optional login, iterates through URLs/pages,
        extracts data, handles pagination, processes data, and ensures WebDriver cleanup.

        Returns:
            A dictionary containing the processed data ('data'), run statistics ('stats'),
            and the original configuration ('config').
        """
        self.stats['start_time'] = time.time() # Record job start time
        all_extracted_data: List[Dict] = []
        initial_urls = self.config.get('urls', [])
        start_url = initial_urls[0] if initial_urls else None # Use first URL as starting point
        driver_initialized = False
        login_required = bool(self.login_config)
        login_successful = not login_required # Assume success if login isn't required

        try:
            # --- Initialize Driver ---
            self.driver = self._init_driver()
            if not self.driver:
                # Error logged in _init_driver
                raise WebDriverException("WebDriver initialization failed. Cannot proceed.")
            driver_initialized = True # Mark driver as successfully initialized

            # --- Perform Login (if configured) ---
            if login_required:
                login_successful = self._perform_login() # Calls updated method
                if not login_successful:
                    # Error logged in _perform_login
                    raise Exception("Login failed, aborting scraping run.")
            # Proceed only if login successful or not required
            if not login_successful:
                self.logger.error("Login failed or was required but not performed successfully. Aborting run.")
                # Skip scraping loop if login failed
            else:
                # --- Main Scraping Loop ---
                scraped_urls: Set[str] = set() # Track visited URLs to prevent loops
                max_pages = float('inf') # Default to no page limit
                if self.pagination_config:
                    max_pages = self.pagination_config.get('max_pages', float('inf'))

                pages_scraped_count = 0
                current_url: Optional[str] = start_url

                # Determine starting point: Use first URL or URL after login redirect
                if not current_url and login_required:
                    current_url = self.driver.current_url # Start from where login landed us
                    self.logger.info(f"No start URL provided, starting scrape from page after login: {current_url}")
                elif not current_url:
                    self.logger.error("No target URLs provided and login not configured. Cannot start scraping.")
                    # Exit loop gracefully if no starting point

                while current_url and pages_scraped_count < max_pages:
                    # Prevent re-scraping the same URL in this run
                    if current_url in scraped_urls:
                        self.logger.warning(f"URL cycle detected or URL already scraped: {current_url}. Stopping pagination.")
                        break

                    self.logger.info(f"Processing target URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")

                    # Navigate if not already on the target URL
                    if self.driver.current_url != current_url:
                        self.throttle_requests() # Apply delay before navigation
                        try:
                             self.logger.debug(f"Navigating WebDriver to: {current_url}")
                             self.driver.get(current_url)
                             # Basic check for obvious proxy errors immediately after get
                             # This might not catch all proxy issues but can be a quick indicator
                             page_source_lower = self.driver.page_source.lower()
                             if "err_proxy_connection_failed" in page_source_lower or \
                                "err_connection_refused" in page_source_lower:
                                  raise WebDriverException(f"Proxy error detected in page source after loading: {current_url}")
                        except WebDriverException as e:
                             self.logger.error(f"WebDriverException during driver.get({current_url}): {e}")
                             self.stats['pages_failed'] += 1
                             # Decide if this is fatal. Re-raising stops the whole run.
                             # Could potentially try next proxy or skip URL. For now, raise.
                             raise

                    # Wait for page elements/time based on config, then extract data
                    self._wait_for_page_load(current_url)
                    scraped_urls.add(current_url) # Mark as visited
                    self.stats['pages_scraped'] += 1
                    pages_scraped_count += 1

                    page_data = self.extract_data(current_url) # Extract from current page
                    all_extracted_data.extend(page_data)

                    # Handle pagination if we haven't hit the max page limit
                    if pages_scraped_count < max_pages:
                         navigated = self._find_and_click_next_page(current_url) # Try to click next
                         if navigated:
                             current_url = self.driver.current_url # Update URL for the next loop
                         else:
                             # Stop if pagination fails or no next page found
                             self.logger.info("No next page found or navigation failed/stopped. Ending pagination.")
                             break # Exit the while loop for URLs
                    else:
                         # Stop if max pages reached
                         self.logger.info(f"Reached maximum page limit ({max_pages}). Stopping pagination.")
                         break # Exit the while loop

        # --- Error Handling for the overall run ---
        except WebDriverException as e:
             # Catch WebDriver errors that might occur outside fetch/login/paginate
             self.logger.error(f"WebDriver error during scraping run: {e}", exc_info=False) # Log less detail
             if self.stats['pages_scraped'] > 0: # Only count as failed if we started scraping
                 self.stats['pages_failed'] += 1 # Increment failed count if error occurs mid-run
        except Exception as e:
             # Catch other unexpected errors during the run
             self.logger.error(f"Unexpected error during scraping run: {e}", exc_info=True)
        # --- Cleanup ---
        finally:
             # Ensure WebDriver is closed *if* it was successfully initialized
             if driver_initialized:
                 self._close_driver()

        # --- Process all aggregated data and return results ---
        processed_data = self._process_extracted_data(all_extracted_data)
        self.stats['end_time'] = time.time()
        return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}


    def _close_driver(self):
         """Safely closes the WebDriver instance if it exists."""
         if self.driver:
             driver_instance = self.driver
             self.driver = None # Prevent accidental reuse after closing
             try:
                 self.logger.info("Closing WebDriver instance...")
                 driver_instance.quit() # Close browser window(s) and end driver process
                 self.logger.info("WebDriver closed successfully.")
             except WebDriverException as e:
                  self.logger.error(f"Error closing WebDriver session: {e}")
             except Exception as e:
                  # Catch other potential errors during quit
                  self.logger.error(f"Unexpected error quitting WebDriver: {e}")
         else:
              self.logger.debug("WebDriver close called, but no active driver instance.")
