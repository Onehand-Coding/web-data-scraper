# File: scraper/dynamic_scraper.py
# - Corrected extract_data to expect XPath selectors to point to elements.
# - Selenium's .text or .get_attribute() will be used on found WebElements.

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
    ElementClickInterceptedException
)

from .base_scraper import BaseScraper

def _proxy_to_str(proxy_dict: Optional[Dict]) -> str:
    if not proxy_dict: return "None"
    url = proxy_dict.get('https', proxy_dict.get('http', 'N/A'))
    return url

class DynamicScraper(BaseScraper):
    def __init__(self, config: Dict):
        super().__init__(config)
        self.driver: Optional[WebDriver] = None
        self.selectors: Dict = config.get('selectors', {})
        self.selector_type: str = self.selectors.get('type', 'css').lower()
        self.pagination_config: Optional[Dict] = config.get('pagination')
        self.login_config: Optional[Dict] = config.get('login_config')
        self.logger.info(f"DynamicScraper initialized (Selector Type: {self.selector_type.upper()})")

    def _init_driver(self) -> Optional[WebDriver]:
        if self.driver:
            self.logger.warning("WebDriver already initialized. Returning existing instance.")
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
                    try:
                        parsed_proxy = urlparse(proxy_url)
                        proxy_host_port = parsed_proxy.netloc.split('@')[-1]
                        proxy_arg = f"--proxy-server={proxy_host_port}"
                        self.logger.info(f"Attempting WebDriver init with proxy: {proxy_host_port}")
                    except Exception as parse_err:
                         self.logger.error(f"Failed to parse proxy URL '{proxy_url}': {parse_err}")
                         current_proxy_config = None
                else:
                     self.logger.warning("Selected proxy config has no valid http/https URL.")
                     current_proxy_config = None
            else:
                 self.logger.warning("Proxy rotator enabled, but no working proxies are available. Will attempt direct connection.")

        options = webdriver.ChromeOptions()
        driver_path = self.config.get('webdriver_path')

        if self.config.get('headless', True): options.add_argument('--headless=new')
        if self.config.get('disable_images', True): options.add_argument('--blink-settings=imagesEnabled=false')
        if proxy_arg: options.add_argument(proxy_arg)

        options.add_argument(f'user-agent={self.session.headers["User-Agent"]}')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        try:
             self.logger.info("Initializing WebDriver...");
             driver: Optional[WebDriver] = None
             if driver_path and os.path.exists(driver_path):
                 from selenium.webdriver.chrome.service import Service
                 service = Service(executable_path=driver_path)
                 driver = webdriver.Chrome(service=service, options=options)
                 self.logger.info(f"Using WebDriver from specified path: {driver_path}")
             else:
                 if driver_path:
                     self.logger.warning(f"WebDriver path '{driver_path}' not found. Attempting to use WebDriver from system PATH.")
                 driver = webdriver.Chrome(options=options)

             page_load_timeout = self.config.get('page_load_timeout', 30)
             driver.set_page_load_timeout(page_load_timeout)
             self.logger.info(f"WebDriver initialized (Proxy: {proxy_display_str}).")
             return driver
        except WebDriverException as e:
             err_msg = str(e).lower()
             is_proxy_error = "proxy" in err_msg or "connection refused" in err_msg or "net::err" in err_msg
             log_level_to_use = logging.WARNING if is_proxy_error else logging.ERROR
             self.logger.log(log_level_to_use, f"WebDriver init failed: {e}", exc_info=False)
             if self.proxy_rotator and current_proxy_config and is_proxy_error:
                 self.logger.warning(f"Marking proxy {proxy_display_str} as bad due to WebDriver init failure.")
                 self.proxy_rotator.mark_bad(current_proxy_config)
                 self.stats['proxy_failures'] += 1
             elif not is_proxy_error:
                 self.logger.error("Ensure WebDriver (e.g., chromedriver) is installed correctly and accessible in your system PATH or specified via 'webdriver_path' in config.")
             return None
        except Exception as e:
             self.logger.error(f"Unexpected error during WebDriver initialization: {e}", exc_info=True)
             return None

    def _perform_login(self) -> bool:
        if not self.login_config or not self.driver:
            self.logger.debug("Login not required or driver not available.")
            return True

        required_keys = ['login_url', 'username_selector', 'password_selector', 'submit_selector', 'username', 'password']
        if not all(self.login_config.get(k) for k in required_keys) or \
           not (self.login_config.get('success_selector') or self.login_config.get('success_url_contains')):
            self.logger.error("Login configuration is missing required keys (selectors, non-empty credentials) or success verification method.")
            return False

        login_url = self.login_config['login_url']
        self.logger.info(f"Attempting login via: {login_url}")
        try:
            self.driver.get(login_url)
            time.sleep(self.config.get('wait_time', 2))

            wait = WebDriverWait(self.driver, 10)
            user_selector = self.login_config['username_selector']
            pass_selector = self.login_config['password_selector']
            submit_selector = self.login_config['submit_selector']

            login_selector_method = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR
            self.logger.debug(f"Using {self.selector_type.upper()} for login element selectors.")

            user_field = wait.until(EC.visibility_of_element_located((login_selector_method, user_selector)))
            pass_field = wait.until(EC.visibility_of_element_located((login_selector_method, pass_selector)))
            submit_button = wait.until(EC.element_to_be_clickable((login_selector_method, submit_selector)))

            self.logger.debug("Entering login credentials.")
            username = self.login_config['username']
            password = self.login_config['password']

            user_field.clear(); user_field.send_keys(username)
            pass_field.clear(); pass_field.send_keys(password)

            self.logger.debug(f"Clicking submit button ('{submit_selector}') using JavaScript.")
            try:
                 self.driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
                 time.sleep(0.5)
                 self.driver.execute_script("arguments[0].click();", submit_button)
            except Exception as js_click_error:
                 self.logger.error(f"JavaScript click failed for submit button ({submit_selector}): {js_click_error}. Trying standard click.")
                 try: submit_button.click()
                 except Exception as std_click_error:
                     self.logger.error(f"Standard click also failed for submit button: {std_click_error}")
                     raise

            wait_after = self.login_config.get('wait_after_login', 3)
            self.logger.debug(f"Waiting {wait_after}s after submitting login form...")
            time.sleep(wait_after)

            success_selector = self.login_config.get('success_selector')
            success_url_contains = self.login_config.get('success_url_contains')
            login_successful = False

            if success_selector:
                self.logger.debug(f"Verifying login success using visibility of {self.selector_type.upper()} selector: '{success_selector}'")
                try:
                    WebDriverWait(self.driver, 5).until(EC.visibility_of_element_located((login_selector_method, success_selector)))
                    self.logger.info("Login successful (success selector found).")
                    login_successful = True
                except TimeoutException:
                    self.logger.warning(f"Login verification failed: Success selector '{success_selector}' not found after timeout.")

            if not login_successful and success_url_contains:
                 self.logger.debug(f"Verifying login success using URL containing: '{success_url_contains}'")
                 current_page_url_after_login = self.driver.current_url
                 if success_url_contains in current_page_url_after_login:
                     self.logger.info(f"Login successful (URL '{current_page_url_after_login}' contains '{success_url_contains}').")
                     login_successful = True
                 else:
                     self.logger.warning(f"Login verification failed: Current URL '{current_page_url_after_login}' doesn't contain '{success_url_contains}'.")

            if not login_successful and not (success_selector or success_url_contains): # Should be caught by schema
                 self.logger.error("No success condition was specified for login verification.")


            return login_successful
        except TimeoutException as e:
            self.logger.error(f"Login failed: Timed out waiting for login page elements on {login_url}. ({e})")
            return False
        except (NoSuchElementException, ElementNotInteractableException, InvalidSelectorException) as e:
            self.logger.error(f"Login failed: Could not find or interact with login elements on {login_url}. ({e})")
            return False
        except WebDriverException as e:
            self.logger.error(f"Login failed due to WebDriverException on {login_url}: {e}", exc_info=False) # Less verbose for common webdriver issues
            return False
        except Exception as e:
            self.logger.error(f"Login failed due to unexpected error on {login_url}: {e}", exc_info=True)
            return False

    def _wait_for_page_load(self, current_url: Optional[str] = None, use_config_selector: bool = True):
        if not self.driver: return

        wait_time = float(self.config.get('wait_time', 5.0))
        wait_selector_from_config = self.config.get('wait_for_selector')
        actual_wait_selector = wait_selector_from_config if use_config_selector else None

        selector_method_for_wait = None
        if actual_wait_selector:
            selector_method_for_wait = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR

        try:
            if actual_wait_selector and selector_method_for_wait:
                self.logger.debug(f"Waiting up to {wait_time}s for element '{actual_wait_selector}' ({self.selector_type}) to be visible.")
                condition = EC.visibility_of_element_located((selector_method_for_wait, actual_wait_selector))
                WebDriverWait(self.driver, wait_time).until(condition)
                self.logger.debug(f"Element '{actual_wait_selector}' is visible.")
            elif wait_time > 0:
                self.logger.debug(f"Applying general wait of {wait_time}s...")
                time.sleep(wait_time)
        except TimeoutException:
             context_url = current_url or getattr(self.driver, 'current_url', 'unknown URL')
             self.logger.warning(f"Timed out after {wait_time}s waiting for condition '{actual_wait_selector or 'general load'}' on {context_url}.")
        except InvalidSelectorException as e:
             self.logger.error(f"Invalid wait_for_selector '{actual_wait_selector}' ({self.selector_type}): {e}")
        except Exception as e:
             context_url = current_url or getattr(self.driver, 'current_url', 'unknown URL')
             self.logger.error(f"Error during explicit wait on {context_url}: {e}")

    def extract_data(self, url: str) -> List[Dict]:
        items = []
        if not self.driver: self.logger.error("WebDriver not available for extraction."); return []
        if not self.selectors: self.logger.error("Selectors config missing for extraction."); return []

        container_selector = self.selectors.get('container')
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})
        current_selector_method = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR

        if not item_selector: self.logger.error("Missing 'item' selector."); return []

        try:
            search_context: Any = self.driver
            if container_selector:
                try:
                    container_element = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((current_selector_method, container_selector))
                    )
                    search_context = container_element
                    self.logger.debug(f"Using container '{container_selector}' for item search.")
                except Exception as e:
                    self.logger.warning(f"Container selector '{container_selector}' ({self.selector_type}) not found/invalid: {e}. Searching page root.")

            self.logger.debug(f"Looking for item elements using {self.selector_type.upper()}: '{item_selector}'")
            elements: List[WebElement] = []
            try:
                if isinstance(search_context, WebDriver):
                    WebDriverWait(search_context, 5 ).until(EC.presence_of_all_elements_located((current_selector_method, item_selector)))
                # If search_context is WebElement, presence_of_all_elements_located might not be appropriate directly
                # find_elements will be used instead.
                elements = search_context.find_elements(current_selector_method, item_selector)
            except TimeoutException:
                 self.logger.warning(f"Timed out waiting for item elements matching '{item_selector}' ({self.selector_type}).")
            except InvalidSelectorException as e:
                 self.logger.error(f"Invalid item selector '{item_selector}' ({self.selector_type}): {e}")
                 return []

            self.logger.debug(f"Found {len(elements)} potential item elements.")
            if not elements: return items

            for i, element_selenium in enumerate(elements):
                item_data = {}
                for field, selector_config in field_selectors.items():
                     value: Optional[str] = None
                     current_field_selector: Optional[str] = None
                     attr: Optional[str] = None

                     if isinstance(selector_config, str): current_field_selector = selector_config
                     elif isinstance(selector_config, dict):
                         current_field_selector = selector_config.get('selector')
                         attr = selector_config.get('attr')
                     if not current_field_selector: continue

                     try:
                        # Selenium's find_elements is used here. It expects selectors that find elements.
                        target_elements_selenium = element_selenium.find_elements(current_selector_method, current_field_selector)

                        if target_elements_selenium:
                            if attr: # If 'attr' is specified in config, use it
                                value_list = [el.get_attribute(attr).strip() for el in target_elements_selenium if el.get_attribute(attr) is not None] # Ensure attribute exists
                                value = ", ".join(filter(None,value_list)) if value_list else None
                            else: # Otherwise, get the text content of the element(s)
                                value_list = [el.text.strip() for el in target_elements_selenium if el.text and el.text.strip()]
                                value = " ".join(filter(None,value_list)) if value_list else None

                        # Post-process value for URLs
                        if attr in ['href', 'src'] and isinstance(value, str) and value:
                            if not value.startswith(('http://','https://','//','data:')):
                                try: value = urljoin(url, value)
                                except ValueError: self.logger.warning(f"Could not resolve relative URL '{value}' vs base '{url}'.")
                        elif isinstance(value, str): # General strip for text values
                            value = value.strip() if value else None

                     except NoSuchElementException:
                         value = None
                         self.logger.debug(f"Field '{field}' selector '{current_field_selector}' not found in item {i+1} using {self.selector_type.upper()}.")
                     except InvalidSelectorException as e:
                         self.logger.error(f"Invalid field selector '{current_field_selector}' ({self.selector_type}) for field '{field}': {e}")
                         value = None
                     except Exception as e:
                         self.logger.error(f"Unexpected error extracting field '{field}' with selector '{current_field_selector}' in item {i+1}: {e}")
                         value = None
                     item_data[field] = value

                if any(v is not None for v in item_data.values()): items.append(item_data)
                else: self.logger.debug(f"Skipping item {i+1} as all fields evaluated to None.")

            if items: self.logger.info(f"Extracted {len(items)} non-empty items from {url} using {self.selector_type.upper()}.")
            elif elements: self.logger.warning(f"Found {len(elements)} item elements, but all extracted items were empty on {url}.")

        except Exception as e:
             self.logger.error(f"Unexpected error during dynamic data extraction on {url}: {e}", exc_info=True)
        return items

    def _find_and_click_next_page(self, current_url_before_click: str) -> bool:
        if not self.pagination_config or not self.driver: return False
        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector: return False

        pagination_selector_method = By.XPATH if self.selector_type == 'xpath' else By.CSS_SELECTOR
        self.logger.debug(f"Looking for next page element using {self.selector_type.upper()}: '{next_page_selector}'")

        try:
            element_selector_for_click = next_page_selector
            attribute_for_href = "href" # Default for <a> tags

            if self.selector_type == 'xpath' and "/@" in next_page_selector:
                # If XPath targets an attribute (e.g., //a/@href), we need the element part for clicking
                # and then get the attribute value from it if needed (though click is primary action)
                xpath_parts = next_page_selector.rsplit("/@",1)
                element_selector_for_click = xpath_parts[0]
                if len(xpath_parts) > 1:
                    # For clicking, we don't need the attribute name, but good to know if it was specified
                    # The Selenium click action acts on the element found by element_selector_for_click
                    pass

            wait_clickable = WebDriverWait(self.driver, 5)
            next_button = wait_clickable.until(EC.element_to_be_clickable((pagination_selector_method, element_selector_for_click)))

            # Check for disabled state more carefully
            disabled_attr = next_button.get_attribute('disabled')
            class_attr = next_button.get_attribute('class') or "" # Ensure string
            if disabled_attr or 'disabled' in class_attr.lower() or 'inactive' in class_attr.lower():
                self.logger.info(f"Next page element '{next_page_selector}' found but appears disabled. Assuming last page.")
                return False

            self.logger.info(f"Found clickable 'Next' page element. Clicking...")
            self.driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(0.3)
            self.driver.execute_script("arguments[0].click();", next_button)

            WebDriverWait(self.driver, 10).until(EC.url_changes(current_url_before_click))
            new_url = self.driver.current_url
            self.logger.info(f"Navigated to next page: {new_url}")
            self._wait_for_page_load(new_url, use_config_selector=True)
            return True

        except TimeoutException:
            self.logger.info(f"Next page element '{next_page_selector}' not found, not clickable, or URL did not change within timeout.")
        except Exception as e:
            self.logger.error(f"Error finding/clicking next page '{next_page_selector}': {e}", exc_info=True)
        return False


    def run(self) -> Dict:
        self.stats['start_time'] = time.time()
        all_extracted_data: List[Dict] = []
        initial_urls = self.config.get("urls", [])
        if isinstance(initial_urls, str): initial_urls = [initial_urls]

        driver_initialized_successfully = False
        try:
            self.driver = self._init_driver()
            if not self.driver: raise WebDriverException("WebDriver initialization failed.")
            driver_initialized_successfully = True

            login_successful = self._perform_login() if self.login_config else True
            if not login_successful: raise Exception("Login failed, aborting scraping run.")

            urls_to_scrape = list(initial_urls)
            current_processing_url = None

            if not urls_to_scrape:
                if self.driver and self.login_config and login_successful:
                    current_processing_url = self.driver.current_url
                    self.logger.info(f"No initial URLs specified, starting scrape from current page after login: {current_processing_url}")
                    if current_processing_url and current_processing_url not in ["about:blank", self.login_config.get('login_url') if self.login_config else ""]:
                        urls_to_scrape.append(current_processing_url)
                    else:
                        self.logger.warning("Landed on login/blank page after login and no target URLs specified.")
                else:
                    self.logger.warning("No target URLs provided and login not configured or failed. Cannot start scraping.")

            if not urls_to_scrape:
                self.stats['end_time'] = time.time(); return {'data': [], 'stats': self.get_stats(), 'config': self.config}

            scraped_urls: Set[str] = set()
            max_pages = float('inf')
            if self.pagination_config:
                max_pages_val = self.pagination_config.get('max_pages')
                if max_pages_val is not None and str(max_pages_val).isdigit(): max_pages = int(max_pages_val)

            pages_scraped_count = 0

            queue_idx = 0
            while queue_idx < len(urls_to_scrape) and pages_scraped_count < max_pages:
                current_url_to_process = urls_to_scrape[queue_idx]; queue_idx += 1
                if current_url_to_process in scraped_urls: continue

                self.logger.info(f"Processing URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'all'}): {current_url_to_process}")

                # Navigate if current URL is not the one we want to process (can happen after login or pagination)
                if self.driver.current_url != current_url_to_process:
                    self.throttle_requests() # Throttle before actual page get
                    self.driver.get(current_url_to_process)

                self._wait_for_page_load(current_url_to_process, use_config_selector=True)

                scraped_urls.add(current_url_to_process)
                self.stats['pages_scraped'] += 1
                pages_scraped_count += 1

                page_data = self.extract_data(current_url_to_process)
                all_extracted_data.extend(page_data)

                if pages_scraped_count < max_pages:
                    navigated_to_next = self._find_and_click_next_page(current_url_to_process)
                    if navigated_to_next:
                        next_actual_url = self.driver.current_url
                        if next_actual_url not in scraped_urls and next_actual_url not in urls_to_scrape:
                             urls_to_scrape.append(next_actual_url)
                    else:
                        self.logger.info("No next page navigated to or end of pagination for this branch.")
                        # If there are other seed URLs, the loop will continue. If not, it ends.
                else:
                    self.logger.info(f"Reached max_pages limit ({max_pages}).")
                    break

        except Exception as e:
            self.logger.error(f"Error during DynamicScraper run: {e}", exc_info=True)
        finally:
            if driver_initialized_successfully:
                self._close_driver()

        processed_data = self._process_extracted_data(all_extracted_data)
        self.stats['end_time'] = time.time()
        return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}

    def _close_driver(self):
         if self.driver:
             driver_instance = self.driver; self.driver = None
             try:
                 self.logger.info("Closing WebDriver instance..."); driver_instance.quit()
                 self.logger.info("WebDriver closed successfully.")
             except Exception as e: self.logger.error(f"Error closing WebDriver: {e}")
         else: self.logger.debug("No active WebDriver instance to close.")
