import time # Already imported in BaseScraper, but good for explicitness if used here
import logging
from urllib.parse import urljoin
from typing import Dict, List, Optional, Any, Set
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
# --- Import LXML ---
try:
    from lxml import etree, html as lxml_html # Use lxml_html for fromstring for safety
    LXML_INSTALLED = True
except ImportError:
    LXML_INSTALLED = False
    # Warning will be logged during __init__ if XPath is requested without lxml


class HTMLScraper(BaseScraper):
    """
    Scraper implementation for static HTML content.

    Uses BeautifulSoup4 for parsing, supporting both CSS selectors (default)
    and XPath selectors (requires `lxml` library). It handles fetching pages
    via the BaseScraper, extracting data based on configured selectors,
    and finding the next page URL for pagination.
    """

    def __init__(self, config: Dict):
        """
        Initializes the HTMLScraper.

        Args:
            config: The validated scraper configuration dictionary. It expects
                    'selectors' (with 'type', 'item', 'fields') and optionally
                    'pagination' keys relevant to HTML scraping.
        """
        super().__init__(config) # Initializes self.logger among other things
        self.selectors: Dict = config.get('selectors', {})
        self.selector_type: str = self.selectors.get('type', 'css').lower()
        self.pagination_config: Optional[Dict] = config.get('pagination')

        if self.selector_type == 'xpath' and not LXML_INSTALLED:
            msg = "lxml library is required for XPath support in HTMLScraper, but it's not installed. Please run: pip install lxml"
            self.logger.error(msg) # Use self.logger
            raise ImportError(msg)

        self.bs_parser: str = 'lxml' if LXML_INSTALLED else 'html.parser'
        self.logger.info(f"HTMLScraper initialized (Selector Type: {self.selector_type.upper()}, BS4 Parser: {self.bs_parser})")


    def extract_data(self, html_content: str, url: str) -> List[Dict]: # Renamed html to html_content
        """
        Extracts structured data from the provided HTML content.
        (This is existing method, ensure it's compatible with lxml elements if using XPath)
        """
        if not html_content:
            self.logger.warning(f"Received empty HTML content for URL: {url}")
            return []

        items = []
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})

        if not item_selector:
            self.logger.error("Configuration error: Missing 'item' selector.")
            return []
        if not field_selectors:
            self.logger.warning("Configuration warning: No 'fields' defined in selectors. No data will be extracted.")
            return []

        try:
            elements: List[Any] = [] # Ensure elements is defined before conditional assignment
            # --- Select top-level item elements using either XPath or CSS ---
            if self.selector_type == 'xpath':
                if not LXML_INSTALLED: # Should have been caught in __init__ but double check
                    self.logger.error("LXML not installed, cannot use XPath for item selection.")
                    return []
                parser = etree.HTMLParser(recover=True)
                tree = lxml_html.fromstring(html_content.encode('utf-8', 'replace'), parser=parser) # Use lxml_html.fromstring
                if tree is None:
                     self.logger.error(f"lxml failed to parse HTML from {url}")
                     return []
                elements = tree.xpath(item_selector)
                self.logger.debug(f"Found {len(elements)} potential item elements using XPath: '{item_selector}'")
            else: # Default to CSS selectors
                soup = BeautifulSoup(html_content, self.bs_parser)
                elements = soup.select(item_selector)
                self.logger.debug(f"Found {len(elements)} potential item elements using CSS: '{item_selector}'")

            if not elements:
                self.logger.warning(f"No items found using {self.selector_type.upper()} selector '{item_selector}' on {url}.")
                return items

            for i, element_context in enumerate(elements): # element_context is either BS4 Tag or lxml Element
                item_data = {}
                for field, selector_config in field_selectors.items():
                    value: Optional[str] = None
                    current_selector: Optional[str] = None
                    attr: Optional[str] = None

                    if isinstance(selector_config, str):
                        current_selector = selector_config
                    elif isinstance(selector_config, dict):
                        current_selector = selector_config.get('selector')
                        attr = selector_config.get('attr')

                    if not current_selector:
                        self.logger.warning(f"Selector missing for field '{field}' in item {i+1}. Skipping field.")
                        continue

                    try:
                        if self.selector_type == 'xpath':
                            if not hasattr(element_context, 'xpath'):
                                 self.logger.warning(f"XPath element type {type(element_context)} invalid for item {i+1}, field {field}. Skipping field.")
                                 continue

                            # Handle relative XPaths (e.g. starting with .//)
                            # lxml's element.xpath() handles this correctly by default.
                            results = element_context.xpath(current_selector)

                            if results:
                                # XPath can return elements, text, attributes, or booleans/numbers.
                                # We are primarily interested in text or attribute strings.
                                first_result = results[0]

                                if isinstance(first_result, etree._Element): # If XPath selected an element
                                    if attr: # If config explicitly asks for an attribute
                                        value = first_result.get(attr)
                                    else: # Otherwise, get its text content
                                        value = etree.tostring(first_result, method="text", encoding="unicode", with_tail=False).strip()
                                elif hasattr(first_result, 'strip'): # If XPath directly returned a string (e.g., from text() or @attr)
                                    value = str(first_result).strip()
                                else: # Other types (e.g. boolean or number from an XPath function)
                                    value = str(first_result)

                                # If XPath selected multiple nodes but we only took the first,
                                # and the user intended to get multiple (e.g. list of tags)
                                # this simplified logic takes the first.
                                # For multiple values (like tags), the selector itself should be designed
                                # to return multiple strings or the processing rule should handle it.
                                # For now, we focus on getting a single representative value.
                                if len(results) > 1 and value is not None:
                                    # If multiple text nodes or attributes were returned, concatenate them.
                                    # This is common for selectors like ".//div/text()" which might return multiple text segments.
                                    if all(isinstance(r, str) for r in results):
                                        value = " ".join(r.strip() for r in results if r.strip()).strip()
                                    elif all(isinstance(r, etree._Element) for r in results):
                                        if attr:
                                            value = ", ".join(r.get(attr, "").strip() for r in results if r.get(attr,"").strip()).strip()
                                        else:
                                            value = " ".join(etree.tostring(r, method="text", encoding="unicode", with_tail=False).strip() for r in results).strip()

                        else: # CSS selector logic
                             # element_context here is a BeautifulSoup Tag
                             target_elements = element_context.select(current_selector) # Use select for potentially multiple
                             if target_elements:
                                 if attr:
                                     value_list = [el.get(attr, "").strip() for el in target_elements if el.get(attr,"").strip()]
                                     value = ", ".join(value_list) if value_list else None
                                 else:
                                     value_list = [el.get_text(strip=True) for el in target_elements if el.get_text(strip=True)]
                                     value = " ".join(value_list) if value_list else None

                        if attr in ['href', 'src'] and isinstance(value, str) and value:
                            if not value.startswith(('http://', 'https://', '//', 'data:')):
                                try: value = urljoin(url, value)
                                except ValueError: self.logger.warning(f"Could not resolve relative URL '{value}' relative to base '{url}'.")
                        elif isinstance(value, str):
                            value = value.strip() if value else None # Ensure None if stripping results in empty

                    except etree.XPathEvalError as e:
                        self.logger.error(f"Invalid XPath expression '{current_selector}' for field '{field}' in item {i+1}: {e}")
                    except Exception as e:
                        self.logger.error(f"Unexpected error extracting field '{field}' with selector '{current_selector}' in item {i+1}: {e}")

                    item_data[field] = value

                if any(v is not None for v in item_data.values()):
                    items.append(item_data)
                else:
                    self.logger.debug(f"Skipping item {i+1} as all fields evaluated to None.")

            if items: # Only log if non-empty items were actually successfully extracted
                self.logger.info(f"Successfully extracted {len(items)} non-empty items from {url} using {self.selector_type.upper()}.")
            elif elements: # Log if item elements were found but no data extracted
                self.logger.warning(f"Found {len(elements)} item elements, but all extracted items were empty on {url}.")


        except etree.XMLSyntaxError as e:
            self.logger.error(f"lxml failed to parse HTML for {url}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error during data extraction on {url}: {e}", exc_info=True)
            return []

        return items

    def _find_next_page_url(self, html_content: str, current_url: str) -> Optional[str]:
        if not self.pagination_config or not html_content:
            return None

        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector:
            self.logger.debug("Pagination enabled in config, but 'next_page_selector' is missing.")
            return None

        next_page_href = None
        self.logger.info(f"Attempting to find next page using {self.selector_type.upper()} selector: '{next_page_selector}'")

        try:
            if self.selector_type == 'xpath':
                if not LXML_INSTALLED:
                    self.logger.error("LXML is required for XPath based pagination but not installed.")
                    return None

                parser = etree.HTMLParser(recover=True)
                tree = lxml_html.fromstring(html_content.encode('utf-8', 'replace'), parser=parser)
                if tree is None:
                    self.logger.error(f"lxml failed to parse HTML for pagination from {current_url}")
                    return None

                # Case 1: XPath selector is for the href attribute directly (e.g., ".../@href")
                if "/@" in next_page_selector:
                    results = tree.xpath(next_page_selector) # lxml xpath can return attribute values as strings
                    if results and isinstance(results, list) and len(results) > 0 and isinstance(results[0], str):
                        next_page_href = results[0].strip()
                        self.logger.debug(f"XPath (attribute) evaluation for pagination found href: {next_page_href}")
                    elif results:
                         self.logger.warning(f"XPath for pagination attribute selector did not return a string as expected. Selector: {next_page_selector}, Got: {results[0]}")
                else:
                    # Case 2: XPath selector is for the <a> element
                    elements = tree.xpath(next_page_selector)
                    if elements and hasattr(elements[0], 'get'):
                        next_page_href = elements[0].get("href")
                        if next_page_href: next_page_href = next_page_href.strip()
                        self.logger.debug(f"XPath (element) evaluation for pagination found href: {next_page_href}")

            else: # CSS Selector
                soup = BeautifulSoup(html_content, self.bs_parser)
                link_element = soup.select_one(next_page_selector)
                if link_element:
                    next_page_href = link_element.get("href")
                    if next_page_href: next_page_href = next_page_href.strip()
                    self.logger.debug(f"CSS evaluation for pagination found href: {next_page_href}")

            if next_page_href: # Ensure href is not empty after stripping
                resolved_url = urljoin(current_url, next_page_href)
                if resolved_url == current_url:
                    self.logger.warning(f"Next page URL '{resolved_url}' is same as current. Stopping pagination to prevent loop.")
                    return None
                return resolved_url

        except etree.XPathEvalError as e_xpath:
             self.logger.error(f"Invalid XPath expression for pagination '{next_page_selector}': {e_xpath}")
        except Exception as e:
            self.logger.error(f"Error processing next page selector '{next_page_selector}' (type: {self.selector_type.upper()}): {e}", exc_info=True)

        return None


    def run(self) -> Dict:
        self.stats['start_time'] = time.time()
        all_extracted_data: List[Dict] = []

        initial_urls = self.config.get("urls", [])
        if isinstance(initial_urls, str): # Handle single URL string
            urls_to_scrape: List[str] = [initial_urls]
        elif isinstance(initial_urls, list):
            urls_to_scrape = list(initial_urls)
        else:
            self.logger.error(f"URLs in config are not a list or string: {initial_urls}")
            urls_to_scrape = []

        scraped_urls: Set[str] = set()

        max_pages = float('inf')
        if self.pagination_config:
            max_pages_config = self.pagination_config.get('max_pages')
            if max_pages_config is not None and str(max_pages_config).isdigit():
                max_pages = int(max_pages_config)
            elif max_pages_config is not None: # If it's set but not a valid number
                 self.logger.warning(f"Invalid 'max_pages' value '{max_pages_config}'. Defaulting to unlimited.")


        pages_scraped_this_run = 0

        if not urls_to_scrape:
            self.logger.warning("No initial URLs provided in configuration.")

        queue_idx = 0
        while queue_idx < len(urls_to_scrape) and pages_scraped_this_run < max_pages:
            current_url = urls_to_scrape[queue_idx]
            queue_idx += 1

            if current_url in scraped_urls:
                self.logger.debug(f"Skipping already scraped URL: {current_url}")
                continue

            self.logger.info(f"Processing URL ({pages_scraped_this_run + 1}/{max_pages if max_pages != float('inf') else 'all available'}): {current_url}")
            html_content = self.fetch_page(current_url) # Uses BaseScraper's fetch_page
            scraped_urls.add(current_url)

            if html_content:
                page_data = self.extract_data(html_content, current_url)
                if page_data: # Only extend if data was actually extracted
                    all_extracted_data.extend(page_data)

                # self.stats['pages_scraped'] is incremented by fetch_page on success
                # We need a separate counter for pagination max_pages logic
                pages_scraped_this_run += 1

                if pages_scraped_this_run < max_pages:
                    next_page_url = self._find_next_page_url(html_content, current_url)
                    if next_page_url:
                        if next_page_url not in scraped_urls and next_page_url not in urls_to_scrape:
                            self.logger.info(f"Adding next page to queue: {next_page_url}")
                            urls_to_scrape.append(next_page_url)
                        else:
                            self.logger.debug(f"Next page URL '{next_page_url}' already visited or queued.")
                    else:
                        self.logger.info("No further pages found for this URL branch based on pagination rules.")
                        # Don't break the main loop if there are other initial URLs or branches
            else:
                self.logger.warning(f"No content fetched for {current_url}. Cannot extract data or find next page.")


            if pages_scraped_this_run >= max_pages:
                self.logger.info(f"Reached maximum page limit ({max_pages}). Stopping further pagination.")
                break

        processed_data = self._process_extracted_data(all_extracted_data) # Uses BaseScraper's method
        self.stats['end_time'] = time.time()
        return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}
