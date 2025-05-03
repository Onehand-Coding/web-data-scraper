from typing import Dict, List, Optional, Any, Set
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
from urllib.parse import urljoin
import time
import logging

# --- Import LXML ---
try:
    # Try importing lxml for XPath support and potentially faster parsing
    from lxml import etree
    LXML_INSTALLED = True
except ImportError:
    LXML_INSTALLED = False
    # Warning will be logged during __init__ if XPath is requested without lxml

# --- Get Logger ---
logger = logging.getLogger(__name__) # Logger specific to this module

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
        super().__init__(config)
        self.selectors: Dict = config.get('selectors', {})
        self.selector_type: str = self.selectors.get('type', 'css').lower()
        self.pagination_config: Optional[Dict] = config.get('pagination')

        # Validate XPath dependency
        if self.selector_type == 'xpath' and not LXML_INSTALLED:
            msg = "lxml library is required for XPath support in HTMLScraper, but it's not installed. Please run: pip install lxml"
            logger.error(msg)
            raise ImportError(msg)

        # Determine the best available parser for BeautifulSoup
        # Prioritize 'lxml' if installed (generally faster and handles broken HTML well),
        # otherwise fallback to Python's built-in 'html.parser'.
        self.bs_parser: str = 'lxml' if LXML_INSTALLED else 'html.parser'
        logger.info(f"HTMLScraper initialized (Selector Type: {self.selector_type.upper()}, BS4 Parser: {self.bs_parser})")


    def extract_data(self, html: str, url: str) -> List[Dict]:
        """
        Extracts structured data from the provided HTML content.

        Uses either CSS selectors (with BeautifulSoup) or XPath expressions (with lxml)
        based on the `selector_type` defined in the configuration.

        Args:
            html: The HTML content string of the page.
            url: The URL of the page, used for resolving relative URLs found in attributes.

        Returns:
            A list of dictionaries, where each dictionary represents an extracted item.
            Returns an empty list if no items are found or if errors occur.
        """
        if not html:
            logger.warning(f"Received empty HTML content for URL: {url}")
            return []

        items = []
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})

        if not item_selector:
            logger.error("Configuration error: Missing 'item' selector.")
            return []
        if not field_selectors:
            logger.warning("Configuration warning: No 'fields' defined in selectors. No data will be extracted.")
            return []

        try:
            elements = []
            # --- Select top-level item elements using either XPath or CSS ---
            if self.selector_type == 'xpath':
                # Requires lxml installed (checked in __init__)
                parser = etree.HTMLParser(recover=True) # Use recover mode for potentially broken HTML
                tree = etree.fromstring(html.encode('utf-8'), parser=parser) # lxml needs bytes
                if tree is None: # Check if parsing failed completely
                     logger.error(f"lxml failed to parse HTML from {url}")
                     return []
                elements = tree.xpath(item_selector)
                logger.debug(f"Found {len(elements)} potential item elements using XPath: '{item_selector}'")
            else: # Default to CSS selectors
                soup = BeautifulSoup(html, self.bs_parser)
                elements = soup.select(item_selector)
                logger.debug(f"Found {len(elements)} potential item elements using CSS: '{item_selector}'")

            if not elements:
                logger.warning(f"No items found using {self.selector_type.upper()} selector '{item_selector}' on {url}.")
                return items

            # --- Iterate through each found item element ---
            for i, element in enumerate(elements):
                item_data = {}
                # --- Extract each defined field within the item element ---
                for field, selector_config in field_selectors.items():
                    value: Optional[str] = None
                    current_selector: Optional[str] = None
                    attr: Optional[str] = None # Attribute to extract (e.g., 'href', 'src')

                    # Determine selector and attribute from config (string or object)
                    if isinstance(selector_config, str):
                        current_selector = selector_config
                    elif isinstance(selector_config, dict):
                        current_selector = selector_config.get('selector')
                        attr = selector_config.get('attr')

                    if not current_selector:
                        logger.warning(f"Selector missing for field '{field}' in item {i+1}. Skipping field.")
                        continue

                    # --- Extract data using the appropriate method (XPath or CSS) ---
                    try:
                        if self.selector_type == 'xpath':
                             # Run XPath relative to the current item element context
                             if not hasattr(element, 'xpath'): # Should not happen if lxml parsing worked
                                 logger.warning(f"XPath element type {type(element)} invalid for item {i+1}, field {field}. Skipping field.")
                                 continue
                             results = element.xpath(current_selector)
                             if results:
                                 first_result = results[0]
                                 # Handle different result types from XPath (element vs text/attribute)
                                 if isinstance(first_result, etree._Element):
                                     value = first_result.get(attr) if attr else first_result.text # Get attribute or text
                                 elif hasattr(first_result, 'strip'):
                                     value = str(first_result).strip() # Handle text nodes, attribute values directly returned
                                 else:
                                     value = first_result # Fallback if type is unexpected
                             # else: value remains None if XPath finds nothing

                        else: # CSS selector logic
                             target_element = element.select_one(current_selector)
                             if target_element:
                                 # Get attribute if specified, otherwise get element's text
                                 value = target_element.get(attr) if attr else target_element.get_text(strip=True)
                             # else: value remains None if select_one finds nothing

                        # --- Post-processing for extracted value ---
                        # 1. Resolve relative URLs if an attribute like 'href' or 'src' was extracted
                        if attr in ['href', 'src'] and isinstance(value, str) and value:
                            # Check if it looks like a relative URL
                            if not value.startswith(('http://', 'https://', '//', 'data:')):
                                try:
                                    value = urljoin(url, value) # Combine with base page URL
                                except ValueError:
                                    logger.warning(f"Could not resolve relative URL '{value}' relative to base '{url}'. Keeping original.")
                        # 2. Strip leading/trailing whitespace from text values
                        elif isinstance(value, str):
                            value = value.strip()

                    except etree.XPathEvalError as e:
                        logger.error(f"Invalid XPath expression '{current_selector}' for field '{field}' in item {i+1}: {e}")
                        value = None # Ensure value is None on error
                    except Exception as e:
                        logger.error(f"Unexpected error extracting field '{field}' with selector '{current_selector}' in item {i+1}: {e}")
                        value = None # Ensure value is None on error

                    item_data[field] = value # Add extracted value (or None) to item dict

                # Only add the item if at least one field was successfully extracted
                if any(v is not None for v in item_data.values()):
                    items.append(item_data)
                else:
                    logger.debug(f"Skipping item {i+1} as all fields evaluated to None.")

            self.logger.info(f"Successfully extracted {len(items)} non-empty items from {url} using {self.selector_type.upper()}.")

        except etree.XMLSyntaxError as e:
            # Catch errors during initial lxml parsing (if using XPath)
            logger.error(f"lxml failed to parse HTML for {url}: {e}")
            return []
        except Exception as e:
            # Catch unexpected errors during the overall extraction process
            logger.error(f"Unexpected error during data extraction on {url}: {e}", exc_info=True)
            return [] # Return empty list on major error

        return items


    def _find_next_page_url(self, html: str, current_url: str) -> Optional[str]:
        """
        Finds the URL for the next page using the `next_page_selector` from the
        pagination configuration. Uses BeautifulSoup with CSS selectors for simplicity.

        Args:
            html: The HTML content of the current page.
            current_url: The URL of the current page, for resolving relative links.

        Returns:
            The absolute URL of the next page, or None if not found or not configured.
        """
        if not self.pagination_config or not html:
            # No pagination configured or no HTML to parse
            return None

        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector:
            logger.debug("Pagination enabled in config, but 'next_page_selector' is missing.")
            return None

        try:
            # Use BeautifulSoup with the configured parser for finding the next link
            soup = BeautifulSoup(html, self.bs_parser)
            next_link = soup.select_one(next_page_selector)

            if next_link and next_link.get('href'):
                next_path = next_link['href']
                # Resolve the potentially relative path against the current URL
                next_url = urljoin(current_url, next_path)

                # Avoid infinite loops if the next link points to the current page
                if next_url == current_url:
                    logger.warning(f"Next page selector '{next_page_selector}' points back to the current URL '{current_url}'. Stopping pagination.")
                    return None

                logger.debug(f"Found next page link via '{next_page_selector}': {next_url}")
                return next_url
            else:
                # Log if the selector didn't find a matching element or the element had no href
                logger.debug(f"Next page CSS selector '{next_page_selector}' did not find a link with an href attribute.")
                return None
        except Exception as e:
            # Catch errors during soup parsing or link selection
            logger.error(f"Error finding next page link using CSS selector '{next_page_selector}': {e}")
            return None


    def run(self) -> Dict:
        """
        Executes the HTML scraping job.

        Handles fetching initial URLs, extracting data, finding and queuing
        subsequent pages based on pagination rules, and processing all
        collected data at the end.

        Returns:
            A dictionary containing the processed data ('data'), run statistics ('stats'),
            and the original configuration ('config').
        """
        self.stats['start_time'] = time.time() # Record start time
        all_extracted_data: List[Dict] = []
        urls_to_scrape: List[str] = list(self.config.get('urls', [])) # Initial URLs
        scraped_urls: Set[str] = set() # Keep track of visited URLs

        max_pages = float('inf') # Default to scrape all pages found
        if self.pagination_config:
            max_pages = self.pagination_config.get('max_pages', float('inf'))

        pages_scraped_count = 0 # Counter for pages successfully scraped

        if not urls_to_scrape:
            logger.warning("No initial URLs provided in configuration.")
            self.stats['end_time'] = time.time()
            return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        # --- Main Scraping Loop ---
        while urls_to_scrape and pages_scraped_count < max_pages:
            current_url = urls_to_scrape.pop(0) # Get the next URL from the queue

            # Avoid scraping the same URL multiple times
            if current_url in scraped_urls:
                logger.debug(f"Skipping already scraped URL: {current_url}")
                continue

            logger.info(f"Processing URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")
            html = self.fetch_page(current_url) # Fetch page using BaseScraper method
            scraped_urls.add(current_url) # Mark URL as visited

            if html:
                # If fetch was successful, extract data
                page_data = self.extract_data(html, current_url)
                all_extracted_data.extend(page_data)
                pages_scraped_count += 1 # Increment counter only on successful scrape

                # Check for next page only if we haven't hit the max page limit
                if pages_scraped_count < max_pages:
                     next_page_url = self._find_next_page_url(html, current_url)
                     if next_page_url:
                         # Add to queue only if not already scraped or queued
                         if next_page_url not in scraped_urls and next_page_url not in urls_to_scrape:
                             logger.info(f"Adding next page to queue: {next_page_url}")
                             urls_to_scrape.append(next_page_url)
                         else:
                             logger.debug(f"Next page URL already visited or queued: {next_page_url}")
                     else:
                         # No more pages found, stop pagination for this branch
                         logger.info("No further pages found based on pagination rules.")
                         # If processing multiple start URLs, the loop will continue with the next one
            # else: fetch_page already logged the failure and updated stats['pages_failed']

            # Check max pages limit again *after* processing the current page
            if pages_scraped_count >= max_pages:
                logger.info(f"Reached maximum page limit ({max_pages}). Stopping pagination.")
                break # Exit the while loop

        # --- Post-Scraping Processing ---
        processed_data = self._process_extracted_data(all_extracted_data)
        self.stats['end_time'] = time.time() # Record end time

        return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}
