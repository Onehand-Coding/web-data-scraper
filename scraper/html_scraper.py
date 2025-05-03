# File: web-data-scraper/scraper/html_scraper.py (Corrected)

from typing import Dict, List, Optional, Any, Set
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
from urllib.parse import urljoin
import time
import logging

# --- Import LXML ---
try:
    from lxml import etree
    LXML_INSTALLED = True
except ImportError:
    LXML_INSTALLED = False
    # Warning logged during __init__ if needed

# --- Get Logger ---
logger = logging.getLogger(__name__)

class HTMLScraper(BaseScraper):
    """Scraper implementation using BeautifulSoup (CSS) or lxml (XPath) for HTML parsing."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.selectors = config.get('selectors', {})
        self.selector_type = self.selectors.get('type', 'css').lower()
        self.pagination_config = config.get('pagination') # <-- Ensure this is initialized

        if self.selector_type == 'xpath' and not LXML_INSTALLED:
            logger.error("lxml library is required for XPath support in HTMLScraper, but it's not installed. Please run: pip install lxml")
            raise ImportError("lxml library not found, required for XPath support.")

        # Use lxml parser if available and preferred for speed/XPath, otherwise default html.parser
        self.bs_parser = 'lxml' if LXML_INSTALLED else 'html.parser'
        logger.info(f"HTMLScraper initialized (Selector Type: {self.selector_type.upper()}, BS4 Parser: {self.bs_parser})")


    def extract_data(self, html: str, url: str) -> List[Dict]:
        """Extract data from HTML using configured selectors (CSS or XPath)."""
        if not html: return []
        items = []; item_selector = self.selectors.get('item'); field_selectors = self.selectors.get('fields', {})
        if not item_selector: logger.error("Missing 'item' selector."); return []

        try:
            elements = []
            if self.selector_type == 'xpath':
                if not LXML_INSTALLED: raise RuntimeError("lxml not installed, cannot use XPath.")
                parser = etree.HTMLParser(recover=True); tree = etree.fromstring(html.encode('utf-8'), parser=parser)
                if tree is None: logger.error(f"lxml failed to parse HTML from {url}"); return []
                elements = tree.xpath(item_selector)
                logger.debug(f"Found {len(elements)} items using XPath '{item_selector}'")
            else: # CSS
                soup = BeautifulSoup(html, self.bs_parser)
                elements = soup.select(item_selector)
                logger.debug(f"Found {len(elements)} items using CSS '{item_selector}'")

            if not elements: logger.warning(f"No items found using {self.selector_type.upper()} selector '{item_selector}' on {url}."); return items

            for i, element in enumerate(elements):
                item_data = {}
                for field, selector_config in field_selectors.items():
                    value = None; current_selector: Optional[str] = None; attr: Optional[str] = None
                    if isinstance(selector_config, str): current_selector = selector_config
                    elif isinstance(selector_config, dict): current_selector = selector_config.get('selector'); attr = selector_config.get('attr')
                    if not current_selector: logger.warning(f"Selector missing for field '{field}'."); continue

                    try:
                        if self.selector_type == 'xpath':
                             # Ensure element is the context node for relative XPath
                             if not hasattr(element, 'xpath'): logger.warning(f"XPath element type {type(element)} invalid for item {i+1}, field {field}."); continue
                             results = element.xpath(current_selector)
                             if results:
                                 first_result = results[0]
                                 if isinstance(first_result, etree._Element): value = first_result.get(attr) if attr else first_result.text
                                 elif hasattr(first_result, 'strip'): value = str(first_result).strip() # Handle text nodes etc.
                                 else: value = first_result # Keep as is if not string/element
                             else: value = None
                        else: # CSS
                             target_element = element.select_one(current_selector)
                             if target_element: value = target_element.get(attr) if attr else target_element.get_text(strip=True)
                             else: value = None

                        # Resolve relative URLs
                        if attr in ['href', 'src'] and isinstance(value, str) and value and not value.startswith(('http://','https://','//','data:')):
                           try: value = urljoin(url, value)
                           except ValueError: logger.warning(f"Could not resolve relative URL '{value}' vs base '{url}'")
                        # Strip whitespace from text values
                        elif isinstance(value, str): value = value.strip()

                    except etree.XPathEvalError as e: logger.error(f"Invalid XPath '{current_selector}' for field '{field}': {e}"); value = None
                    except Exception as e: logger.error(f"Error extracting field '{field}' ({self.selector_type}): {e}"); value = None
                    item_data[field] = value

                # Only add item if it contains *some* data
                if any(v is not None for v in item_data.values()): items.append(item_data)
                else: logger.debug(f"Skipping item {i+1} as all fields were None.")
            logger.info(f"Extracted {len(items)} non-empty items from {url} using {self.selector_type.upper()}.")

        except etree.XMLSyntaxError as e: logger.error(f"lxml parse error on {url}: {e}"); return []
        except Exception as e: logger.error(f"Unexpected error during extraction on {url}: {e}", exc_info=True); return []
        return items

    def _find_next_page_url(self, html: str, current_url: str) -> Optional[str]:
        """Finds the URL for the next page based on pagination config (using BeautifulSoup/CSS)."""
        if not self.pagination_config or not html: return None
        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector: logger.debug("No 'next_page_selector'."); return None
        try:
            soup = BeautifulSoup(html, self.bs_parser); next_link = soup.select_one(next_page_selector)
            if next_link and next_link.get('href'):
                next_path = next_link['href']; next_url = urljoin(current_url, next_path)
                if next_url == current_url: logger.warning(f"Next page selector points to current URL '{current_url}'."); return None
                logger.debug(f"Found next page link: {next_url}"); return next_url
            else: logger.debug(f"Next page CSS selector '{next_page_selector}' not found."); return None
        except Exception as e: logger.error(f"Error finding next page link using CSS '{next_page_selector}': {e}"); return None

    def run(self) -> Dict:
        """Execute scraping job, handling pagination if configured."""
        self.stats['start_time'] = time.time() # Set start time
        all_extracted_data = []; urls_to_scrape = list(self.config.get('urls', [])); scraped_urls: Set[str] = set()
        max_pages = float('inf')

        if self.pagination_config:
            max_pages = self.pagination_config.get('max_pages', float('inf'))

        pages_scraped_count = 0
        if not urls_to_scrape: logger.warning("No initial URLs."); self.stats['end_time'] = time.time(); return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        while urls_to_scrape and pages_scraped_count < max_pages:
            current_url = urls_to_scrape.pop(0)
            if current_url in scraped_urls: logger.debug(f"Skipping already scraped: {current_url}"); continue

            logger.info(f"Processing URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")
            html = self.fetch_page(current_url) # fetch_page handles stats['pages_scraped/failed']
            scraped_urls.add(current_url)

            if html:
                page_data = self.extract_data(html, current_url); all_extracted_data.extend(page_data); pages_scraped_count += 1
                # Find next page only if we successfully processed the current one and haven't hit the limit
                if pages_scraped_count < max_pages:
                     next_page_url = self._find_next_page_url(html, current_url)
                     if next_page_url and next_page_url not in scraped_urls and next_page_url not in urls_to_scrape:
                         logger.info(f"Adding next page to queue: {next_page_url}"); urls_to_scrape.append(next_page_url)
                     elif next_page_url: logger.debug(f"Next page already visited/queued: {next_page_url}")
                     else: logger.debug("No next page found or pagination stopped.") # Log explicit stop
            # Check max pages again after processing, before next loop iteration
            if pages_scraped_count >= max_pages: logger.info(f"Max page limit ({max_pages}) reached."); break

        # Apply processing rules to all collected data at the end
        processed_data = self._process_extracted_data(all_extracted_data)
        self.stats['end_time'] = time.time()
        return {'data': processed_data, 'stats': self.get_stats(), 'config': self.config}
