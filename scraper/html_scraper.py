# File: web-data-scraper/scraper/html_scraper.py (Corrected)

from typing import Dict, List, Optional, Any, Set
from bs4 import BeautifulSoup
from .base_scraper import BaseScraper
from urllib.parse import urljoin
import time
import logging

# Explicitly use html.parser
LXML_AVAILABLE = False

class HTMLScraper(BaseScraper):
    """Scraper implementation using BeautifulSoup for HTML parsing."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.selectors = config['selectors']
        self.parser = 'html.parser'
        self.pagination_config = config.get('pagination') # Store pagination config
        self.logger.info(f"Using HTML parser: {self.parser}")

    def extract_data(self, html: str, url: str) -> List[Dict]:
        """Extract data from HTML using configured selectors."""
        if not html:
            return []

        soup = BeautifulSoup(html, self.parser)
        items = []
        item_selector = self.selectors.get('item')
        field_selectors = self.selectors.get('fields', {})

        if not item_selector:
            self.logger.error("Missing 'item' selector in configuration.")
            return []

        elements = soup.select(item_selector)

        if not elements:
             self.logger.warning(f"No items found using item selector '{item_selector}' on page {url}.")
             return items

        self.logger.debug(f"Found {len(elements)} potential items using selector '{item_selector}' on {url}.")

        for i, element in enumerate(elements):
            item_data = {}
            for field, selector_config in field_selectors.items():
                value = None
                try:
                    if isinstance(selector_config, str):
                        target_element = element.select_one(selector_config)
                        value = target_element.get_text(strip=True) if target_element else None
                    elif isinstance(selector_config, dict):
                        sel = selector_config.get('selector')
                        attr = selector_config.get('attr')
                        if sel:
                            target_element = element.select_one(sel)
                            if target_element:
                                if attr:
                                     value = target_element.get(attr)
                                     if attr in ['href', 'src'] and value and not value.startswith(('http://', 'https://', '//', 'data:')):
                                         try:
                                             value = urljoin(url, value)
                                         except ValueError:
                                             self.logger.warning(f"Could not resolve relative URL '{value}' against base '{url}'")
                                else:
                                    value = target_element.get_text(strip=True)
                        else:
                             self.logger.warning(f"Selector dictionary for field '{field}' is missing 'selector' key.")
                except Exception as e:
                    self.logger.error(f"Error extracting field '{field}' with selector '{selector_config}': {e}")
                    value = None
                item_data[field] = value

            if any(v is not None for v in item_data.values()):
                items.append(item_data)
            else:
                self.logger.debug(f"Skipping item {i+1} as all extracted fields were None.")


        self.logger.info(f"Extracted {len(items)} non-empty items from {url}.")
        return items

    def _find_next_page_url(self, html: str, current_url: str) -> Optional[str]:
        """Finds the URL for the next page based on pagination config."""
        if not self.pagination_config or not html:
            return None

        next_page_selector = self.pagination_config.get('next_page_selector')
        if not next_page_selector:
            self.logger.debug("No 'next_page_selector' defined in pagination config.")
            return None

        try:
            soup = BeautifulSoup(html, self.parser)
            next_link_element = soup.select_one(next_page_selector)

            if next_link_element and next_link_element.get('href'):
                next_path = next_link_element['href']
                next_url = urljoin(current_url, next_path)
                if next_url == current_url:
                     self.logger.warning(f"Next page selector '{next_page_selector}' points back to the current URL '{current_url}'. Stopping pagination.")
                     return None
                self.logger.debug(f"Found next page link: {next_url}")
                return next_url
            else:
                self.logger.debug(f"Next page selector '{next_page_selector}' did not find a link with an href attribute.")
                return None
        except Exception as e:
            self.logger.error(f"Error finding next page link using selector '{next_page_selector}': {e}")
            return None

    def run(self) -> Dict:
        """Execute scraping job, handling pagination if configured."""
        self.stats['start_time'] = time.time() # Reset start time directly
        all_extracted_data = []
        urls_to_scrape = list(self.config.get('urls', []))
        scraped_urls: Set[str] = set()

        # Ensure pagination_config exists before accessing keys
        max_pages = float('inf')
        if self.pagination_config:
            max_pages = self.pagination_config.get('max_pages', float('inf'))
        pages_scraped_count = 0


        if not urls_to_scrape:
             self.logger.warning("No initial URLs provided in configuration.")
             self.stats['end_time'] = time.time()
             return {'data': [], 'stats': self.get_stats(), 'config': self.config}

        while urls_to_scrape and pages_scraped_count < max_pages:
            current_url = urls_to_scrape.pop(0)

            if current_url in scraped_urls:
                self.logger.debug(f"Skipping already scraped URL: {current_url}")
                continue

            self.logger.info(f"Processing URL ({pages_scraped_count + 1}/{max_pages if max_pages != float('inf') else 'unlimited'}): {current_url}")
            # Pass url to extract_data for context
            html = self.fetch_page(current_url)
            scraped_urls.add(current_url)

            if html:
                # Pass url to extract_data
                page_data = self.extract_data(html, current_url)
                all_extracted_data.extend(page_data)
                pages_scraped_count += 1 # Increment count only if page was successfully processed

                if pages_scraped_count < max_pages:
                     # Pass url to _find_next_page_url
                     next_page_url = self._find_next_page_url(html, current_url)
                     if next_page_url and next_page_url not in scraped_urls and next_page_url not in urls_to_scrape:
                         self.logger.info(f"Adding next page to queue: {next_page_url}")
                         urls_to_scrape.append(next_page_url)
                     elif next_page_url:
                          self.logger.debug(f"Next page URL already visited or queued: {next_page_url}")

            if pages_scraped_count >= max_pages:
                 self.logger.info(f"Reached maximum page limit ({max_pages}). Stopping pagination.")
                 break

        processed_data = self._process_extracted_data(all_extracted_data)
        self.stats['end_time'] = time.time()

        return {
            'data': processed_data,
            'stats': self.get_stats(),
            'config': self.config
        }
