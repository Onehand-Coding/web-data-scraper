"""
API-based data collection implementation.
"""

import requests
from typing import Dict, List, Optional
from .base_scraper import BaseScraper
import json

class APIScraper(BaseScraper):
    """Scraper implementation for API endpoints."""

    def __init__(self, config: Dict):
        super().__init__(config)
        self.api_config = config.get('api_config', {})

    def fetch_data(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Fetch data from API endpoint."""
        self.throttle_requests()
        url = f"{self.api_config['base_url']}{endpoint}"

        try:
            response = self.session.request(
                method=self.api_config.get('method', 'GET'),
                url=url,
                params=params,
                headers=self.api_config.get('headers', {}),
                json=self.api_config.get('body', None)
            )
            response.raise_for_status()
            self.stats['pages_scraped'] += 1
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            self.stats['requests_failed'] += 1
            return None

    def extract_data(self, response: Dict) -> List[Dict]:
        """Extract and transform API response data."""
        try:
            items = []
            data = response

            # Handle nested data paths if specified
            if 'data_path' in self.api_config:
                for path in self.api_config['data_path'].split('.'):
                    data = data.get(path, {})

            if isinstance(data, list):
                items = data
            else:
                items = [data]

            # Apply field mappings if specified
            if 'field_mappings' in self.api_config:
                mapped_items = []
                for item in items:
                    mapped_item = {}
                    for target_field, source_field in self.api_config['field_mappings'].items():
                        mapped_item[target_field] = item.get(source_field)
                    mapped_items.append(mapped_item)
                items = mapped_items

            self.stats['items_collected'] += len(items)
            return items
        except Exception as e:
            self.logger.error(f"API data extraction failed: {e}")
            return []

    def run(self) -> Dict:
        """Execute API scraping job."""
        all_data = []

        for endpoint in self.api_config.get('endpoints', ['']):
            params = self.api_config.get('params', {})
            response = self.fetch_data(endpoint, params)

            if response:
                page_data = self.extract_data(response)
                all_data.extend(page_data)
                self.logger.info(f"Collected {len(page_data)} items from {endpoint}")

        return {
            'data': all_data,
            'stats': self.get_stats(),
            'config': self.config
        }
