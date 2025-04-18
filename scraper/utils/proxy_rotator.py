"""
Proxy rotation and management utilities.
"""

import random
from typing import List, Dict, Optional
import requests
from requests.exceptions import ProxyError
import logging
from urllib.parse import urlparse

class ProxyRotator:
    """Manages rotation of proxy servers for requests."""

    def __init__(self, proxies: List[Dict] = None):
        """
        Initialize with list of proxy configurations.

        Args:
            proxies: List of proxy dicts with format:
                    {
                        'http': 'http://user:pass@host:port',
                        'https': 'https://user:pass@host:port'
                    }
        """
        self.proxies = proxies or []
        self.current_proxy = None
        self.bad_proxies = set()
        self.logger = logging.getLogger(__name__)

    def get_proxy(self) -> Optional[Dict]:
        """Get a random working proxy."""
        if not self.proxies:
            return None

        available_proxies = [
            p for p in self.proxies
            if self._proxy_key(p) not in self.bad_proxies
        ]

        if not available_proxies:
            self.logger.warning("No working proxies available")
            return None

        self.current_proxy = random.choice(available_proxies)
        return self.current_proxy

    def mark_bad(self, proxy: Dict) -> None:
        """Mark a proxy as bad (failed)."""
        proxy_key = self._proxy_key(proxy)
        self.bad_proxies.add(proxy_key)
        self.logger.warning(f"Marked proxy as bad: {proxy_key}")

    def test_proxy(self, proxy: Dict, test_url: str = 'https://www.google.com') -> bool:
        """Test if a proxy is working."""
        try:
            response = requests.get(
                test_url,
                proxies=proxy,
                timeout=10
            )
            return response.status_code == 200
        except (ProxyError, requests.RequestException) as e:
            self.logger.debug(f"Proxy test failed: {e}")
            return False

    def _proxy_key(self, proxy: Dict) -> str:
        """Get unique key for a proxy configuration."""
        http_proxy = proxy.get('http', '')
        if not http_proxy:
            return ''
        parsed = urlparse(http_proxy)
        return f"{parsed.hostname}:{parsed.port}"

    def rotate(self) -> Optional[Dict]:
        """Rotate to next available proxy."""
        return self.get_proxy()
