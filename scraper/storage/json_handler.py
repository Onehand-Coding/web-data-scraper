"""
JSON storage implementation.
"""
import time
import json
from pathlib import Path
from typing import List, Dict
from .base_storage import BaseStorage
import logging

class JSONStorage(BaseStorage):
    """JSON file storage handler."""

    def save(self, data: List[Dict], filename: str = None) -> str:
        """Save data to JSON file."""
        if not filename:
            filename = f"scraped_data_{int(time.time())}.json"

        filepath = self.output_dir / filename

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Data saved to {filepath}")
            return str(filepath)
        except Exception as e:
            self.logger.error(f"Failed to save JSON: {e}")
            raise
