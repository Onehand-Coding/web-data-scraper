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
            # Generate filename based on job name if available, else timestamp
            job_name_part = "".join(c if c.isalnum() else '_' for c in self.config.get('name', 'scraped_data'))
            timestamp = int(time.time()) # <-- Use imported time
            filename = f"{job_name_part}_{timestamp}.json"

        filepath = self.output_dir / filename

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Data saved to {filepath}")
            return str(filepath)
        except Exception as e:
            self.logger.error(f"Failed to save JSON: {e}")
            raise
