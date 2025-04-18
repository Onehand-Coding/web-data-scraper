"""
CSV storage implementation.
"""

import pandas as pd
import time
from pathlib import Path
from typing import List, Dict
from .base_storage import BaseStorage
import logging

class CSVStorage(BaseStorage):
    """CSV storage handler."""

    def save(self, data: List[Dict], filename: str = None) -> str:
        """Save data to CSV file."""
        if not filename:
            # Generate filename based on job name if available, else timestamp
            job_name = self.config.get('name', 'scraped_data').replace(' ', '_').lower()
            timestamp = int(time.time())
            filename = f"{job_name}_{timestamp}.csv"

        filepath = self.output_dir / filename

        if not data:
            self.logger.warning("No data provided to save.")
            # Create an empty file with headers if possible, or just return path
            try:
                # Attempt to get headers from config if data is empty
                headers = list(self.config.get('selectors', {}).get('fields', {}).keys())
                if headers:
                    pd.DataFrame(columns=headers).to_csv(filepath, index=False, encoding='utf-8')
                    self.logger.info(f"Empty CSV with headers saved to {filepath}")
                else:
                    # Create a completely empty file
                    open(filepath, 'a').close()
                    self.logger.info(f"Empty CSV file created at {filepath}")
                return str(filepath)
            except Exception as e:
                 self.logger.error(f"Failed to create empty CSV: {e}")
                 raise

        try:
            df = pd.DataFrame(data)
            # Ensure consistent column order based on the first item's keys
            df = df[list(data[0].keys())]
            df.to_csv(filepath, index=False, encoding='utf-8')
            self.logger.info(f"Data saved to {filepath}")
            return str(filepath)
        except Exception as e:
            self.logger.error(f"Failed to save CSV: {e}")
            raise
