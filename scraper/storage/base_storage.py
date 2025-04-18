"""
Abstract base class for storage handlers.
"""

from abc import ABC, abstractmethod
from typing import List, Dict
import os
from pathlib import Path
import logging

class BaseStorage(ABC):
    """Base class for data storage handlers."""

    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.output_dir = self._get_output_dir()

    def _get_output_dir(self) -> Path:
        """Get or create output directory."""
        output_dir = Path(self.config.get('output_dir', 'outputs'))
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @abstractmethod
    def save(self, data: List[Dict], filename: str = None) -> str:
        """Save data to storage."""
        pass

# Note: CSVStorage class has been moved to csv_handler.py
