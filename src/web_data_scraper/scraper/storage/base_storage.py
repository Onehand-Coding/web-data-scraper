"""
Abstract base class for storage handlers.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path


class BaseStorage(ABC):
    """Base class for data storage handlers."""

    def __init__(self, config: dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.output_dir = self._get_output_dir()

    def _get_output_dir(self) -> Path:
        """Get or create output directory."""
        output_dir = Path(self.config.get("output_dir", "outputs"))
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @abstractmethod
    def save(self, data: list[dict], filename: str = None) -> str:
        """Save data to storage."""
        pass


# Note: CSVStorage class has been moved to csv_handler.py
