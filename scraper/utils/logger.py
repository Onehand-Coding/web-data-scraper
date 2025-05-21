"""
Logging configuration and utilities.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
from typing import Optional

# --- Define Logs Directory relative to this file's parent's parent (project root) ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = PROJECT_ROOT / 'logs'

def setup_logging(
    log_filename: Optional[str] = 'scraper.log', # Default filename within logs dir
    log_dir: Path = LOGS_DIR, # Use the defined logs directory
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    level: int = logging.INFO,
    console_level: Optional[int] = None # Allow different level for console
) -> None:
    """Configure logging for the application."""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S' # Added date format
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(min(level, console_level or level)) # Set root logger to lowest level needed

    # Remove existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close() # Close handler before removing

    # Console handler
    if console_level is None:
         console_level = level # Default console level to file level

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(console_level)
    root_logger.addHandler(console_handler)

    # File handler if log filename specified
    if log_filename:
        # Ensure the log directory exists
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / log_filename

        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level) # File handler level
        root_logger.addHandler(file_handler)
        logging.info(f"Logging setup complete. File handler writing to: {log_path}")
    else:
         logging.info("Logging setup complete. Console handler only.")


# --- LoggingMixin remains the same ---
class LoggingMixin:
    """Mixin class that provides logging functionality."""
    @property
    def logger(self):
        """Return a logger named for the current class."""
        # Use self.__module__ + '.' + self.__class__.__name__ for more specific logger names
        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        if not hasattr(self, '_logger'):
            self._logger = logging.getLogger(logger_name)
        return self._logger
