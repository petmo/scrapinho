"""Enhanced logging configuration for the Oda scraper."""

import logging
import sys
from pathlib import Path
from typing import Optional


class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages."""

    # ANSI color codes
    COLORS = {
        "RESET": "\033[0m",
        "BOLD": "\033[1m",
        "DEBUG": "\033[38;5;245m",  # Gray
        "INFO": "\033[38;5;39m",  # Blue
        "WARNING": "\033[38;5;208m",  # Orange
        "ERROR": "\033[38;5;196m",  # Red
        "CRITICAL": "\033[48;5;196m\033[38;5;231m",  # White on Red
        "NAME": "\033[38;5;85m",  # Light green for logger name
        "TIME": "\033[38;5;240m",  # Dark gray for timestamp
    }

    def format(self, record):
        """Format the log record with colors."""
        # Save original values to restore them later
        orig_levelname = record.levelname
        orig_name = record.name
        orig_msg = record.msg

        # Apply colors
        record.levelname = f"{self.COLORS[record.levelname]}{record.levelname:^8}{self.COLORS['RESET']}"
        record.name = f"{self.COLORS['NAME']}{record.name}{self.COLORS['RESET']}"

        # Colorize the message based on level
        record.msg = f"{self.COLORS[orig_levelname]}{record.msg}{self.COLORS['RESET']}"

        # Call the original formatter
        result = super().format(record)

        # Restore original values
        record.levelname = orig_levelname
        record.name = orig_name
        record.msg = orig_msg

        return result


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Set up enhanced logging configuration.

    Args:
        level: Logging level
        log_file: Path to log file, if specified
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set up console handler with colored output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Use colored formatter for console
    colored_format = "%(asctime)s │ %(levelname)s │ %(name)s │ %(message)s"
    console_handler.setFormatter(ColoredFormatter(colored_format))
    root_logger.addHandler(console_handler)

    # Set up file handler if specified
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)

        # Use standard formatter for file
        standard_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        file_handler.setFormatter(logging.Formatter(standard_format))
        root_logger.addHandler(file_handler)
