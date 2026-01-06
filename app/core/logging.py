"""
Logging configuration for the application.

Provides structured logging with proper formatting and log levels.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from app.core.config import get_settings


def setup_logging(log_file: Optional[str] = None) -> None:
    """
    Configure application logging.

    Args:
        log_file: Optional log file path. If None, logs to 'logs/pipeline.log'.
    """
    settings = get_settings()

    # Default log file if not provided
    if log_file is None:
        log_file = "logs/pipeline.log"

    # Create logs directory
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(funcName)s:%(lineno)d - %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode='a'),  # Append mode
        ],
    )

    # Set third-party library log levels
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Logger: Configured logger instance
    """
    return logging.getLogger(name)
