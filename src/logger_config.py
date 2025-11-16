"""
Logging configuration for Irish Property Price Register ETL Pipeline.

Provides centralized logging setup with file rotation and console output.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from config import config


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_to_file: bool = True,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Configure and return a logger instance.
    
    Args:
        name: Name of the logger (typically module name)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to write logs to file
        log_to_console: Whether to write logs to console
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers if logger already configured
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    logger.propagate = False
    
    # Create formatter
    formatter = logging.Formatter(
        fmt=config.LOG_FORMAT,
        datefmt=config.LOG_DATE_FORMAT
    )
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_to_file:
        # Ensure logs directory exists
        config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Create daily log file
        log_filename = config.LOG_FILE_PATTERN.format(
            date=datetime.now().strftime("%Y-%m-%d")
        )
        log_path = config.LOGS_DIR / log_filename
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def cleanup_old_logs(retention_days: Optional[int] = None) -> None:
    """
    Remove log files older than retention period.
    
    Args:
        retention_days: Number of days to keep logs (default from config)
    """
    if retention_days is None:
        retention_days = config.LOG_RETENTION_DAYS
    
    if not config.LOGS_DIR.exists():
        return
    
    logger = logging.getLogger(__name__)
    now = datetime.now()
    
    for log_file in config.LOGS_DIR.glob("etl_pipeline_*.log"):
        try:
            # Get file modification time
            mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
            age_days = (now - mtime).days
            
            if age_days > retention_days:
                log_file.unlink()
                logger.info(f"Deleted old log file: {log_file.name} ({age_days} days old)")
        except Exception as e:
            logger.warning(f"Failed to delete old log file {log_file.name}: {e}")


def get_logger(name: str, verbose: bool = False) -> logging.Logger:
    """
    Convenience function to get a configured logger.
    
    Args:
        name: Logger name (typically __name__ from calling module)
        verbose: If True, set level to DEBUG
    
    Returns:
        Configured logger instance
    """
    level = logging.DEBUG if verbose else logging.INFO
    return setup_logger(name, level=level)


# Create a default logger for this module
logger = get_logger(__name__)
