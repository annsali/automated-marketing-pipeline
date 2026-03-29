"""
Centralized Logging Configuration
"""

import logging
import logging.config
from pathlib import Path
from datetime import datetime

from config import LOGGING_CONFIG, LOGS_DIR


def setup_logging(log_dir: Path = None, log_level: str = "DEBUG"):
    """
    Set up centralized logging for the pipeline.
    
    Args:
        log_dir: Directory for log files (default: LOGS_DIR from config)
        log_level: Minimum logging level
    """
    if log_dir is None:
        log_dir = LOGS_DIR
    
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create daily log file
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    config = LOGGING_CONFIG.copy()
    config["handlers"]["file"]["filename"] = str(log_file)
    config["handlers"]["file"]["level"] = log_level
    
    logging.config.dictConfig(config)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name."""
    return logging.getLogger(name)
