"""
Logging utilities.
Handles logging configuration and provides a function to get a logger instance.
"""

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

@dataclass
class LogConfig:
    """Configuration for logging setup."""
    log_level: int
    log_format: str
    date_format: str
    log_file: Optional[Path] = None

    @classmethod
    def get_default_config(cls) -> 'LogConfig':
        """Create default logging configuration."""
        return cls(
            log_level=logging.INFO,
            log_format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            date_format='%Y-%m-%d %H:%M:%S'
        )

def get_logger(
    name: str,
    level: Optional[int] = None,
    log_file: Optional[Union[str, Path]] = None
) -> logging.Logger:
    """
    Configure and return a logger instance.
    
    Args:
        name: The name of the logger
        level: Optional logging level override
        log_file: Optional path to log file
        
    Returns:
        Configured logger instance
    """
    config = LogConfig.get_default_config()
    if level is not None:
        config.log_level = level
    if log_file is not None:
        config.log_file = Path(log_file)

    logger = logging.getLogger(name)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt=config.log_format,
            datefmt=config.date_format
        )
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        if config.log_file:
            try:
                config.log_file.parent.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(config.log_file)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
            except Exception as e:
                logger.error(f"Failed to create log file handler: {str(e)}")
        
        logger.setLevel(config.log_level)
        logger.propagate = False
    
    return logger