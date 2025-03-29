import logging
import sys
import os
from datetime import datetime
from config import LOG_LEVEL

class BotFormatter(logging.Formatter):
    def __init__(self, include_timestamp=True):
        self.include_timestamp = include_timestamp
        super().__init__()

    def format(self, record):
        if self.include_timestamp:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return f"[{timestamp}] [{record.levelname}] {record.getMessage()}"
        else:
            return f"[{record.levelname}] {record.getMessage()}"

def setup_logger(level=None):
    """
    Set up the application logger with appropriate configuration.

    Args:
        level: Optional logging level override (default: from config)
    """
    log_level = level or getattr(logging, LOG_LEVEL, logging.INFO)

    for logger_name in ["httpx", "telegram", "apscheduler", "asyncio"]:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    root_logger.handlers = []

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(BotFormatter(include_timestamp=True))
    root_logger.addHandler(console_handler)

    root_logger.debug(f"Logger initialized with level {logging.getLevelName(log_level)}")