# logger_config.py
import logging
import sys

# Custom formatter without timestamps
class SimpleFormatter(logging.Formatter):
    def format(self, record):
        return f"{record.getMessage()}"

# Configure root logger
def setup_logger():
    # Disable all external loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    root_logger.handlers = []
    
    # Add console handler with simple formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(SimpleFormatter())
    root_logger.addHandler(console_handler)