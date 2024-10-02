# logger.py
import logging
from config import settings

def setup_logger():
    logger = logging.getLogger("arbitrage_bot")
    logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    return logger

logger = setup_logger()
