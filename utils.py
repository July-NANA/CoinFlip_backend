# utils.py
import asyncio
import functools
from logger import logger

def retry(exceptions, tries=4, delay=3, backoff=2, logger_func=None):
    """
    Retry decorator with exponential backoff.
    """
    def decorator_retry(func):
        @functools.wraps(func)
        async def wrapper_retry(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    msg = f"{func.__name__} failed with {e}, retrying in {_delay} seconds..."
                    if logger_func:
                        logger_func(msg)
                    else:
                        logger.warning(msg)
                    await asyncio.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)
        return wrapper_retry
    return decorator_retry
