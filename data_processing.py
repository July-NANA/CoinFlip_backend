# data_processing.py
import asyncio
import datetime
import aioredis
from config import settings
from logger import logger


class PriceData:
    def __init__(self, exchange: str, symbol: str, price: float):
        self.exchange = exchange
        self.symbol = symbol
        self.price = price
        self.timestamp = datetime.datetime.utcnow().isoformat()

    def to_dict(self):
        return {
            "exchange": self.exchange,
            "price": self.price,
            "timestamp": self.timestamp
        }


class DataProcessor:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.redis = None

    async def connect_redis(self):
        self.redis = await aioredis.create_redis_pool(
            (settings.REDIS_HOST, settings.REDIS_PORT),
            db=settings.REDIS_DB,
            maxsize=settings.MAX_CONCURRENT_CONNECTIONS,
            timeout=settings.HTTP_TIMEOUT
        )
        logger.info("Connected to Redis")

    async def close_redis(self):
        self.redis.close()
        await self.redis.wait_closed()
        logger.info("Redis connection closed")

    async def update_price_data(self, price_data: PriceData):
        key = f"price:{price_data.symbol}"
        async with self.lock:
            await self.redis.hset(key, price_data.exchange, price_data.price)
            await self.redis.hset(key, "timestamp", price_data.timestamp)
            logger.debug(f"Updated price data for {price_data.symbol}")

    async def get_all_price_data(self):
        keys = await self.redis.keys("price:*")
        symbols = [key.decode().split(":")[1] for key in keys]
        price_data = {}
        for symbol in symbols:
            data = await self.redis.hgetall(f"price:{symbol}", encoding='utf-8')
            exchanges = {k: float(v) for k, v in data.items() if k != "timestamp"}
            timestamp = data.get("timestamp", None)
            price_data[symbol] = {
                "exchanges": exchanges,
                "timestamp": timestamp
            }
        return price_data
