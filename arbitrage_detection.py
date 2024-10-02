# arbitrage_detection.py
import asyncio
from config import settings
from data_processing import DataProcessor
from logger import logger

class ArbitrageDetector:
    def __init__(self, data_processor: DataProcessor):
        self.data_processor = data_processor
        self.threshold = settings.ARBITRAGE_THRESHOLD
        self.interval = settings.DETECTION_INTERVAL
        self.running = False
        self.task = None

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self.detect_loop())

    async def detect_loop(self):
        while self.running:
            try:
                await self.detect_arbitrage_opportunities()
                await asyncio.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error in detect_loop: {e}")
                await asyncio.sleep(5)  # 等待后重试

    async def detect_arbitrage_opportunities(self):
        price_data = await self.data_processor.get_all_price_data()
        for symbol, data in price_data.items():
            exchanges = data['exchanges']
            if len(exchanges) < 2:
                continue  # 需要至少两个交易所的数据
            prices = list(exchanges.values())
            max_price = max(prices)
            min_price = min(prices)
            price_diff_percent = (max_price - min_price) / min_price * 100
            if price_diff_percent >= self.threshold:
                # 获取对应交易所
                max_exchange = [k for k, v in exchanges.items() if v == max_price][0]
                min_exchange = [k for k, v in exchanges.items() if v == min_price][0]
                logger.info(f"Arbitrage opportunity detected for {symbol}: {price_diff_percent:.2f}% difference")
                logger.info(f"Buy on {min_exchange} at {min_price}, sell on {max_exchange} at {max_price}")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
        logger.info("ArbitrageDetector stopped")
