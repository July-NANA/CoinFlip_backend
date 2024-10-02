# binance_client.py
import aiohttp
import asyncio
import json
from config import settings
from logger import logger
from utils import retry
from data_processing import PriceData, DataProcessor
from utils import retry

class BinanceClient:
    def __init__(self, data_processor: DataProcessor):
        self.session = None
        self.data_processor = data_processor
        self.ws_url = settings.BINANCE_WS_URL
        self.api_url = settings.BINANCE_API_URL
        self.max_streams_per_connection = 1024  # Binance限制
        self.symbols = []
        self.ws_tasks = []
        self.running = False

    @retry(Exception, tries=5, delay=5, backoff=2, logger_func=logger.warning)
    async def fetch_symbols(self):
        async with self.session.get(self.api_url, timeout=settings.HTTP_TIMEOUT) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch symbols: {response.status}")
            data = await response.json()
            self.symbols = [item['symbol'] for item in data['symbols']]
            logger.info(f"Fetched {len(self.symbols)} symbols from Binance")

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.HTTP_TIMEOUT))
        await self.fetch_symbols()
        self.running = True
        self.create_ws_tasks()

    def create_ws_tasks(self):
        for i in range(0, len(self.symbols), self.max_streams_per_connection):
            batch_symbols = self.symbols[i:i+self.max_streams_per_connection]
            streams = '/'.join([f"{symbol.lower()}@ticker" for symbol in batch_symbols])
            ws_url = f"{self.ws_url}?streams={streams}"
            task = asyncio.create_task(self.ws_connection(ws_url))
            self.ws_tasks.append(task)
            logger.info(f"Created WebSocket task for {len(batch_symbols)} symbols")

    @retry(Exception, tries=5, delay=5, backoff=2, logger_func=logger.warning)
    async def ws_connection(self, ws_url):
        while self.running:
            try:
                async with self.session.ws_connect(ws_url) as ws:
                    logger.info(f"Connected to Binance WebSocket: {ws_url}")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            await self.handle_message(data.get('data'))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning(f"WebSocket closed with status {msg.type}")
                            break
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                await asyncio.sleep(5)  # 等待后重试

    async def handle_message(self, data):
        try:
            symbol = data['s']
            price = float(data['c'])
            price_data = PriceData(exchange='Binance', symbol=symbol, price=price)
            await self.data_processor.update_price_data(price_data)
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    async def stop(self):
        self.running = False
        for task in self.ws_tasks:
            task.cancel()
        await self.session.close()
        logger.info("BinanceClient stopped")
