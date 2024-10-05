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
        # 从配置中读取代理设置
        self.proxy_host = settings.PROXY_HOST
        self.proxy_port = settings.PROXY_PORT
        self.proxy_type = settings.PROXY_TYPE

    @retry(Exception, tries=5, delay=5, backoff=2, logger_func=logger.warning)
    async def fetch_symbols(self):
        async with self.session.get(self.api_url, timeout=settings.HTTP_TIMEOUT) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch symbols: {response.status}")
            data = await response.json()
            self.symbols = [item['symbol'] for item in data['symbols']]
            logger.info(f"Fetched {len(self.symbols)} symbols from Binance")

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=settings.HTTP_TIMEOUT)

        # 如果配置了代理，使用 proxy 参数
        if self.proxy_host and self.proxy_port:
            proxy = f"{self.proxy_type}://{self.proxy_host}:{self.proxy_port}"
            connector = aiohttp.TCPConnector()
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                trust_env=True
            )
            await self.session.post(proxy)
        else:
            # 没有代理时直接设置超时
            self.session = aiohttp.ClientSession(timeout=timeout)
        # self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.HTTP_TIMEOUT))

        await self.fetch_symbols()
        self.running = True
        self.create_ws_tasks()

    def create_ws_tasks(self):
        for i in range(0, len(self.symbols), self.max_streams_per_connection):
            batch_symbols = self.symbols[i:i + self.max_streams_per_connection]
            streams = '/'.join([f"{symbol.lower()}@ticker" for symbol in batch_symbols])
            ws_url = f"{self.ws_url}?streams={streams}"
            logger.info(f"ws_url: {ws_url}")
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
        if self.session:
            await self.session.close()  # 关闭 session 之前检查是否为 None
            self.session = None  # 关闭后将 session 设置为 None
        logger.info("BinanceClient stopped")


# 添加一个主函数
async def main():
    logger.info("Starting BinanceClient")

    # 初始化数据处理器（假设你有 DataProcessor 的具体实现）
    data_processor = DataProcessor()

    # 初始化 BinanceClient
    client = BinanceClient(data_processor)

    try:
        # 运行start方法
        await client.start()
        logger.info("BinanceClient has started successfully")

        # 可以在这里等待一段时间，模拟 WebSocket 正在接收数据
        await asyncio.sleep(10)

    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")
    finally:
        # 停止客户端
        await client.stop()
        logger.info("BinanceClient has stopped")


# 运行主函数
if __name__ == "__main__":
    asyncio.run(main())
