# uniswap_client.py
import asyncio
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from config import settings
from logger import logger
from utils import retry
from data_processing import PriceData, DataProcessor

class UniswapClient:
    def __init__(self, data_processor: DataProcessor):
        self.transport = AIOHTTPTransport(url=settings.UNISWAP_GRAPHQL_URL)
        self.client = Client(transport=self.transport, fetch_schema_from_transport=True)
        self.data_processor = data_processor
        self.batch_size = 1000  # The Graph默认最大1000
        self.running = False
        self.task = None

    @retry(Exception, tries=5, delay=5, backoff=2, logger_func=logger.warning)
    async def fetch_all_pairs(self):
        pairs = []
        query_template = gql("""
        query ($first: Int!, $skip: Int!) {
          pairs(first: $first, skip: $skip) {
            id
            token0Price
            token1Price
            token0 {
              symbol
            }
            token1 {
              symbol
            }
          }
        }
        """)
        skip = 0
        while True:
            variables = {"first": self.batch_size, "skip": skip}
            result = await self.client.execute_async(query_template, variable_values=variables)
            batch = result.get('pairs', [])
            if not batch:
                break
            pairs.extend(batch)
            logger.info(f"Fetched {len(batch)} pairs from Uniswap")
            skip += self.batch_size
        logger.info(f"Total Uniswap pairs fetched: {len(pairs)}")
        return pairs

    async def start(self):
        self.running = True
        self.task = asyncio.create_task(self.periodic_fetch())

    async def periodic_fetch(self):
        while self.running:
            try:
                pairs = await self.fetch_all_pairs()
                await self.process_pairs(pairs)
                await asyncio.sleep(settings.DETECTION_INTERVAL)  # 根据需要调整间隔
            except Exception as e:
                logger.error(f"Error in periodic_fetch: {e}")
                await asyncio.sleep(5)  # 等待后重试

    async def process_pairs(self, pairs):
        for pair in pairs:
            try:
                token0_symbol = pair['token0']['symbol']
                token1_symbol = pair['token1']['symbol']
                token0_price = float(pair['token0Price'])
                token1_price = float(pair['token1Price'])

                # 假设我们关注某一侧的价格，这里以token0Price为例
                price_data0 = PriceData(exchange='Uniswap', symbol=token0_symbol, price=token0_price)
                await self.data_processor.update_price_data(price_data0)

                price_data1 = PriceData(exchange='Uniswap', symbol=token1_symbol, price=token1_price)
                await self.data_processor.update_price_data(price_data1)
            except Exception as e:
                logger.error(f"Error processing pair {pair.get('id')}: {e}")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
        await self.client.transport.close()
        logger.info("UniswapClient stopped")
