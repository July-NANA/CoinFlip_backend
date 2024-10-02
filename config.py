# config.py
import os
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    # Binance API
    BINANCE_API_URL: str = Field(default='https://api.binance.com/api/v3/exchangeInfo')
    BINANCE_WS_URL: str = Field(default='wss://stream.binance.com:9443/stream')

    # Uniswap API
    UNISWAP_GRAPHQL_URL: str = Field(default='https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')

    # Arbitrage settings
    ARBITRAGE_THRESHOLD: float = Field(default=0.5, description="Arbitrage threshold percentage")
    DETECTION_INTERVAL: int = Field(default=1, description="Arbitrage detection interval in seconds")

    # Aiohttp settings
    HTTP_TIMEOUT: int = Field(default=10, description="HTTP request timeout in seconds")
    MAX_CONCURRENT_CONNECTIONS: int = Field(default=100, description="Maximum concurrent HTTP connections")

    # Redis settings
    REDIS_HOST: str = Field(default='localhost', description="Redis host")
    REDIS_PORT: int = Field(default=6379, description="Redis port")
    REDIS_DB: int = Field(default=0, description="Redis database number")

    # Logging
    LOG_LEVEL: str = Field(default='INFO', description="Logging level")

    class Config:
        env_file = ".env"

settings = Settings()