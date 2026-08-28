"""
Configuration settings for the data pipeline project.
Loads settings from environment variables (.env file).
"""

from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = 'localhost:9092'
    topic_name: str = 'market_data'
    consumer_group: str = 'processors'
    producer_threads: int = 4
    consumer_threads: int = 3

    # Producer performance
    producer_batch_size: int = 16384
    producer_linger_ms: int = 10
    producer_compression_type: str = 'lz4'
    producer_queue_buffering_max_kbytes: int = 32768

    # PostgreSQL
    postgres_host: str = 'localhost'
    postgres_port: int = 5432
    postgres_db: str = 'data_pipeline'
    postgres_user: str = 'ches'
    postgres_password: str = ''

    # Fetcher
    fetcher_type: str = 'simulated'
    simulated_min_price: int = 100
    simulated_max_price: int = 500
    simulated_delay_min: float = 0.2
    simulated_delay_max: float = 1.0

    tickers_raw: str = Field(
        default='AAPL,GOOGL,MSFT,TSLA,AMZN,META,NVDA,NFLX,INTC,AMD',
        alias='TICKERS',
    )

    @property
    def tickers(self) -> List[str]:
        """Return tickers as a list of strings."""
        return [item.strip() for item in self.tickers_raw.split(',') if item.strip()]
        
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore" # ignore unknown environment variables
    )

settings = Settings()
