from src.fetchers import Fetcher
import random
import time
from src.config import settings

class SimulatedFetcher(Fetcher):
    """
    SimulatedFetcher generates random stock prices for given tickers.
    """
    def fetch(self, ticker: str) -> dict:
        price = random.uniform(settings.simulated_min_price, settings.simulated_max_price)
        return {
            "ticker": ticker,
            "price": round(price,2),
            "timestamp": time.time()
        }
