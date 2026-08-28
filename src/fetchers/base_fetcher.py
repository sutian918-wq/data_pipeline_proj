from abc import ABC, abstractmethod

class Fetcher(ABC):
    @abstractmethod
    def fetch(self, ticker: str) -> dict:
        """
        Fetch data for a given ticker.

        Returns:
        {
            "ticker": str,
            "price": float,
            "timestamp": float
        }
        """
        pass

