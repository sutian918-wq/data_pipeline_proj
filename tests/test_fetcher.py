from src.fetchers import SimulatedFetcher
from src.config import settings

def test_simulated_fetcher():
    fetcher = SimulatedFetcher()
    ticker = settings.tickers[0]
    result = fetcher.fetch(ticker)

    print("-" * 50)
    print("SimulatedFetcher Test")
    print("-" * 50)
    print(f" Ticker   : {result['ticker']}")
    print(f" Price    : ${result['price']}")
    print(f" Timestamp: {result['timestamp']}")
    print("-" * 50)

    assert result['ticker'] == ticker
    assert 0 < result['price'] < 1000
    assert result['timestamp'] > 0

if __name__ == "__main__":
    test_simulated_fetcher()