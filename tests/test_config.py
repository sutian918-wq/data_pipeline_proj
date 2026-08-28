from src.config import settings

def test_config():
    print("Config loaded successfully!")
    print(f"Kafka: {settings.kafka_bootstrap_servers}")
    print(f"Tickers: {settings.tickers}")
    print(f"Fetcher: {settings.fetcher_type}")

if __name__ == "__main__":
    test_config()