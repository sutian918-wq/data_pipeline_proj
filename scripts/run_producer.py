from src.config import settings
from src.fetchers import SimulatedFetcher
from src.producers import KafkaProducer
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Start running producer")
    try:
        match settings.fetcher_type:
            case 'simulated':
                fetcher = SimulatedFetcher()
            case _:
                logger.error(f"Unknown etcher type: {settings.fetcher_type}")
                sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to create fetcher: {e}", exc_info=True)
        sys.exit(1)

    tickers = settings.tickers
    num_threads = settings.producer_threads
    logger.info(
        f"Initialising producer with {len(tickers)} tickers, "
        f"{num_threads} threads, fetcher: {settings.fetcher_type}"
    )

    try:
        producer = KafkaProducer(fetcher,tickers,num_threads)
        producer.run()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received - shutting down")
    except Exception as e:
        logger.error(f"Producer failed: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Producer finished")

if __name__ == "__main__":
    main()


        