import logging
import sys

from src.consumers import KafkaConsumer
from src.processors import SimpleProcessor
from src.writers import PostgresWriter
from src.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

def main() -> None:
    print("main called")
    logger.info("Starting consumer...")
    processor = SimpleProcessor()

    try:
        writer = PostgresWriter(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
            min_conn=1,
            max_conn=settings.consumer_threads + 2
        )
    except Exception as e:
        logger.error(f"Failed to create writer: {e}", exc_info=True) 
        sys.exit(1)

    consumer = KafkaConsumer(
        processor=processor,
        writer=writer,
        num_workers=settings.consumer_threads,
        queue_size=settings.consumer_queue_size,
        max_poll_records=settings.consumer_max_poll_records,
    )

    consumer.run()

if __name__ == "__main__":
    main()
