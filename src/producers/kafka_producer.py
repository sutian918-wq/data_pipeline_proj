from src.config import settings
from src.fetchers import Fetcher
from confluent_kafka import Producer
from src.producers import BaseProducer
import logging
import json
import time

logger = logging.getLogger(__name__)

class KafkaProducer(BaseProducer):
    def __init__(self, fetcher: 'Fetcher', tickers: list[str], num_threads: int, config: dict | None = None):
        super().__init__(fetcher, tickers, num_threads, config)
        kafka_conf = {
            'bootstrap.servers': settings.kafka_bootstrap_servers, 
            'queue.buffering.max.kbytes': settings.producer_queue_buffering_max_kbytes, # 32 MB max buffer size
            'batch.size': settings.producer_batch_size, # 16 KB max per batch, larger means fewer network requests
            'linger.ms': settings.producer_linger_ms, # the max time producer will wait before sending a batch of messages
            'compression.type': settings.producer_compression_type, # save bandwidth and improve throughput
            'on_delivery': self._delivery_report,
        }
        final_conf = {**kafka_conf, **(config or {})}
        self.producer = Producer(final_conf)

        self.topic = settings.topic_name

        self.message_count = 0
        self.error_count = 0
        
        logger.info(
            f"Kafka Producer initialised with {len(tickers)} tickers, "
            f"{num_threads} threads, topic: {self.topic}"
        )

    def _worker(self, chunk: list[str]) -> None:
        logger.info(f"Worker started for {len(chunk)} tickers: {chunk}")
        while not self.stop_event.is_set():
            try:
                for ticker in chunk:
                    data = self.fetcher.fetch(ticker)
                    message = json.dumps(data).encode('utf-8')
                    self.producer.produce(
                        topic=self.topic, 
                        value=message,
                        key=ticker.encode('utf-8')
                    )
                    # controls message rate
                    time.sleep(0.00004)
                # Process delivery callbacks without blocking
                self.producer.poll(0)

            except BufferError as e:
                # Handle buffer full
                logger.warning(f"Producer buffer full, waiting... ({e})")
                self.producer.poll(0)
                time.sleep(0.1)  # wait a bit before retrying
                continue  # retry the same ticker

            except Exception as e:
                logger.error(f"Tick failed: {e} for ticker {ticker}", exc_info=True)
                time.sleep(0.1)  # wait a bit before retrying

        logger.info(f"Worker stopping for tickers: {chunk}")

    def _delivery_report(self, err, msg):      
        if err is not None:
            self.error_count += 1
            logger.error(
                f"Message delivery failed: {err}"
                f"(topic: {msg.topic()}, partition: {msg.partition()})"
            )
        else:
            self.message_count += 1
            if self.message_count % 100000 == 0:
                logger.info(
                    f"Delivered {self.message_count} messages. "
                    f"Last: {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
                )

    def _cleanup(self) -> None:
        logger.info("Flushing producer...")
        try:
            remaining = self.producer.flush()
            if remaining > 0:
                logger.warning(f"Flush completed with {remaining} messages remained")
            else:
                logger.info(f"Flush completed successfully.")
        except Exception as e:
            logger.error(f"Error during flush: {e}")

        logger.info(
            f"Final stats: {self.message_count} messages delivered, "
            f"{self.error_count} errors"
        )

