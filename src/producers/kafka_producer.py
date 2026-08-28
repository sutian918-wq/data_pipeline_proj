from src.config import settings
from src.fetchers import Fetcher, SimulatedFetcher
from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, fetcher: Fetcher):
        self.server = settings.kafka_bootstrap_servers

    if len(chunk) == 0:
        print("No tickers in this chunk")
    for t in chunk:
        data = self.fetcher.fetch(t)
        time.sleep(random.uniform(settings.simulated_delay_min, settings.simulated_delay_max))
        self.producer.produce(settings.topic_name, json.dumps(data))
    self.producer.flush()
    
    def _delivery_report(self, err, msg):        
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message {msg.value().decode("utf-8")} delivered to {msg.topic()} [{msg.partition()}]')
tickers = settings.tickers

fetcher = SimulatedFetcher()