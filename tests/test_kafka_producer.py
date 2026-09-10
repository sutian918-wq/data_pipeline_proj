from src.producers import KafkaProducer

class TestKafkaProducer:
    def test_worker(self):
        producer = KafkaProducer(None, ['A'], 1):
        
