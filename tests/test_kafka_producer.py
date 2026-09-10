from unittest.mock import Mock, patch
import pytest
import threading
import time
from src.fetchers import Fetcher, SimulatedFetcher
from src.producers import KafkaProducer
from testcontainers.kafka import RedpandaContainer
from confluent_kafka import Consumer

@pytest.fixture
def fake_fetcher():
    """
    A fake fetcher that returns predictable data.
    Using Mock(spec=Fetcher) ensures the mock only accepts methods that Fetcher actually defines.
    """
    fetcher = Mock(spec=Fetcher)
    fetcher.fetch.return_value = {
        "ticker": "AAPL",
        "price": 150.0,
        "timestamp": 1234567890.0,
    }
    return fetcher

@pytest.fixture
def mock_kafka_client():
    """
    Patch the KafkaProducerClient so no real Kafka broker is needed.

    Yields the patched class, so {mock_kafka_client.return_value} is the mocked Producer instance created inside KafkaProducer.
    """
    with patch('src.producers.kafka_producer.Producer') as mock:
        yield mock

@pytest.fixture
def producer(fake_fetcher, mock_kafka_client):
    """
    A KafkaProducer instance configured with fakes.
    Automatically cleaned up after each test.
    """
    p = KafkaProducer(
        fetcher=fake_fetcher,
        tickers=["AAPL", "GOOGL"],
        num_threads=2,
    )
    yield p
    # Ensure cleanup runs even if the test fails
    if not p._cleaned_up:
        try:
            p.stop()
        except Exception:
            pass


def test_worker_calls_produce(fake_fetcher, mock_kafka_client):
    """_worker should call produce() with the right topic and key."""
    # Create the producer
    producer = KafkaProducer(
        fetcher=fake_fetcher,
        tickers=['AAPL'],
        num_threads=1
    )

    # Get the mocked producer instance
    mock_producer_instance = mock_kafka_client.return_value

    # Run _worker in a background thread and can stop it after a moment
    worker_thread = threading.Thread(
        target=producer._worker,
        args=(['AAPL'],),
        daemon=True,
    )
    worker_thread.start()

    time.sleep(0.2)

    producer.stop_event.set()
    worker_thread.join(timeout=2)

    assert mock_producer_instance.produce.called
    _, kwargs = mock_producer_instance.produce.call_args
    assert kwargs['topic'] == 'market_data'
    assert kwargs['key'] == b'AAPL'

def test_delivery_report_increments_count(fake_fetcher, mock_kafka_client):
    producer = KafkaProducer(
        fetcher=fake_fetcher,
        tickers=["AAPL"],
        num_threads=1,
    )
    
    # Create a fake message
    mock_msg = Mock()
    mock_msg.topic.return_value = "market_data"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 1
    
    # Call the callback with no error
    producer._delivery_report(err=None, msg=mock_msg)
    
    # Assert: message_count incremented
    assert producer.message_count == 1
    assert producer.error_count == 0

def test_delivery_report_handles_error(fake_fetcher, mock_kafka_client):
    producer = KafkaProducer(
        fetcher=fake_fetcher,
        tickers=["AAPL"],
        num_threads=1,
    )
    
    mock_msg = Mock()
    mock_msg.topic.return_value = "market_data"
    mock_msg.partition.return_value = 0
    
    # Call with an error
    producer._delivery_report(err="Some error", msg=mock_msg)
    
    # Assert: error_count incremented
    assert producer.error_count == 1
    assert producer.message_count == 0

def test_worker_handles_buffer_error(fake_fetcher, mock_kafka_client):
    """Worker should not crash when produce() raises BufferError."""
    producer = KafkaProducer(
        fetcher=fake_fetcher,
        tickers=["AAPL"],
        num_threads=1,
    )
    
    # Make produce() raise BufferError
    mock_producer_instance = mock_kafka_client.return_value
    mock_producer_instance.produce.side_effect = BufferError("Buffer full")
    
    worker_thread = threading.Thread(
        target=producer._worker,
        args=(['AAPL'],),
        daemon=True
    )
    worker_thread.start()
    time.sleep(0.2)

    producer.stop_event.set()
    worker_thread.join(timeout=2)
    
    # produce() was called, and poll() was called (as recovery)
    assert mock_producer_instance.produce.called
    assert mock_producer_instance.poll.called

def test_cleanup_calls_flush(producer, mock_kafka_client):
    """_cleanup should call producer.flush()."""
    producer._cleanup()
    
    mock_producer_instance = mock_kafka_client.return_value
    mock_producer_instance.flush.assert_called_once()

def test_stop_calls_cleanup_and_shutdown(producer, mock_kafka_client):
    producer.start()
    producer.stop()

    mock_producer_instance = mock_kafka_client.return_value
    mock_producer_instance.flush.assert_called_once()
    assert producer._cleaned_up is True

def test_stop_is_idempotent(producer, mock_kafka_client):
    producer.start()
    producer.stop()
    producer.stop()  # Second call should do nothing
    
    mock_producer_instance = mock_kafka_client.return_value
    assert mock_producer_instance.flush.call_count == 1


@pytest.fixture(scope="session")
def kafka_broker():
    """Start a real kafka container for test"""
    # Redpanda is a Kafka-compatible streaming data platform written in C++
    # It has much lower latency than kafka cuz 
    ## - written in C++ does not suffer from JVM's garbage collector's periodic stop to clean up memory
    ## - uses thread-per-core architechture (Seastar framework) which pins each CPU core to a dedicated thread and uses asynchronous message passing between cores instead of locks. This eliminates context switching and lock contention, making better use of modern multi-core hardware
    # Kafka written in java
    with RedpandaContainer() as kafka:
        yield kafka.get_bootstrap_server()


# Integration test using a real Redpanda broker.
# These tests are slow and require Docker. Run them with:
# pytest tests/test_kafka_producer.py -v -m integration
@pytest.mark.integration
def test_producer_send_to_real_kafka(kafka_broker):
    """Create a producer pointing to test broker"""
    fetcher = SimulatedFetcher()
    producer = KafkaProducer(
        fetcher=fetcher,
        tickers=["AAPL"],
        num_threads=1,
        config={"bootstrap.servers": kafka_broker}
    )

    producer.start()
    time.sleep(1)
    producer.stop()

    consumer = Consumer({
        'bootstrap.servers': kafka_broker,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['market_data'])

    msg = consumer.poll(timeout=5.0)
    assert msg is not None
    assert b'AAPL' in msg.value()
    consumer.close()
