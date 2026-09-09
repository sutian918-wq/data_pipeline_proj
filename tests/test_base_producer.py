import logging 
import threading 
import time
from src.producers import BaseProducer
from concurrent.futures import Future

class TestableProducer(BaseProducer):
    """Minimal concrete producer for testing BaseProducer"""
    def __init__(self, fetcher, tickers, num_threads, config = None):
        super().__init__(fetcher, tickers, num_threads, config)
        self.worker_call_count = 0
        self.cleanup_call_count = 0
        self.worker_should_loop = False

    def _worker(self, chunk):
        """Mock worker"""
        self.worker_call_count += 1

        # If simulating a looping worker
        if self.worker_should_loop:
            while not self.stop_event.is_set():
                time.sleep(0.01)


    def _cleanup(self):
        """Mock cleanup"""
        self.cleanup_call_count += 1

class TestBaseProducer:
    """Test suite for BaseProducer"""

    def test_chunk_tickers(self):
        producer = TestableProducer(None, [], 0)

        # 7 tickers, 4 threads 
        chunks = producer._chunk_tickers(['A', 'B', 'C', 'D', 'E', 'F', 'G'], 4)
        assert len(chunks) == 4
        assert sum(len(c) for c in chunks) == 7

        # Fewer tickers than threads
        chunks = producer._chunk_tickers(['A', 'B', 'C'], 5)
        assert len(chunks) == 5

        # Empty tickers
        chunks = producer._chunk_tickers([], 2)
        assert chunks == []

        # 0 threads
        # Fewer tickers than threads
        chunks = producer._chunk_tickers(['A', 'B', 'C'], 0)
        assert len(chunks) == 1

    def test_start_creates_executor(self):
        """Check if start creates an executor"""
        producer = TestableProducer(None, ['A', 'B'], 2)
        assert producer.executor is None

        producer.start()
        assert producer.executor is not None
        producer.stop()

    def test_start_idempotent(self):
        """Check if starting a second producer creates a new executor"""
        producer = TestableProducer(None, ['A', 'B'], 2)
        assert producer.executor is None

        producer.start()
        first_executor = producer.executor

        producer.start()
        assert first_executor is producer.executor
        producer.stop()

    def test_start_with_no_tickers(self, caplog):
        producer = TestableProducer(None, [], 2)
        # context manager to capture ERROR and above logs
        with caplog.at_level(logging.ERROR):
            producer.start()

        assert "No tickers to process" in caplog.text
        assert producer.executor is None

    def test_stop_sets_event_and_cleanup(self):
        producer = TestableProducer(None, ['A', 'B'], 2)
        producer.start()

        assert not producer.stop_event.is_set()
        assert producer.cleanup_call_count == 0

        producer.stop()
        assert producer.stop_event.is_set()
        assert producer.cleanup_call_count == 1

    def test_stop_wout_start(self, caplog):
        producer = TestableProducer(None, ['A', 'B'], 2)
        assert producer.executor is None

        with caplog.at_level(logging.DEBUG):
            producer.stop()

        assert "No executor to stop" in caplog.text
        assert producer.cleanup_call_count == 1

    def test_stop_calls_cleanup_once(self):
        producer = TestableProducer(None, ['A', 'B'], 2)
        producer.start()

        producer.stop()
        first_cleanup_count = producer.cleanup_call_count

        producer.stop()
        # Ensure cleanup is not called again
        assert producer.cleanup_call_count == first_cleanup_count

    def test_run_handles_keyboard_interrupt(self):
        producer = TestableProducer(None, ['A', 'B'], 2)
        # tell the worker to loop and so the run() blocks
        producer.worker_should_loop = True
        
        def run_producer():
            producer.run()

        thread = threading.Thread(target=run_producer)
        thread.start()

        # let the producer start up
        time.sleep(0.1)

        # simulate Ctrl+C
        producer.stop_event.set()

        thread.join(timeout=2)

        assert producer.cleanup_call_count == 1
        
    def test_handle_worker_done_logs_exception(self, caplog):
        producer = TestableProducer(None,['A'],1)

        future = Future()
        future.set_exception(RuntimeError("Worker crashed"))

        with caplog.at_level(logging.ERROR):
            producer._handle_worker_done(future)

        assert "Worker thread died with exception" in caplog.text
        assert "Worker crashed" in caplog.text

    def test_handle_worker_done_success_no_log(self, caplog):
        producer = TestableProducer(None, ['A'], 1)

        future = Future()
        future.set_result(None)

        with caplog.at_level(logging.INFO):
            producer._handle_worker_done(future)
        
        assert "Worker thread died" not in caplog.text


