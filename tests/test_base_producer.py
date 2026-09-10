"""
Unit tests for BaseProducer.

These tests verify:
- Chunking logic (even distribution, edge cases)
- Lifecycle (start, stop, run)
- Error handling (worker exceptions)
- Graceful shutdown
"""

import time
import threading
import pytest
from typing import List
from unittest.mock import Mock

from src.producers import BaseProducer
from src.fetchers import Fetcher


class TestableProducer(BaseProducer):
    """
    Minimal concrete implementation of BaseProducer for testing.

    It doesn't send anything, only records how many times the worker
    and cleanup methods were called.
    """

    def __init__(self, fetcher, tickers, num_threads, config=None):
        super().__init__(fetcher, tickers, num_threads, config)
        self.worker_calls = 0
        self.cleanup_calls = 0
        self.worker_exception = None  # Set this to force a worker exception

    def _worker(self, chunk: List[str]) -> None:
        """Record the call, optionally raise, then exit immediately."""
        self.worker_calls += 1
        if self.worker_exception:
            raise self.worker_exception
        # Do nothing – return immediately so tests are fast

    def _cleanup(self) -> None:
        """Record the call."""
        self.cleanup_calls += 1


@pytest.fixture
def fake_fetcher():
    """A dummy fetcher - never actually used in these tests."""
    return Mock(spec=Fetcher)



class TestChunkTickers:
    """Tests for _chunk_tickers."""

    def test_even_distribution(self, fake_fetcher):
        """7 tickers, 4 threads -> chunks of size 2,2,2,1"""
        producer = TestableProducer(fake_fetcher, [], 4)
        chunks = producer._chunk_tickers(list("ABCDEFG"), 4)

        assert len(chunks) == 4
        assert sum(len(c) for c in chunks) == 7
        assert chunks == [["A", "B"], ["C", "D"], ["E", "F"], ["G"]]

    def test_evenly_divisible(self, fake_fetcher):
        """8 tickers, 4 threads → chunks of size 2,2,2,2."""
        producer = TestableProducer(fake_fetcher, [], 4)
        chunks = producer._chunk_tickers(list("ABCDEFGH"), 4)

        assert chunks == [["A", "B"], ["C", "D"], ["E", "F"], ["G", "H"]]

    def test_fewer_tickers_than_threads(self, fake_fetcher):
        """3 tickers, 5 threads → 3 chunks of size 1."""
        producer = TestableProducer(fake_fetcher, [], 5)
        chunks = producer._chunk_tickers(["A", "B", "C"], 5)

        assert chunks == [["A"], ["B"], ["C"]]

    def test_empty_tickers(self, fake_fetcher):
        """Empty list -> empty list of chunks."""
        producer = TestableProducer(fake_fetcher, [], 2)
        chunks = producer._chunk_tickers([], 2)

        assert chunks == []

    def test_zero_threads_returns_single_chunk(self, fake_fetcher):
        """num_threads=0 -> one chunk with all tickers."""
        producer = TestableProducer(fake_fetcher, [], 0)
        chunks = producer._chunk_tickers(["A", "B", "C"], 0)

        assert chunks == [["A", "B", "C"]]

    def test_negative_threads_returns_single_chunk(self, fake_fetcher):
        """num_threads=-1 -> one chunk with all tickers."""
        producer = TestableProducer(fake_fetcher, [], -1)
        chunks = producer._chunk_tickers(["A", "B", "C"], -1)

        assert chunks == [["A", "B", "C"]]

    def test_single_ticker(self, fake_fetcher):
        """1 ticker, 4 threads -> 1 non-empty chunk + 3 empty."""
        producer = TestableProducer(fake_fetcher, [], 4)
        chunks = producer._chunk_tickers(["A"], 4)

        assert chunks == [["A"]]



class TestLifecycle:
    """Tests for start / stop / run."""

    def test_start_creates_executor(self, fake_fetcher):
        """start() should create the executor."""
        producer = TestableProducer(fake_fetcher, ["A", "B"], 2)
        assert producer.executor is None

        producer.start()

        assert producer.executor is not None
        producer.stop()  # clean up

    def test_start_is_idempotent(self, fake_fetcher):
        """Calling start() twice should not create a second executor."""
        producer = TestableProducer(fake_fetcher, ["A"], 1)
        producer.start()
        first_executor = producer.executor

        producer.start()  # second call – should warn and return

        assert producer.executor is first_executor
        producer.stop()

    def test_workers_are_called(self, fake_fetcher):
        """Each chunk should be processed by exactly one worker call."""
        producer = TestableProducer(fake_fetcher, list("ABCD"), 2)
        producer.start()

        # Wait until workers have run (they exit immediately)
        time.sleep(0.1)

        assert producer.worker_calls == 2  # 4 tickers / 2 threads = 2 chunks
        producer.stop()

    def test_cleanup_is_called_on_stop(self, fake_fetcher):
        """_cleanup() should be invoked when stop() runs."""
        producer = TestableProducer(fake_fetcher, ["A"], 1)
        producer.start()
        producer.stop()

        assert producer.cleanup_calls == 1

    def test_stop_without_start_calls_cleanup(self, fake_fetcher):
        """Calling stop() before start() should still call _cleanup()."""
        producer = TestableProducer(fake_fetcher, ["A"], 1)
        producer.stop()

        assert producer.cleanup_calls == 1

    def test_stop_is_idempotent(self, fake_fetcher):
        """Calling stop() twice should not raise."""
        producer = TestableProducer(fake_fetcher, ["A"], 1)
        producer.start()
        producer.stop()
        producer.stop()  # second call – should not crash

        assert producer.cleanup_calls == 1  # called twice



class TestErrorHandling:
    """Tests for worker exceptions and error handling."""

    def test_worker_exception_is_logged(self, fake_fetcher, caplog):
        """If a worker raises, the callback should log the error."""
        import logging
        caplog.set_level(logging.ERROR)

        producer = TestableProducer(fake_fetcher, ["A"], 1)
        producer.worker_exception = RuntimeError("Worker crashed!")

        producer.start()
        time.sleep(0.1)
        producer.stop()

        assert "Worker crashed!" in caplog.text
        assert "Worker thread died" in caplog.text

    def test_producer_survives_worker_exception(self, fake_fetcher):
        """A dying worker should not crash the whole producer."""
        producer = TestableProducer(fake_fetcher, ["A", "B"], 2)
        producer.worker_exception = RuntimeError("Worker crashed!")

        producer.start()
        time.sleep(0.1)

        # Producer should still be able to stop cleanly
        producer.stop()
        assert producer.cleanup_calls == 1



class TestRun:
    """Tests for run()"""

    def test_run_stops_when_event_is_set(self, fake_fetcher):
        """
        run() should block until stop_event is set.
        simulate a stop by setting the event from another thread.
        """
        producer = TestableProducer(fake_fetcher, ["A"], 1)

        def set_stop_after_delay():
            time.sleep(0.3)
            producer.stop_event.set()

        threading.Thread(target=set_stop_after_delay, daemon=True).start()

        # This should return after ~0.3 seconds
        start = time.time()
        producer.run()
        elapsed = time.time() - start

        assert elapsed < 1.5  # Give it a generous margin
        assert producer.cleanup_calls == 1
