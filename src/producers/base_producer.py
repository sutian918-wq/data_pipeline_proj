"""
Abstract base class for all producers.
This module defines the interface for data producers that fetch data
from a source and send it to a message broker (e.g., Kafka).
"""

from abc import ABC, abstractmethod
from src.fetchers import Fetcher
from src.config import settings
from threading import Event
from concurrent.futures import ThreadPoolExecutor, Future
import logging

logger = logging.getLogger(__name__)

class BaseProducer(ABC):
    """
    Manages a pool of worker threads, each processing a chunk of tickers.
    Subclasses must implement `_worker` and `_cleanup` methods.
    """

    # Class constants for configuration
    DEFAULT_SHUTDOWN_TIMEOUT: float = 5.0  # seconds to wait for threads to finish
    DEFAULT_POLL_INTERVAL: float = 1.0     # seconds between stop event checks

    def __init__(self, fetcher: 'Fetcher', tickers: list[str], num_threads: int, config: dict | None):
        """
        Initialise the producer.
        Args:
            fetcher (Fetcher): An instance of a Fetcher subclass.
            tickers (list): A list of stock tickers to produce data for.
            num_threads (int): Number of worker threads.
            config (dict): Configuration options for the producer (optional).
        """
        self.fetcher = fetcher
        self.tickers = tickers
        self.num_threads = num_threads
        self.config = config or {}
        # threading control
        self.stop_event = Event()
        self.executor: ThreadPoolExecutor | None

        logger.info(
            f"Initialised {self.__class__.__name__} with {len(tickers)} tickers "
            f"and {num_threads} threads"
        )

    def _chunk_tickers(self, tickers: list[str], num_threads: int) -> list[list[str]]:
        """
        Split tickers into roughly equal chunks.
        
        Example: 10 tickers, 4 threads → chunks of size 3,3,2,2
        """
        if num_threads <= 0:
            return [tickers]
        
        chunk_size = max (1, len(tickers) // num_threads)
        return [tickers[i:i+chunk_size] for i in range(0, len(tickers), chunk_size)]

    @abstractmethod
    def _worker(self, chunk: list[str]):
        """
        Worker method for each thread.
        
        This must be implemented by subclasses.
        It should fetch data and send it to the message broker.
        
        Args:
            chunk: List of tickers assigned to this worker.
        """
        pass

    @abstractmethod
    def _cleanup(self):
        """
        Cleanup method called before shutdown.
        
        Subclasses should flush buffers, close connections, etc.
        """
        pass

    def _shutdown_executor(self) -> None:
        """
        Safely shut down the thread pool executor.
        """
        if self.executor is None:
            logger.debug("No executor to shut down")
            return

        try:
            self.executor.shutdown(wait=True)
            logger.debug("Executor shut down successfully")
        except Exception as e:
            logger.error(f"Error shutting down executor: {e}")

    def start(self) -> None:
        if self.executor is not None:
            logger.warning("Producer already started")
            return
        
        chunks = self._chunk_tickers(self.tickers, self.num_threads)

        non_empty_chunks = [chunk for chunk in chunks if chunk]  # filter out empty chunks
        if not non_empty_chunks:
            logger.error("No tickers to process")
            return
        
        self.executor = ThreadPoolExecutor(max_workers=self.num_threads) 
        for chunk in non_empty_chunks:
            future = self.executor.submit(self._worker, chunk) 
            # Add exception handling for the future (optional)
            future.add_done_callback(self._handle_worker_done)

        logger.info(
            f"Started {len(non_empty_chunks)} worker threads"
            f"for {len(self.tickers)} tickers"
        )

    def _handle_worker_done(self, future: Future) -> None:
        """
        Callback invoked when a worker future completes.
        Logs any exceptions that occurred.
        """
        try:
            # This will raise if the worker raised an exception
            future.result()
        except Exception as e:
            logger.error(f"Worker thread died with exception: {e}")
            # Optionally, implement a restart mechanism here

    def stop(self) -> None:
        if self.executor is None:
            logger.debug("No executor to stop")
            self._cleanup()
            return

        logger.info("Stopping producer...")
        self.stop_event.set()  # Signal workers to exit

        # Wait for tasks to finish (they will exit when they see the event)
        self._shutdown_executor()

        # Perform any additional cleanup
        self._cleanup()

        logger.info("Producer stopped")

    def run(self) -> None:
        self.start()
        try:
            # Wait until stop is requested
            while not self.stop_event.is_set():
                # Use wait with timeout to keep responsive
                self.stop_event.wait(timeout=self.DEFAULT_POLL_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.stop()