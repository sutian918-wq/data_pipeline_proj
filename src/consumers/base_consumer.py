"""
Abstract base class for all consumers.

This module defines the interface for data consumers that fetch
messages from a message broker (e.g., Kafka) and process them.
"""

from abc import ABC, abstractmethod
from threading import Event
import logging 

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    """
    Abstract base class for consumers.

    Manages lifecycle (start, stop, run) and delegates the actual
    poll-and-process logic to subclasses via `_consume_loop()`.
    """

    def __init__(self, processor, writer, config):
        """
        Initialise the consumer.

        Args:
            processor: A Processor instance
            writer: A Writer instance
            config: Settings object or dict-like config
        """
        self.processor = processor
        self.writer = writer
        self.config = config
        self.stop_event = Event()
        self._cleaned_up = False

        logger.info(f"Initialised {self.__class__.__name__}")

    @abstractmethod
    def _consume_loop(self) -> None:
        """
        The main poll-and-dispatch loop.
        Must be implemented by subclasses.
        Should exit when `self.stop_event.is_set()` becomes True.
        """
        pass

    @abstractmethod
    def _cleanup(self) -> None:
        """
        Release resources (close consumer, writer, thread pools).
        Must be implemented by subclasses.
        """
        pass

    def start(self) -> None:
        """
        Start the consumer loop.
        """
        self._consume_loop()

    def stop(self) -> None:
        """
        Signal the consumer to stop and run cleanup exactly once.
        """
        if self._cleaned_up:
            logger.debug("Consumer already stopped - skipping clean up.")
            return 

        logger.info("Stopping consumer...")
        self.stop_event.set()
        self._cleanup()
        self._cleaned_up = True
        logger.info("Consumer stopped")

    def run(self) -> None:
        """
        Run the consumer until interrupted.
        """
        try:
            self.start()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received")
        finally:
            self.stop()

        
