"""
Abstract base class for data writers.
"""

from abc import ABC, abstractmethod

class BaseWriter(ABC):
    """
    Abstract base class for writing processed data to a data store.

    Implementations should handle connection pooling and batching internally.
    """

    @abstractmethod
    def insert(self, data: dict) -> None:
        """Insert a single record"""
        pass

    @abstractmethod
    def insert_batch(self, data: list[dict]) -> None:
        """Insert a batch of records"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connections and release resources"""
        pass
