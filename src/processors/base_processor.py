"""
Abstract base class for message processors
"""

from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    """
    Abstract base class for message processors.

    A processor takes a message (as a dict) and performs some
    transformation, validation, or enrichment. Returns processed data on
    success, None on failure.
    """

    @abstractmethod
    def process(self, data: dict) -> dict:
        """
        Process a single message.

        Args:
            data: Parsed message data (dict)

        Returns:
            processed data on success, None otherwise
        """
        pass
