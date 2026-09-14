"""
Abstract base class for message processors
"""

from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    """
    Abstract base class for message processors.

    A processor takes a message (as a dict) and performs some
    transformation, validation, or enrichment. Returns True on
    success, False on failure.
    """

    @abstractmethod
    def process(self, data: dict) -> bool:
        """
        Process a single message.

        Args:
            data: Parsed message data (dict)

        Returns:
            True if processing succeeded, False otherwise
        """
        pass
