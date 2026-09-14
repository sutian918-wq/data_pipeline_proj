"""
Simple message processor - validates required fields.
"""

import logging
from src.processors import BaseProcessor

logger = logging.getLogger(__name__)

class SimpleProcessor(BaseProcessor):
    """
    Validates that incoming messages have the required fields.
    Can extend this with transformations, indicators, etc.
    """

    REQUIRED_FIELDS = {"ticker", "price", "timestamp"}

    def process(self, data: dict) -> bool:
        # Check for required fields
        missing = self.REQUIRED_FIELDS - data.keys()
        if missing:
            logger.warning(f"Message missing fields {missing}: {data}")
            return False

        if not isinstance(data['price'], (int, float)):
            logger.warning(f"Invalid price type: {data}. Expected int or float.")
            return False

        if not isinstance(data['ticker'], str):
            logger.warning(f"Invalid ticker type: {data}. Expected str.")
            return False

        return True
    



