"""Factory function for creating product processors."""

import logging
from typing import List

from models.product import Product


def get_processor(scraper_type):
    """Get the appropriate processor for the scraper type.

    Args:
        scraper_type: Type of scraper ('oda' or 'meny')

    Returns:
        Processor instance
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Creating processor for scraper type: {scraper_type}")

    if scraper_type.lower() == "oda":
        from .oda_processor import OdaProcessor

        return OdaProcessor()
    elif scraper_type.lower() == "meny":
        # If you have a MenyProcessor class, use it here
        # For now, we'll use OdaProcessor as a fallback
        logger.warning(
            f"No specific processor for {scraper_type}, using OdaProcessor as fallback"
        )
        from .oda_processor import OdaProcessor

        return OdaProcessor()
    else:
        raise ValueError(f"Unsupported processor type for scraper: {scraper_type}")
