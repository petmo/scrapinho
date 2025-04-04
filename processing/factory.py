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
        from .meny_processor import MenyProcessor

        return MenyProcessor()
    else:
        raise ValueError(f"Unsupported processor type for scraper: {scraper_type}")
