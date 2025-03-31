"""Factory module for creating scraper instances."""

import logging
from typing import Dict, Any, Optional

from scraper.base_scraper import BaseScraper
from scraper.oda_scraper import OdaScraper


def get_scraper(scraper_type: str, **kwargs) -> BaseScraper:
    """Create a scraper instance of the specified type.

    Args:
        scraper_type: Type of scraper to create (e.g., "oda", "meny")
        **kwargs: Additional arguments to pass to the scraper constructor

    Returns:
        Configured scraper instance

    Raises:
        ValueError: If the scraper type is not supported
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Creating scraper of type: {scraper_type}")

    if scraper_type.lower() == "oda":
        return OdaScraper(**kwargs)
    # Add other scraper types here as they are implemented
    # elif scraper_type.lower() == "meny":
    #     return MenyScraper(**kwargs)
    else:
        raise ValueError(f"Unsupported scraper type: {scraper_type}")
