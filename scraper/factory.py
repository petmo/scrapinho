"""Factory function for creating scrapers."""

import logging


def create_scraper(scraper_type, config):
    """Create a scraper instance based on configuration.

    Args:
        scraper_type: Type of scraper to create ('oda' or 'meny')
        config: Configuration object

    Returns:
        Configured scraper instance
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Creating scraper of type: {scraper_type}")

    scraper_config = config.scraper

    # Common settings
    settings = {
        "base_url": scraper_config.get(scraper_type, {}).get("base_url"),
        "user_agent": scraper_config.get("user_agent"),
        "request_delay": scraper_config.get("request_delay", 1.0),
        "max_retries": scraper_config.get("max_retries", 3),
        "timeout": scraper_config.get("timeout", 30),
    }

    if scraper_type.lower() == "oda":
        from .oda_scraper import OdaScraper

        return OdaScraper(**settings)
    elif scraper_type.lower() == "meny":
        # Add Meny-specific settings
        settings.update(
            {
                "products_per_page": scraper_config.get("meny", {}).get(
                    "products_per_page", 24
                ),
                "max_pages": scraper_config.get("meny", {}).get("max_pages", 20),
            }
        )
        from .meny_scraper import MenyScraper

        return MenyScraper(**settings)
    else:
        raise ValueError(f"Unsupported scraper type: {scraper_type}")
