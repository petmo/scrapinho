"""Oda-specific pipeline for scraping, processing, and storing product data."""

import logging
from typing import List, Dict, Any, Optional

from scraper.oda_scraper import OdaScraper
from storage.base_storage import BaseStorage
from processing.oda_processor import OdaProcessor
from models.product import Product
from pipelines.base_pipeline import BasePipeline


class OdaPipeline(BasePipeline):
    """Pipeline for orchestrating Oda scraping, processing, and storage."""

    def __init__(self, scraper: OdaScraper, storage: BaseStorage):
        """Initialize the Oda pipeline.

        Args:
            scraper: Oda scraper instance
            storage: Storage backend to save the data
        """
        super().__init__(scraper, storage)
        self.processor = OdaProcessor()
        self.logger = logging.getLogger(__name__)

    def process_products(self, products: List[Product]) -> List[Product]:
        """Process the scraped products using the Oda processor.

        Args:
            products: List of products to process

        Returns:
            Processed products
        """
        return self.processor.process_products(products)

    @classmethod
    def create_from_config(
        cls, config: Dict[str, Any], category_url: Optional[str] = None
    ) -> "OdaPipeline":
        """Create an OdaPipeline instance from configuration.

        Args:
            config: Configuration dictionary
            category_url: Optional override for category URL

        Returns:
            Configured OdaPipeline instance
        """
        from scraper import get_scraper
        from storage import get_storage

        # Initialize scraper
        scraper_config = config.get("scraper", {})
        oda_config = scraper_config.get("oda", {})

        scraper_settings = {
            "base_url": oda_config.get("base_url", "https://oda.com"),
            "user_agent": scraper_config.get("user_agent"),
            "request_delay": scraper_config.get("request_delay", 1.5),
            "max_retries": scraper_config.get("max_retries", 3),
            "timeout": scraper_config.get("timeout", 30),
        }

        scraper = get_scraper("oda", **scraper_settings)

        # Initialize storage
        storage_config = config.get("storage", {})
        storage_type = storage_config.get("type", "csv")
        storage_settings = storage_config.get(storage_type, {})
        storage = get_storage(storage_type, **storage_settings)

        # Create and return the pipeline
        return cls(scraper, storage)


def run_oda_pipeline(
    config: Dict[str, Any],
    category_url: Optional[str] = None,
    max_products: Optional[int] = None,
) -> List[Product]:
    """Run the Oda pipeline.

    Args:
        config: Configuration dictionary
        category_url: URL of the product category to scrape (overrides config)
        max_products: Maximum number of products to scrape

    Returns:
        List of processed products
    """
    pipeline = OdaPipeline.create_from_config(config)

    # Determine category URL
    if not category_url:
        oda_config = config.get("scraper", {}).get("oda", {})
        categories = oda_config.get("categories", [])
        if not categories:
            raise ValueError("No category URL provided and none found in config")
        category = categories[0]
        category_url = category.get("url")

    # Ensure category URL is absolute
    if category_url and not category_url.startswith(("http://", "https://")):
        base_url = (
            config.get("scraper", {}).get("oda", {}).get("base_url", "https://oda.com")
        )
        category_url = f"{base_url}{category_url}"

    # Run the pipeline
    return pipeline.run_pipeline(category_url, max_products)
