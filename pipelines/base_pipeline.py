"""Base pipeline interface for scraping, processing, and storing data."""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from models.product import Product
from scraper.base_scraper import BaseScraper
from storage.base_storage import BaseStorage


class BasePipeline(ABC):
    """Abstract base class for data pipelines.

    This class defines the interface that all pipeline implementations must follow
    and provides common functionality for orchestrating scraping, processing, and
    storage operations.
    """

    def __init__(self, scraper: BaseScraper, storage: BaseStorage):
        """Initialize the base pipeline.

        Args:
            scraper: Scraper instance
            storage: Storage backend
        """
        self.scraper = scraper
        self.storage = storage
        self.logger = logging.getLogger(__name__)

    def run_pipeline(
        self, category_url: str, max_products: Optional[int] = None
    ) -> List[Product]:
        """Run the complete pipeline.

        Args:
            category_url: URL of the category to scrape
            max_products: Maximum number of products to scrape

        Returns:
            List of processed products
        """
        self.logger.info(f"Starting pipeline for category URL: {category_url}")

        # Step 1: Initialize storage
        self.logger.info("Initializing storage")
        self.storage.initialize()

        try:
            # Step 2: Scrape products
            self.logger.info(f"Scraping products from {category_url}")
            products = self.scraper.get_products(category_url, max_products)
            self.logger.info(f"Scraped {len(products)} products")

            # Step 3: Process products
            self.logger.info("Processing products")
            processed_products = self.process_products(products)
            self.logger.info(f"Processed {len(processed_products)} products")

            # Step 4: Store products
            self.logger.info("Storing products")
            success = self.storage.save_products(processed_products)
            if success:
                self.logger.info("Successfully saved products")
            else:
                self.logger.error("Failed to save products")

            return processed_products
        except Exception as e:
            self.logger.error(f"Pipeline encountered an error: {e}", exc_info=True)
            return []
        finally:
            # Close resources
            self.logger.info("Closing pipeline resources")
            self.scraper.close()
            self.storage.close()

    @abstractmethod
    def process_products(self, products: List[Product]) -> List[Product]:
        """Process the scraped products.

        Args:
            products: List of products to process

        Returns:
            Processed products
        """
        pass

    @classmethod
    @abstractmethod
    def create_from_config(
        cls, config: Dict[str, Any], category_url: Optional[str] = None
    ) -> "BasePipeline":
        """Create a pipeline instance from configuration.

        Args:
            config: Configuration dictionary
            category_url: Optional override for category URL

        Returns:
            Configured pipeline instance
        """
        pass
