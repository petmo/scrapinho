"""Base processor interface for product attribute extraction."""

import logging
import re
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
import pandas as pd

from models.product import Product


class BaseProcessor(ABC):
    """Abstract base class for product processors.

    This class defines the interface that all product processors must follow.
    """

    def __init__(self):
        """Initialize the processor."""
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def process_product(self, product: Product) -> Product:
        """Process a single product to extract and add attributes.

        Args:
            product: The product to process

        Returns:
            Processed product with additional attributes
        """
        pass

    def process_products(self, products: List[Product]) -> List[Product]:
        """Process multiple products.

        Args:
            products: List of products to process

        Returns:
            List of processed products
        """
        self.logger.info(f"Processing {len(products)} products")
        processed_products = []

        for product in products:
            try:
                processed_product = self.process_product(product)
                processed_products.append(processed_product)
            except Exception as e:
                self.logger.error(
                    f"Error processing product {product.product_id}: {e}", exc_info=True
                )
                processed_products.append(product)  # Keep original product on error

        self.logger.info(f"Successfully processed {len(processed_products)} products")
        return processed_products

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process a DataFrame of products.

        Args:
            df: DataFrame containing product data

        Returns:
            Processed DataFrame with additional attributes
        """
        self.logger.info(f"Processing DataFrame with {len(df)} rows")

        # Implementation will be provided by concrete subclasses
        return df

    def clean_text(self, text: str) -> str:
        """Clean and normalize text for processing.

        Args:
            text: Input text to clean

        Returns:
            Cleaned text
        """
        if not text or not isinstance(text, str):
            return ""

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        # Normalize commas, semicolons
        text = text.replace(";", ",")

        # Remove unwanted characters
        text = re.sub(r'["\']', "", text)

        return text
