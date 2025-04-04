"""Base storage interface for the grocery scraper."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

from models.product import Product


class BaseStorage(ABC):
    """Abstract base class for storage backends.

    This class defines the interface that all storage implementations must follow.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the storage backend.

        This method should be called before any other methods to set up
        the storage backend (create tables, files, etc.)
        """
        pass

    @abstractmethod
    def save_product(self, product: Product, replace_existing: bool = False) -> bool:
        """Save a single product to the storage backend.

        Args:
            product: The product to save
            replace_existing: Whether to replace existing product with same ID

        Returns:
            True if the product was saved successfully, False otherwise
        """
        pass

    @abstractmethod
    def save_products(
        self, products: List[Product], replace_existing: bool = False
    ) -> bool:
        """Save multiple products to the storage backend.

        Args:
            products: The list of products to save
            replace_existing: Whether to replace existing products with same IDs

        Returns:
            True if all products were saved successfully, False otherwise
        """
        pass

    @abstractmethod
    def get_product(self, product_id: str) -> Optional[Product]:
        """Retrieve a product by its ID.

        Args:
            product_id: The ID of the product to retrieve

        Returns:
            The product if found, None otherwise
        """
        pass

    @abstractmethod
    def get_products(
        self,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        run_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Product]:
        """Retrieve products with optional filtering.

        Args:
            category: Filter by category
            subcategory: Filter by subcategory
            run_id: Filter by run ID
            limit: Maximum number of products to return

        Returns:
            List of products matching the filters
        """
        pass

    @abstractmethod
    def clear_all(self) -> bool:
        """Clear all data from the storage.

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the storage backend and release any resources."""
        pass
