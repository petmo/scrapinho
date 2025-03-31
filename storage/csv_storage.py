"""CSV storage implementation for the Oda scraper."""

import os
import csv
import logging
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

from models.product import Product
from storage.base_storage import BaseStorage


class CSVStorage(BaseStorage):
    """CSV storage backend for the Oda scraper.

    This class implements the BaseStorage interface for storing products in CSV files.

    Args:
        output_dir: Directory to store CSV files
        filename_prefix: Prefix for CSV filenames
    """

    def __init__(
        self, output_dir: str = "data", filename_prefix: str = "oda_products"
    ) -> None:
        """Initialize the CSV storage backend.

        Args:
            output_dir: Directory to store CSV files
            filename_prefix: Prefix for CSV filenames
        """
        self.output_dir = output_dir
        self.filename_prefix = filename_prefix
        self.logger = logging.getLogger(__name__)

    def initialize(self) -> None:
        """Initialize the CSV storage backend by creating the output directory."""
        try:
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Initialized CSV storage in {self.output_dir}")
        except Exception as e:
            self.logger.error(f"Failed to initialize CSV storage: {e}")
            raise

    def _get_current_filename(self, category: Optional[str] = None) -> str:
        """Get the filename for the current date and optional category.

        Args:
            category: Optional category to include in the filename

        Returns:
            The full path to the CSV file
        """
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        category_part = f"_{category}" if category else ""
        filename = f"{self.filename_prefix}{category_part}_{today}.csv"
        return os.path.join(self.output_dir, filename)

    def save_product(self, product: Product) -> bool:
        """Save a single product to a CSV file.

        Args:
            product: The product to save

        Returns:
            True if the product was saved successfully, False otherwise
        """
        return self.save_products([product])

    def save_products(self, products: List[Product]) -> bool:
        """Save multiple products to a CSV file.

        Args:
            products: The list of products to save

        Returns:
            True if all products were saved successfully, False otherwise
        """
        if not products:
            self.logger.warning("No products to save")
            return True

        try:
            # Group products by category
            products_by_category = {}
            for product in products:
                category = product.category or "uncategorized"
                if category not in products_by_category:
                    products_by_category[category] = []
                products_by_category[category].append(product.to_dict())

            # Save each category to a separate file
            for category, category_products in products_by_category.items():
                filename = self._get_current_filename(category)
                file_exists = os.path.exists(filename)

                with open(filename, mode="a", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(
                        file, fieldnames=list(category_products[0].keys())
                    )
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(category_products)

            self.logger.info(f"Saved {len(products)} products to CSV files")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save products to CSV: {e}")
            return False

    def get_product(self, product_id: str) -> Optional[Product]:
        """Retrieve a product by its ID.

        Args:
            product_id: The ID of the product to retrieve

        Returns:
            The product if found, None otherwise
        """
        try:
            # Search in all CSV files in the output directory
            for file in Path(self.output_dir).glob(f"{self.filename_prefix}*.csv"):
                df = pd.read_csv(file)
                product_row = df[df["product_id"] == product_id]
                if not product_row.empty:
                    product_dict = product_row.iloc[0].to_dict()
                    # Convert back to Product object
                    return Product(
                        product_id=product_dict["product_id"],
                        name=product_dict["name"],
                        brand=product_dict.get("brand"),
                        info=product_dict["info"],
                        price=float(product_dict["price"]),
                        price_text=product_dict["price_text"],
                        unit_price=product_dict.get("unit_price"),
                        image_url=product_dict.get("image_url"),
                        category=product_dict.get("category"),
                        subcategory=product_dict.get("subcategory"),
                        url=product_dict.get("url"),
                        attributes={},  # This would need additional parsing
                        scraped_at=datetime.datetime.fromisoformat(
                            product_dict["scraped_at"]
                        ),
                    )
            return None
        except Exception as e:
            self.logger.error(f"Failed to get product {product_id}: {e}")
            return None

    def get_products(
        self,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Product]:
        """Retrieve products with optional filtering.

        Args:
            category: Filter by category
            subcategory: Filter by subcategory
            limit: Maximum number of products to return

        Returns:
            List of products matching the filters
        """
        products = []
        try:
            # Determine which files to search
            if category:
                files = list(
                    Path(self.output_dir).glob(f"{self.filename_prefix}_{category}_*.csv")
                )
            else:
                files = list(Path(self.output_dir).glob(f"{self.filename_prefix}*.csv"))

            # Read and filter products
            for file in files:
                df = pd.read_csv(file)
                if subcategory:
                    df = df[df["subcategory"] == subcategory]

                # Apply limit if needed
                if limit is not None and len(products) + len(df) > limit:
                    df = df.iloc[: limit - len(products)]

                # Convert rows to Product objects
                for _, row in df.iterrows():
                    product_dict = row.to_dict()
                    products.append(
                        Product(
                            product_id=str(product_dict["product_id"]),
                            name=product_dict["name"],
                            brand=product_dict.get("brand"),
                            info=product_dict["info"],
                            price=float(product_dict["price"]),
                            price_text=product_dict["price_text"],
                            unit_price=product_dict.get("unit_price"),
                            image_url=product_dict.get("image_url"),
                            category=product_dict.get("category"),
                            subcategory=product_dict.get("subcategory"),
                            url=product_dict.get("url"),
                            attributes={},  # Would need additional parsing
                            scraped_at=datetime.datetime.fromisoformat(
                                product_dict["scraped_at"]
                            ),
                        )
                    )

                # Check if we've reached the limit
                if limit is not None and len(products) >= limit:
                    break

            return products
        except Exception as e:
            self.logger.error(f"Failed to get products: {e}")
            return []

    def close(self) -> None:
        """Close the CSV storage backend (no-op for CSV)."""
        pass