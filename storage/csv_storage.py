"""CSV storage implementation for the grocery scraper."""

import os
import csv
import logging
import datetime
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

from models.product import Product
from storage.base_storage import BaseStorage


class CSVStorage(BaseStorage):
    """CSV storage backend for the grocery product scraper.

    This class implements the BaseStorage interface for storing products in CSV files.

    Args:
        output_dir: Directory to store CSV files
        filename_prefix: Prefix for CSV filenames
    """

    def __init__(
        self, output_dir: str = "data", filename_prefix: str = "products"
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
            self.logger.error(f"Failed to initialize CSV storage: {e}", exc_info=True)
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

    def save_product(self, product: Product, replace_existing: bool = False) -> bool:
        """Save a single product to a CSV file.

        Args:
            product: The product to save
            replace_existing: Whether to replace an existing product with the same ID

        Returns:
            True if the product was saved successfully, False otherwise
        """
        return self.save_products([product], replace_existing)

    def save_products(
        self, products: List[Product], replace_existing: bool = False
    ) -> bool:
        """Save multiple products to a CSV file.

        Args:
            products: The list of products to save
            replace_existing: Whether to replace existing products with the same ID

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

                if replace_existing and os.path.exists(filename):
                    # Load existing file and replace or append products
                    self._replace_or_append_products(filename, category_products)
                else:
                    # Just append to file (or create new)
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
            self.logger.error(f"Failed to save products to CSV: {e}", exc_info=True)
            return False

    def _replace_or_append_products(
        self, filename: str, new_products: List[Dict]
    ) -> None:
        """Replace or append products in a CSV file.

        Args:
            filename: Path to the CSV file
            new_products: New products to save
        """
        temp_file = tempfile.NamedTemporaryFile(
            mode="w", delete=False, newline="", encoding="utf-8"
        )

        try:
            # Create a dictionary of new products indexed by product_id
            new_products_dict = {p["product_id"]: p for p in new_products}

            # Keep track of products we've written
            written_product_ids = set()

            # Read the existing file
            with open(filename, "r", newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)

                # Set up the writer with the same fieldnames
                writer = csv.DictWriter(temp_file, fieldnames=reader.fieldnames)
                writer.writeheader()

                # Process each existing row
                for row in reader:
                    product_id = row["product_id"]

                    # If this product is in our new products, replace it
                    if product_id in new_products_dict:
                        writer.writerow(new_products_dict[product_id])
                        written_product_ids.add(product_id)
                    else:
                        # Otherwise keep the existing row
                        writer.writerow(row)

            # Add any new products that weren't replacements
            for product_id, product in new_products_dict.items():
                if product_id not in written_product_ids:
                    writer.writerow(product)

            # Close the temp file
            temp_file.close()

            # Replace the original file with the temp file
            shutil.move(temp_file.name, filename)

        except Exception as e:
            # Clean up the temp file
            temp_file.close()
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            raise e

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
                        run_id=product_dict.get("run_id"),
                    )
            return None
        except Exception as e:
            self.logger.error(f"Failed to get product {product_id}: {e}", exc_info=True)
            return None

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
        products = []
        try:
            # Determine which files to search
            if category:
                files = list(
                    Path(self.output_dir).glob(
                        f"{self.filename_prefix}_{category}_*.csv"
                    )
                )
            else:
                files = list(Path(self.output_dir).glob(f"{self.filename_prefix}*.csv"))

            # Read and filter products
            for file in files:
                df = pd.read_csv(file)

                # Apply filters
                if subcategory and "subcategory" in df.columns:
                    df = df[df["subcategory"] == subcategory]

                if run_id and "run_id" in df.columns:
                    df = df[df["run_id"] == run_id]

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
                            run_id=product_dict.get("run_id"),
                        )
                    )

                # Check if we've reached the limit
                if limit is not None and len(products) >= limit:
                    break

            return products
        except Exception as e:
            self.logger.error(f"Failed to get products: {e}", exc_info=True)
            return []

    def close(self) -> None:
        """Close the CSV storage backend (no-op for CSV)."""
        pass

    def clear_all(self) -> bool:
        """Clear all data from the CSV storage.

        This deletes all CSV files in the output directory that match the filename prefix.

        Returns:
            True if successful, False otherwise
        """
        try:
            import os
            from pathlib import Path

            # Ensure the output directory exists
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)

            # Find all CSV files matching the prefix
            csv_files = list(Path(self.output_dir).glob(f"{self.filename_prefix}*.csv"))

            # Log the files that will be deleted
            self.logger.info(f"Found {len(csv_files)} CSV files to delete")
            for csv_file in csv_files:
                self.logger.debug(f"Deleting file: {csv_file}")
                try:
                    os.remove(csv_file)
                except Exception as e:
                    self.logger.error(
                        f"Error deleting file {csv_file}: {e}", exc_info=True
                    )
                    # Continue with other files even if one fails

            self.logger.info(f"Cleared all CSV files in {self.output_dir}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to clear CSV storage: {e}", exc_info=True)
            return False
