"""Supabase storage implementation for the Oda scraper."""

import os
import logging
import datetime
from typing import List, Dict, Any, Optional

from supabase import create_client, Client
from dotenv import load_dotenv

from models.product import Product
from storage.base_storage import BaseStorage


class SupabaseStorage(BaseStorage):
    """Supabase storage backend for the Oda scraper.

    This class implements the BaseStorage interface for storing products in Supabase.

    Args:
        table_name: Name of the Supabase table to store products
    """

    def __init__(self, table_name: str = "products") -> None:
        """Initialize the Supabase storage backend.

        Args:
            table_name: Name of the Supabase table to store products
        """
        self.table_name = table_name
        self.logger = logging.getLogger(__name__)
        self.client = None

    def initialize(self) -> None:
        """Initialize the Supabase client and check table existence."""
        try:
            load_dotenv()
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")

            if not url or not key:
                raise ValueError(
                    "Missing Supabase URL or key. Please check your .env file."
                )

            self.client = create_client(url, key)
            self.logger.info(
                f"Initialized Supabase storage with table {self.table_name}"
            )

            # Check if table exists by trying to fetch one row
            try:
                self.client.table(self.table_name).select("product_id").limit(
                    1
                ).execute()
            except Exception as e:
                self.logger.warning(f"Table check failed: {e}")
                self.logger.info("Creating table schema...")
                # Note: In a real implementation, you would create the table schema
                # But Supabase Python client doesn't support schema creation,
                # you would typically use migrations or the Supabase UI

        except Exception as e:
            self.logger.error(f"Failed to initialize Supabase storage: {e}")
            raise

    def save_product(self, product: Product) -> bool:
        """Save a single product to Supabase.

        Args:
            product: The product to save

        Returns:
            True if the product was saved successfully, False otherwise
        """
        return self.save_products([product])

    def save_products(self, products: List[Product]) -> bool:
        """Save multiple products to Supabase.

        Args:
            products: The list of products to save

        Returns:
            True if all products were saved successfully, False otherwise
        """
        if not products:
            self.logger.warning("No products to save")
            return True

        if not self.client:
            self.logger.error("Supabase client not initialized")
            return False

        try:
            # Convert products to dictionaries
            product_dicts = [product.to_dict() for product in products]

            # Upsert data to Supabase (insert or update based on product_id)
            result = (
                self.client.table(self.table_name)
                .upsert(product_dicts, on_conflict=["product_id"])
                .execute()
            )

            self.logger.info(f"Saved {len(products)} products to Supabase")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save products to Supabase: {e}")
            return False

    def get_product(self, product_id: str) -> Optional[Product]:
        """Retrieve a product by its ID.

        Args:
            product_id: The ID of the product to retrieve

        Returns:
            The product if found, None otherwise
        """
        if not self.client:
            self.logger.error("Supabase client not initialized")
            return None

        try:
            result = (
                self.client.table(self.table_name)
                .select("*")
                .eq("product_id", product_id)
                .execute()
            )

            if result.data and len(result.data) > 0:
                product_dict = result.data[0]
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
                    attributes=product_dict.get("attributes", {}),
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
        if not self.client:
            self.logger.error("Supabase client not initialized")
            return []

        try:
            query = self.client.table(self.table_name).select("*")

            # Apply filters
            if category:
                query = query.eq("category", category)
            if subcategory:
                query = query.eq("subcategory", subcategory)
            if limit:
                query = query.limit(limit)

            result = query.execute()

            products = []
            for item in result.data:
                products.append(
                    Product(
                        product_id=item["product_id"],
                        name=item["name"],
                        brand=item.get("brand"),
                        info=item["info"],
                        price=float(item["price"]),
                        price_text=item["price_text"],
                        unit_price=item.get("unit_price"),
                        image_url=item.get("image_url"),
                        category=item.get("category"),
                        subcategory=item.get("subcategory"),
                        url=item.get("url"),
                        attributes=item.get("attributes", {}),
                        scraped_at=datetime.datetime.fromisoformat(item["scraped_at"]),
                    )
                )
            return products
        except Exception as e:
            self.logger.error(f"Failed to get products: {e}")
            return []

    def close(self) -> None:
        """Close the Supabase client and release resources."""
        self.client = None
        self.logger.info("Closed Supabase connection")
