"""Supabase storage implementation for the grocery scraper."""

import os
import logging
import json
import datetime
from typing import List, Dict, Any, Optional

from supabase import create_client, Client
from dotenv import load_dotenv

from models.product import Product
from storage.base_storage import BaseStorage


class SupabaseStorage(BaseStorage):
    """Supabase storage backend for the grocery product scraper.

    This class implements the BaseStorage interface for storing products in Supabase.

    Args:
        table_name: Name of the Supabase table to store products
        runs_table_name: Name of the Supabase table to store scraping runs
    """

    def __init__(
        self, table_name: str = "products", runs_table_name: str = "scraping_runs"
    ) -> None:
        """Initialize the Supabase storage backend.

        Args:
            table_name: Name of the Supabase table to store products
            runs_table_name: Name of the Supabase table to store scraping runs
        """
        self.table_name = table_name
        self.runs_table_name = runs_table_name
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
                f"Initialized Supabase storage with tables: {self.table_name}, {self.runs_table_name}"
            )

            # Check if tables exist by trying to fetch one row
            try:
                self.client.table(self.table_name).select("product_id").limit(
                    1
                ).execute()
                self.client.table(self.runs_table_name).select("run_id").limit(
                    1
                ).execute()
            except Exception as e:
                self.logger.warning(f"Table check failed: {e}")
                self.logger.warning("Make sure database tables are properly set up")

        except Exception as e:
            self.logger.error(f"Failed to initialize Supabase storage: {e}")
            raise

    def start_run(
        self,
        run_id: str,
        scraper_type: str,
        category_url: str,
        max_products: Optional[int] = None,
        replace_existing: bool = False,
        config: Optional[Dict] = None,
    ) -> bool:
        """Record the start of a scraping run.

        Args:
            run_id: Unique ID for this scraping run
            scraper_type: Type of scraper being used
            category_url: URL being scraped
            max_products: Maximum number of products to scrape
            replace_existing: Whether existing products will be replaced
            config: Configuration snapshot (optional)

        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            self.logger.error("Supabase client not initialized")
            return False

        try:
            run_data = {
                "run_id": run_id,
                "scraper_type": scraper_type,
                "category_url": category_url,
                "max_products": max_products,
                "replace_existing": replace_existing,
                "config_snapshot": json.dumps(config) if config else None,
                "status": "running",
                "start_time": datetime.datetime.now().isoformat(),
            }

            self.client.table(self.runs_table_name).insert(run_data).execute()
            self.logger.info(f"Started scraping run: {run_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to record start of run {run_id}: {e}")
            return False

    def end_run(
        self,
        run_id: str,
        status: str = "completed",
        num_products: int = 0,
        error_message: Optional[str] = None,
    ) -> bool:
        """Record the end of a scraping run.

        Args:
            run_id: Unique ID for this scraping run
            status: Final status ('completed' or 'failed')
            num_products: Number of products scraped
            error_message: Error message if failed

        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            self.logger.error("Supabase client not initialized")
            return False

        try:
            # Get current time with timezone to match PostgreSQL's TIMESTAMPTZ
            end_time = datetime.datetime.now(datetime.timezone.utc)

            # Get the start time
            response = (
                self.client.table(self.runs_table_name)
                .select("start_time")
                .eq("run_id", run_id)
                .execute()
            )
            if not response.data:
                self.logger.error(f"Run ID {run_id} not found")
                return False

            # Parse the start time from the database
            start_time_str = response.data[0]["start_time"]

            # Make sure to parse it as a timezone-aware datetime
            # If the format includes timezone info (ends with +00:00 or Z)
            if "Z" in start_time_str or "+" in start_time_str:
                start_time = datetime.datetime.fromisoformat(
                    start_time_str.replace("Z", "+00:00")
                )
            else:
                # If no timezone in string, assume UTC
                start_time = datetime.datetime.fromisoformat(start_time_str).replace(
                    tzinfo=datetime.timezone.utc
                )

            # Now both datetimes have timezone info and can be subtracted
            duration_seconds = int((end_time - start_time).total_seconds())

            run_data = {
                "status": status,
                "end_time": end_time.isoformat(),
                "duration_seconds": duration_seconds,
                "num_products": num_products,
            }

            if error_message:
                run_data["error_message"] = error_message

            self.client.table(self.runs_table_name).update(run_data).eq(
                "run_id", run_id
            ).execute()
            self.logger.info(f"Ended scraping run: {run_id} with status: {status}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to record end of run {run_id}: {e}")
            return False

    def save_product(self, product: Product, replace_existing: bool = False) -> bool:
        """Save a single product to Supabase.

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
        """Save multiple products to Supabase.

        Args:
            products: The list of products to save
            replace_existing: Whether to replace existing products with the same ID

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
            product_dicts = []
            for product in products:
                product_dict = product.to_dict()

                # Convert attributes to JSON string for Supabase
                if isinstance(product_dict["attributes"], dict):
                    product_dict["attributes"] = json.dumps(product_dict["attributes"])

                product_dicts.append(product_dict)

            # Upsert or insert based on replace_existing flag
            if replace_existing:
                # Upsert data to Supabase (insert or update based on product_id)
                result = (
                    self.client.table(self.table_name)
                    .upsert(product_dicts, on_conflict=["product_id"])
                    .execute()
                )
                self.logger.info(f"Upserted {len(products)} products to Supabase")
            else:
                # Check if products already exist (to avoid unintentionally overwriting)
                product_ids = [p["product_id"] for p in product_dicts]
                existing = (
                    self.client.table(self.table_name)
                    .select("product_id")
                    .in_("product_id", product_ids)
                    .execute()
                )

                existing_ids = [item["product_id"] for item in existing.data]
                new_products = [
                    p for p in product_dicts if p["product_id"] not in existing_ids
                ]

                if new_products:
                    result = (
                        self.client.table(self.table_name)
                        .insert(new_products)
                        .execute()
                    )
                    self.logger.info(
                        f"Inserted {len(new_products)} new products to Supabase"
                    )
                else:
                    self.logger.info(
                        f"No new products to insert (all {len(product_ids)} already exist)"
                    )

            # Update the run statistics if we have a run ID
            if products and products[0].run_id:
                self.end_run(
                    products[0].run_id, status="completed", num_products=len(products)
                )

            return True
        except Exception as e:
            self.logger.error(f"Failed to save products to Supabase: {e}")

            # Try to update run status if we have a run ID
            if products and products[0].run_id:
                self.end_run(products[0].run_id, status="failed", error_message=str(e))

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

                # Parse attributes if it's a JSON string
                attributes = {}
                if "attributes" in product_dict and product_dict["attributes"]:
                    if isinstance(product_dict["attributes"], str):
                        attributes = json.loads(product_dict["attributes"])
                    else:
                        attributes = product_dict["attributes"]

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
                    attributes=attributes,
                    scraped_at=datetime.datetime.fromisoformat(
                        product_dict["scraped_at"]
                    ),
                    run_id=product_dict.get("run_id"),
                )
            return None
        except Exception as e:
            self.logger.error(f"Failed to get product {product_id}: {e}")
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
            if run_id:
                query = query.eq("run_id", run_id)
            if limit:
                query = query.limit(limit)

            result = query.execute()

            products = []
            for item in result.data:
                # Parse attributes if it's a JSON string
                attributes = {}
                if "attributes" in item and item["attributes"]:
                    if isinstance(item["attributes"], str):
                        attributes = json.loads(item["attributes"])
                    else:
                        attributes = item["attributes"]

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
                        attributes=attributes,
                        scraped_at=datetime.datetime.fromisoformat(item["scraped_at"]),
                        run_id=item.get("run_id"),
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
