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
            self.logger.error(
                f"Failed to initialize Supabase storage: {e}", exc_info=True
            )
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
            self.logger.error("Supabase client not initialized", exc_info=True)
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
            self.logger.error(
                f"Failed to record start of run {run_id}: {e}", exc_info=True
            )
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
            # Get current time with timezone
            end_time = datetime.datetime.now(datetime.timezone.utc)

            # Try to get the run record
            response = (
                self.client.table(self.runs_table_name)
                .select("start_time")
                .eq("run_id", run_id)
                .execute()
            )

            # If not found by exact match, try to find a related run
            if not response.data:
                self.logger.debug(
                    f"Run ID {run_id} not found, trying to find a related run"
                )

                # Try to find by base run_id (without category suffix)
                if "_" in run_id:
                    base_run_id = run_id.split("_")[0]
                    if len(base_run_id) >= 8:  # Make sure it's meaningful
                        response = (
                            self.client.table(self.runs_table_name)
                            .select("run_id", "start_time")
                            .like("run_id", f"{base_run_id}%")
                            .execute()
                        )

                        if response.data:
                            # Use the first matching run
                            run_id = response.data[0]["run_id"]
                            self.logger.info(f"Found related run: {run_id}")
                        else:
                            self.logger.warning(f"No related run found for {run_id}")
                            # Create a new run record
                            self.start_run(
                                run_id, "unknown", "unknown", replace_existing=False
                            )
                            return self.end_run(
                                run_id, status, num_products, error_message
                            )

            if not response.data:
                self.logger.error(
                    f"Run ID {run_id} not found and could not find related run"
                )
                return False

            # Parse the start time (with dateutil for robustness)
            from dateutil import parser

            start_time_str = response.data[0]["start_time"]

            # Prepare update data
            run_data = {
                "status": status,
                "end_time": end_time.isoformat(),
                "num_products": num_products,
            }

            # Try to calculate duration
            try:
                start_time = parser.parse(start_time_str)
                if start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=datetime.timezone.utc)

                duration_seconds = int((end_time - start_time).total_seconds())
                run_data["duration_seconds"] = duration_seconds
            except Exception as e:
                self.logger.warning(f"Could not calculate duration: {e}")

            # Add error message if provided
            if error_message:
                run_data["error_message"] = error_message

            # Update the run record
            self.client.table(self.runs_table_name).update(run_data).eq(
                "run_id", run_id
            ).execute()
            self.logger.info(f"Updated run status for {run_id}: {status}")
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
        """Save multiple products to Supabase using chunking to handle large batches.

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

        # Track the run ID for later
        run_id = products[0].run_id if products else None

        # Track success
        overall_success = True

        try:
            # Save products in chunks to avoid payload size limits
            chunk_size = 50  # A conservative size that should work with most payloads
            total_chunks = (len(products) + chunk_size - 1) // chunk_size

            self.logger.info(
                f"Saving {len(products)} products in {total_chunks} chunks of {chunk_size}"
            )

            # Process each chunk
            for i in range(0, len(products), chunk_size):
                chunk = products[i : i + chunk_size]
                chunk_num = (i // chunk_size) + 1

                try:
                    # Convert products to dictionaries
                    product_dicts = []
                    for product in chunk:
                        try:
                            product_dict = product.to_dict()

                            # Ensure attributes is a dictionary, not a string
                            if isinstance(product_dict["attributes"], dict):
                                # Already a dict, good
                                pass
                            elif isinstance(product_dict["attributes"], str):
                                # Try to parse JSON string
                                try:
                                    product_dict["attributes"] = json.loads(
                                        product_dict["attributes"]
                                    )
                                except json.JSONDecodeError:
                                    # If parsing fails, use an empty dict
                                    product_dict["attributes"] = {}
                            else:
                                # Something unexpected, use empty dict
                                product_dict["attributes"] = {}

                            product_dicts.append(product_dict)
                        except Exception as e:
                            self.logger.warning(
                                f"Error converting product to dict: {e}"
                            )
                            # Skip this product but continue with others

                    if not product_dicts:
                        self.logger.warning(
                            f"Chunk {chunk_num}/{total_chunks} had no valid products"
                        )
                        continue

                    self.logger.info(
                        f"Processing chunk {chunk_num}/{total_chunks} with {len(product_dicts)} products"
                    )

                    # Upsert or insert based on replace_existing flag
                    if replace_existing:
                        # Upsert data to Supabase (insert or update based on product_id)
                        result = (
                            self.client.table(self.table_name)
                            .upsert(product_dicts, on_conflict=["product_id"])
                            .execute()
                        )
                        self.logger.info(
                            f"Upserted {len(product_dicts)} products in chunk {chunk_num}"
                        )
                    else:
                        # Check if products already exist (to avoid unintentionally overwriting)
                        product_ids = [p["product_id"] for p in product_dicts]

                        # Split into smaller batches for the query too
                        id_batch_size = 100
                        existing_ids = set()

                        for j in range(0, len(product_ids), id_batch_size):
                            id_batch = product_ids[j : j + id_batch_size]
                            existing = (
                                self.client.table(self.table_name)
                                .select("product_id")
                                .in_("product_id", id_batch)
                                .execute()
                            )
                            existing_ids.update(
                                item["product_id"] for item in existing.data
                            )

                        # Filter out existing products
                        new_products = [
                            p
                            for p in product_dicts
                            if p["product_id"] not in existing_ids
                        ]

                        if new_products:
                            result = (
                                self.client.table(self.table_name)
                                .insert(new_products)
                                .execute()
                            )
                            self.logger.info(
                                f"Inserted {len(new_products)} new products in chunk {chunk_num}"
                            )
                        else:
                            self.logger.info(
                                f"No new products to insert in chunk {chunk_num}"
                            )

                except Exception as e:
                    self.logger.error(f"Failed to save chunk {chunk_num}: {e}")
                    overall_success = False
                    # Continue with next chunk despite error

            # Update the run statistics if we have a run ID
            if overall_success and run_id:
                try:
                    # Try to find a matching run ID in the database
                    matching_run_id = self._find_matching_run_id(run_id)
                    if matching_run_id:
                        self.end_run(
                            matching_run_id,
                            status="completed",
                            num_products=len(products),
                        )
                except Exception as e:
                    self.logger.error(f"Failed to update run statistics: {e}")
                    # Don't affect overall success for run statistics failure

            return overall_success
        except Exception as e:
            self.logger.error(f"Failed to save products to Supabase: {e}")

            # Try to update run status if we have a run ID
            if run_id:
                try:
                    matching_run_id = self._find_matching_run_id(run_id)
                    if matching_run_id:
                        self.end_run(
                            matching_run_id, status="failed", error_message=str(e)
                        )
                except Exception as e:
                    self.logger.error(f"Failed to update run statistics: {e}")
                    pass  # Ignore errors in failure handling

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

                # Properly handle attributes
                attributes = {}
                if "attributes" in product_dict and product_dict["attributes"]:
                    if isinstance(product_dict["attributes"], dict):
                        # Already a dictionary, use as is
                        attributes = product_dict["attributes"]
                    elif isinstance(product_dict["attributes"], str):
                        # Try to parse JSON string
                        try:
                            attributes = json.loads(product_dict["attributes"])
                        except json.JSONDecodeError:
                            self.logger.warning(
                                f"Failed to parse attributes JSON for product {product_id}"
                            )

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
                # Properly handle attributes - either as dict or parse from JSON string
                attributes = {}
                if "attributes" in item and item["attributes"]:
                    if isinstance(item["attributes"], dict):
                        attributes = item["attributes"]
                    elif isinstance(item["attributes"], str):
                        try:
                            attributes = json.loads(item["attributes"])
                        except json.JSONDecodeError:
                            self.logger.warning(
                                f"Failed to parse attributes for product {item.get('product_id')}"
                            )

                # Parse scraped_at with timezone handling
                if "scraped_at" in item and item["scraped_at"]:
                    if item["scraped_at"].endswith("Z"):
                        # Handle UTC time ending with Z
                        scraped_at = datetime.datetime.fromisoformat(
                            item["scraped_at"].replace("Z", "+00:00")
                        )
                    else:
                        # Regular ISO format
                        scraped_at = datetime.datetime.fromisoformat(item["scraped_at"])
                else:
                    scraped_at = datetime.datetime.now(datetime.timezone.utc)

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
                        scraped_at=scraped_at,
                        run_id=item.get("run_id"),
                    )
                )
            return products
        except Exception as e:
            self.logger.error(f"Failed to get products: {e}", exc_info=True)
            return []

    def close(self) -> None:
        """Close the Supabase client and release resources."""
        self.client = None
        self.logger.info("Closed Supabase connection")

    def clear_all(self) -> bool:
        """Clear all data from the Supabase tables, respecting foreign key constraints.

        This method first clears child tables, then parent tables to avoid constraint violations.

        Returns:
            True if successful, False otherwise
        """
        if not self.client:
            self.logger.error("Supabase client not initialized")
            return False

        try:
            # Keep track of overall success
            success = True

            # Step 1: Get all run IDs to track deletion progress
            try:
                response = (
                    self.client.table(self.runs_table_name).select("run_id").execute()
                )
                if response.data:
                    run_ids = [item["run_id"] for item in response.data]
                    self.logger.info(f"Found {len(run_ids)} run IDs to delete")
                else:
                    run_ids = []
                    self.logger.info("No run IDs found in the database")
            except Exception as e:
                self.logger.warning(f"Failed to retrieve run IDs: {e}")
                run_ids = []

            # Step 2: Clear product_prices table FIRST (child table that depends on products)
            try:
                prices_table = "product_prices"
                self.logger.info(f"Clearing {prices_table} table")
                # Delete using a valid WHERE clause
                self.client.table(prices_table).delete().gte("id", 0).execute()
                self.logger.info(f"Successfully cleared {prices_table} table")
            except Exception as e:
                self.logger.warning(f"Failed to clear {prices_table} table: {e}")
                # Continue with other tables even if this fails

            # Step 3: Clear products table (now safe since child table is cleared)
            try:
                self.logger.info(f"Clearing {self.table_name} table")
                # Delete using valid WHERE clause
                self.client.table(self.table_name).delete().neq(
                    "product_id", "no-match-placeholder"
                ).execute()
                self.logger.info(f"Successfully cleared {self.table_name} table")
            except Exception as e:
                self.logger.error(f"Failed to clear {self.table_name} table: {e}")
                success = False

            # Step 4: Clear scraping_runs table
            try:
                self.logger.info(f"Clearing {self.runs_table_name} table")
                if run_ids:
                    self.logger.info(f"Deleting {len(run_ids)} run records")
                    # If we have run IDs, log them and delete
                    self.client.table(self.runs_table_name).delete().in_(
                        "run_id", run_ids
                    ).execute()
                else:
                    # Otherwise just delete everything
                    self.client.table(self.runs_table_name).delete().neq(
                        "run_id", "no-match-placeholder"
                    ).execute()
                self.logger.info(f"Successfully cleared {self.runs_table_name} table")
            except Exception as e:
                self.logger.error(f"Failed to clear {self.runs_table_name} table: {e}")
                success = False

            return success
        except Exception as e:
            self.logger.error(f"Failed to clear Supabase tables: {e}")
            return False

    def _find_matching_run_id(self, run_id: str) -> Optional[str]:
        """Find a matching run ID in the database.

        This handles cases where the product's run_id might not exactly match
        the one in the scraping_runs table (e.g., if a category suffix was added).

        Args:
            run_id: The run ID to look for

        Returns:
            The matching run ID from the database, or None if not found
        """
        if not self.client:
            return None

        try:
            # First try exact match
            response = (
                self.client.table(self.runs_table_name)
                .select("run_id")
                .eq("run_id", run_id)
                .execute()
            )
            if response.data:
                return response.data[0]["run_id"]

            # If not found, try finding a run_id that contains our run_id as a prefix
            # This handles cases where products have the base run_id but the run was created with category suffix
            parts = run_id.split("_")
            if len(parts) >= 1:
                base_id = parts[0]  # Get just the date part or UUID part
                if len(base_id) >= 8:  # Make sure it's long enough to be meaningful
                    response = (
                        self.client.table(self.runs_table_name)
                        .select("run_id")
                        .like("run_id", f"{base_id}%")
                        .execute()
                    )
                    if response.data:
                        return response.data[0]["run_id"]

            # If not found, try as a suffix (if base run_id was used for the run but products have category-specific IDs)
            if "_" in run_id:
                response = (
                    self.client.table(self.runs_table_name)
                    .select("run_id")
                    .like("run_id", f"%{run_id}")
                    .execute()
                )
                if response.data:
                    return response.data[0]["run_id"]

            return None
        except Exception as e:
            self.logger.error(f"Error finding matching run ID: {e}")
            return None
