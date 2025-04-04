#!/usr/bin/env python3
"""Main entry point for the grocery product scraper."""

import argparse
import logging
import sys
import json
from pathlib import Path

import yaml
from box import Box
from dotenv import load_dotenv

from scraper import create_scraper
from processing import get_processor
from storage import save_to_storage, clear_storage
from scraper.logger import setup_logging
from utils.run_id import generate_run_id, format_run_id
from storage.supabase_storage import SupabaseStorage


def load_config(config_path="config.yaml"):
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return Box(config)
    except Exception as e:
        logging.error(f"Failed to load configuration from {config_path}: {e}")
        sys.exit(1)


def get_categories(config, scraper_type, provided_url=None):
    """Get list of categories to scrape.

    Args:
        config: Configuration object
        scraper_type: Type of scraper ('oda' or 'meny')
        provided_url: Optional specific URL provided by command line

    Returns:
        List of dictionaries with category name and URL
    """
    # If a specific URL is provided, use only that
    if provided_url:
        # Try to extract name from URL
        url_parts = provided_url.rstrip("/").split("/")
        name = url_parts[-1] if url_parts else "custom-category"
        return [{"name": name, "url": provided_url}]

    # Get categories from configuration
    scraper_config = config.scraper.get(scraper_type, {})
    categories = scraper_config.get("categories", [])

    # Make sure base_url is applied
    base_url = scraper_config.get("base_url")
    if base_url:
        for category in categories:
            if category.get("url") and not category["url"].startswith(
                ("http://", "https://")
            ):
                category["url"] = f"{base_url}{category['url']}"

    return categories


def add_run_id_to_products(products, run_id):
    """Add run ID to all products."""
    for product in products:
        product.run_id = run_id
    return products


def track_run_with_supabase(
    config,
    run_id,
    scraper_type,
    category_name,
    category_url,
    max_products=None,
    replace=False,
):
    """Initialize run tracking with Supabase.

    Args:
        config: Configuration object
        run_id: Run ID to track
        scraper_type: Type of scraper being used
        category_name: Name of the category
        category_url: URL being scraped
        max_products: Maximum number of products
        replace: Whether we're replacing existing products

    Returns:
        SupabaseStorage instance if successful, None otherwise
    """
    try:
        # Only if we're using Supabase storage
        if config.storage.type.lower() != "supabase":
            return None

        storage_config = config.storage.get("supabase", {})
        supabase = SupabaseStorage(**storage_config)
        supabase.initialize()

        # Create a simplified config snapshot (removing sensitive info)
        config_snapshot = {
            "scraper": {
                "type": scraper_type,
                "request_delay": config.scraper.get("request_delay"),
                "max_retries": config.scraper.get("max_retries"),
            },
            "category": category_name,
        }

        # Record run start
        supabase.start_run(
            run_id=run_id,
            scraper_type=scraper_type,
            category_url=category_url,
            max_products=max_products,
            replace_existing=replace,
            config=config_snapshot,
        )

        return supabase
    except Exception as e:
        logging.error(f"Failed to initialize run tracking: {e}")
        return None


def main():
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Grocery Product Scraper")
    parser.add_argument(
        "-c", "--config", default="config.yaml", help="Configuration file path"
    )
    parser.add_argument(
        "-u", "--category", help="Specific category URL to scrape (overrides config)"
    )
    parser.add_argument(
        "-m", "--max-products", type=int, help="Maximum products to scrape per category"
    )
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    parser.add_argument(
        "-s", "--scraper", choices=["oda", "meny"], help="Scraper type to use"
    )
    parser.add_argument("--run-id", help="Specify a run ID, otherwise auto-generated")
    parser.add_argument("--seed", help="Seed for generating deterministic run ID")
    parser.add_argument(
        "--replace", action="store_true", help="Replace existing products with same ID"
    )
    parser.add_argument(
        "--category-filter", help="Only scrape categories containing this string"
    )
    parser.add_argument(
        "--clear-tables", action="store_true", help="Clear all tables before scraping"
    )
    parser.add_argument(
        "--clear-only", action="store_true", help="Only clear tables without scraping"
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Load configuration
    config = load_config(args.config)

    # Override scraper type if specified
    if args.scraper:
        config.scraper.type = args.scraper

    # Set up logging
    log_level = "DEBUG" if args.debug else config.logging.level
    setup_logging(level=log_level, log_file=config.logging.file)

    logger = logging.getLogger(__name__)

    # Clear tables if requested
    storage_type = config.storage.type
    storage_config = config.storage.get(storage_type, {})

    if args.clear_tables or args.clear_only:
        logger.info("Clearing tables before scraping")
        success = clear_storage(storage_type, storage_config)
        if success:
            logger.info("Tables cleared successfully")
        else:
            logger.error("Failed to clear tables", exc_info=True)
            if args.clear_only:
                return

    # If only clearing tables was requested, exit now
    if args.clear_only:
        logger.info("Tables cleared. Exiting as requested.")
        return

    # Generate or use provided run ID
    run_id = args.run_id
    if not run_id:
        run_id = format_run_id(generate_run_id(args.seed))

    # Create the scraper
    scraper_type = config.scraper.type
    scraper = create_scraper(scraper_type, config)

    # Create processor
    processor = get_processor(scraper_type)

    # Get categories to scrape
    categories = get_categories(config, scraper_type, args.category)

    # Filter categories if requested
    if args.category_filter and not args.category:
        categories = [
            cat
            for cat in categories
            if args.category_filter.lower() in cat.get("name", "").lower()
        ]
        if not categories:
            logger.error(
                f"No categories match filter: {args.category_filter}", exc_info=True
            )
            return

    logger.info(f"Starting scraper with {scraper_type} scraper, run ID: {run_id}")
    logger.info(f"Will scrape {len(categories)} categories")

    total_products = 0

    try:
        # Process each category
        for category_index, category in enumerate(categories):
            category_name = category.get("name", "unknown")
            category_url = category.get("url")

            if not category_url:
                logger.warning(f"Skipping category {category_name}: No URL specified")
                continue

            # Log category information
            logger.info(
                f"Scraping category {category_index + 1}/{len(categories)}: {category_name}"
            )
            logger.info(f"URL: {category_url}")

            # Create a category-specific run ID
            category_run_id = f"{run_id}_{category_name}"

            # Initialize category run tracking with the category-specific run ID
            supabase_tracker = track_run_with_supabase(
                config,
                category_run_id,
                scraper_type,
                category_name,
                category_url,
                args.max_products,
                args.replace,
            )

            try:
                # Scrape products for this category
                products = scraper.get_products(category_url, args.max_products)
                logger.info(f"Scraped {len(products)} products from {category_name}")

                # Skip if no products found
                if not products:
                    logger.warning(f"No products found in category: {category_name}")
                    continue

                # Add run ID and category to all products - USE THE SAME CATEGORY-SPECIFIC RUN ID
                for product in products:
                    product.run_id = category_run_id  # Use category-specific run ID to match tracking
                    if not product.category:
                        product.category = category_name

                # Process products
                processed_products = processor.process_products(products)

                # Save products
                success = save_to_storage(
                    processed_products,
                    storage_type,
                    storage_config,
                    replace_existing=args.replace,
                )

                if success:
                    logger.info(
                        f"Successfully saved {len(processed_products)} products from {category_name}"
                    )
                    total_products += len(processed_products)
                else:
                    logger.error(
                        f"Failed to save products from {category_name}", exc_info=True
                    )

                    # Record failure if using run tracking
                    if supabase_tracker:
                        supabase_tracker.end_run(
                            f"{run_id}_{category_name}",
                            status="failed",
                            error_message="Failed to save products",
                        )

            except Exception as e:
                logger.error(
                    f"Error processing category {category_name}: {e}", exc_info=True
                )

                # Record failure if using run tracking
                if supabase_tracker:
                    supabase_tracker.end_run(
                        f"{run_id}_{category_name}",
                        status="failed",
                        error_message=str(e),
                    )

        # Log summary
        logger.info(f"Scraping run {run_id} completed")
        logger.info(
            f"Total products scraped across {len(categories)} categories: {total_products}"
        )

    except Exception as e:
        logger.error(f"Error during scraping: {e}", exc_info=True)
    finally:
        # Clean up resources
        if "scraper" in locals():
            scraper.close()


if __name__ == "__main__":
    main()
