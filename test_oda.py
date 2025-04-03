#!/usr/bin/env python3
"""Minimal test script that truly limits to just a few products."""

import os
import sys
import time
import logging
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scraper.oda_scraper import OdaScraper
from storage.csv_storage import CSVStorage
from processing.oda_processor import OdaProcessor
from scraper.logger import setup_logging

# Configure logging
setup_logging(level="INFO")
logger = logging.getLogger("minimal_test")


def minimal_test(
    url="https://oda.com/no/categories/1283-meieri-ost-og-egg/",
    total_products=10,  # Total products to scrape across ALL subcategories
    max_subcategories=1,  # Maximum number of subcategories to test
    request_delay=0.2,  # Reduced delay
):
    """Run a truly minimal test with just a few products total."""

    start_time = time.time()
    logger.info(f"Starting minimal test to get {total_products} products total")

    # Create test directory
    test_dir = Path("minimal_test_data")
    test_dir.mkdir(exist_ok=True)

    # Create scraper with minimal delay
    scraper = OdaScraper(
        base_url="https://oda.com",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        request_delay=request_delay,
        max_retries=1,
        timeout=15,
    )

    # Create storage
    storage = CSVStorage(output_dir=str(test_dir), filename_prefix="minimal_test")
    storage.initialize()

    # Create processor
    processor = OdaProcessor()

    try:
        # Get subcategories - we'll only use the first one or two
        subcategories = scraper._extract_subcategories(url)
        logger.info(f"Found {len(subcategories)} subcategories")

        if not subcategories:
            logger.error("No subcategories found!")
            return

        # Limit subcategories
        subcategories = subcategories[:max_subcategories]
        logger.info(f"Using first {len(subcategories)} subcategories")

        all_products = []
        products_needed = total_products

        # Process each subcategory until we have enough products
        for i, subcategory in enumerate(subcategories):
            if products_needed <= 0:
                break

            subcat_name = subcategory["name"]
            subcat_url = subcategory["url"]

            logger.info(f"Getting product URLs from subcategory: {subcat_name}")

            # IMPORTANT: Instead of using get_products_from_subcategory which does pagination,
            # we'll manually get just the first page of product URLs
            product_urls = scraper._extract_product_urls(subcat_url)
            logger.info(f"Found {len(product_urls)} product URLs on first page")

            # Take only the number of products we need
            product_urls = product_urls[:products_needed]
            logger.info(f"Using {len(product_urls)} product URLs")

            # Manually scrape these products
            subcat_products = []
            for url in product_urls:
                product = scraper.get_product(url)
                if product:
                    product.category = "test-category"
                    product.subcategory = subcat_name
                    subcat_products.append(product)
                    logger.info(f"Scraped product: {product.name}")

            # Process and save these products
            processed_products = processor.process_products(subcat_products)
            storage.save_products(processed_products)

            # Add to our total and update products needed
            all_products.extend(subcat_products)
            products_needed -= len(subcat_products)

            logger.info(f"Total products so far: {len(all_products)}/{total_products}")

        # Calculate total time
        total_time = time.time() - start_time

        # Print summary
        logger.info("=" * 60)
        logger.info(f"Test completed in {total_time:.2f} seconds")
        logger.info(f"Scraped {len(all_products)}/{total_products} products")
        if all_products:
            logger.info(
                f"Average time per product: {total_time / len(all_products):.2f}s"
            )
        logger.info(f"Results saved to {test_dir.absolute()}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during test: {e}", exc_info=True)
    finally:
        scraper.close()
        storage.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Minimal Oda scraper test")

    parser.add_argument(
        "-u",
        "--url",
        default="https://oda.com/no/categories/1283-meieri-ost-og-egg/",
        help="Category URL to scrape",
    )

    parser.add_argument(
        "-n",
        "--num-products",
        type=int,
        default=10,
        help="Total number of products to scrape",
    )

    parser.add_argument(
        "-s",
        "--subcategories",
        type=int,
        default=1,
        help="Maximum subcategories to test",
    )

    parser.add_argument(
        "-d", "--delay", type=float, default=0.2, help="Request delay in seconds"
    )

    args = parser.parse_args()

    # Run the minimal test
    minimal_test(
        url=args.url,
        total_products=args.num_products,
        max_subcategories=args.subcategories,
        request_delay=args.delay,
    )
