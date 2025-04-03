#!/usr/bin/env python3
"""Main entry point for the grocery product scraper."""

import os
import sys
import logging
import argparse
from pathlib import Path
from typing import Dict, Any

import yaml
from box import Box
from dotenv import load_dotenv

from pipelines import get_pipeline
from scraper.logger import setup_logging


def load_config(config_path: str = "config.yaml") -> Box:
    """Load configuration from YAML file.

    Args:
        config_path: Path to configuration file

    Returns:
        Box object with configuration
    """
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return Box(config)
    except Exception as e:
        logging.error(f"Failed to load configuration from {config_path}: {e}")
        sys.exit(1)


def run_scraper(
    config: Box, category_url: str = None, max_products: int = None, debug: bool = False
) -> None:
    """Run the scraper with configured pipeline.

    Args:
        config: Configuration object
        category_url: URL of the category to scrape, if not using config
        max_products: Maximum number of products to scrape per subcategory
        debug: Whether to run in debug mode
    """
    logger = logging.getLogger(__name__)

    # Set debug mode in configuration if enabled
    if debug:
        logger.info("Running in DEBUG mode - more detailed logging will be displayed")
        # Reduce request delay in debug mode for faster testing
        config.scraper.request_delay = 0.2

    # Determine which pipeline to use
    pipeline_type = config.scraper.type
    logger.info(f"Using {pipeline_type} pipeline")

    try:
        # Create and run the pipeline
        pipeline = get_pipeline(pipeline_type, config)

        # Determine category URL to scrape
        if category_url is None:
            # Get from configuration
            scraper_config = config.scraper.get(pipeline_type, {})
            categories = scraper_config.get("categories", [])
            if not categories:
                logger.error("No categories specified in configuration")
                return

            category = categories[0]
            category_url = category.url
            logger.info(f"Using category from config: {category.name} ({category_url})")
        else:
            logger.info(f"Using provided category URL: {category_url}")

        # Ensure category URL is absolute
        base_url = config.scraper.get(pipeline_type, {}).get("base_url")
        if base_url and not category_url.startswith(("http://", "https://")):
            category_url = f"{base_url}{category_url}"

        # Run the pipeline
        products = pipeline.run_pipeline(category_url, max_products)
        logger.info(f"Pipeline completed with {len(products)} products processed")

    except Exception as e:
        logger.error(f"Scraper encountered an error: {e}", exc_info=True)


def main() -> None:
    """Main entry point."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Grocery Product Scraper")

    parser.add_argument(
        "-c", "--config", default="config.yaml", help="Path to configuration file"
    )

    parser.add_argument(
        "-u", "--category", help="URL of the category to scrape (overrides config)"
    )

    parser.add_argument(
        "-m",
        "--max-products",
        type=int,
        help="Maximum products to scrape per subcategory",
    )

    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode (equivalent to --log-level=DEBUG)",
    )

    parser.add_argument(
        "-s",
        "--scraper",
        choices=["oda", "meny"],
        help="Scraper type to use (overrides config)",
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Load configuration
    config = load_config(args.config)

    # Override scraper type if specified in command line
    if args.scraper:
        config.scraper.type = args.scraper

    # Set up logging
    log_config = config.logging
    log_level = "DEBUG" if args.debug else (args.log_level or log_config.level)
    setup_logging(
        level=log_level,
        log_file=log_config.file,
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Starting scraper with {config.scraper.type} pipeline")

    # Run the scraper
    run_scraper(config, args.category, args.max_products, args.debug)


if __name__ == "__main__":
    main()
