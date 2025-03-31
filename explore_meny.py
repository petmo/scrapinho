#!/usr/bin/env python3
"""Script to explore the Meny website structure."""

import sys
import os
import argparse
import json
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from scraper.logger import setup_logging


def explore_meny_category(url: str, output_dir: str = "debug", page: int = 1) -> None:
    """Explore a Meny category page.

    Args:
        url: Category URL to explore
        output_dir: Directory to save output files
        page: Page number to explore
    """
    # Configure session
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        }
    )

    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Add page parameter if specified
    if page > 1:
        if "?" in url:
            page_url = f"{url}&page={page}"
        else:
            page_url = f"{url}?page={page}"
    else:
        page_url = url

    print(f"Fetching Meny category URL: {page_url}")
    response = session.get(page_url, timeout=30)
    print(f"Status code: {response.status_code}")

    if response.status_code != 200:
        print(f"Error: Failed to fetch page (status code: {response.status_code})")
        return

    # Save raw HTML
    html_file = os.path.join(output_dir, f"meny_category_page{page}.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"Saved raw HTML to {html_file}")

    soup = BeautifulSoup(response.text, "lxml")
    print(f"Page title: {soup.title.text if soup.title else 'No title'}")

    # Check for the "Vis flere" (Show more) button
    show_more_button = soup.select_one("button.ngr-button")
    if show_more_button:
        button_text = show_more_button.get_text(strip=True)
        if "Vis flere" in button_text:
            print(f"Found 'Vis flere' button: {button_text}")
            # Look for JavaScript events
            button_attrs = {k: v for k, v in show_more_button.attrs.items()}
            print(f"Button attributes: {json.dumps(button_attrs, indent=2)}")
        else:
            print(f"Found button but not 'Vis flere': {button_text}")
    else:
        print("No 'Vis flere' button found")

    # Look for pagination elements
    pagination_elements = soup.find_all(attrs={"data-page": True})
    if pagination_elements:
        for elem in pagination_elements:
            current_page = elem.get("data-page")
            total_pages = elem.get("data-total-pages", "unknown")
            print(f"Pagination info: Page {current_page} of {total_pages}")
    else:
        print("No explicit pagination elements found")

    # Find product cards
    product_cards = soup.select("li div.ws-product-vertical")
    if not product_cards:
        product_cards = soup.find_all(
            "div", class_=lambda c: c and "product" in c.lower()
        )

    print(f"Found {len(product_cards)} product cards")

    if product_cards:
        # Analyze first product card
        card = product_cards[0]
        print("\n=== First Product Card Analysis ===")

        # Product name
        name_element = card.select_one("h3 a") or card.select_one(
            "a.ws-product-vertical__link"
        )
        if name_element:
            print(f"Product name: {name_element.get_text(strip=True)}")
            print(f"Product URL: {name_element.get('href', 'No URL')}")
        else:
            print("No product name found")

        # Product info
        info_element = card.select_one(
            "p.ws-product-vertical__subtitle"
        ) or card.select_one("div p[itemprop='name']")
        if info_element:
            print(f"Product info: {info_element.get_text(strip=True)}")
        else:
            print("No product info found")

        # Price
        price_element = card.select_one(
            "div.ws-product-vertical__price"
        ) or card.select_one("[itemprop='offers']")
        if price_element:
            print(f"Price: {price_element.get_text(strip=True)}")
        else:
            print("No price found")

        # Unit price
        unit_price_element = card.select_one("p.ws-product-vertical__price-unit")
        if unit_price_element:
            print(f"Unit price: {unit_price_element.get_text(strip=True)}")
        else:
            print("No unit price found")

        # Image
        img_element = card.select_one("img")
        if img_element:
            print(f"Image URL: {img_element.get('src', 'No src')}")
        else:
            print("No image found")

        # Save sample card
        sample_file = os.path.join(output_dir, "meny_sample_product.html")
        with open(sample_file, "w", encoding="utf-8") as f:
            f.write(card.prettify())
        print(f"Saved sample product card to {sample_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Meny Website Explorer")
    parser.add_argument("url", help="Category URL to explore")
    parser.add_argument(
        "-o", "--output", default="debug", help="Output directory for debug files"
    )
    parser.add_argument(
        "-p", "--page", type=int, default=1, help="Page number to explore"
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debug logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level="DEBUG" if args.debug else "INFO")

    # Run the explorer
    explore_meny_category(args.url, args.output, args.page)


if __name__ == "__main__":
    main()
