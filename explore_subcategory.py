#!/usr/bin/env python3
"""Script to explore a specific subcategory page on Oda."""

import sys
import os
import argparse
import json
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scraper.logger import setup_logging


def explore_subcategory(
    url: str, output_dir: str = "debug", check_pagination: bool = False
) -> None:
    """Explore a subcategory page and extract product information.

    Args:
        url: Subcategory URL to explore
        output_dir: Directory to save output files
        check_pagination: Whether to check for pagination
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

    print(f"Fetching subcategory URL: {url}")
    response = session.get(url, timeout=30)
    print(f"Status code: {response.status_code}")

    if response.status_code != 200:
        print(f"Error: Failed to fetch page (status code: {response.status_code})")
        return

    # Save raw HTML
    html_file = os.path.join(output_dir, "subcategory.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"Saved raw HTML to {html_file}")

    soup = BeautifulSoup(response.text, "lxml")
    print(f"Page title: {soup.title.text if soup.title else 'No title'}")

    # Check for pagination elements
    if check_pagination:
        print("\n=== Pagination Analysis ===")

        # Look for common pagination elements
        pagination_elements = []

        # Method 1: Look for elements with class names related to pagination
        pagination_classes = ["pagination", "pager", "pages", "page-numbers"]
        for cls in pagination_classes:
            elements = soup.find_all(class_=lambda c: c and cls in c.lower())
            pagination_elements.extend(elements)

        # Method 2: Look for elements with navigation roles
        pagination_elements.extend(soup.find_all(attrs={"role": "navigation"}))

        # Method 3: Look for links containing page numbers
        page_links = soup.find_all(
            "a",
            href=lambda href: href and re.search(r"[?&]page=\d+|[?&]cursor=\d+", href),
        )
        pagination_elements.extend(page_links)

        if pagination_elements:
            print(f"Found {len(pagination_elements)} potential pagination elements")
            for i, elem in enumerate(pagination_elements[:3]):  # Show first 3
                print(f"Element {i + 1}: <{elem.name} class='{elem.get('class', '')}'>")
                print(f"  Content: {elem.get_text(strip=True)[:50]}...")
                print(f"  Links: {len(elem.find_all('a'))}")
        else:
            print("No obvious pagination elements found")

        # Look for URLs with cursor parameters
        links = soup.find_all("a", href=True)
        cursor_links = [link for link in links if "cursor=" in link.get("href", "")]

        if cursor_links:
            print(f"\nFound {len(cursor_links)} links with cursor parameter")
            for i, link in enumerate(cursor_links[:5]):  # Show first 5
                href = link.get("href")
                text = link.get_text(strip=True)
                print(f"  Link {i + 1}: text='{text}', href='{href}'")
        else:
            print("\nNo links with cursor parameter found")

        # Test pagination by fetching next page
        if "cursor=" in url:
            current_cursor = re.search(r"cursor=(\d+)", url)
            if current_cursor:
                current_cursor = int(current_cursor.group(1))
                next_cursor = current_cursor + 1
                next_url = re.sub(r"cursor=\d+", f"cursor={next_cursor}", url)
            else:
                next_url = url
        else:
            if "?" in url:
                next_url = f"{url}&cursor=2"
            else:
                next_url = f"{url}?filters=&cursor=2"

        print(f"\nTesting next page: {next_url}")
        try:
            next_response = session.get(next_url, timeout=30)
            print(f"Next page status code: {next_response.status_code}")

            if next_response.status_code == 200:
                next_soup = BeautifulSoup(next_response.text, "lxml")
                next_products = next_soup.find_all("article")
                print(f"Products on next page: {len(next_products)}")

                # Save next page HTML for comparison
                next_html_file = os.path.join(output_dir, "subcategory_next_page.html")
                with open(next_html_file, "w", encoding="utf-8") as f:
                    f.write(next_response.text)
                print(f"Saved next page HTML to {next_html_file}")
        except Exception as e:
            print(f"Error testing next page: {e}")

    # Find product cards/articles
    product_cards = soup.find_all("article")
    print(f"Found {len(product_cards)} product cards (article elements)")

    # If no articles found, look for other common product elements
    if not product_cards:
        print(
            "No article elements found, searching for alternative product containers..."
        )
        product_cards = soup.find_all(
            ["div", "li"],
            class_=lambda c: c and any(cls in c for cls in ["product", "card", "item"]),
        )
        print(f"Found {len(product_cards)} alternative product containers")

    # Analyze a sample product card
    if product_cards:
        sample_card = product_cards[0]
        print("\n=== Sample Product Card Structure ===")
        print(f"Tag: {sample_card.name}")
        print(f"Classes: {sample_card.get('class', '')}")

        # Find all direct child elements
        print("\nDirect children:")
        for i, child in enumerate([c for c in sample_card.children if c.name]):
            if child.name:
                print(f"{i + 1}. <{child.name}> with classes: {child.get('class', '')}")

        # Extract product information
        print("\nProduct information:")

        # Look for product name
        name_candidates = sample_card.find_all(["h2", "h3", "h4"])
        if name_candidates:
            print(
                f"Name element: <{name_candidates[0].name}> with text: {name_candidates[0].get_text(strip=True)}"
            )
        else:
            print("No name element found")

        # Look for price
        price_candidates = sample_card.find_all(
            ["span", "div"],
            class_=lambda c: c and any(cls in c for cls in ["price", "cost", "amount"]),
        )
        if price_candidates:
            print(
                f"Price element: <{price_candidates[0].name}> with text: {price_candidates[0].get_text(strip=True)}"
            )
        else:
            print("No price element found")

        # Look for product info
        info_candidates = sample_card.find_all(
            ["p", "span", "div"],
            class_=lambda c: c
            and any(cls in c for cls in ["info", "description", "details", "subdued"]),
        )
        if info_candidates:
            print(
                f"Info element: <{info_candidates[0].name}> with text: {info_candidates[0].get_text(strip=True)}"
            )
        else:
            print("No info element found")

        # Look for image
        img_candidates = sample_card.find_all("img")
        if img_candidates:
            print(f"Image element: <img> with src: {img_candidates[0].get('src', '')}")
        else:
            print("No image element found")

        # Save sample card HTML
        sample_file = os.path.join(output_dir, "sample_product_card.html")
        with open(sample_file, "w", encoding="utf-8") as f:
            f.write(str(sample_card.prettify()))
        print(f"\nSaved sample product card to {sample_file}")

    # Output XPaths for key elements
    print("\n=== XPath Information ===")
    if product_cards:
        # Find all product cards
        all_cards = soup.find_all("article")

        # Get XPath for the first and second product cards
        if len(all_cards) >= 2:
            print("XPath for product cards:")

            # Find the index of product cards in their parent
            parent = all_cards[0].parent
            card_indices = [
                i for i, child in enumerate(parent.children) if child in all_cards
            ]

            if card_indices:
                print(f"First card: //article[{card_indices[0] + 1}]")
                if len(card_indices) > 1:
                    print(f"Second card: //article[{card_indices[1] + 1}]")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Oda Subcategory Explorer")
    parser.add_argument("url", help="Subcategory URL to explore")
    parser.add_argument(
        "-o", "--output", default="debug", help="Output directory for debug files"
    )
    parser.add_argument(
        "-p", "--pagination", action="store_true", help="Check for pagination"
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debug logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level="DEBUG" if args.debug else "INFO")

    # Run the explorer
    explore_subcategory(args.url, args.output, check_pagination=args.pagination)


if __name__ == "__main__":
    main()
