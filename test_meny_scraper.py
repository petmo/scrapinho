"""Simple script to test the Meny scraper directly."""

import sys
import requests
from bs4 import BeautifulSoup
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler()],
)


def test_product_extraction(html_file=None):
    """Test product extraction from Meny website or HTML file."""
    logger = logging.getLogger("test_scraper")

    # Get HTML content
    if html_file and Path(html_file).exists():
        logger.info(f"Loading HTML from file: {html_file}")
        with open(html_file, "r", encoding="utf-8") as f:
            html_content = f.read()
    else:
        url = "https://meny.no/varer/meieri-egg/"
        logger.info(f"Fetching HTML from URL: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"Failed to fetch URL: {response.status_code}")
            return
        html_content = response.text

    # Parse HTML
    soup = BeautifulSoup(html_content, "lxml")

    # Extract products using different methods
    logger.info("Trying different extraction methods:")

    # Method 1: Find schema.org product markup
    schema_products = soup.find_all(attrs={"itemtype": "http://schema.org/Product"})
    logger.info(f"1. Schema.org products: {len(schema_products)}")

    # Save one sample if found
    if schema_products:
        with open("schema_product_sample.html", "w", encoding="utf-8") as f:
            f.write(str(schema_products[0].prettify()))
        logger.info("Saved schema product sample to schema_product_sample.html")

    # Method 2: Find product list items
    lists = soup.find_all("ul")
    logger.info(f"Found {len(lists)} lists")

    for i, ul in enumerate(lists):
        li_count = len(ul.find_all("li", recursive=False))
        if li_count > 5:
            logger.info(f"List {i + 1}: Has {li_count} direct li children")

            # Check product characteristics
            product_like = 0
            for li in ul.find_all("li", recursive=False):
                has_price = bool(li.find(string=lambda s: s and "kr" in s))
                has_image = bool(li.find("img"))

                if has_price and has_image:
                    product_like += 1

            logger.info(f"  - Product-like items: {product_like}")

            # If this looks like a product list, save it
            if product_like > 5:
                logger.info(f"  - This appears to be a product list!")
                first_li = ul.find("li")
                if first_li:
                    with open(f"product_list_{i}.html", "w", encoding="utf-8") as f:
                        f.write(str(ul.prettify()))
                    with open(f"product_item_{i}.html", "w", encoding="utf-8") as f:
                        f.write(str(first_li.prettify()))
                    logger.info(f"  - Saved product list and first item to files")

    # Method 3: Find divs with specific classes
    ws_products = soup.find_all("div", class_="ws-product-vertical")
    logger.info(f"3. ws-product-vertical divs: {len(ws_products)}")

    if ws_products:
        with open("ws_product_sample.html", "w", encoding="utf-8") as f:
            f.write(str(ws_products[0].prettify()))
        logger.info("Saved ws-product sample to ws_product_sample.html")


if __name__ == "__main__":
    html_file = sys.argv[1] if len(sys.argv) > 1 else None
    test_product_extraction(html_file)
