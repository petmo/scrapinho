"""Meny-specific scraper implementation."""

import logging
import re
import time
import uuid
from typing import List, Dict, Any, Optional, Generator, Tuple
import json

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

from models.product import Product
from scraper.base_scraper import BaseScraper


class MenyScraper(BaseScraper):
    """Scraper for Meny.no.

    This class implements the BaseScraper interface for Meny's website.

    Meny's website loads more products via a "Vis flere" (Show more) button,
    which performs an AJAX request to load the next batch of products.
    """

    def __init__(
        self,
        base_url: str = "https://meny.no",
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        request_delay: float = 1.5,
        max_retries: int = 3,
        timeout: int = 30,
        products_per_page: int = 24,
        max_pages: int = 20,
    ) -> None:
        """Initialize the Meny scraper.

        Args:
            base_url: Base URL for Meny.no
            user_agent: User agent string to use for requests
            request_delay: Delay between requests in seconds
            max_retries: Maximum number of retries for failed requests
            timeout: Timeout for requests in seconds
            products_per_page: Number of products per page/load
            max_pages: Maximum number of pages to load
        """
        super().__init__(
            base_url=base_url,
            user_agent=user_agent,
            request_delay=request_delay,
            max_retries=max_retries,
            timeout=timeout,
        )
        self.logger = logging.getLogger(__name__)
        self.products_per_page = products_per_page
        self.max_pages = max_pages

    def _extract_product_cards(self, soup: BeautifulSoup) -> List[BeautifulSoup]:
        """Extract product card elements from a page.

        Args:
            soup: BeautifulSoup object of the page

        Returns:
            List of BeautifulSoup objects representing product cards
        """
        # Primary selector for Meny's product list items
        product_cards = soup.select("li.ws-product-list-vertical__item")

        if product_cards:
            self.logger.debug(
                f"Found {len(product_cards)} products using li.ws-product-list-vertical__item"
            )
            return product_cards

        # Secondary selector for the product vertical divs
        product_divs = soup.select("div.ws-product-vertical")
        if product_divs:
            self.logger.debug(
                f"Found {len(product_divs)} products using div.ws-product-vertical"
            )
            return product_divs

        # Fallback to the product list container
        product_list = soup.select_one("ul.ws-product-list-vertical")
        if product_list:
            list_items = product_list.find_all("li")
            # Filter list items to include only those with product structure
            valid_product_items = []
            for item in list_items:
                # Check for product characteristics
                has_price = bool(item.find(string=lambda s: s and "kr" in s))
                has_image = bool(item.find("img"))

                if has_price and has_image:
                    valid_product_items.append(item)

            self.logger.debug(
                f"Found {len(valid_product_items)} valid products by extracting list items from the product list"
            )
            return valid_product_items

        # Last resort fallbacks
        self.logger.warning(
            "No product cards found using standard selectors, trying fallback approach"
        )

        fallback_cards = []
        # Look for any list items with product structure
        all_list_items = soup.find_all("li")
        for item in all_list_items:
            product_div = item.find(
                "div", class_=lambda c: c and "product" in c.lower()
            )
            has_price = bool(item.find(string=lambda s: s and "kr" in s))
            has_image = bool(item.find("img"))

            if product_div and has_price and has_image:
                fallback_cards.append(item)

        if fallback_cards:
            self.logger.debug(
                f"Found {len(fallback_cards)} products using fallback selectors"
            )
            return fallback_cards

        self.logger.warning("No product cards found on the page")
        return []

    def _parse_price(self, price_text: str) -> float:
        """Parse a price string to extract the numeric value.

        Args:
            price_text: Price text to parse (e.g., "kr 35,30")

        Returns:
            Numeric price value
        """
        try:
            # Extract numbers with currency
            price_match = re.search(r"(?:kr|kr\s+)?(\d+[,.]\d+|\d+)", price_text)
            if price_match:
                # Extract the matched price and clean it
                price_str = price_match.group(1)
                # Replace comma with dot for decimal point
                price_str = price_str.replace(",", ".")
                return float(price_str)

            # If pattern doesn't match, try general cleaning
            price_text = price_text.replace("kr", "").replace("&nbsp;", " ").strip()
            price_text = price_text.replace(",", ".")
            # Remove any remaining non-numeric characters except dot
            price_text = re.sub(r"[^\d.]", "", price_text)

            if price_text:
                return float(price_text)

            self.logger.warning(f"Could not parse price from '{price_text}'")
            return 0.0
        except Exception as e:
            self.logger.warning(f"Failed to parse price '{price_text}': {e}")
            return 0.0

    def _extract_product_info_from_card(
        self, card: BeautifulSoup, category: str
    ) -> Optional[Product]:
        """Extract product information from a product card.

        Args:
            card: BeautifulSoup object of the product card
            category: Product category

        Returns:
            Product object if successful, None otherwise
        """
        try:
            # Find the main product div inside the list item
            product_div = card.select_one("div.ws-product-vertical")
            if not product_div:
                product_div = card  # If not found, use the card itself

            # Generate a product ID from the URL since Meny has product IDs in URLs
            name_link = product_div.select_one("a.ws-product-vertical__link")
            if not name_link:
                name_link = product_div.select_one("h3 a")

            if not name_link:
                self.logger.warning("Could not find product link")
                return None

            product_url = name_link.get("href", "")

            # Extract product ID from URL or generate one
            if "/varer/" in product_url and product_url.split("/varer/")[-1]:
                product_id = product_url.split("/varer/")[-1].rstrip("/")
            else:
                product_id = str(uuid.uuid4())

            if product_url and not product_url.startswith(("http://", "https://")):
                product_url = urljoin(self.base_url, product_url)

            # Extract product name
            name_elem = product_div.select_one("h3.ws-product-vertical__title")
            if not name_elem:
                name_elem = name_link

            name = name_elem.get_text(strip=True) if name_elem else ""

            # Extract product info
            info_elem = product_div.select_one("p.ws-product-vertical__subtitle")
            info = info_elem.get_text(strip=True) if info_elem else ""

            # Extract brand from info
            brand = None
            if info:
                # Brand is often the last part of info
                info_parts = info.split()
                if len(info_parts) > 1:
                    brand = info_parts[-1]

            # Extract price
            price_elem = product_div.select_one("div.ws-product-vertical__price")
            if not price_elem:
                self.logger.warning(f"No price found for {name}")
                return None

            price_text = price_elem.get_text(strip=True)
            price = self._parse_price(price_text)

            # Extract unit price
            unit_price_elem = product_div.select_one(
                "p.ws-product-vertical__price-unit"
            )
            unit_price = (
                unit_price_elem.get_text(strip=True) if unit_price_elem else None
            )

            # Extract image URL
            img_elem = product_div.select_one("img")
            image_url = img_elem.get("src") if img_elem else None

            # Ensure image URL is absolute
            if image_url and not image_url.startswith(("http://", "https://")):
                image_url = urljoin(self.base_url, image_url)

            return Product(
                product_id=product_id,
                name=name,
                brand=brand,
                info=info,
                price=price,
                price_text=price_text,
                unit_price=unit_price,
                image_url=image_url,
                category=category,
                subcategory=None,
                url=product_url,
            )
        except Exception as e:
            self.logger.error(f"Failed to extract product info: {e}", exc_info=True)
            return None

    def _get_next_page_url(self, current_url: str, page: int) -> str:
        """Generate the URL for the next page.

        For Meny, this involves adding or updating the page parameter.

        Args:
            current_url: Current page URL
            page: Next page number

        Returns:
            URL for the next page
        """
        # Parse the current URL
        parsed_url = urlparse(current_url)
        query_params = parse_qs(parsed_url.query)

        # Update the page parameter
        query_params["page"] = [str(page)]

        # Rebuild the URL
        new_query = urlencode(query_params, doseq=True)
        parts = list(parsed_url)
        parts[4] = new_query

        return urljoin(self.base_url, parsed_url.path + "?" + new_query)

    def _is_valid_product_url(self, url: str) -> bool:
        """Check if a URL is a valid product URL.

        Args:
            url: URL to check

        Returns:
            True if it's a valid product URL, False otherwise
        """
        if not url or not isinstance(url, str):
            return False

        # Must contain /varer/ path
        if "/varer/" not in url:
            return False

        # Exclude base category URLs and special sections
        invalid_patterns = ["/varer/tilbud/", "/varer/nyheter/", "/varer/oppskrifter/"]

        # Check for invalid patterns
        if any(pattern in url for pattern in invalid_patterns):
            return False

        # Must have something after /varer/ - either a product ID or category
        parts = url.split("/varer/")
        if len(parts) < 2 or not parts[1]:
            return False

        return True

    def get_product(self, product_url: str) -> Optional[Product]:
        """Scrape a single product.

        Args:
            product_url: URL of the product to scrape

        Returns:
            Product object if successful, None otherwise
        """
        try:
            # Validate URL before processing
            if not self._is_valid_product_url(product_url):
                self.logger.warning(f"Invalid product URL: {product_url}")
                return None

            response = self._make_request(product_url)
            soup = BeautifulSoup(response.text, "lxml")

            # Extract product information from the product page
            product_id = (
                product_url.split("/varer/")[-1].rstrip("/")
                if "/varer/" in product_url
                else str(uuid.uuid4())
            )

            # Extract product name
            name_element = soup.select_one("h1")
            if not name_element:
                self.logger.warning(f"No product name found at {product_url}")
                return None
            name = name_element.get_text(strip=True)

            # Extract price
            price_element = soup.select_one("[itemprop='price']") or soup.select_one(
                ".ws-product-price-regular"
            )
            if not price_element:
                self.logger.warning(f"No price found for {name} at {product_url}")
                return None
            price_text = price_element.get_text(strip=True)
            price = self._parse_price(price_text)

            # Extract other information
            info_element = soup.select_one(
                "[itemprop='description']"
            ) or soup.select_one(".ws-product-description")
            info = info_element.get_text(strip=True) if info_element else ""

            # Extract image URL
            image_element = soup.select_one("[itemprop='image']") or soup.select_one(
                ".ws-product-image img"
            )
            image_url = image_element.get("src") if image_element else None

            # Extract brand
            brand_element = soup.select_one("[itemprop='brand']") or soup.select_one(
                ".ws-product-brand"
            )
            brand = brand_element.get_text(strip=True) if brand_element else None

            # Extract category from breadcrumbs
            category = "unknown"
            breadcrumbs = soup.select(".breadcrumbs a")
            if len(breadcrumbs) >= 2:  # Skip "Home" breadcrumb
                category = breadcrumbs[1].get_text(strip=True)

            # Create and return the product
            return Product(
                product_id=product_id,
                name=name,
                info=info,
                price=price,
                price_text=price_text,
                unit_price=None,  # Extract from product page if available
                brand=brand,
                image_url=image_url,
                category=category,
                subcategory=None,
                url=product_url,
            )
        except Exception as e:
            self.logger.error(
                f"Failed to scrape product from {product_url}: {e}", exc_info=True
            )
            return None

    def get_products(
        self, category_url: str, max_products: Optional[int] = None
    ) -> List[Product]:
        """Scrape products from a category page, handling pagination.

        Args:
            category_url: URL of the category to scrape
            max_products: Maximum number of products to scrape

        Returns:
            List of scraped products
        """
        # Import tqdm.auto which automatically selects the best available progress bar
        from tqdm.auto import tqdm

        all_products = []

        # Extract category name from URL
        category_parts = category_url.strip("/").split("/")
        category_name = category_parts[-1] if category_parts else "unknown"
        # Clean up category name
        category_name = category_name.replace("-", " ").title()

        try:
            # Start with page 1
            current_page = 1
            total_pages = self.max_pages  # Default value

            # Create progress bar for pages
            with tqdm(
                desc=f"Pages in {category_name}",
                unit="page",
                total=total_pages,
                dynamic_ncols=True,  # Automatically adjust width
                leave=True,  # Keep the progress bar after completion
                colour="green",
            ) as page_progress:

                while current_page <= self.max_pages:
                    # Construct URL with page parameter
                    if current_page == 1:
                        page_url = category_url
                    else:
                        page_url = self._get_next_page_url(category_url, current_page)

                    page_progress.set_description(
                        f"Page {current_page} of {category_name}"
                    )
                    self.logger.debug(
                        f"Fetching page {current_page} of category '{category_name}': {page_url}"
                    )

                    # Get the page content
                    response = self._make_request(page_url)
                    soup = BeautifulSoup(response.text, "lxml")

                    # Check for pagination information
                    pagination_element = soup.select_one("[data-page]")
                    if pagination_element:
                        current_page_attr = pagination_element.get("data-page")
                        total_pages_attr = pagination_element.get("data-total-pages")

                        if current_page_attr and total_pages_attr:
                            total_pages = min(int(total_pages_attr), self.max_pages)
                            page_progress.total = total_pages
                            page_progress.refresh()

                    # Extract product cards from this page
                    product_cards = self._extract_product_cards(soup)

                    # If no products found, we've reached the end
                    if not product_cards:
                        self.logger.info(
                            f"No more products found on page {current_page}, ending pagination"
                        )
                        break

                    # Process products from this page
                    page_products = []

                    # Create progress bar for products on this page
                    with tqdm(
                        total=len(product_cards),
                        desc=f"Products on page {current_page}",
                        unit="product",
                        leave=False,  # Remove after completion
                        dynamic_ncols=True,
                        colour="blue",
                    ) as product_progress:

                        for card in product_cards:
                            product = self._extract_product_info_from_card(
                                card, category_name
                            )
                            if product:
                                page_products.append(product)
                            product_progress.update(1)

                    self.logger.info(
                        f"Extracted {len(page_products)} products from page {current_page} of {category_name}"
                    )
                    all_products.extend(page_products)

                    # Check if we've reached the maximum products limit
                    if max_products is not None and len(all_products) >= max_products:
                        self.logger.info(
                            f"Reached maximum product limit ({max_products}), stopping pagination"
                        )
                        all_products = all_products[:max_products]
                        break

                    # Check if there's a "Vis flere" (Show more) button
                    show_more_button = soup.select_one("button.ngr-button")
                    has_more_button = False

                    if show_more_button:
                        button_text = show_more_button.get_text(strip=True)
                        has_more_button = "Vis flere" in button_text

                    if not has_more_button:
                        # Alternative ways to detect if there are more pages
                        if pagination_element:
                            if int(current_page_attr) >= int(total_pages_attr):
                                self.logger.info(
                                    f"Reached last page ({current_page_attr}/{total_pages_attr})"
                                )
                                break
                        else:
                            # No pagination info found, we'll assume we're at the end
                            self.logger.info(
                                "No 'Vis flere' button or pagination info found, assuming last page"
                            )
                            break

                    # Move to next page
                    current_page += 1
                    page_progress.update(1)

            self.logger.info(
                f"Total products scraped from category '{category_name}': {len(all_products)}"
            )
            return all_products
        except Exception as e:
            self.logger.error(
                f"Failed to scrape products from category {category_url}: {e}",
                exc_info=True,
            )
            return all_products
