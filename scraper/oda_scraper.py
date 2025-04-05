"""Oda-specific scraper implementation."""

import logging
import re
import time
import uuid
from typing import List, Dict, Any, Optional, Generator, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from models.product import Product
from scraper.base_scraper import BaseScraper


class OdaScraper(BaseScraper):
    """Scraper for Oda.com.

    This class implements the BaseScraper interface for Oda's website.
    """

    def __init__(
        self,
        base_url: str = "https://oda.com",
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        request_delay: float = 1.5,
        max_retries: int = 3,
        timeout: int = 30,
    ) -> None:
        """Initialize the Oda scraper.

        Args:
            base_url: Base URL for Oda.com
            user_agent: User agent string to use for requests
            request_delay: Delay between requests in seconds
            max_retries: Maximum number of retries for failed requests
            timeout: Timeout for requests in seconds
        """
        super().__init__(
            base_url=base_url,
            user_agent=user_agent,
            request_delay=request_delay,
            max_retries=max_retries,
            timeout=timeout,
        )
        self.logger = logging.getLogger(__name__)
        self.skip_subcategories = ["Alle i Meieri, ost og egg", "Alle i Drikke"]

    def _extract_subcategories(self, category_url: str) -> List[Dict[str, str]]:
        """Extract subcategory URLs from a category page.

        Args:
            category_url: URL of the category page

        Returns:
            List of dictionaries with subcategory name and URL
        """
        try:
            response = self._make_request(category_url)
            soup = BeautifulSoup(response.text, "lxml")
            self.logger.debug(
                f"Fetched category page with status code {response.status_code}"
            )

            subcategories = []

            # Debug info about the page structure
            self.logger.debug(
                f"Page title: {soup.title.text if soup.title else 'No title'}"
            )

            # Find all section elements
            sections = soup.find_all("section")
            self.logger.debug(f"Found {len(sections)} section elements on the page")

            # Look specifically for sections with subcategory links
            # Based on the XPath provided, subcategories seem to be in sections 2 and 3
            for i, section in enumerate(sections):
                self.logger.debug(
                    f"Examining section {i+1}, class: {section.get('class', 'no-class')}"
                )

                # Look for elements with anchor tags (links)
                subcategory_links = section.find_all("a", href=True)
                self.logger.debug(
                    f"Found {len(subcategory_links)} links in section {i+1}"
                )

                for j, link in enumerate(subcategory_links):
                    # Get the URL
                    url = link.get("href")

                    # Extract from span elements, which is how Oda structures their subcategory links
                    # First, try to find spans inside the link
                    span_elements = link.find_all("span")

                    if span_elements:
                        # The text content is often nested in a structure like:
                        # <span>...<span>✓</span>Melk (84)</span>
                        # Extract the text and clean it up
                        full_text = span_elements[0].get_text(strip=True)

                        # Remove the checkmark if present
                        full_text = full_text.replace("✓", "").strip()

                        # Extract the main category name (remove count in parentheses)
                        import re

                        name_match = re.match(r"([^(]+)", full_text)
                        name = name_match.group(1).strip() if name_match else full_text
                    else:
                        # Fallback to the link's text content
                        name = link.get_text(strip=True)

                    # Log the potential subcategory information
                    self.logger.debug(
                        f"Link {j+1} in section {i+1}: URL={url}, Name={name}"
                    )

                    if name and url and "/categories/" in url:
                        full_url = urljoin(self.base_url, url)
                        self.logger.debug(f"Adding subcategory: {name} -> {full_url}")

                        subcategories.append({"name": name, "url": full_url})

            # If no subcategories found through the standard approach, use the XPath approach as a fallback
            if not subcategories:
                self.logger.warning(
                    "No subcategories found using standard approach, trying XPath approach"
                )

                # Based on the provided XPath, try to find subcategory links directly
                # XPath examples from the prompt:
                # /html/body/div/div[3]/main/div/div/div/section[2]/a[1]
                # /html/body/div/div[3]/main/div/div/div/section[3]/a[1]

                # Find the relevant sections
                main_element = soup.find("main")
                if main_element:
                    div_wrappers = main_element.find_all("div")
                    for div in div_wrappers:
                        sections = div.find_all("section")
                        for i, section in enumerate(sections):
                            links = section.find_all("a", href=True)
                            for j, link in enumerate(links):
                                url = link.get("href")
                                # Try to get text from any element inside the link
                                name_element = link.find(
                                    ["h2", "h3", "h4", "span", "div", "p"]
                                )
                                if name_element:
                                    name = name_element.get_text(strip=True)
                                else:
                                    name = link.get_text(strip=True)

                                if name and url:
                                    full_url = urljoin(self.base_url, url)
                                    self.logger.debug(
                                        f"Adding subcategory from XPath approach: {name} -> {full_url}"
                                    )

                                    subcategories.append(
                                        {"name": name, "url": full_url}
                                    )

                # Additional fallback: try to find links containing subcategory-like words
                if not subcategories:
                    all_links = soup.find_all("a", href=True)
                    subcategory_keywords = [
                        "melk",
                        "plantebaserte",
                        "smør",
                        "egg",
                        "fløte",
                        "yogurt",
                        "ost",
                    ]

                    for link in all_links:
                        url = link.get("href")
                        text = link.get_text(strip=True).lower()

                        if (
                            any(
                                keyword in text.lower()
                                for keyword in subcategory_keywords
                            )
                            and url
                        ):
                            full_url = urljoin(self.base_url, url)
                            self.logger.debug(
                                f"Adding subcategory from keyword match: {text} -> {full_url}"
                            )

                            subcategories.append({"name": text, "url": full_url})

            self.logger.info(
                f"Found {len(subcategories)} subcategories in {category_url}"
            )
            return subcategories
        except Exception as e:
            self.logger.error(
                f"Failed to extract subcategories from {category_url}: {e}",
                exc_info=True,
            )
            return []

    def _extract_product_urls(self, category_url: str) -> List[str]:
        """Extract product URLs from a category page.

        Args:
            category_url: URL of the category page

        Returns:
            List of product URLs
        """
        try:
            response = self._make_request(category_url)
            soup = BeautifulSoup(response.text, "lxml")
            self.logger.debug(f"Fetched product category page: {category_url}")

            product_urls = []

            # First attempt: Find product articles with more specific criteria
            product_cards = []

            # Look for all article elements
            all_articles = soup.find_all("article")

            # Filter articles to only include those that appear to be product cards
            for article in all_articles:
                # Check for product characteristics
                has_price = bool(article.find(string=lambda s: s and "kr" in s))
                has_image = bool(article.find("img"))

                # Check for valid product links (must have more than just "/products/")
                product_links = article.find_all(
                    "a",
                    href=lambda h: h
                    and "/products/" in h
                    and h != "/products/"
                    and not h.endswith("/products/"),
                )
                has_valid_product_link = len(product_links) > 0

                # Only include articles that have pricing and either an image or valid product link
                if has_price and (has_image or has_valid_product_link):
                    product_cards.append(article)

            self.logger.debug(
                f"Found {len(product_cards)} valid product article elements out of {len(all_articles)} total articles"
            )

            for card in product_cards:
                # Find the product link - specifically looking for valid product URLs
                valid_product_links = card.find_all(
                    "a", href=lambda h: h and self._is_valid_product_url(h)
                )

                if valid_product_links:
                    url = valid_product_links[0].get("href")
                    full_url = urljoin(self.base_url, url)
                    if full_url not in product_urls:  # Avoid duplicates
                        product_urls.append(full_url)

            # If no products found through the refined approach, fall back to looking for valid product links
            if not product_urls:
                self.logger.debug(
                    "No products found using filtered article elements, trying alternative approach"
                )

                # Look for product links with specific validation
                all_links = soup.find_all("a", href=True)

                for link in all_links:
                    href = link.get("href")
                    if self._is_valid_product_url(href):
                        full_url = urljoin(self.base_url, href)
                        if full_url not in product_urls:  # Avoid duplicates
                            product_urls.append(full_url)

            self.logger.info(
                f"Found {len(product_urls)} product URLs in {category_url}"
            )
            return product_urls
        except Exception as e:
            self.logger.error(
                f"Failed to extract product URLs from {category_url}: {e}",
                exc_info=True,
            )
            return []

    def _is_valid_product_url(self, url: str) -> bool:
        """Check if a URL is a valid product URL.

        Args:
            url: URL to check

        Returns:
            True if it's a valid product URL, False otherwise
        """
        if not url or not isinstance(url, str):
            return False

        # Must contain /products/ path
        if "/products/" not in url:
            return False

        # Exclude base product URLs and special sections
        invalid_patterns = [
            "/products/news/",
            "/products/discounts/",
            "/products/favourites/",
            "/products/search",
        ]
        if (
            url == "/products/"
            or url.endswith("/products/")
            or any(pattern in url for pattern in invalid_patterns)
        ):
            return False

        # Must have something after /products/ - either a product ID or category/subcategory
        parts = url.split("/products/")
        if len(parts) < 2 or not parts[1]:
            return False

        # Check for common product URL patterns (numbers for IDs or multiple segments)
        product_part = parts[1]

        # Check if it contains digits (likely a product ID)
        contains_digits = any(c.isdigit() for c in product_part)

        # Check if it contains multiple segments (e.g., category/product)
        has_multiple_segments = "/" in product_part

        # URL should have either digits (product ID) or multiple path segments
        return contains_digits or has_multiple_segments

    def _parse_price(self, price_text: str) -> float:
        """Parse a price string to extract the numeric value.

        Args:
            price_text: Price text to parse (e.g., "kr 35,30")

        Returns:
            Numeric price value
        """
        try:
            # Remove non-price content
            if "Hopp til hovedinnhold" in price_text:
                self.logger.warning(
                    f"Found header text instead of price: '{price_text}'"
                )
                return 0.0

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

    def _extract_product_info(
        self,
        soup: BeautifulSoup,
        product_url: str,
        category: Optional[str] = None,
        subcategory: Optional[str] = None,
    ) -> Optional[Product]:
        """Extract product information from a product page.

        Args:
            soup: BeautifulSoup object of the product page
            product_url: URL of the product page
            category: Product category
            subcategory: Product subcategory

        Returns:
            Product object if successful, None otherwise
        """
        try:
            # Generate a unique ID for the product (in a real implementation,
            # you would extract this from the page or URL)
            product_id = str(uuid.uuid4())

            # Extract product name
            name_element = soup.select_one("h2")
            if not name_element:
                self.logger.warning(f"No product name found at {product_url}")
                return None
            name = name_element.get_text(strip=True)

            # Extract product info (brand, size)
            info_element = soup.select_one("p.k-text-style--body-s")
            info = info_element.get_text(strip=True) if info_element else ""

            # Extract price - try multiple selectors
            price_element = None
            # Try multiple selectors for price elements
            price_selectors = [
                "span.k-text-style--label-m.k-text--weight-bold",  # Bold label is often price
                "span.k-text-color--default",  # Default color text often contains price
                "div.price span",  # Generic price span
                "span[class*='price']",  # Any span with 'price' in class
                "span.k-text-style--label-m",  # Fallback to any label-m span
            ]

            for selector in price_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    # Check if the text contains currency symbol or digits
                    if "kr" in text or re.search(r"\d", text):
                        price_element = element
                        break
                if price_element:
                    break

            # If still not found, try looking for elements containing currency
            if not price_element:
                for elem in soup.find_all(["span", "div", "p"]):
                    text = elem.get_text(strip=True)
                    if "kr" in text and re.search(r"\d", text):
                        price_element = elem
                        break

            # If no price found through selectors, try XPath-like approach
            if not price_element:
                # Look for elements in the product card's price section
                article = soup.find("article")
                if article:
                    divs = article.find_all("div")
                    # Price is often in the second div or nested deeper
                    for div in divs:
                        spans = div.find_all("span")
                        for span in spans:
                            text = span.get_text(strip=True)
                            if "kr" in text and re.search(r"\d", text):
                                price_element = span
                                break

            if not price_element:
                self.logger.warning(f"No price found for {name} at {product_url}")
                self.logger.debug(
                    f"HTML structure around product card: {soup.find('article')}"
                )
                return None

            price_text = price_element.get_text(strip=True)
            price = self._parse_price(price_text)

            # Extract unit price with similar fallback approach
            unit_price_element = None
            unit_price_selectors = [
                "p.k-text-style--label-s.k-text-color--subdued",  # Typical unit price style
                "p.k-text-style--label-s",  # Any small label text
                "p[class*='subdued']",  # Any subdued paragraph
                "span[class*='unit']",  # Any span with 'unit' in class
            ]

            for selector in unit_price_selectors:
                elements = soup.select(selector)
                for element in elements:
                    text = element.get_text(strip=True)
                    # Unit prices typically contain "/" character (e.g., kr/kg)
                    if "/" in text and "kr" in text:
                        unit_price_element = element
                        break
                if unit_price_element:
                    break

            unit_price = (
                unit_price_element.get_text(strip=True) if unit_price_element else None
            )

            # Extract image URL
            image_url = self._extract_product_image(soup, name, product_url)

            if name == "Tine Lettmelk":
                print("ho")

            # Create and return product
            return Product(
                product_id=product_id,
                name=name,
                info=info,
                price=price,
                price_text=price_text,
                unit_price=unit_price,
                image_url=image_url,
                category=category,
                subcategory=subcategory,
                url=product_url,
            )
        except Exception as e:
            self.logger.error(
                f"Failed to extract product info from {product_url}: {e}", exc_info=True
            )
            return None

    def get_product(self, product_url: str) -> Optional[Product]:
        """Scrape a single product.

        Args:
            product_url: URL of the product to scrape

        Returns:
            Product object if successful, None otherwise
        """
        try:
            response = self._make_request(product_url)
            soup = BeautifulSoup(response.text, "lxml")
            return self._extract_product_info(soup, product_url)
        except Exception as e:
            self.logger.error(
                f"Failed to scrape product from {product_url}: {e}", exc_info=True
            )
            return None

    def get_products_from_subcategory(
        self,
        subcategory_url: str,
        subcategory_name: str,
        category: str,
        max_products: Optional[int] = None,
    ) -> List[Product]:
        """Scrape products from a subcategory page, handling pagination.

        Args:
            subcategory_url: URL of the subcategory page
            subcategory_name: Name of the subcategory
            category: Parent category name
            max_products: Maximum number of products to scrape

        Returns:
            List of scraped products
        """
        # Import tqdm.auto which automatically selects the best available progress bar
        from tqdm.auto import tqdm

        products = []
        product_urls = []
        cursor = 1
        max_pagination_attempts = 20  # Safety limit to prevent infinite loops

        try:
            # Normalize subcategory URL to prevent duplicate scraping
            base_subcategory_url = subcategory_url.split("?")[0].rstrip("/")
            if base_subcategory_url.endswith("filters="):
                base_subcategory_url = base_subcategory_url[:-8]

            # Handle pagination by incrementing the cursor
            with tqdm(
                total=max_pagination_attempts,
                desc=f"Pages in {subcategory_name}",
                unit="page",
                leave=True,  # Keep the progress bar after completion
                ncols=80,  # Narrower to ensure it fits in terminals
                colour="cyan",
                dynamic_ncols=True,  # Automatically adjust width
            ) as page_progress:
                while cursor <= max_pagination_attempts:
                    # Construct URL with pagination cursor
                    if "?" in subcategory_url:
                        # URL already has parameters
                        if "cursor=" in subcategory_url:
                            # Replace existing cursor
                            paginated_url = re.sub(
                                r"cursor=\d+", f"cursor={cursor}", subcategory_url
                            )
                        else:
                            # Add cursor parameter
                            paginated_url = f"{subcategory_url}&cursor={cursor}"
                    else:
                        # No parameters yet, add cursor
                        paginated_url = f"{subcategory_url}?filters=&cursor={cursor}"

                    self.logger.debug(
                        f"Fetching page {cursor} of subcategory '{subcategory_name}': {paginated_url}"
                    )
                    page_progress.set_description(
                        f"Page {cursor} of {subcategory_name}"
                    )

                    # Fetch product URLs from this page
                    page_product_urls = self._extract_product_urls(paginated_url)

                    # If no products found on this page, we've reached the end
                    if not page_product_urls:
                        self.logger.debug(
                            f"No more products found on page {cursor}, ending pagination"
                        )
                        break

                    self.logger.debug(
                        f"Found {len(page_product_urls)} products on page {cursor}"
                    )
                    product_urls.extend(page_product_urls)

                    # Check if we've reached the maximum products limit
                    if max_products is not None and len(product_urls) >= max_products:
                        self.logger.debug(
                            f"Reached maximum product limit ({max_products}), stopping pagination"
                        )
                        product_urls = product_urls[:max_products]
                        break

                    # Move to next page
                    cursor += 1
                    page_progress.update(1)

            # Set actual total number of products for progress bar
            product_count = len(product_urls)
            self.logger.info(f"Found {product_count} product URLs to process")

            # Process each product URL with a progress bar
            with tqdm(
                total=product_count,
                desc=f"Products in {subcategory_name}",
                unit="product",
                leave=True,  # Keep the bar after completion
                ncols=80,
                colour="blue",
                dynamic_ncols=True,  # Automatically adjust width
            ) as product_progress:
                for product_url in product_urls:
                    try:
                        response = self._make_request(product_url)
                        soup = BeautifulSoup(response.text, "lxml")
                        product = self._extract_product_info(
                            soup,
                            product_url,
                            category=category,
                            subcategory=subcategory_name,
                        )
                        if product:
                            products.append(product)
                    except Exception as e:
                        self.logger.error(
                            f"Failed to scrape product from {product_url}: {e}",
                            exc_info=True,
                        )
                    finally:
                        product_progress.update(1)

            self.logger.info(
                f"Scraped {len(products)} products from subcategory '{subcategory_name}'"
            )
            return products
        except Exception as e:
            self.logger.error(
                f"Failed to scrape products from subcategory {subcategory_url}: {e}",
                exc_info=True,
            )
            return products

    def get_products(
        self, category_url: str, max_products: Optional[int] = None
    ) -> List[Product]:
        """Scrape products from a category and all its subcategories.

        Args:
            category_url: URL of the category to scrape
            max_products: Maximum number of products to scrape per subcategory

        Returns:
            List of scraped products
        """
        from tqdm.auto import tqdm
        import sys

        all_products = []
        processed_urls = set()  # Track already processed URLs to avoid duplicates

        # Extract category name from URL
        category_match = re.search(r"/categories/\d+-([^/]+)/", category_url)
        category_name = category_match.group(1) if category_match else "unknown"

        # Get subcategories
        subcategories = self._extract_subcategories(category_url)
        self.logger.info(
            f"Found {len(subcategories)} subcategories in category '{category_name}'"
        )

        if not subcategories:
            self.logger.warning(f"No subcategories found for {category_url}")
            return []

        # Add the main category as a "subcategory" to scrape its products too
        # but only if it's not already in the subcategories list
        main_category_url = category_url
        if not any(sub["url"] == main_category_url for sub in subcategories):
            subcategories.append(
                {
                    "name": f"Alle i {category_name.replace('-', ' ').title()}",
                    "url": main_category_url,
                }
            )

        # Deduplicate subcategories by URL
        unique_subcategories = []
        unique_urls = set()

        for subcategory in subcategories:
            url = subcategory["url"]
            # Normalize URL by removing any query parameters
            base_url = url.split("?")[0]

            if (
                base_url not in unique_urls
                and subcategory["name"] not in self.skip_subcategories
            ):
                unique_urls.add(base_url)
                unique_subcategories.append(subcategory)

        # Set up standard output logger for tqdm
        class TqdmToLogger:
            def __init__(self, logger):
                self.logger = logger
                self.level = logging.INFO
                self.buf = ""

            def write(self, buf):
                self.buf = buf.strip("\r\n\t ")

            def flush(self):
                pass

        tqdm_out = TqdmToLogger(self.logger)

        # Create a progress bar
        with tqdm(
            total=len(unique_subcategories),
            desc="Scraping subcategories",
            file=tqdm_out,
            ncols=80,
            unit="subcategory",
        ) as pbar:

            # Scrape products from each unique subcategory
            for subcategory in unique_subcategories:
                subcat_url = subcategory["url"]
                subcat_name = subcategory["name"]

                # Skip if this URL has already been processed
                if subcat_url in processed_urls:
                    self.logger.debug(f"Skipping already processed URL: {subcat_url}")
                    pbar.update(1)
                    continue

                # Mark this URL as processed
                processed_urls.add(subcat_url)

                # Update progress bar description
                pbar.set_description(f"Scraping: {subcat_name}")

                # Get products from this subcategory
                subcategory_products = self.get_products_from_subcategory(
                    subcat_url, subcat_name, category_name, max_products
                )

                all_products.extend(subcategory_products)
                pbar.update(1)

        self.logger.info(
            f"Total products scraped from category '{category_name}': {len(all_products)}"
        )
        return all_products

    def _extract_product_image(
        self, soup: BeautifulSoup, product_name: str, product_url: str
    ) -> Optional[str]:
        """Extract the product image URL from the soup.

        Args:
            soup: BeautifulSoup object of the page
            product_name: Name of the product for image matching
            product_url: URL of the product page for logging

        Returns:
            Image URL if found, None otherwise
        """
        import html

        # Initialize
        image_element = None

        # Strategy 1: Target the exact structure where product images are found
        article_elem = soup.find("article")
        if article_elem:
            # First div inside article usually has the product image
            first_div = article_elem.select_one("div:first-child")
            if first_div:
                image_element = first_div.find("img")
            # Fallback to any div in article if first-child selector doesn't work
            if not image_element and article_elem.find("div"):
                image_element = article_elem.find("div").find("img")

        # Strategy 2: Look for images with matching alt text to product name
        if not image_element and product_name:
            for img in soup.find_all("img"):
                alt_text = img.get("alt", "")
                if product_name.lower() in alt_text.lower():
                    image_element = img
                    break

        # Strategy 3: Look for images with product-related classes
        if not image_element:
            for img_selector in [
                "img.k-image.k-image--contain",
                "img.k-image",
                "img[class*='product']",
                "img[alt*='product']",
            ]:
                image_element = soup.select_one(img_selector)
                if image_element:
                    break

        # Strategy 4: Try direct CSS path based on XPath structure
        if not image_element:
            css_selectors = [
                "section > div > div > article > div > img",
                "main div article div img",
                "div[class*='product'] img",
            ]
            for selector in css_selectors:
                image_element = soup.select_one(selector)
                if image_element:
                    break

        # Extract URL and process it
        if image_element:
            # Get the raw URL
            image_url = image_element.get("src")

            if image_url:
                # Decode HTML entities (like &amp; to &)
                image_url = html.unescape(image_url)

                # Ensure it's an absolute URL
                if not image_url.startswith(("http://", "https://")):
                    image_url = urljoin(self.base_url, image_url)

                # Log success
                self.logger.debug(f"Found product image: {image_url}")
                return image_url

        # Log failure and return None
        self.logger.warning(f"No product image found for {product_url}")
        return None
