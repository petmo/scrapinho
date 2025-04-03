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
                page_progress.set_description(f"Page {cursor} of {subcategory_name}")

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
                        f"Failed to scrape product from {product_url}: {e}"
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
