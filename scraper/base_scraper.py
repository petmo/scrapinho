"""Base scraper interface for the Oda scraper."""

import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Generator, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from models.product import Product


class BaseScraper(ABC):
    """Abstract base class for web scrapers.

    This class defines the interface that all scraper implementations must follow
    and provides common functionality for making HTTP requests, handling retries,
    and basic error handling.

    Args:
        base_url: Base URL for the website to scrape
        user_agent: User agent string to use for requests
        request_delay: Delay between requests in seconds
        max_retries: Maximum number of retries for failed requests
        timeout: Timeout for requests in seconds
    """

    def __init__(
        self,
        base_url: str,
        user_agent: str = "Mozilla/5.0",
        request_delay: float = 1.0,
        max_retries: int = 3,
        timeout: int = 30,
    ) -> None:
        """Initialize the base scraper.

        Args:
            base_url: Base URL for the website to scrape
            user_agent: User agent string to use for requests
            request_delay: Delay between requests in seconds
            max_retries: Maximum number of retries for failed requests
            timeout: Timeout for requests in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.session = self._create_session()
        self.last_request_time = 0

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic.

        Returns:
            Configured requests session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            backoff_factor=1,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update({"User-Agent": self.user_agent})

        return session

    def _make_request(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Make an HTTP request with rate limiting and error handling.

        Args:
            url: URL to request
            params: Query parameters for the request

        Returns:
            HTTP response

        Raises:
            requests.RequestException: If the request fails after retries
        """
        # Apply rate limiting
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)

        # Make the request
        full_url = (
            url if url.startswith(("http://", "https://")) else f"{self.base_url}{url}"
        )
        self.logger.debug(f"Making request to {full_url}")

        try:
            response = self.session.get(full_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            self.last_request_time = time.time()
            return response
        except requests.RequestException as e:
            self.logger.error(f"Request to {full_url} failed: {e}")
            raise

    def close(self) -> None:
        """Close the scraper and release resources."""
        self.session.close()
        self.logger.info("Scraper closed")

    @abstractmethod
    def get_product(self, product_url: str) -> Optional[Product]:
        """Scrape a single product.

        Args:
            product_url: URL of the product to scrape

        Returns:
            Product object if successful, None otherwise
        """
        pass

    @abstractmethod
    def get_products(
        self, category_url: str, max_products: Optional[int] = None
    ) -> List[Product]:
        """Scrape products from a category.

        Args:
            category_url: URL of the category to scrape
            max_products: Maximum number of products to scrape

        Returns:
            List of scraped products
        """
        pass
