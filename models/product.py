"""Product data model for the grocery scraper."""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime


@dataclass
class Product:
    """Data class for storing product information.

    Args:
        product_id: Unique identifier for the product
        name: Product name
        brand: Brand name
        info: Additional product information
        price: Current price in kr
        unit_price: Price per unit (e.g., kr/liter)
        image_url: URL to the product image
        category: Product category
        subcategory: Product subcategory
        url: Product page URL
        attributes: Additional product attributes
        scraped_at: Timestamp when the product was scraped
        run_id: ID of the scraping run that produced this product
    """

    product_id: str
    name: str
    info: str
    price: float
    price_text: str
    unit_price: Optional[str] = None
    brand: Optional[str] = None
    image_url: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    url: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    scraped_at: datetime = field(default_factory=datetime.now)
    run_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert Product to dictionary.

        Returns:
            Dictionary representation of the Product
        """
        return {
            "product_id": self.product_id,
            "name": self.name,
            "brand": self.brand,
            "info": self.info,
            "price": self.price,
            "price_text": self.price_text,
            "unit_price": self.unit_price,
            "image_url": self.image_url,
            "category": self.category,
            "subcategory": self.subcategory,
            "url": self.url,
            "attributes": self.attributes,
            "scraped_at": self.scraped_at.isoformat(),
            "run_id": self.run_id,
        }
