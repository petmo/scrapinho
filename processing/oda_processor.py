"""Oda-specific processor for extracting structured data from product information."""

import re
import logging
import uuid

import pandas as pd
from typing import Dict, Any, List, Optional, Union

from models.product import Product
from processing.base_processor import BaseProcessor


class OdaProcessor(BaseProcessor):
    """Processor for extracting structured attributes from Oda product information."""

    # Common Norwegian brand mapping for Oda
    BRANDS = {
        "tine": "TINE",
        "oatly": "OATLY",
        "prior": "Prior",
        "q meieriene": "Q",
        "q-meieriene": "Q",
        "q": "Q",
        "melange": "Melange",
        "soft flora": "Soft Flora",
        "alpro": "Alpro",
        "synnøve": "Synnøve",
        "synnøve finden": "Synnøve Finden",
        "rørosmeieriet": "Rørosmeieriet",
        "castello": "Castello",
        "kavli": "Kavli",
        "fjordland": "Fjordland",
        "yoplait": "Yoplait",
        "danonino": "Danonino",
        "helios": "Helios",
        "stange": "Stange",
        "vita hjertego'": "Vita hjertego'",
        "sproud": "Sproud",
        "bremykt": "Bremykt",
        "mills": "Mills",
        "arla": "Arla",
        "go'": "Go'",
        "go'morgen": "Go'morgen",
        "go'dag": "Go'dag",
        "galbani": "Galbani",
        "président": "Président",
        "becel": "Becel",
    }

    # Subcategory mapping based on keywords in product name or info
    SUBCATEGORY_MAPPING = {
        "melk": [
            "melk",
            "lettmelk",
            "skummet",
            "mjølk",
            "litago",
            "helmelk",
            "kulturmjølk",
        ],
        "plantebasert": [
            "havredrikk",
            "soyadrikk",
            "mandeldrikk",
            "mylk",
            "ikaffe",
            "plantebasert",
            "plantedrikk",
        ],
        "ost": [
            "ost",
            "norvegia",
            "jarlsberg",
            "geitost",
            "mozzarella",
            "cheddar",
            "pizzaost",
            "manchego",
            "blue",
            "selbu blå",
            "parmigiano",
            "grana padano",
        ],
        "smør": [
            "smør",
            "meierismør",
            "margarin",
            "melange",
            "soft flora",
            "brelett",
            "bremykt",
        ],
        "egg": ["egg", "høner"],
        "fløte_rømme": [
            "rømme",
            "crème fraîche",
            "matfløte",
            "havrefløte",
            "imat",
            "matfrisk",
            "matfløyel",
            "fløte",
            "kremfløte",
        ],
        "yoghurt": ["yoghurt", "skyr", "biola", "go'morgen", "activia", "go'dag"],
        "kjølte_desserter": ["pudding", "risgrøt", "rispudding", "risengrynsgrøt"],
        "cottage_cheese": ["cottage cheese", "kesam", "kvarg", "cottage"],
    }

    # Patterns for extraction
    PATTERNS = {
        "size": r"(\d+(?:[.,]\d+)?)\s*(l|ml|g|kg|dl|cl|stk)",
        "percentage": r"(\d+(?:[.,]\d+)?)%(?:\s+fett)?",
        "multipack": r"(\d+)x(\d+(?:[.,]\d+)?)\s*(g|ml|l|kg|stk)",
        "pack_quantity": r"(\d+)\s*(?:pk|stk|pakk|pakning)",
        "egg_size": r"(?:str\.?|størrelse)\s*(xs|s|m|l|xl)",
        "egg_quantity": r"(\d+)\s*(?:stk|egg)",
    }

    def __init__(self):
        """Initialize the Oda processor."""
        super().__init__()

    def determine_subcategory(self, name: str, info: str = "") -> str:
        """Determine product subcategory based on product name and info.

        Args:
            name: Product name
            info: Additional product info

        Returns:
            Subcategory name
        """
        text = f"{name} {info}".lower()

        for subcategory, keywords in self.SUBCATEGORY_MAPPING.items():
            if any(keyword.lower() in text for keyword in keywords):
                return subcategory

        return "other"

    def extract_brand(self, info: str, name: str = "") -> Optional[str]:
        """Extract brand from product information.

        Args:
            info: Product information
            name: Product name (optional, for additional context)

        Returns:
            Brand name if found, None otherwise
        """
        text = f"{info} {name}".lower()

        # First try direct matching from our brand list
        for brand_key, brand_name in self.BRANDS.items():
            if brand_key.lower() in text:
                return brand_name

        # Try to find the brand as a standalone word (often the last word in info)
        parts = info.split(",")
        for part in parts:
            part = part.strip()
            if part.isupper() and len(part) > 1:  # Often brands are uppercase
                return part

        # Try to find brand at the end of info string
        if "," in info:
            last_part = info.split(",")[-1].strip()
            if len(last_part) < 20:  # Avoid long descriptions
                return last_part

        return None

    def extract_size(self, info: str) -> Dict[str, Any]:
        """Extract size information from product info.

        Args:
            info: Product information

        Returns:
            Dictionary with size quantity and unit
        """
        result = {"size_quantity": None, "size_unit": None}

        size_match = re.search(self.PATTERNS["size"], info, re.IGNORECASE)
        if size_match:
            result["size_quantity"] = float(size_match.group(1).replace(",", "."))
            result["size_unit"] = size_match.group(2).lower()

        return result

    def extract_fat_content(self, info: str) -> Optional[float]:
        """Extract fat content percentage from product info.

        Args:
            info: Product information

        Returns:
            Fat content as float if found, None otherwise
        """
        fat_match = re.search(self.PATTERNS["percentage"], info)
        if fat_match:
            return float(fat_match.group(1).replace(",", "."))
        return None

    def extract_multipack_info(self, info: str) -> Dict[str, Any]:
        """Extract multipack information from product info.

        Args:
            info: Product information

        Returns:
            Dictionary with multipack details
        """
        result = {"pack_quantity": None, "unit_size": None, "unit_size_unit": None}

        # Match patterns like "4x125g" or "6x1.5l"
        multipack_match = re.search(self.PATTERNS["multipack"], info, re.IGNORECASE)
        if multipack_match:
            result["pack_quantity"] = int(multipack_match.group(1))
            result["unit_size"] = float(multipack_match.group(2).replace(",", "."))
            result["unit_size_unit"] = multipack_match.group(3).lower()
        else:
            # Try to match just pack quantity
            pack_match = re.search(self.PATTERNS["pack_quantity"], info, re.IGNORECASE)
            if pack_match:
                result["pack_quantity"] = int(pack_match.group(1))

        return result

    def extract_egg_info(self, info: str) -> Dict[str, Any]:
        """Extract egg specific information from product info.

        Args:
            info: Product information

        Returns:
            Dictionary with egg details
        """
        result = {"egg_size": None, "egg_quantity": None, "egg_type": None}

        # Extract egg size
        size_match = re.search(self.PATTERNS["egg_size"], info, re.IGNORECASE)
        if size_match:
            result["egg_size"] = size_match.group(1).upper()

        # Extract egg quantity
        quantity_match = re.search(self.PATTERNS["egg_quantity"], info, re.IGNORECASE)
        if quantity_match:
            result["egg_quantity"] = int(quantity_match.group(1))

        # Extract egg type
        if "frittgående" in info.lower():
            result["egg_type"] = "Frittgående"
        elif "økologisk" in info.lower():
            result["egg_type"] = "Økologisk"
        elif "friland" in info.lower():
            result["egg_type"] = "Friland"

        return result

    def extract_cheese_info(self, info: str) -> Dict[str, Any]:
        """Extract cheese specific information from product info.

        Args:
            info: Product information

        Returns:
            Dictionary with cheese details
        """
        result = {"cheese_type": None, "aging": None, "preparation": None}

        # Preparation style
        if any(term in info.lower() for term in ["skivet", "skiver"]):
            result["preparation"] = "Skivet"
        elif any(term in info.lower() for term in ["revet", "raspet"]):
            result["preparation"] = "Revet"
        elif "hel" in info.lower():
            result["preparation"] = "Hel"

        # Aging information
        aging_match = re.search(r"(\d+)\s*(?:mnd|måned)", info, re.IGNORECASE)
        if aging_match:
            result["aging"] = f"{aging_match.group(1)} måneder"

        # Cheese type
        cheese_types = [
            "hvit",
            "blå",
            "brie",
            "cheddar",
            "feta",
            "parmesan",
            "geitost",
            "brunost",
        ]
        for cheese_type in cheese_types:
            if cheese_type in info.lower():
                result["cheese_type"] = cheese_type.capitalize()
                break

        return result

    def extract_dietary_info(self, info: str) -> Dict[str, Any]:
        """Extract dietary information from product info.

        Args:
            info: Product information

        Returns:
            Dictionary with dietary details
        """
        result = {
            "lactose_free": False,
            "gluten_free": False,
            "organic": False,
            "vegan": False,
        }

        info_lower = info.lower()

        result["lactose_free"] = any(
            term in info_lower for term in ["laktosefri", "uten laktose"]
        )
        result["gluten_free"] = any(
            term in info_lower for term in ["glutenfri", "uten gluten"]
        )
        result["organic"] = any(
            term in info_lower for term in ["økologisk", "organic", "eco", "øko"]
        )
        result["vegan"] = any(
            term in info_lower for term in ["vegansk", "vegan", "plantebasert"]
        )

        return result

    def extract_features(self, name: str, info: str) -> Dict[str, Any]:
        """Extract special features from product name and info.

        Args:
            name: Product name
            info: Product information

        Returns:
            Dictionary with feature flags
        """
        result = {}
        combined = f"{name} {info}".lower()

        # Extract flavor
        flavors = [
            "jordbær",
            "vanilje",
            "sjokolade",
            "bringebær",
            "blåbær",
            "skogsbær",
            "kakao",
            "karamell",
            "sitron",
            "eple",
            "kanel",
            "kardemomme",
            "banan",
        ]

        for flavor in flavors:
            if flavor in combined:
                result["flavor"] = flavor.capitalize()
                break

        # Product type
        product_types = {
            "lettmelk": ["lettmelk", "lett melk"],
            "helmelk": ["helmelk", "hel melk"],
            "skummet": ["skummet", "skumma"],
            "kefir": ["kefir"],
            "kulturmelk": ["kulturmelk", "kulturmjølk"],
            "ekstra lett": ["ekstra lett", "extra lett"],
        }

        for type_name, keywords in product_types.items():
            if any(kw in combined for kw in keywords):
                result["product_type"] = type_name.capitalize()
                break

        return result

    def process_product(self, product: Product) -> Product:
        """Process a single product to extract attributes.

        Args:
            product: The product to process

        Returns:
            Processed product with extracted attributes
        """
        # Skip if no info
        if not product.info:
            return product

        # Clean inputs
        clean_name = self.clean_text(product.name)
        clean_info = self.clean_text(product.info)

        # Set brand if none exists
        if not product.brand:
            product.brand = self.extract_brand(clean_info, clean_name)

        # Determine subcategory if none exists
        if not product.subcategory:
            product.subcategory = self.determine_subcategory(clean_name, clean_info)

        # Extract size information
        size_info = self.extract_size(clean_info)
        product.attributes.update(size_info)

        # Extract fat content
        fat_content = self.extract_fat_content(clean_info)
        if fat_content is not None:
            product.attributes["fat_content"] = fat_content

        # Extract multipack information
        multipack_info = self.extract_multipack_info(clean_info)
        product.attributes.update(multipack_info)

        # Extract dietary information
        dietary_info = self.extract_dietary_info(clean_info)
        product.attributes.update(dietary_info)

        # Extract features
        features = self.extract_features(clean_name, clean_info)
        product.attributes.update(features)

        # Extract subcategory-specific attributes
        if product.subcategory == "egg":
            egg_info = self.extract_egg_info(clean_info)
            product.attributes.update(egg_info)
        elif product.subcategory == "ost":
            cheese_info = self.extract_cheese_info(clean_info)
            product.attributes.update(cheese_info)

        return product

    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process a DataFrame of products.

        Args:
            df: DataFrame containing product data

        Returns:
            Processed DataFrame with additional attributes
        """
        # Check required columns
        if "name" not in df.columns or "info" not in df.columns:
            self.logger.error(
                "DataFrame must contain 'name' and 'info' columns", exc_info=True
            )
            return df

        # Create a dictionary for each row with extracted attributes
        processed_rows = []

        for _, row in df.iterrows():
            # Convert row to dictionary
            row_dict = row.to_dict()

            # Create a temporary Product object
            product = Product(
                product_id=row_dict.get("product_id", str(uuid.uuid4())),
                name=row_dict.get("name", ""),
                info=row_dict.get("info", ""),
                price=row_dict.get("price", 0.0),
                price_text=row_dict.get("price_text", ""),
                unit_price=row_dict.get("unit_price"),
                brand=row_dict.get("brand"),
                image_url=row_dict.get("image_url"),
                category=row_dict.get("category"),
                subcategory=row_dict.get("subcategory"),
                url=row_dict.get("url"),
            )

            # Process the product
            processed_product = self.process_product(product)

            # Update the row dictionary with processed attributes
            processed_row = processed_product.to_dict()
            processed_rows.append(processed_row)

        # Create a new DataFrame
        processed_df = pd.DataFrame(processed_rows)

        return processed_df
