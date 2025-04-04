"""Meny-specific processor for extracting structured data from product information."""

import re
import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Union

from models.product import Product
from processing.base_processor import BaseProcessor


class MenyProcessor(BaseProcessor):
    """Processor for extracting structured attributes from Meny product information."""

    # Common Norwegian brand mapping for Meny
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
        """Initialize the Meny processor."""
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
        # If brand is already available, keep it
        if getattr(self, "_current_brand", None):
            return self._current_brand

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

        # Save brand for later reference
        self._current_brand = product.brand

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

        # Extract subcategory-specific attributes
        if product.subcategory == "egg":
            egg_info = self.extract_egg_info(clean_info)
            product.attributes.update(egg_info)

        return product
