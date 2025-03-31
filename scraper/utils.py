"""Utility functions for the Oda scraper."""

import re
import logging
from typing import Dict, Any, Tuple, Optional


def parse_product_info(info_text: str) -> Dict[str, Any]:
    """Parse product info text to extract structured information.

    Args:
        info_text: Product info text (e.g., "1% fett, 1,75 l, TINE")

    Returns:
        Dictionary of extracted information
    """
    result = {}

    if not info_text:
        return result

    # Split by comma to get individual parts
    parts = [part.strip() for part in info_text.split(",")]

    # Try to identify parts based on patterns
    for part in parts:
        # Match volume patterns (e.g., "1,75 l")
        volume_match = re.search(
            r"(\d+[,.]?\d*)\s*(ml|l|dl|cl|g|kg)", part, re.IGNORECASE
        )
        if volume_match:
            value, unit = volume_match.groups()
            value = float(value.replace(",", "."))
            result["volume"] = value
            result["volume_unit"] = unit.lower()
            continue

        # Match fat percentage (e.g., "1% fett")
        fat_match = re.search(r"(\d+[,.]?\d*)\s*%\s*fett", part, re.IGNORECASE)
        if fat_match:
            result["fat_percentage"] = float(fat_match.group(1).replace(",", "."))
            continue

        # Assume uppercase words are brands
        if part.isupper():
            result["brand"] = part
            continue

        # Check for common brand patterns
        if any(
            brand in part.upper() for brand in ["TINE", "Q", "SYNNØVE", "ARLA", "OATLY"]
        ):
            result["brand"] = part

    return result


def parse_unit_price(unit_price_text: str) -> Tuple[Optional[float], Optional[str]]:
    """Parse unit price text to extract price and unit.

    Args:
        unit_price_text: Unit price text (e.g., "kr 20,17 /l")

    Returns:
        Tuple of (price, unit)
    """
    if not unit_price_text:
        return None, None

    try:
        # Remove currency symbol and non-breaking spaces
        cleaned_text = unit_price_text.replace("kr", "").replace("&nbsp;", " ").strip()

        # Match price and unit pattern
        match = re.search(r"(\d+[,.]?\d*)\s*/\s*(\w+)", cleaned_text)
        if match:
            price_str, unit = match.groups()
            price = float(price_str.replace(",", "."))
            return price, unit

        return None, None
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"Failed to parse unit price '{unit_price_text}': {e}"
        )
        return None, None


def generate_product_id(name: str, info: str) -> str:
    """Generate a consistent product ID from name and info.

    Args:
        name: Product name
        info: Product info

    Returns:
        A consistent ID string
    """
    # Remove special characters and lowercase
    cleaned = re.sub(r"[^\w]", "", f"{name}_{info}").lower()
    # Take first 32 characters as ID
    return cleaned[:32]
