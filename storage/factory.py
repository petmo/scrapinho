"""Factory functions for storage operations."""

import logging
from typing import List, Optional

from models.product import Product


def save_to_storage(
    products: List[Product],
    storage_type: str,
    storage_config: dict,
    replace_existing: bool = False,
):
    """Save products to the specified storage.

    Args:
        products: List of products to save
        storage_type: Type of storage ('csv' or 'supabase')
        storage_config: Storage configuration
        replace_existing: Whether to replace existing products with same product_id

    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Saving {len(products)} products to {storage_type} storage")
    logger.info(f"Mode: {'Replace' if replace_existing else 'Append'}")

    # Additional info about the run
    if products and products[0].run_id:
        logger.info(f"Run ID: {products[0].run_id}")

    if storage_type.lower() == "csv":
        from .csv_storage import CSVStorage

        storage = CSVStorage(**storage_config)
        storage.initialize()
        success = storage.save_products(products, replace_existing=replace_existing)
        storage.close()
        return success
    elif storage_type.lower() == "supabase":
        from .supabase_storage import SupabaseStorage

        storage = SupabaseStorage(**storage_config)
        storage.initialize()
        success = storage.save_products(products, replace_existing=replace_existing)
        storage.close()
        return success
    else:
        logger.error(f"Unsupported storage type: {storage_type}")
        return False


def get_from_storage(
    storage_type: str,
    storage_config: dict,
    category: Optional[str] = None,
    subcategory: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Product]:
    """Retrieve products from storage.

    Args:
        storage_type: Type of storage ('csv' or 'supabase')
        storage_config: Storage configuration
        category: Optional category filter
        subcategory: Optional subcategory filter
        run_id: Optional run ID filter
        limit: Optional maximum number of products

    Returns:
        List of products
    """
    logger = logging.getLogger(__name__)

    if storage_type.lower() == "csv":
        from .csv_storage import CSVStorage

        storage = CSVStorage(**storage_config)
        storage.initialize()
        products = storage.get_products(
            category=category, subcategory=subcategory, run_id=run_id, limit=limit
        )
        storage.close()
        return products
    elif storage_type.lower() == "supabase":
        from .supabase_storage import SupabaseStorage

        storage = SupabaseStorage(**storage_config)
        storage.initialize()
        products = storage.get_products(
            category=category, subcategory=subcategory, run_id=run_id, limit=limit
        )
        storage.close()
        return products
    else:
        logger.error(f"Unsupported storage type: {storage_type}")
        return []
