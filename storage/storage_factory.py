"""Factory module for creating storage instances."""

import logging
from typing import Dict, Any, Optional

from storage.base_storage import BaseStorage
from storage.csv_storage import CSVStorage
from storage.supabase_storage import SupabaseStorage


def get_storage(storage_type: str, **kwargs) -> BaseStorage:
    """Create a storage instance of the specified type.

    Args:
        storage_type: Type of storage to create (e.g., "csv", "supabase")
        **kwargs: Additional arguments to pass to the storage constructor

    Returns:
        Configured storage instance

    Raises:
        ValueError: If the storage type is not supported
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Creating storage of type: {storage_type}")

    if storage_type.lower() == "csv":
        return CSVStorage(**kwargs)
    elif storage_type.lower() == "supabase":
        return SupabaseStorage(**kwargs)
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")
