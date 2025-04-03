"""Utilities for generating and managing run IDs."""

import uuid
import hashlib
import datetime
from typing import Optional


def generate_run_id(seed: Optional[str] = None) -> str:
    """Generate a unique run ID.

    Args:
        seed: Optional seed value to make the run ID deterministic

    Returns:
        Unique run ID string
    """
    if seed:
        # Create deterministic ID from seed
        hash_obj = hashlib.md5(seed.encode())
        return hash_obj.hexdigest()[:12]
    else:
        # Create random ID based on time and random UUID
        timestamp = datetime.datetime.now().isoformat()
        random_part = str(uuid.uuid4())
        combined = f"{timestamp}-{random_part}"
        hash_obj = hashlib.md5(combined.encode())
        return hash_obj.hexdigest()[:12]


def format_run_id(run_id: str, timestamp: bool = True) -> str:
    """Format a run ID with optional timestamp.

    Args:
        run_id: The run ID
        timestamp: Whether to include a timestamp

    Returns:
        Formatted run ID string
    """
    if timestamp:
        date_str = datetime.datetime.now().strftime("%Y%m%d")
        return f"{date_str}_{run_id}"
    return run_id
