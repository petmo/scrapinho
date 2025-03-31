"""Factory module for creating pipeline instances."""

import logging
from typing import Dict, Any, Optional

from pipelines.base_pipeline import BasePipeline
from pipelines.oda_pipeline import OdaPipeline


def get_pipeline(pipeline_type: str, config: Dict[str, Any]) -> BasePipeline:
    """Create a pipeline instance of the specified type.

    Args:
        pipeline_type: Type of pipeline to create (e.g., "oda", "meny")
        config: Configuration dictionary

    Returns:
        Configured pipeline instance

    Raises:
        ValueError: If the pipeline type is not supported
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Creating pipeline of type: {pipeline_type}")

    if pipeline_type.lower() == "oda":
        return OdaPipeline.create_from_config(config)
    # Add other pipeline types here as they are implemented
    # elif pipeline_type.lower() == "meny":
    #     return MenyPipeline.create_from_config(config)
    else:
        raise ValueError(f"Unsupported pipeline type: {pipeline_type}")
