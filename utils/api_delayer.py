# utils/api_delayer.py
import asyncio
import logging
import os
from typing import Optional, Any

from .delay_calculator import calculate_dynamic_delay # Import the calculator

logger = logging.getLogger(__name__)

async def apply_post_api_call_delay(platform_name: str, config_obj: Optional[Any], num_repos_in_target: Optional[int] = None):
    """
    Applies a configured delay after an API call for a specific platform.
    This is a simple, fixed delay intended for general throttling.
    It reads the delay duration from the configuration object or environment variables.

    Args:
        platform_name: The name of the platform (e.g., "GITHUB", "GITLAB", "AZURE_DEVOPS").
        config_obj: The application's configuration object.
        num_repos_in_target: Optional number of repositories in the current target, for dynamic scaling.
    """
    base_delay_seconds = 0.0
    env_var_key_suffix = "_POST_API_CALL_DELAY_SECONDS"
    base_delay_config_attr_name = f"{platform_name.upper()}{env_var_key_suffix}_ENV"
    base_delay_env_var_name = f"{platform_name.upper()}{env_var_key_suffix}"

    if config_obj and hasattr(config_obj, base_delay_config_attr_name):
        try:
            base_delay_seconds = float(getattr(config_obj, base_delay_config_attr_name))
        except (ValueError, TypeError):
            logger.warning(f"Invalid value for {base_delay_config_attr_name} in config. Using 0.0s for base delay.")
    else:
        try:
            base_delay_seconds = float(os.getenv(base_delay_env_var_name, "0.0"))
        except (ValueError, TypeError):
            logger.warning(f"Invalid value for environment variable {base_delay_env_var_name}. Using 0.0s for base delay.")

    final_delay_seconds = base_delay_seconds

    if num_repos_in_target is not None and config_obj:
        # Read dynamic scaling parameters from config_obj (or fallback to env if needed)
        # These are generic for now, but could be made platform-specific if required.
        threshold = int(getattr(config_obj, 'DYNAMIC_DELAY_THRESHOLD_REPOS_ENV', os.getenv("DYNAMIC_DELAY_THRESHOLD_REPOS", "100")))
        scale_factor = float(getattr(config_obj, 'DYNAMIC_DELAY_SCALE_FACTOR_ENV', os.getenv("DYNAMIC_DELAY_SCALE_FACTOR", "1.5")))
        max_delay = float(getattr(config_obj, 'DYNAMIC_DELAY_MAX_SECONDS_ENV', os.getenv("DYNAMIC_DELAY_MAX_SECONDS", "1.0")))

        final_delay_seconds = calculate_dynamic_delay(
            base_delay_seconds=base_delay_seconds,
            num_items=num_repos_in_target,
            threshold_items=threshold,
            scale_factor=scale_factor,
            max_delay_seconds=max_delay
        )
        if final_delay_seconds > base_delay_seconds:
            logger.debug(f"Dynamic delay for {platform_name} (target size: {num_repos_in_target}): {final_delay_seconds:.2f}s (base: {base_delay_seconds:.2f}s)")

    if final_delay_seconds > 0:
        logger.debug(f"Applying ASYNC post-API call delay for {platform_name}: {final_delay_seconds:.2f}s")
        await asyncio.sleep(final_delay_seconds)