# utils/dateparse.py
"""
Utility functions for parsing dates.
"""
import logging
from typing import Optional, Any # Added Any for cfg_obj type hint
from datetime import datetime, timezone

def parse_repos_created_after_date(date_str: Optional[str], logger_instance: logging.Logger) -> Optional[datetime]:
    """Parses a YYYY-MM-DD date string to a datetime object (start of day, UTC)."""
    if not date_str:
        return None
    try:
        # Assuming YYYY-MM-DD format from .env
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        # Make it timezone-aware, UTC, representing the beginning of that day
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        logger_instance.warning(f"Invalid format for REPOS_CREATED_AFTER_DATE: '{date_str}'. Expected YYYY-MM-DD. This filter will be ignored.")
        return None

def get_fixed_private_filter_date(cfg_obj: Any, logger_instance: logging.LoggerAdapter) -> datetime: # type: ignore
    """Gets and validates the fixed private repository/project filter date from config."""
    # Ensure logger_instance is a LoggerAdapter or base Logger
    actual_logger = logger_instance.logger if isinstance(logger_instance, logging.LoggerAdapter) else logger_instance
    fixed_private_filter_date_str = getattr(cfg_obj, 'FIXED_PRIVATE_REPO_FILTER_DATE_ENV', "2021-04-21")
    try:
        fixed_private_filter_date = datetime.strptime(fixed_private_filter_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        actual_logger.info(f"Using fixed date for private repository/project filtering: {fixed_private_filter_date_str}", extra=getattr(logger_instance, 'extra', {}))
    except ValueError:
        actual_logger.error(f"Invalid FIXED_PRIVATE_REPO_FILTER_DATE_ENV: '{fixed_private_filter_date_str}'. Using default 2021-04-21.", extra=getattr(logger_instance, 'extra', {}))
        fixed_private_filter_date = datetime.strptime("2021-04-21", "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return fixed_private_filter_date