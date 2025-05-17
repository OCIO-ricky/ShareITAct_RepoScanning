# utils/dateparse.py
"""
Utility functions for parsing dates.
"""
import logging
from typing import Optional
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