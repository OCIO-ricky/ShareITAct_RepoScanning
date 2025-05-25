# d:\src\OCIO-Ricky\ShareITAct_RepoScanning\utils\fetch_utils.py
"""
Utility functions for fetching content from APIs with retry mechanisms.
"""
import time
import logging
from typing import Callable, Optional, Any, Dict, Tuple

# Constants for error types returned by the utility, providing a common interface
FETCH_ERROR_FORBIDDEN = "FORBIDDEN"  # Access denied after retries
FETCH_ERROR_NOT_FOUND = "NOT_FOUND"  # Resource does not exist (expected for optional files)
FETCH_ERROR_EMPTY_REPO_API = "EMPTY_REPO_API_SIGNAL"  # API explicitly indicates repo is empty
FETCH_ERROR_API_ERROR = "API_ERROR"  # Other platform-specific API errors
FETCH_ERROR_UNEXPECTED = "UNEXPECTED_ERROR" # General unexpected Python exceptions


def fetch_optional_content_with_retry(
    fetch_callable: Callable[[], Any],
    content_description: str,
    repo_identifier: str,
    platform_exception_map: Dict[str, Any],
    max_quick_retries: int,
    quick_retry_delay_seconds: float,
    logger_instance: logging.Logger,
    dynamic_delay_func: Optional[Callable[[], None]] = None,
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Safely fetches optional content with quick retries for specific errors like 403 Forbidden.

    Args:
        fetch_callable: A function that performs the actual API call to get the raw content object.
                        It is expected to raise platform-specific exceptions.
        content_description: Human-readable description of the content (e.g., "README file").
        repo_identifier: String identifying the repository (e.g., "org/repo_name") for logging.
        platform_exception_map: A dictionary mapping generic error keys to platform-specific
                                  exception types or callable checker functions. Expected keys:
                                  - 'forbidden_exception': The exception type for 403 errors.
                                  - 'not_found_exception': The exception type for 404/not found errors.
                                  - 'empty_repo_check_func': A callable (lambda e: bool) to identify
                                                             API signals indicating an empty repository.
                                  - 'generic_platform_exception': Base exception type for other platform API errors.
        max_quick_retries: Number of retries specifically for 'forbidden_exception' errors.
        quick_retry_delay_seconds: Delay in seconds between quick retries for 'forbidden_exception'.
        logger_instance: The logger instance to use for logging messages.
        dynamic_delay_func: An optional callable that applies any necessary pre-API call dynamic delay.

    Returns:
        A tuple: (raw_file_object_or_None, error_type_string_or_None).
        'raw_file_object_or_None' is the object returned by 'fetch_callable' on success.
        'error_type_string_or_None' can be one of the FETCH_ERROR_* constants if an error occurs,
        or None if successful.
    """
    forbidden_check = platform_exception_map.get('forbidden_exception')
    not_found_check = platform_exception_map.get('not_found_exception')
    empty_repo_check = platform_exception_map.get('empty_repo_check_func')
    generic_platform_exception_type = platform_exception_map.get('generic_platform_exception')

    for attempt in range(max_quick_retries + 1): # Includes the initial attempt
        try:
            if dynamic_delay_func:
                dynamic_delay_func() # Apply dynamic delay before the API call

            raw_file_object = fetch_callable()
            logger_instance.debug(f"Successfully fetched raw object for {content_description} from {repo_identifier}")
            return raw_file_object, None # Success
        except Exception as e:
             # Check for forbidden error
            is_forbidden = False
            if forbidden_check:
                if callable(forbidden_check): # If it's a checker function (like for GitLab/Azure)
                    is_forbidden = forbidden_check(e)
                else: # If it's an exception type (like for GitHub's UnknownObjectException)
                    is_forbidden = isinstance(e, forbidden_check)
            
            if is_forbidden:
                logger_instance.warning(
                    f"Attempt {attempt + 1}/{max_quick_retries + 1}: "
                    f"403 Forbidden accessing {content_description} for {repo_identifier}. "
                    f"Details: {getattr(e, 'status', 'N/A')} {getattr(e, 'data', str(e))}"
                )
                if attempt < max_quick_retries:
                    time.sleep(quick_retry_delay_seconds)
                    continue # Retry
                logger_instance.error(f"Failed to access {content_description} for {repo_identifier} after {max_quick_retries + 1} attempts due to 403 Forbidden.")
                return None, FETCH_ERROR_FORBIDDEN
            if empty_repo_check and empty_repo_check(e):
                logger_instance.info(f"Fetching {content_description} for {repo_identifier} failed: API indicates repository is empty. Details: {getattr(e, 'status', getattr(e, 'response_code', 'N/A'))} {getattr(e, 'data', str(e))}")
                return None, FETCH_ERROR_EMPTY_REPO_API
            # Check for not found error
            is_not_found = False
            if not_found_check:
                if callable(not_found_check): # If it's a checker function
                    is_not_found = not_found_check(e)
                else: # If it's an exception type
                    is_not_found = isinstance(e, not_found_check)
            
            if is_not_found:
                logger_instance.debug(f"{content_description} not found for {repo_identifier} (expected for optional files).")
                return None, FETCH_ERROR_NOT_FOUND
            if generic_platform_exception_type and isinstance(e, generic_platform_exception_type):
                logger_instance.error(f"Platform API error fetching {content_description} for {repo_identifier}: {getattr(e, 'status', getattr(e, 'response_code', 'N/A'))} {getattr(e, 'data', str(e))}", exc_info=False)
                return None, FETCH_ERROR_API_ERROR
            
            logger_instance.error(f"Unexpected error fetching {content_description} for {repo_identifier} (Attempt {attempt + 1}): {e}", exc_info=True)
            return None, FETCH_ERROR_UNEXPECTED # Stop on first unexpected error
            
    return None, FETCH_ERROR_UNEXPECTED # Should ideally not be reached if loop completes due to return/continue