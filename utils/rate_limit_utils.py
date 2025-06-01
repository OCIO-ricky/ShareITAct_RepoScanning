# utils/rate_limit_utils.py
"""
Utilities for fetching API rate limit status and calculating processing delays.
"""
import os
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Any

from github import Github, RateLimitExceededException as PyGithubRateLimitExceededException
import gitlab # For GitLab client type hint and exceptions # type: ignore
# Make AzureConnection import conditional for type hinting
if os.getenv("AZURE_SDK_AVAILABLE", "False").lower() == "true": # Check an env var or a global const
    from azure.devops.connection import Connection as AzureConnection # For ADO client type hint

logger = logging.getLogger(__name__)

# --- Helper functions for safe type conversion ---
def _try_int(val: Optional[str]) -> Optional[int]:
    """Safely converts a string to an int, returning None on failure."""
    try:
        return int(val) # type: ignore
    except (TypeError, ValueError):
        return None

def _try_float(val: Optional[str]) -> Optional[float]:
    """Safely converts a string to a float, returning None on failure."""
    try:
        return float(val) # type: ignore
    except (TypeError, ValueError):
        return None

def get_github_rate_limit_status(
    gh_client: Github,
    logger_instance: Optional[logging.Logger] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetches the current REST API rate limit status from GitHub.
    Returns a dictionary with 'remaining', 'limit', and 'reset_at_datetime' (UTC).
    Returns None if fetching fails.
    """
    active_logger = logger_instance if logger_instance else logger

    try:
        rate_limit = gh_client.get_rate_limit()
        core_limit = rate_limit.core
        reset_datetime_utc = datetime.fromtimestamp(core_limit.reset.timestamp(), tz=timezone.utc)
        
        status = {
            "remaining": core_limit.remaining,
            "limit": core_limit.limit,
            "reset_at_datetime": reset_datetime_utc,
        }
        active_logger.debug(f"GitHub REST Rate Limit Status: Remaining {status['remaining']}/{status['limit']}, Resets at {status['reset_at_datetime'].isoformat()}")
        return status
    except PyGithubRateLimitExceededException:
        active_logger.warning("Cannot fetch GitHub rate limit status because the rate limit is currently exceeded for that specific call.")
        # We might be at 0, reset time is still useful if we could get it, but PyGithub might not provide it here.
        # For now, return None, forcing a more conservative delay.
        return None
    except Exception as e:
        active_logger.error(f"Error fetching GitHub rate limit status: {e}", exc_info=True)
        return None

def get_gitlab_rate_limit_status(
    gl_client: gitlab.Gitlab,
    logger_instance: Optional[logging.Logger] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetches the current API rate limit status from GitLab by making a lightweight call.
    Returns a dictionary with 'remaining', 'limit', and 'reset_at_datetime' (UTC).
    Returns None if fetching fails or headers are not present.
    """
    try:
        active_logger = logger_instance if logger_instance else logger
        # Attempt a lightweight API call to ensure last_response_headers is populated.
        # First, try getting the current user's info if gl_client.user is available.
        user_fetched_for_headers = False
        if gl_client.user and hasattr(gl_client.user, 'id') and gl_client.user.id is not None:
            try:
                active_logger.debug(f"GitLab: Attempting to fetch user {gl_client.user.id} to refresh rate limit headers.")
                gl_client.users.get(gl_client.user.id)
                user_fetched_for_headers = True
            except Exception as e_user_get:
                active_logger.warning(f"GitLab: Failed to fetch user {gl_client.user.id} for rate limit headers: {e_user_get}. Will try gl_client.version().")
        
        if not user_fetched_for_headers:
            active_logger.debug("GitLab: gl_client.user.id not available or user fetch failed. Calling gl_client.version() to refresh rate limit headers.")
            gl_client.version() # This is a very lightweight call.

        headers = getattr(gl_client, 'last_response_headers', {})
        if not headers:
            active_logger.warning("GitLab: Could not access last_response_headers from the client. Rate limit status unknown.")
            return None

        remaining_str = headers.get('RateLimit-Remaining')
        limit_str = headers.get('RateLimit-Limit')
        reset_timestamp_str = headers.get('RateLimit-ResetTime') # Unix timestamp

        if remaining_str is not None and limit_str is not None and reset_timestamp_str is not None:
            reset_datetime_utc = datetime.fromtimestamp(float(reset_timestamp_str), tz=timezone.utc)
            status = {
                "remaining": int(remaining_str),
                "limit": int(limit_str),
                "reset_at_datetime": reset_datetime_utc,
            }
            active_logger.debug(f"GitLab Rate Limit Status: Remaining {status['remaining']}/{status['limit']}, Resets at {status['reset_at_datetime'].isoformat()}")
            return status
        else:
            active_logger.warning(f"GitLab rate limit headers not fully populated. Headers: {headers}")
            return None
    except gitlab.exceptions.GitlabError as e:
        active_logger.error(f"GitLab API error while trying to determine rate limit: {e}", exc_info=True)
        return None
    except Exception as e:
        active_logger.error(f"Unexpected error fetching GitLab rate limit status: {e}", exc_info=True)
        return None

def get_azure_devops_rate_limit_status(ado_connection: Optional[Any], # Changed to Optional[Any] to avoid import error if SDK not there
                                       organization_name: str,
                                       logger_instance: Optional[logging.Logger] = None) -> Optional[Dict[str, Any]]:
    """
    Fetches Azure DevOps rate limit status.
    Attempts to read X-RateLimit-* headers from the last response on the connection's session.
    If headers are not found or the SDK is unavailable, returns a conservative placeholder.
    """
    active_logger = logger_instance if logger_instance else logger

    if not ado_connection or not hasattr(ado_connection, 'session'):
        active_logger.warning(
            f"Azure DevOps connection object is not available or invalid for org '{organization_name}'. "
            "Cannot fetch rate limit status. Returning conservative placeholder."
        )
        return {
            "remaining": 100, # Conservative placeholder
            "limit": 200,     # Conservative placeholder
            "reset_at_datetime": datetime.now(timezone.utc) + timedelta(minutes=15)
        }

    try:
        last_response = getattr(ado_connection.session, 'last_response', None)
        headers_from_response = {} # Will be CaseInsensitiveDict from requests
        if last_response and hasattr(last_response, 'headers'):
            headers_from_response = last_response.headers
            active_logger.debug(f"Azure DevOps (org: {organization_name}): Attempting to use headers from last session response: {headers_from_response}")
        else:
            active_logger.info(f"Azure DevOps (org: {organization_name}): No last_response found on session or headers missing. Using placeholder.")
            # Fall through to default placeholder if no headers are available

        # Parse specific Azure DevOps headers using .get() which is case-insensitive for requests' headers
        remaining = _try_int(headers_from_response.get('X-RateLimit-Remaining'))
        limit = _try_int(headers_from_response.get('X-RateLimit-Limit'))
        reset_timestamp_epoch_str = headers_from_response.get('X-RateLimit-Reset') # This is an Epoch timestamp
        retry_after_seconds_str = headers_from_response.get('Retry-After') # Seconds to wait, usually on 429

        reset_at_datetime: Optional[datetime] = None
        if reset_timestamp_epoch_str:
            reset_timestamp_epoch = _try_float(reset_timestamp_epoch_str)
            if reset_timestamp_epoch is not None:
                reset_at_datetime = datetime.fromtimestamp(reset_timestamp_epoch, tz=timezone.utc)

        # Handle Retry-After if present (e.g., after a 429 response)
        # This can provide a more immediate reset time if X-RateLimit-Reset is far off or missing.
        if retry_after_seconds_str:
            retry_after = _try_float(retry_after_seconds_str)
            if retry_after is not None and retry_after > 0:
                active_logger.info(f"Azure DevOps (org: {organization_name}): Retry-After header found: {retry_after}s. This suggests a recent 429 response.")
                # If X-RateLimit-Reset was not found or is very far, Retry-After can be a temporary guide.
                # For the purpose of this function (informing inter_submission_delay),
                # we prioritize X-RateLimit-Reset for the window end.
                # However, if X-RateLimit-Reset is missing, Retry-After can inform a temporary reset_at_datetime.
                if reset_at_datetime is None:
                    reset_at_datetime = datetime.now(timezone.utc) + timedelta(seconds=retry_after)

        if remaining is not None and limit is not None and reset_at_datetime is not None:
                status = {
                    "remaining": remaining,
                    "limit": limit,
                    "reset_at_datetime": reset_at_datetime,
                }
                active_logger.info(
                    f"Azure DevOps Rate Limit Status (org: {organization_name}, from response headers): "
                    f"Remaining {status['remaining']}/{status['limit']}, "
                    f"Resets at {status['reset_at_datetime'].isoformat()}"
                )
                return status
        else:
            active_logger.info(
                f"Azure DevOps (org: {organization_name}): Relevant X-RateLimit-* headers not fully parsed or found. "
                f"Parsed: remaining={remaining}, limit={limit}, reset_at_datetime={reset_at_datetime}. "
                f"Using placeholder."
            )
    except Exception as e:
        active_logger.warning(f"Azure DevOps (org: {organization_name}): Error attempting to get rate limit status from session: {e}. Using placeholder.")

    active_logger.debug(f"Azure DevOps (org: {organization_name}) rate limit status check is using a placeholder. Actual limits are complex.")
    return {
        "remaining": 5000, # Default placeholder
        "limit": 5000,     # Default placeholder
        "reset_at_datetime": datetime.now(timezone.utc) + timedelta(minutes=5)
    }

def calculate_inter_submission_delay(
    rate_limit_status: Optional[Dict[str, Any]],
    estimated_api_calls_for_target: int,
    num_workers: int,
    safety_factor: float,
    min_delay_seconds: float,
    max_delay_seconds: float
) -> float:
    """
    Calculates an ideal delay to apply *before submitting each repository* to the thread pool.
    """
    if not rate_limit_status or estimated_api_calls_for_target <= 0:
        logger.warning("Rate limit status unavailable or no estimated calls, using max_delay_seconds.")
        return max_delay_seconds # Fallback to a safe, potentially slow delay

    remaining_calls = rate_limit_status["remaining"]
    reset_at_datetime = rate_limit_status["reset_at_datetime"]
    now_utc = datetime.now(timezone.utc)
    seconds_to_reset = max(0, (reset_at_datetime - now_utc).total_seconds())

    permissible_calls = remaining_calls * safety_factor

    if permissible_calls <= 0: # No calls left in current window or safety factor makes it so
        if seconds_to_reset > 0:
            delay = (seconds_to_reset / num_workers) + min_delay_seconds # Wait for reset, then add min
            logger.warning(f"Rate limit nearly/fully depleted. Waiting for reset. Calculated delay: {delay:.2f}s")
            return min(delay, max_delay_seconds * 2) # Allow a bit longer for full reset wait
        return max_delay_seconds # No calls, no reset time known, use max delay

    if estimated_api_calls_for_target <= permissible_calls:
        # Enough quota for all estimated calls in the current window.
        # Spread them out over the remaining time in the window.
        if seconds_to_reset > 0 and estimated_api_calls_for_target > 0:
            # Delay per effective "slot" considering workers
            delay_per_submission_slot = seconds_to_reset / (estimated_api_calls_for_target / num_workers) if num_workers > 0 else seconds_to_reset
            calculated_delay = max(delay_per_submission_slot, min_delay_seconds)
        else:
            calculated_delay = min_delay_seconds # Go fast if no reset time or no calls
    else:
        # Not enough quota for the whole target in this window.
        # Calculate delay to spread calls over the time it would take for quota to replenish for the remaining calls.
        calls_needed_beyond_window = estimated_api_calls_for_target - permissible_calls
        # Assuming limit is per hour (3600s) for simplicity if reset_at is far or insufficient
        # This part can be more sophisticated by considering multiple reset windows.
        # For now, a simpler approach: slow down significantly.
        # Effective rate: permissible_calls / seconds_to_reset (if seconds_to_reset > 0)
        # We need to make 'estimated_api_calls_for_target' calls.
        # Time needed = estimated_api_calls_for_target / (permissible_calls / seconds_to_reset)
        if permissible_calls > 0 and seconds_to_reset > 0:
            effective_rate_per_second = permissible_calls / seconds_to_reset
            time_required_for_all_calls = estimated_api_calls_for_target / effective_rate_per_second
            delay_per_submission_slot = time_required_for_all_calls / (estimated_api_calls_for_target / num_workers) if num_workers > 0 else time_required_for_all_calls
            calculated_delay = max(delay_per_submission_slot, min_delay_seconds)
        else: # Cannot calculate rate, fallback
            calculated_delay = max_delay_seconds

    final_delay = min(max(calculated_delay, min_delay_seconds), max_delay_seconds)
    logger.info(f"Calculated inter-submission delay: {final_delay:.3f}s (Remaining: {remaining_calls}, SecToReset: {seconds_to_reset:.0f}, EstCalls: {estimated_api_calls_for_target}, Workers: {num_workers})")
    return final_delay