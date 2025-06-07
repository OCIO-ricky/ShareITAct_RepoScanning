# utils/rate_limit_utils.py
"""
Utilities for fetching API rate limit status and calculating processing delays.
"""
import os
import time
import logging
import random # Added for jitter in retry logic
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
    logger_instance: Optional[logging.Logger] = None,
    is_graphql_context: bool = False # Added for consistent signature
) -> Optional[Dict[str, Any]]:
    """
    Fetches the current REST API rate limit status from GitHub.
    Returns a dictionary with 'remaining', 'limit', and 'reset_at_datetime' (UTC).
    Returns None if fetching fails.
    """
    active_logger = logger_instance if logger_instance else logger
    if is_graphql_context: # GitHub GQL provides rateLimit object in query, not headers for this check
        active_logger.debug("GitHub GraphQL rate limits are checked via the 'rateLimit' object in GQL queries, not this REST API header check.")
        # Proceeding with REST check as it might still be informative for overall token health.

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

def _calculate_wait_time(
    attempt: int, # Current attempt number (0-indexed for calculation)
    retry_after_header: Optional[str],
    default_initial_delay: float,
    default_backoff_multiplier: float
) -> float:
    """
    Calculates wait time for retries. Prioritizes Retry-After header.
    Falls back to exponential backoff with jitter.
    """
    if retry_after_header:
        try:
            wait_seconds = float(retry_after_header)
            if wait_seconds > 0:
                return wait_seconds
        except ValueError:
            logger.warning(f"Could not parse Retry-After header value: '{retry_after_header}'. Falling back to exponential backoff.")
            pass # Fall through to exponential backoff
    
    # Exponential backoff with jitter
    # Jitter helps prevent thundering herd problems. Here, +/- 20% of the calculated delay.
    jitter_factor = random.uniform(0.8, 1.2) 
    wait_time = (default_initial_delay * (default_backoff_multiplier ** attempt)) * jitter_factor
    return wait_time

def _refresh_gitlab_headers(gl_client: gitlab.Gitlab, active_logger: logging.Logger):
    """
    Makes lightweight API calls to refresh gl_client.last_response_headers.
    Raises GitlabHttpError if API calls fail with HTTP errors (e.g., 429).
    """
    api_call_made_successfully = False
    # Attempt 1: Fetch current user (if user object seems valid)
    if gl_client.user and hasattr(gl_client.user, 'id') and gl_client.user.id is not None:
        try:
            active_logger.debug(f"GitLab RateLimit: Attempting to fetch user {gl_client.user.id} to refresh headers.")
            user_obj = gl_client.users.get(gl_client.user.id) # Make the call
            if user_obj: # Check if we got a user object back
                active_logger.debug(f"GitLab RateLimit: Successfully fetched user {user_obj.username}. Headers should be fresh.")
                api_call_made_successfully = True
            else:
                active_logger.warning(f"GitLab RateLimit: Fetching user {gl_client.user.id} returned None/empty. Headers might not be fresh.")
        except gitlab.exceptions.GitlabHttpError as e_user_http:
            active_logger.warning(f"GitLab RateLimit: HTTP error fetching user {gl_client.user.id} for headers: {e_user_http}.")
            raise # Re-raise to be caught by the main retry loop if it's 429
        except gitlab.exceptions.GitlabError as e_user_get: # Other GitlabErrors
            active_logger.warning(f"GitLab RateLimit: Non-HTTP API error fetching user {gl_client.user.id} for headers: {e_user_get}. Will try gl_client.version().")
    else:
        active_logger.debug("GitLab RateLimit: gl_client.user or gl_client.user.id not available. Skipping user fetch for headers.")

    # Attempt 2: Fetch GitLab version (if user fetch didn't happen or failed with non-HTTP error)
    if not api_call_made_successfully:
        try:
            active_logger.debug("GitLab RateLimit: Attempting to call gl_client.version() to refresh headers.")
            version_info = gl_client.version() # Make the call
            if version_info: # Check if we got version info back
                active_logger.debug(f"GitLab RateLimit: Successfully fetched version {version_info.get('version')}. Headers should be fresh.")
            else:
                 active_logger.warning("GitLab RateLimit: gl_client.version() returned None/empty. Headers might not be fresh.")
        except gitlab.exceptions.GitlabHttpError as e_version_http:
            active_logger.warning(f"GitLab RateLimit: HTTP error calling gl_client.version() for headers: {e_version_http}.")
            raise # Re-raise for 429 handling
        except gitlab.exceptions.GitlabError as e_version_get: # Other GitlabErrors
            active_logger.error(f"GitLab RateLimit: Non-HTTP API error calling gl_client.version() for headers: {e_version_get}. Headers likely not fresh.")
            # If this also fails, it's unlikely headers will be populated.

def get_gitlab_rate_limit_status(
    gl_client: gitlab.Gitlab,
    logger_instance: Optional[logging.Logger] = None,
    is_graphql_context: bool = False,
    max_retries: int = 3,
    initial_delay_seconds_for_backoff: float = 1.0, # Base for exponential backoff
    backoff_factor: float = 2.0 # Multiplier for exponential backoff
) -> Optional[Dict[str, Any]]:
    """
    Fetches the current API rate limit status from GitLab by making a lightweight call.
    Returns a dictionary with 'remaining', 'limit', and 'reset_at_datetime' (UTC).
    Returns None if fetching fails or headers are not present.
    Handles 429 errors with retries.
    """
    active_logger = logger_instance if logger_instance else logger

    if is_graphql_context:
       # active_logger.warning(
       #     "GitLab GraphQL API does not return rate limit headers. "
       #     "Cannot determine rate limit status programmatically for GraphQL via this method. "
       #     "GraphQL rate limits are typically handled by reacting to 429 errors."
       # )
        return None

    for attempt in range(max_retries + 1):
        try:
            active_logger.debug(f"GitLab RateLimit: Attempt {attempt + 1}/{max_retries + 1} to fetch and parse headers.")
            
            # Make lightweight API calls to refresh headers. This may raise GitlabHttpError (e.g., 429).
            _refresh_gitlab_headers(gl_client, active_logger)

            # Now parse the refreshed headers
            headers = getattr(gl_client, 'last_response_headers', {})
            if not headers:
                active_logger.warning(f"GitLab: last_response_headers empty after refresh attempt {attempt + 1}. Cannot determine rate limit.")
                return None # Not a 429 to retry for if _refresh_gitlab_headers succeeded.

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
                return status # Success
            else:
                active_logger.warning(f"GitLab rate limit headers not fully populated after attempt {attempt + 1}. Headers: {headers}")
                return None # Headers present but incomplete, not a 429 to retry for.

        except gitlab.exceptions.GitlabHttpError as e:
            if e.response_code == 429:
                if attempt < max_retries:
                    retry_after_header = e.response_headers.get('Retry-After')
                    wait_time = _calculate_wait_time(
                        attempt, # Pass 0-indexed attempt for backoff calculation
                        retry_after_header, 
                        initial_delay_seconds_for_backoff, 
                        backoff_factor
                    )
                    active_logger.warning(
                        f"GitLab RateLimit: Rate limited on attempt {attempt + 1}/{max_retries + 1}. "
                        f"Retrying in {wait_time:.2f}s. Error: {e}"
                    )
                    time.sleep(wait_time)
                    continue # To the next attempt
                else:
                    active_logger.error(
                        f"GitLab RateLimit: Rate limited and max retries ({max_retries}) reached. Error: {e}"
                    )
                    return None # Max retries for 429
            else: # Other HTTP errors
                active_logger.error(f"GitLab HTTP error (not 429) on attempt {attempt + 1}: {e}", exc_info=True)
                return None # Non-429 HTTP error, do not retry further in this function
        except gitlab.exceptions.GitlabError as e: # Other Gitlab non-HTTP errors
            active_logger.error(f"GitLab API error (non-HTTP) on attempt {attempt + 1}: {e}", exc_info=True)
            return None # Non-HTTP Gitlab error, do not retry
        except Exception as e: # Catch-all for unexpected errors during an attempt
            active_logger.error(f"Unexpected error on attempt {attempt + 1} fetching GitLab rate limit status: {e}", exc_info=True)
            return None # Unexpected error, do not retry

    # If loop completes (all retries for 429 exhausted without success)
    active_logger.error(f"GitLab RateLimit: Failed to get rate limit status after {max_retries + 1} attempts due to persistent rate limiting or other errors.")
    return None

def get_azure_devops_rate_limit_status(ado_connection: Optional[Any], # Changed to Optional[Any] to avoid import error if SDK not there
                                       organization_name: str,
                                       logger_instance: Optional[logging.Logger] = None,
                                       is_graphql_context: bool = False) -> Optional[Dict[str, Any]]: # Added for consistent signature
    """
    Fetches Azure DevOps rate limit status.
    Attempts to read X-RateLimit-* headers from the last response on the connection's session.
    If headers are not found or the SDK is unavailable, returns a conservative placeholder.
    """
    active_logger = logger_instance if logger_instance else logger

    if is_graphql_context: # ADO doesn't have GraphQL for this
        active_logger.debug("Azure DevOps does not use GraphQL for repository data in this tool; is_graphql_context flag ignored for ADO rate limit check.")
        
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