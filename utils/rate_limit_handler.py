# utils/rate_limit_handler.py
import time
import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict

logger = logging.getLogger(__name__)

# ANSI escape codes for coloring output (optional, for emphasis in logs)
ANSI_YELLOW = "\x1b[33;1m"
ANSI_RESET = "\x1b[0m"

class GitHubRateLimitHandler:
    """
    Manages GitHub API rate limit status and enforces delays.
    Designed for use with asyncio.
    """
    def __init__(self, 
                 safety_buffer_remaining: int = 10, 
                 min_sleep_if_limited: float = 1.0,
                 max_sleep_duration: float = 3600.0): # Max sleep 1 hour
        self.remaining: Optional[int] = None
        self.limit: Optional[int] = None
        self.reset_time: Optional[float] = None  # Unix timestamp (UTC)
        
        # Pause if remaining calls are below this threshold
        self.safety_buffer_remaining = safety_buffer_remaining
        # Minimum sleep duration if rate limited, even if reset time is very soon or past
        self.min_sleep_if_limited = min_sleep_if_limited
        # Maximum duration to sleep to prevent excessively long sleeps if reset time is far
        self.max_sleep_duration = max_sleep_duration 
        
        self._lock = asyncio.Lock() # Ensures atomic updates and checks in async context

    async def update_from_headers(self, headers: Dict[str, str]):
        """Updates rate limit status from GitHub API response headers."""
        async with self._lock:
            try:
                new_remaining = headers.get('X-RateLimit-Remaining')
                new_limit = headers.get('X-RateLimit-Limit')
                new_reset_timestamp_str = headers.get('X-RateLimit-Reset')

                if new_remaining is not None:
                    self.remaining = int(new_remaining)
                if new_limit is not None:
                    self.limit = int(new_limit)
                if new_reset_timestamp_str is not None:
                    self.reset_time = float(new_reset_timestamp_str)
                
                if self.remaining is not None and self.reset_time is not None:
                    reset_dt_utc = datetime.fromtimestamp(self.reset_time, tz=timezone.utc)
                    logger.debug(
                        f"GitHub Rate Limit: Remaining={self.remaining}, Limit={self.limit}, "
                        f"ResetAt={reset_dt_utc.isoformat()}"
                    )
                else:
                    logger.debug(f"GitHub Rate Limit: Headers not fully populated. Current state: Remaining={self.remaining}, Limit={self.limit}, ResetTime={self.reset_time}")

            except (ValueError, TypeError) as e:
                logger.warning(f"Could not parse GitHub rate limit headers: {e}. Headers: {headers}")

    async def wait_if_critically_low(self):
        """
        Checks if the API call quota is critically low and waits until the reset time if needed.
        This should be called *before* making an API call.
        """
        async with self._lock:
            # Check if we are critically near the rate limit and need to wait.
            # The proactive fixed delay is now handled by a separate post-call delay mechanism.
            if self.remaining is not None and self.reset_time is not None:
                if self.remaining < self.safety_buffer_remaining:
                    current_time_utc = time.time() # time.time() is generally UTC-based epoch
                    sleep_duration = self.reset_time - current_time_utc
                    
                    if sleep_duration <= 0:
                        # Reset time is in the past or now.
                        # This might happen if we hit the limit exactly, or headers are slightly stale.
                        # A short sleep is advisable to allow the reset to propagate.
                        effective_sleep = self.min_sleep_if_limited
                        logger.warning(
                            f"{ANSI_YELLOW}GitHub rate limit critically low (Remaining: {self.remaining}). "
                            f"Reset time was {datetime.fromtimestamp(self.reset_time, tz=timezone.utc).isoformat()} (in the past/now). "
                            f"Sleeping for {effective_sleep:.2f}s as a precaution.{ANSI_RESET}"
                        )
                    else:
                        # Add a small buffer (e.g., 1 second) to sleep duration to ensure reset has occurred.
                        effective_sleep = sleep_duration + 1.0 
                        logger.warning(
                            f"{ANSI_YELLOW}GitHub rate limit critically low (Remaining: {self.remaining}). "
                            f"Reset time is {datetime.fromtimestamp(self.reset_time, tz=timezone.utc).isoformat()}. "
                            f"Sleeping for {effective_sleep:.2f}s until reset.{ANSI_RESET}"
                        )
                    
                    # Cap the sleep duration
                    actual_sleep = min(effective_sleep, self.max_sleep_duration)
                    if actual_sleep < effective_sleep:
                         logger.warning(f"Sleep duration capped at {self.max_sleep_duration:.2f}s (was {effective_sleep:.2f}s).")
                    
                    await asyncio.sleep(max(actual_sleep, 0)) # Ensure sleep is not negative

                    # After sleeping, optimistically assume the rate limit has reset.
                    # A more complex system might make a cheap API call (e.g., to /rate_limit)
                    # to get fresh headers, but this adds an API call.
                    if self.limit is not None:
                        self.remaining = self.limit 
                    else: # If limit was never known, set remaining to a safe high number or None
                        self.remaining = None # Forces re-check on next header update
                    logger.info(
                        f"Finished sleep for GitHub rate limit. Assuming limits are refreshed (New est. remaining: {self.remaining})."
                    )
 