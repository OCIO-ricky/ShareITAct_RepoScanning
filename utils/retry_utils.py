# utils/retry_utils.py
import time
import logging
import random
from typing import Callable, Optional, Any

logger = logging.getLogger(__name__)

def execute_with_retry(
    api_call_func: Callable[[], Any],
    is_rate_limit_error_func: Callable[[Exception], bool],
    get_retry_after_seconds_func: Optional[Callable[[Exception], Optional[float]]] = None,
    max_retries: int = 3,
    initial_delay_seconds: float = 10.0,  # Lower default
    backoff_factor: float = 2.0,
    max_individual_delay_seconds: float = 900.0,
    error_logger: Optional[logging.Logger] = None,
    log_context: str = ""
) -> Any:
    """
    Executes a function with a retry mechanism for rate limit errors.
    """
    current_logger = error_logger if error_logger else logger
    for attempt in range(max_retries + 1):
        try:
            return api_call_func()
        except Exception as e:
            if is_rate_limit_error_func(e):
                current_logger.warning(f"RATE LIMIT detected on attempt {attempt + 1} for {log_context}. Error: {str(e)[:200]}")
                if attempt < max_retries:
                    delay = initial_delay_seconds * (backoff_factor ** attempt)
                    retry_after = get_retry_after_seconds_func(e) if get_retry_after_seconds_func else None
                    if retry_after is not None:
                        delay = retry_after
                    # Add jitter: +/- up to 10% of delay
                    jitter = delay * 0.1
                    delay = min(delay + random.uniform(-jitter, jitter), max_individual_delay_seconds)
                    current_logger.info(f"Rate limit: Will retry {log_context} after {delay:.2f}s (Retry {attempt + 1}/{max_retries}).")
                    time.sleep(delay)
                    continue
                current_logger.error(f"RATE LIMIT: Max retries ({max_retries}) reached for {log_context}. Propagating error.")
            raise
