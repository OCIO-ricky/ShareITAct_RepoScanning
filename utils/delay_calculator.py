# utils/delay_calculator.py
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

def calculate_dynamic_delay(
    base_delay_seconds: float,
    num_items: Optional[int],
    threshold_items: int,
    scale_factor: float,
    max_delay_seconds: float
) -> float:
    """
    Calculates a dynamic delay.

    If num_items is None or below threshold_items, base_delay_seconds is returned.
    Otherwise, the delay is scaled up based on how many items exceed the threshold,
    capped by max_delay_seconds.

    Args:
        base_delay_seconds: The fundamental delay to apply.
        num_items: The number of items (e.g., repositories in a target).
        threshold_items: The number of items after which scaling begins.
        scale_factor: Multiplier for scaling. E.g., 1.5 means delay increases by 50%
                      of base for each 'threshold_items' block over the initial threshold.
                      A simpler interpretation: how much to multiply the base delay by
                      if the number of items is (e.g.) double the threshold.
                      Let's use a simpler linear scaling: delay = base + (excess_blocks * base * (scale_factor-1))
        max_delay_seconds: The absolute maximum delay.

    Returns:
        The calculated delay in seconds.
    """
    if num_items is None or num_items <= threshold_items or threshold_items <= 0:
        return base_delay_seconds

    excess_items = num_items - threshold_items
    # Linear scaling: for every 'threshold_items' items over the threshold, add 'scale_factor' * 'base_delay_seconds'
    # A simpler scaling: increase = base_delay * ( (num_items / threshold_items) -1 ) * (scale_factor -1), capped.
    # Let's try: calculated_delay = base_delay_seconds * (1 + (excess_items / threshold_items) * (scale_factor -1 if scale_factor > 1 else 0.5) )
    calculated_delay = base_delay_seconds * (1 + (excess_items / threshold_items) * (scale_factor - 1.0 if scale_factor >= 1.0 else 0.0))
    
    final_delay = min(calculated_delay, max_delay_seconds)
    return max(final_delay, base_delay_seconds) # Ensure it's at least the base delay