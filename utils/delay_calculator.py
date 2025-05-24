from typing import Optional

def calculate_dynamic_delay(
    base_delay_seconds: float,
    num_items: Optional[int],
    threshold_items: int = 100,
    scale_factor: float = 1.5,
    max_delay_seconds: float = 1.0,
    num_workers: int = 1  # Add this parameter
) -> float:
    """
    Calculate a dynamic delay based on the number of items and workers.
    
    Args:
        base_delay_seconds: Base delay in seconds
        num_items: Number of items to process
        threshold_items: Threshold above which to scale the delay
        scale_factor: Factor to scale the delay by
        max_delay_seconds: Maximum delay in seconds
        num_workers: Number of concurrent workers (default: 1)
    
    Returns:
        Calculated delay in seconds
    """
    # Original delay calculation
    if num_items is None or num_items <= 0 or threshold_items <= 0:
        return base_delay_seconds
    
    if num_items <= threshold_items:
        calculated_delay = base_delay_seconds
    else:
        excess_items = num_items - threshold_items
        calculated_delay = base_delay_seconds * (1 + (excess_items / threshold_items) * scale_factor)
    
    # Adjust for number of workers
    worker_factor = 1.0 + (0.2 * (num_workers - 1))  # 20% increase per additional worker
    worker_adjusted_delay = calculated_delay * worker_factor
    
    # Cap at maximum delay
    max_with_workers = max_delay_seconds * min(2.0, worker_factor)  # Allow up to 2x max for many workers
    return min(worker_adjusted_delay, max_with_workers)