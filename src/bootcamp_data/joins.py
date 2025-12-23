"""
Join/merge helper functions for safe and consistent data operations
"""

import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str | list[str],
    how: str = "left",
    validate: str | None = None,
) -> pd.DataFrame:
    """
    Perform a safe left join with validation
    
    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Column name or list of column names to join on
        how: Type of join ("left", "inner", "outer", "right")
        validate: Validation mode ("1:1", "1:m", "m:1", "m:m")
        
    Returns:
        Merged DataFrame
        
    Raises:
        ValueError: If validation fails
    """
    result = left.merge(right, on=on, how=how, validate=validate)
    return result
