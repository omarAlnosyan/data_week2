"""
Lightweight data quality checks using assertions
"""

import pandas as pd


def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    """
    Assert that all required columns exist in dataframe
    
    Args:
        df: DataFrame to check
        cols: List of required column names
        
    Raises:
        AssertionError: If any columns are missing
    """
    missing = [c for c in cols if c not in df.columns]
    assert not missing, f"Missing columns: {missing}"


def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    """
    Assert that dataframe is not empty
    
    Args:
        df: DataFrame to check
        name: Name for error message
        
    Raises:
        AssertionError: If dataframe has 0 rows
    """
    assert len(df) > 0, f"{name} has 0 rows"


def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    """
    Assert that key column has unique values
    
    Args:
        df: DataFrame to check
        key: Column name to check for uniqueness
        allow_na: Whether to allow NA values
        
    Raises:
        AssertionError: If key has duplicates or unexpected NAs
    """
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"
    dup = df[key].duplicated(keep=False) & df[key].notna()
    assert not dup.any(), f"{key} not unique; {dup.sum()} duplicate rows"


def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None:
    """
    Assert that series values are within specified range
    
    Args:
        s: Series to check
        lo: Minimum value (inclusive)
        hi: Maximum value (inclusive)
        name: Name for error message
        
    Raises:
        AssertionError: If values are outside range
    """
    x = s.dropna()
    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"
    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"
