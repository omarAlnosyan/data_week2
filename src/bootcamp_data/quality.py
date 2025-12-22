"""
Data quality checks for bootcamp data
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple


def check_missing_values(df: pd.DataFrame) -> Dict[str, int]:
    """
    Check for missing values in dataframe
    
    Args:
        df: DataFrame to check
        
    Returns:
        Dictionary with column names and missing value counts
    """
    return df.isnull().sum().to_dict()


def check_duplicates(df: pd.DataFrame, subset: List[str] = None) -> int:
    """
    Check for duplicate rows
    
    Args:
        df: DataFrame to check
        subset: Columns to check for duplicates
        
    Returns:
        Number of duplicate rows
    """
    return df.duplicated(subset=subset).sum()


def check_data_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Get data types of dataframe columns
    
    Args:
        df: DataFrame to check
        
    Returns:
        Dictionary with column names and data types
    """
    return df.dtypes.astype(str).to_dict()


def check_value_ranges(df: pd.DataFrame, column: str, min_val=None, max_val=None) -> Tuple[int, int]:
    """
    Check values outside expected range
    
    Args:
        df: DataFrame to check
        column: Column to check
        min_val: Minimum expected value
        max_val: Maximum expected value
        
    Returns:
        Tuple of (below_min_count, above_max_count)
    """
    below_min = 0
    above_max = 0
    
    if min_val is not None:
        below_min = (df[column] < min_val).sum()
    if max_val is not None:
        above_max = (df[column] > max_val).sum()
    
    return below_min, above_max


def quality_report(df: pd.DataFrame) -> Dict:
    """
    Generate comprehensive quality report
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with quality metrics
    """
    return {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': check_missing_values(df),
        'duplicate_rows': df.duplicated().sum(),
        'data_types': check_data_types(df)
    }
