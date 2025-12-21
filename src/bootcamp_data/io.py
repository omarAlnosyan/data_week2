"""
Input/Output module for bootcamp data
"""

from pathlib import Path
import pandas as pd

NA = ["", "NA", "N/A", "null", "None", "not_a_number"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    """
    Read orders from CSV file with custom NA values handling
    
    Args:
        path: Path to CSV file
        
    Returns:
        DataFrame with orders data
    """
    return pd.read_csv(
        path,
        dtype={"order_id": "string", "user_id": "string"},
        na_values=NA,
        keep_default_na=True,
    )


def read_orders_json(path: str | Path) -> pd.DataFrame:
    """
    Read orders from JSON file
    
    Args:
        path: Path to JSON file
        
    Returns:
        DataFrame with orders data
    """
    return pd.read_json(path)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Write DataFrame to parquet file
    
    Args:
        df: DataFrame to write
        path: Path where to save parquet file
        
    Returns:
        None
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def read_parquet(path: str | Path) -> pd.DataFrame:
    """
    Read DataFrame from parquet file
    
    Args:
        path: Path to parquet file
        
    Returns:
        DataFrame with data from parquet file
    """
    return pd.read_parquet(path)
