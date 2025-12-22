"""
Data transformation and cleaning functions
"""

import pandas as pd
import re
from datetime import datetime

# Regex pattern for multiple whitespace
_ws = re.compile(r"\s+")


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce correct data types for orders data
    
    Args:
        df: DataFrame to transform
        
    Returns:
        DataFrame with enforced schema
    """
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )


def clean_amount(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean amount column: remove negative values, handle NaN
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame with cleaned amount
    """
    df = df.copy()
    df.loc[df['amount'] < 0, 'amount'] = None
    return df


def standardize_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize status values to lowercase
    
    Args:
        df: DataFrame to standardize
        
    Returns:
        DataFrame with standardized status
    """
    df = df.copy()
    if 'status' in df.columns:
        df['status'] = df['status'].str.lower().str.strip()
    return df


def remove_duplicates(df: pd.DataFrame, subset=None) -> pd.DataFrame:
    """
    Remove duplicate rows
    
    Args:
        df: DataFrame to clean
        subset: Columns to consider for duplicates
        
    Returns:
        DataFrame without duplicates
    """
    return df.drop_duplicates(subset=subset)


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Complete cleaning pipeline for orders data
    
    Args:
        df: Raw orders DataFrame
        
    Returns:
        Cleaned DataFrame
    """
    df = enforce_schema(df)
    df = clean_amount(df)
    df = standardize_status(df)
    df = remove_duplicates(df)
    return df


def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate missingness report for all columns
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        DataFrame with n_missing and p_missing columns, sorted by p_missing
    """
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )


def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Add boolean flag columns for missing values
    
    Args:
        df: DataFrame to transform
        cols: List of column names to flag
        
    Returns:
        DataFrame with additional __isna columns
    """
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out


def normalize_text(s: pd.Series) -> pd.Series:
    """
    Normalize text: strip, lowercase, collapse whitespace
    
    Args:
        s: Series to normalize
        
    Returns:
        Normalized series (Paid/PAID/paid → paid)
    """
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )


def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    """
    Apply dictionary mapping to series values
    
    Args:
        s: Series to map
        mapping: Dictionary of value replacements
        
    Returns:
        Series with mapped values (unmapped values stay unchanged)
    """
    return s.map(lambda x: mapping.get(x, x))


def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    """
    Deduplicate by keeping latest record based on timestamp
    
    Args:
        df: DataFrame to deduplicate
        key_cols: List of columns that define a unique record
        ts_col: Timestamp column to sort by
        
    Returns:
        Deduplicated DataFrame with latest records kept
    """
    return (
        df.sort_values(ts_col)
          .drop_duplicates(subset=key_cols, keep="last")
          .reset_index(drop=True)
    )