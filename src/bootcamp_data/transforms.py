"""
Data transformation and cleaning functions
"""

import pandas as pd
from datetime import datetime


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