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


# ============================================================================
# DATETIME HELPERS (Day 3)
# ============================================================================

def parse_datetime(df: pd.DataFrame, col: str, *, utc: bool = True) -> pd.DataFrame:
    """
    Parse string column to datetime
    
    Args:
        df: DataFrame to transform
        col: Column name to parse
        utc: Whether to convert to UTC timezone
        
    Returns:
        DataFrame with parsed datetime column
    """
    dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df.assign(**{col: dt})


def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """
    Extract time parts from datetime column (date, year, month, day_of_week, hour)
    
    Args:
        df: DataFrame to transform
        ts_col: Datetime column name
        
    Returns:
        DataFrame with additional time columns (date, year, month, dow, hour)
    """
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )


# ============================================================================
# OUTLIER HELPERS (Day 3)
# ============================================================================

def iqr_bounds(s: pd.Series, k: float = 1.5) -> tuple[float, float]:
    """
    Calculate IQR-based outlier bounds
    
    Args:
        s: Series to analyze
        k: IQR multiplier (default 1.5)
        
    Returns:
        Tuple of (lower_bound, upper_bound)
    """
    x = s.dropna()
    q1 = x.quantile(0.25)
    q3 = x.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    """
    Clip values at percentile bounds
    
    Args:
        s: Series to winsorize
        lo: Lower percentile (default 0.01)
        hi: Upper percentile (default 0.99)
        
    Returns:
        Winsorized Series (values clipped to percentile bounds)
    """
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)


def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    """
    Add boolean flag for outlier detection using IQR method
    
    Args:
        df: DataFrame to transform
        col: Column name to check for outliers
        k: IQR multiplier (default 1.5)
        
    Returns:
        DataFrame with new {col}__is_outlier boolean column
    """
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})