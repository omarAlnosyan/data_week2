"""
Day 3 Simplified Helper Functions - Alternative Implementations
This shows simpler versions of datetime/outlier/join helpers
"""

import pandas as pd
import re

# ============================================================================
# DATETIME - SIMPLIFIED VERSION
# ============================================================================

def parse_dt_simple(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Simpler: Just parse to datetime"""
    return df.assign(**{col: pd.to_datetime(df[col], errors="coerce")})


def add_dt_features_simple(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Simpler: Add just year, month, day_name"""
    ts = df[ts_col]
    return df.assign(
        year=ts.dt.year,
        month=ts.dt.month,
        dow=ts.dt.day_name(),
    )


# ============================================================================
# OUTLIERS - SIMPLIFIED VERSION
# ============================================================================

def get_outlier_bounds_simple(s: pd.Series) -> tuple[float, float]:
    """Simpler: Just use percentiles directly"""
    lo = s.quantile(0.05)  # 5th percentile
    hi = s.quantile(0.95)  # 95th percentile
    return float(lo), float(hi)


def clip_outliers_simple(s: pd.Series) -> pd.Series:
    """Simpler: Clip to 5th-95th percentiles"""
    lo, hi = get_outlier_bounds_simple(s)
    return s.clip(lower=lo, upper=hi)


def mark_outliers_simple(df: pd.DataFrame, col: str, pct_lo=5, pct_hi=95) -> pd.DataFrame:
    """Simpler: Mark outliers using percentiles"""
    lo = df[col].quantile(pct_lo / 100)
    hi = df[col].quantile(pct_hi / 100)
    flag = (df[col] < lo) | (df[col] > hi)
    return df.assign(**{f"{col}__outlier": flag})


# ============================================================================
# JOINS - SIMPLIFIED VERSION
# ============================================================================

def simple_left_join(left: pd.DataFrame, right: pd.DataFrame, on: str) -> pd.DataFrame:
    """Simpler: Just merge left, no validation"""
    return left.merge(right, on=on, how="left")


# ============================================================================
# COMPARISON
# ============================================================================

if __name__ == "__main__":
    # Example usage
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "amount": [100, 2500, 150, 200, 3000],
        "date": ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"],
    })
    
    print("Original:")
    print(df)
    
    print("\n✓ Parse datetime (simple):")
    df2 = parse_dt_simple(df, "date")
    print(df2.dtypes)
    
    print("\n✓ Mark outliers (simple):")
    df3 = mark_outliers_simple(df, "amount", pct_lo=10, pct_hi=90)
    print(df3[["amount", "amount__outlier"]])
    
    print("\n✓ Clip outliers (simple):")
    df4 = df.assign(amount_clipped=clip_outliers_simple(df["amount"]))
    print(df4[["amount", "amount_clipped"]])
