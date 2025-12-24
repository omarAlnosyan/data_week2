"""
Bootstrap statistical utilities for group comparisons and confidence intervals
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def bootstrap_diff_means(
    a: pd.Series,
    b: pd.Series,
    *,
    n_boot: int = 2000,
    seed: int = 0,
) -> dict[str, float]:
    """
    Bootstrap confidence interval for difference in means (A - B).
    
    For rates, pass 0/1 Series (e.g., is_refund.astype(int)).
    For medians, use numpy.median instead of .mean() in the loop.
    
    Args:
        a: First group (Series or array-like)
        b: Second group (Series or array-like)
        n_boot: Number of bootstrap samples (default 2000)
        seed: Random seed for reproducibility (default 0)
        
    Returns:
        Dictionary with:
        - diff_mean: observed difference (A - B)
        - ci_low: lower 95% CI bound (2.5th percentile)
        - ci_high: upper 95% CI bound (97.5th percentile)
    """
    # Setup random number generator
    rng = np.random.default_rng(seed)
    
    # Convert to numpy, handle NaN
    a = pd.to_numeric(a, errors="coerce").dropna().to_numpy()
    b = pd.to_numeric(b, errors="coerce").dropna().to_numpy()
    
    assert len(a) > 0 and len(b) > 0, "Empty group after cleaning"
    
    # Store bootstrap differences
    diffs = []
    
    # Run bootstrap
    for _ in range(n_boot):
        # Resample with replacement
        sa = rng.choice(a, size=len(a), replace=True)
        sb = rng.choice(b, size=len(b), replace=True)
        
        # Compute difference in means
        diffs.append(sa.mean() - sb.mean())
    
    diffs = np.array(diffs)
    
    # Return observed diff + CI
    return {
        "diff_mean": float(a.mean() - b.mean()),
        "ci_low": float(np.quantile(diffs, 0.025)),
        "ci_high": float(np.quantile(diffs, 0.975)),
    }
