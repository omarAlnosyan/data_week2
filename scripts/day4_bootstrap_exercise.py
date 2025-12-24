"""
Day 4 Exercise: Bootstrap confidence intervals for refund rates
Compare refund rates between Saudi Arabia (SA) and UAE (AE)
"""
import pandas as pd
from pathlib import Path

from bootcamp_data.bootstrap import bootstrap_diff_means

ROOT = Path(__file__).parent.parent

def main():
    # Load analytics table
    df = pd.read_parquet(ROOT / "data/processed/analytics_table.parquet")
    
    print("=" * 80)
    print("DAY 4 EXERCISE: BOOTSTRAP REFUND RATE COMPARISON")
    print("=" * 80)
    
    # Step 1: Create is_refund flag
    print("\n1. Creating is_refund flag...")
    d = df.assign(is_refund=df["status_clean"].eq("refund"))
    print(f"   ✓ is_refund created")
    print(f"   Total refunds: {d['is_refund'].sum()} / {len(d)} ({100*d['is_refund'].mean():.2f}%)")
    
    # Step 2: Extract groups for SA and AE
    print("\n2. Extracting country groups...")
    a = d.loc[d["country"].eq("SA"), "is_refund"].astype(int)
    b = d.loc[d["country"].eq("AE"), "is_refund"].astype(int)
    
    print(f"   ✓ SA: n={len(a)}, refund_rate={100*a.mean():.2f}%")
    print(f"   ✓ AE: n={len(b)}, refund_rate={100*b.mean():.2f}%")
    
    # Step 3: Run bootstrap
    print("\n3. Running bootstrap (n_boot=2000)...")
    res = bootstrap_diff_means(a, b, n_boot=2000, seed=0)
    
    print(f"   ✓ Bootstrap complete")
    print(f"\n   RESULTS (SA - AE):")
    print(f"   Observed difference: {100*res['diff_mean']:+.2f} percentage points")
    print(f"   95% CI: [{100*res['ci_low']:+.2f}%, {100*res['ci_high']:+.2f}%]")
    
    # Step 4: Interpretation
    print("\n4. Interpretation:")
    if res['ci_low'] > 0:
        print(f"   ✓ CI entirely ABOVE 0 → SA likely has HIGHER refund rate than AE")
    elif res['ci_high'] < 0:
        print(f"   ✓ CI entirely BELOW 0 → SA likely has LOWER refund rate than AE")
    else:
        print(f"   ⚠ CI overlaps 0 → difference could be noise (inconclusive)")
    
    print("\n" + "=" * 80)
    print("OPERATIONAL SUMMARY")
    print("=" * 80)
    print(f":")
    print(f"  'In our sample, SA has a refund rate {100*a.mean():.1f}% vs AE at {100*b.mean():.1f}%'")
    print(f"  'The difference is {100*res['diff_mean']:+.2f} pp with 95% CI [{100*res['ci_low']:+.2f}%, {100*res['ci_high']:+.2f}%]'")
    if -0.05 < res['diff_mean'] < 0.05:
        print(f"  'This small difference likely reflects data noise, not a real business difference.'")
    print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
