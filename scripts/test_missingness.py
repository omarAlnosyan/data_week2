"""
Test missingness helpers on generated messy data
"""

from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import read_orders_csv, read_parquet
from bootcamp_data.config import make_paths
from bootcamp_data.transforms import missingness_report, add_missing_flags


def main():
    """Test missingness helpers"""
    
    root = Path(__file__).parent.parent
    paths = make_paths(root)
    
    print("=" * 60)
    print("MISSINGNESS HELPERS TEST")
    print("=" * 60)
    
    # Step 1: Read messy orders data
    print("\n1. Reading messy orders data...")
    csv_path = paths.raw / "orders.csv"
    df = read_orders_csv(csv_path)
    print(f"   ✓ Loaded {len(df)} rows, {len(df.columns)} columns")
    
    # Step 2: Generate missingness report
    print("\n2. Generating missingness report...")
    missing_df = missingness_report(df)
    print(f"\n{missing_df}")
    
    # Step 3: Add missing flags
    print("\n3. Adding missing value flags...")
    cols_to_flag = ['amount', 'quantity', 'created_at', 'status']
    df_flagged = add_missing_flags(df, cols_to_flag)
    print(f"   ✓ Added {len(cols_to_flag)} flag columns")
    print(f"   ✓ New columns: {[f'{c}__isna' for c in cols_to_flag]}")
    
    # Step 4: Show sample with flags
    print("\n4. Sample rows with missing flags:")
    sample = df_flagged[['order_id', 'amount', 'amount__isna', 'quantity', 'quantity__isna']].head(10)
    print(f"\n{sample.to_string()}")
    
    # Step 5: Analyze flag usage
    print("\n5. Missing value analysis by column:")
    for col in cols_to_flag:
        flag_col = f"{col}__isna"
        count = df_flagged[flag_col].sum()
        pct = (count / len(df_flagged) * 100)
        print(f"   - {col}: {count} missing ({pct:.1f}%)")
    
    print("\n" + "=" * 60)
    print("✓ Missingness helpers working correctly!")
    print("=" * 60)


if __name__ == "__main__":
    main()
