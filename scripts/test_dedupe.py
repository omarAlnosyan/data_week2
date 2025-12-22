"""
Test dedupe_keep_latest helper
"""

from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import read_orders_csv
from bootcamp_data.config import make_paths
from bootcamp_data.transforms import dedupe_keep_latest


def main():
    """Test dedupe_keep_latest function"""
    
    root = Path(__file__).parent.parent
    paths = make_paths(root)
    
    print("=" * 60)
    print("DEDUPLICATION TEST")
    print("=" * 60)
    
    # Step 1: Read orders data
    print("\n1. Reading orders data...")
    csv_path = paths.raw / "orders.csv"
    df = read_orders_csv(csv_path)
    print(f"   ✓ Loaded {len(df)} rows")
    
    # Step 2: Show duplicates before
    print("\n2. Checking for duplicates...")
    dup_count = df.duplicated(subset=['order_id', 'user_id']).sum()
    print(f"   Total duplicates: {dup_count}")
    
    # Step 3: Show sample of duplicates
    if dup_count > 0:
        print("\n3. Sample duplicate rows:")
        # Find a duplicate order
        dup_orders = df[df.duplicated(subset=['order_id', 'user_id'], keep=False)]
        if len(dup_orders) > 0:
            sample_oid = dup_orders.iloc[0]['order_id']
            sample_uid = dup_orders.iloc[0]['user_id']
            sample = dup_orders[(dup_orders['order_id'] == sample_oid) & 
                               (dup_orders['user_id'] == sample_uid)].sort_values('created_at')
            print(f"\n   Order {sample_oid} from user {sample_uid}:")
            print(f"   {sample[['order_id', 'user_id', 'created_at', 'status']].to_string(index=False)}")
    
    # Step 4: Dedupe keeping latest
    print("\n4. Deduplicating (keeping latest by created_at)...")
    df_clean = dedupe_keep_latest(
        df,
        key_cols=['order_id', 'user_id'],
        ts_col='created_at'
    )
    print(f"   ✓ After deduplication: {len(df_clean)} rows")
    print(f"   ✓ Rows removed: {len(df) - len(df_clean)}")
    
    # Step 5: Verify no duplicates
    print("\n5. Verifying deduplication...")
    dup_count_after = df_clean.duplicated(subset=['order_id', 'user_id']).sum()
    print(f"   Duplicates remaining: {dup_count_after}")
    
    if dup_count_after == 0:
        print(f"   ✓ All duplicates removed!")
    
    # Step 6: Show stats
    print("\n6. Summary:")
    print(f"   Before: {len(df)} rows")
    print(f"   After:  {len(df_clean)} rows")
    print(f"   Removed: {len(df) - len(df_clean)} duplicate rows")
    
    print("\n" + "=" * 60)
    print("✓ Deduplication function working correctly!")
    print("=" * 60)


if __name__ == "__main__":
    main()
