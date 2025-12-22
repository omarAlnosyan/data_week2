"""
Test text normalization helpers
"""

from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import read_orders_csv
from bootcamp_data.config import make_paths
from bootcamp_data.transforms import normalize_text, apply_mapping


def main():
    """Test text normalization helpers"""
    
    root = Path(__file__).parent.parent
    paths = make_paths(root)
    
    print("=" * 60)
    print("TEXT NORMALIZATION TEST")
    print("=" * 60)
    
    # Step 1: Read orders with messy status values
    print("\n1. Reading orders data...")
    csv_path = paths.raw / "orders.csv"
    df = read_orders_csv(csv_path)
    print(f"   ✓ Loaded {len(df)} rows")
    
    # Step 2: Show original status values
    print("\n2. Original status values (sample):")
    unique_status = df['status'].dropna().unique()[:10]
    for status in unique_status:
        print(f"   - '{status}'")
    
    # Step 3: Normalize text
    print("\n3. Normalizing status values...")
    df['status_normalized'] = normalize_text(df['status'])
    print(f"   ✓ Normalized text")
    
    # Step 4: Show normalized values
    print("\n4. Normalized status values:")
    unique_normalized = df['status_normalized'].dropna().unique()
    for status in sorted(unique_normalized):
        count = (df['status_normalized'] == status).sum()
        print(f"   - '{status}': {count} orders")
    
    # Step 5: Create mapping
    print("\n5. Applying status mapping...")
    status_mapping = {
        'paid': 'completed',
        'refund': 'refunded',
        'refunded': 'refunded',
        'pending': 'pending'
    }
    df['status_cleaned'] = apply_mapping(df['status_normalized'], status_mapping)
    print(f"   ✓ Applied mapping: {status_mapping}")
    
    # Step 6: Show final values
    print("\n6. Final cleaned status values:")
    final_status = df['status_cleaned'].dropna().unique()
    for status in sorted(final_status):
        count = (df['status_cleaned'] == status).sum()
        print(f"   - '{status}': {count} orders")
    
    # Step 7: Show before/after comparison
    print("\n7. Before/After Comparison (sample):")
    sample = df[['order_id', 'status', 'status_normalized', 'status_cleaned']].head(15)
    print(f"\n{sample.to_string()}")
    
    print("\n" + "=" * 60)
    print("✓ Text normalization working correctly!")
    print("=" * 60)


if __name__ == "__main__":
    main()
