"""
Read CSV and convert to parquet using io functions
"""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import read_orders_csv, write_parquet
from bootcamp_data.config import make_paths

# Get paths
root = Path(__file__).parent.parent
paths = make_paths(root)

# Read CSV
csv_path = paths.raw / "orders.csv"
df = read_orders_csv(csv_path)

print("=== Data from CSV ===")
print(df)
print(f"\nShape: {df.shape}")
print(f"\nData types:\n{df.dtypes}")
print(f"\nMissing values:\n{df.isnull().sum()}")

# Write to parquet
parquet_path = paths.processed / "orders.parquet"
write_parquet(df, parquet_path)

print(f"\n✓ Saved to: {parquet_path}")

