"""
Process orders data: remove NaN values and save to parquet
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

print("=== Original Data ===")
print(f"Shape: {df.shape}")
print(f"Missing values:\n{df.isnull().sum()}")
print("\nData:")
print(df)

# Remove rows with NaN values
df_clean = df.dropna()

print("\n=== Cleaned Data (NaN removed) ===")
print(f"Shape: {df_clean.shape}")
print(f"Missing values:\n{df_clean.isnull().sum()}")
print("\nData:")
print(df_clean)

# Save cleaned data to parquet
output_path = paths.processed / "order.parquet"
write_parquet(df_clean, output_path)

print(f"\n✓ Cleaned data saved to: {output_path}")
print(f"✓ Rows removed: {len(df) - len(df_clean)}")
print(f"✓ Rows kept: {len(df_clean)}")
