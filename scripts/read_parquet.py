"""
Read and display parquet file
"""

from pathlib import Path
import pandas as pd
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bootcamp_data.io import read_parquet
from bootcamp_data.config import make_paths

# Get paths
root = Path(__file__).parent.parent
paths = make_paths(root)

# Read parquet
parquet_path = paths.processed / "orders.parquet"
df = read_parquet(parquet_path)

print("=== Data from Parquet ===")
print(df)
print(f"\nShape: {df.shape}")
print(f"\nData types:\n{df.dtypes}")
print(f"\nMissing values:\n{df.isnull().sum()}")
print(f"\nFile location: {parquet_path}")
