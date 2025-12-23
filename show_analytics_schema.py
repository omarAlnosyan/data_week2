import pandas as pd

df = pd.read_parquet('data/processed/analytics_table.parquet')

print("=" * 90)
print("ANALYTICS_TABLE.PARQUET - COMPLETE SCHEMA")
print("=" * 90)

print("\nCOLUMNS:")
print("-" * 90)
for i, (col, dtype) in enumerate(df.dtypes.items(), 1):
    print(f"{i:2d}. {col:25s} | {str(dtype):20s}")

print(f"\nSHAPE: {df.shape[0]:,} rows × {df.shape[1]} columns")

print("\n" + "=" * 90)
print("SAMPLE DATA (first 5 rows)")
print("=" * 90)
print(df.head().to_string())

print("\n" + "=" * 90)
print("DATA TYPES SUMMARY")
print("=" * 90)
print(df.dtypes)

print("\n" + "=" * 90)
print("MISSING VALUES")
print("=" * 90)
for col in df.columns:
    missing = df[col].isna().sum()
    if missing > 0:
        pct = (missing / len(df)) * 100
        print(f"{col:25s} | {missing:5d} missing ({pct:5.2f}%)")

print("\n" + "=" * 90)
