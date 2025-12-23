import pandas as pd

print("=" * 60)
print("DAY 3 VERIFICATION")
print("=" * 60)

# Check analytics table
analytics = pd.read_parquet("data/processed/analytics_table.parquet")
print("\n✓ Analytics table loaded")
print(f"  Shape: {analytics.shape}")
print(f"  Columns ({len(analytics.columns)}):")
for col in analytics.columns:
    print(f"    - {col}")

# Show data types
print("\n✓ Data types:")
print(f"  created_at: {analytics['created_at'].dtype}")
print(f"  signup_date: {analytics['signup_date'].dtype}")
print(f"  amount__is_outlier: {analytics['amount__is_outlier'].dtype}")

# Sample data
print("\n✓ Sample data (first 5 rows):")
print(analytics[['order_id', 'user_id', 'amount', 'date', 'dow', 'amount__is_outlier', 'country']].head().to_string())

# Outlier analysis
print(f"\n✓ Outlier Analysis:")
print(f"  Total outliers: {analytics['amount__is_outlier'].sum()}")
print(f"  Outlier percentage: {(analytics['amount__is_outlier'].sum() / len(analytics)) * 100:.2f}%")

print("\n" + "=" * 60)
